#!/usr/bin/env python3
"""
SkyFollower Isle of Man ARDIS Data Runner

Fetches the Isle of Man Aircraft Registry (ARDIS) from the search form at
ardis.iomaircraftregistry.com, parses the HTML results table, filters out
deregistered aircraft, and writes enrichment data to
aircraft:registry:{icao_hex} using the Mode S Number column directly.
No RediSearch reverse-lookup is needed — ICAO hex is supplied in the register.
Publishes MQTT completion stats, then exits.

Data source: https://ardis.iomaircraftregistry.com/register/search
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.redis_keys import aircraft_registry_key
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client

logger = logging.getLogger("im-ardis")

SEARCH_URL = "https://ardis.iomaircraftregistry.com/register/search"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/im-ardis"

_STATUS_DEREGISTERED = "Deregistered"

# ARDIS header cells contain a sort link whose text is "Sort column by X"
# concatenated with the actual column name X, yielding "Sort column by XX".
# The backreference strips that prefix leaving just X.
_SORT_HEADER_RE = re.compile(r"^Sort column by (.+)\1$")

_POST_DATA = {
    "prs_rm__ptt": "8",
    "prs_rm__tt": "8",
    "prs_rm__v1": "",
    "prs_rm__pv1": "",
    "prs_rm__v2": "",
    "prs_rm__pv2": "",
    "prs_sn__ptt": "8",
    "prs_sn__tt": "8",
    "prs_sn__v1": "",
    "prs_sn__pv1": "",
    "prs_sn__v2": "",
    "prs_sn__pv2": "",
    "prs_on__ptt": "8",
    "prs_on__tt": "8",
    "prs_on__v1": "",
    "prs_on__pv1": "",
    "prs_on__v2": "",
    "prs_on__pv2": "",
    "prs_ma__ptt": "8",
    "prs_ma__tt": "8",
    "prs_ma__v1": "",
    "prs_ma__pv1": "",
    "prs_ma__v2": "",
    "prs_ma__pv2": "",
    "F__ptt": "8",
    "F__tt": "8",
    "F__v1": "",
    "F__pv1": "",
    "F__v2": "",
    "F__pv2": "",
    "prs_as__pv": "__any__",
    "prs_as__v": "__any__",
    "prs__adidx": "",
    "prs__reidx": "",
    "prp__ttb__0__np": "",
    "prp__ttb__1__cs": "00101111010000100",
    "prp__ttb__1__cst": "00101111010000100",
    "prp__btb__0__np": "",
    "prp__btb__1__nps": "",
    "prp_csv__sub": "",
    "prp__ids": "593,68,137,466,115,635,912,258,139,217,345,509,239,603,534,785,511,679,290,293,874,525,661,130,841",
    "prp__fc": "False",
    "prp__ps": "50000",
    "prp__fl": "0",
    "prp__sc": "",
    "prp__sd": "",
}


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def _header_text(cell) -> str:
    """Extract column header text, stripping ARDIS sort-link prefix if present.

    ARDIS header cells contain a sort link whose get_text() concatenates as
    'Sort column by Registration MarkRegistration Mark'. The regex uses a
    backreference to detect this pattern and extract just the column name.
    """
    text = cell.get_text(strip=True)
    m = _SORT_HEADER_RE.match(text)
    return m.group(1) if m else text


def _fetch_token(session: requests.Session) -> str:
    """GET the search page and extract the CSRF verification token."""
    logger.info("Fetching CSRF token from %s", SEARCH_URL)
    resp = session.get(SEARCH_URL, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"CSRF token fetch failed with HTTP {resp.status_code}")
    soup = BeautifulSoup(resp.text, "lxml")
    token_input = soup.find("input", {"name": "__RequestVerificationToken"})
    if not token_input:
        raise RuntimeError("Could not find __RequestVerificationToken in ARDIS page.")
    return token_input["value"]


def download_and_parse(session: requests.Session) -> list[dict]:
    """POST the ARDIS full-list search and parse the HTML results table."""
    token = _fetch_token(session)

    post_data = dict(_POST_DATA)
    post_data["__RequestVerificationToken"] = token

    logger.info("Downloading Isle of Man ARDIS aircraft register from %s", SEARCH_URL)
    resp = session.post(SEARCH_URL, data=post_data, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {resp.status_code}")

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("Could not find results table on ARDIS search results page.")

    rows = table.find_all("tr")
    if not rows:
        raise RuntimeError("Results table on ARDIS page is empty.")

    header_cells = rows[0].find_all(["th", "td"])
    headers = [_header_text(cell) for cell in header_cells]
    logger.info("Table headers: %s", headers)

    records: list[dict] = []
    for row in rows[1:]:
        cells = row.find_all(["th", "td"])
        if not cells:
            continue
        values = [cell.get_text(strip=True) for cell in cells]
        records.append(dict(zip(headers, values)))

    logger.info("Parsed %d total rows from register.", len(records))
    return records


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict) -> dict | None:
    """Build detail enrichment record from a parsed row. Returns None to skip."""
    if row.get("Aircraft Status", "").strip() == _STATUS_DEREGISTERED:
        return None

    icao_hex = row.get("Mode S Number", "").strip().upper()
    if not icao_hex:
        return None

    registration = row.get("Registration Mark", "").strip()
    if not registration:
        return None

    aircraft_fields: dict = {}
    registrant_fields: dict = {}

    manufacturer = row.get("Aircraft Manufacturer", "").strip()
    if manufacturer:
        aircraft_fields["manufacturer"] = manufacturer

    model = row.get("Aircraft Type", "").strip()
    if model:
        aircraft_fields["model"] = model

    serial = row.get("Serial Number", "").strip()
    if serial:
        aircraft_fields["serial_number"] = serial

    owners_raw = row.get("Registered Owners", "").strip()
    if owners_raw:
        if "," in owners_raw:
            name_part, street_part = owners_raw.split(",", 1)
            name_part = name_part.strip()
            street_part = street_part.strip()
            if name_part:
                registrant_fields["names"] = [name_part]
            if street_part:
                registrant_fields["street"] = [street_part]
        else:
            registrant_fields["names"] = [owners_raw]

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "im-ardis",
    }
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
    if registrant_fields:
        record["registrant"] = registrant_fields

    return record


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Write ARDIS data to aircraft:detail keys in Redis. Returns count written."""
    count = 0
    errors = 0
    pipe = r.pipeline()
    pipe_count = 0

    for row in rows:
        record = _build_record(row)
        if record is None:
            continue

        key = aircraft_registry_key(record["icao_hex"])
        set_json(pipe, key, record)
        pipe.expire(key, ttl)
        count += 1
        pipe_count += 1

        if pipe_count >= 1000:
            try:
                pipe.execute()
            except Exception as exc:
                logger.warning("Redis pipeline failed: %s", exc)
                errors += pipe_count
            pipe = r.pipeline()
            pipe_count = 0

    if pipe_count:
        try:
            pipe.execute()
        except Exception as exc:
            logger.warning("Redis pipeline failed: %s", exc)
            errors += pipe_count

    logger.info("Finished: %d written, %d errors.", count, errors)
    return count


# ---------------------------------------------------------------------------
# MQTT
# ---------------------------------------------------------------------------

def publish_completion_stats(cfg: dict, records_imported: int, status: str) -> None:
    """Publish completion statistics to MQTT."""
    mc = cfg.get("mqtt")
    if not mc:
        logger.info("No MQTT config; skipping stats publish.")
        return

    run_at = datetime.now(timezone.utc).isoformat()
    client = build_mqtt_client(mc)
    connected = False

    def _on_connect(c, userdata, flags, reason_code, properties):
        nonlocal connected
        connected = True

    client.on_connect = _on_connect

    try:
        client.connect(mc["host"], port=mc.get("port", 1883), keepalive=60)
        client.loop_start()

        deadline = time.monotonic() + 5
        while not connected and time.monotonic() < deadline:
            time.sleep(0.05)

        if not connected:
            logger.warning("MQTT connect timed out; skipping stats publish.")
            client.loop_stop()
            return

        base = MQTT_ROOT + "/statistic"
        client.publish(f"{base}/records_imported", str(records_imported), retain=True)
        client.publish(f"{base}/last_run_at", run_at, retain=True)
        client.publish(f"{base}/last_run_status", status, retain=True)

        _publish_ha_autodiscovery(client)

        time.sleep(0.5)
        client.loop_stop()
        client.disconnect()
        logger.info("MQTT stats published (status=%s, records=%d).", status, records_imported)

    except Exception as exc:
        logger.warning("MQTT publish failed: %s", exc)
        try:
            client.loop_stop()
        except Exception:
            pass


def _publish_ha_autodiscovery(client: mqtt.Client) -> None:
    device = {
        "ids": "SkyFollower_runner_im_ardis",
        "name": "SkyFollower Isle of Man ARDIS Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Isle of Man ARDIS Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Isle of Man ARDIS Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Isle of Man ARDIS Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_im_ardis_{name}",
            "object_id": f"SkyFollower_runner_im_ardis_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_im_ardis_{name}/config",
            json.dumps(payload),
            retain=True,
        )


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    path = os.environ.get("SETTINGS_PATH", "/app/settings.json")
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        stream=sys.stdout,
    )

    try:
        cfg = _load_config()
    except FileNotFoundError as exc:
        logger.critical("Settings file not found: %s", exc)
        sys.exit(1)

    rc = cfg["redis"]
    r = redis_lib.Redis(
        host=rc["host"],
        port=rc.get("port", 6379),
        decode_responses=True,
    )

    ttl_days = cfg.get("redis_ttl_days", 14)
    ttl = ttl_days * 86400

    session = requests.Session()
    session.headers.update({"User-Agent": "P5Software SkyFollower"})

    status = "failure"
    records_imported = 0

    try:
        rows = download_and_parse(session)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info(
            "Isle of Man ARDIS runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Isle of Man ARDIS runner failed: %s", exc, exc_info=True)

    finally:
        session.close()
        try:
            publish_completion_stats(cfg, records_imported, status)
        except Exception as exc:
            logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
