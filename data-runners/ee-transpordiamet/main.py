#!/usr/bin/env python3
"""
SkyFollower Estonia Transpordiamet Data Runner

Fetches the Estonian Transport Administration civil aircraft register page,
parses the single HTML table, normalizes ES- registration marks (removing
internal whitespace), looks up each registration in the Redis simple search
index to find the ICAO hex (provided by Mictronics), writes enrichment data
to aircraft:registry:{icao_hex}, publishes MQTT completion stats, then exits.

Table columns (0-based, 9 columns total):
  0: blank
  1: Registration mark (ES-prefix) — normalize: re.sub(r'\\s+', '', mark)
  2: blank
  3: blank
  4: Type of Aircraft  → aircraft.model
  5: Serial number     → aircraft.serial_number
  6: Owner             → registrant.names[0]
  7: Operator          (not imported)
  8: blank

Data source: https://transpordiamet.ee/ohusoidukite-register
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

from redis.commands.search.query import Query

from shared.redis_keys import (
    AIRCRAFT_MICTRONICS_SEARCH_INDEX,
    aircraft_registry_key,
)
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client

logger = logging.getLogger("ee-transpordiamet")

_PAGE_URL = "https://transpordiamet.ee/ohusoidukite-register"

REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/ee-transpordiamet"
BATCH_SIZE = 100
HEADER_ROWS = 2

_WHITESPACE_RE = re.compile(r"\s+")

_COL_REGISTRATION = 1
_COL_MODEL = 4
_COL_SERIAL = 5
_COL_OWNER = 6
_MIN_COLS = 8


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def download_and_parse(session: requests.Session) -> list[dict]:
    """Fetch the Transpordiamet register page and return normalized records."""
    logger.info("Downloading Estonia Transpordiamet aircraft register from %s", _PAGE_URL)
    resp = session.get(_PAGE_URL, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Page request failed with HTTP {resp.status_code}")

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table")
    if not table:
        raise RuntimeError("No table found on Estonia Transpordiamet register page.")

    rows = table.find_all("tr")
    records = []

    for tr in rows[HEADER_ROWS:]:
        cells = [_WHITESPACE_RE.sub(" ", td.get_text(strip=True)) for td in tr.find_all("td")]
        if len(cells) < _MIN_COLS:
            continue

        raw_reg = cells[_COL_REGISTRATION]
        registration = re.sub(r"\s+", "", raw_reg)
        if not registration.startswith("ES-"):
            continue

        records.append({
            "registration": registration,
            "model": cells[_COL_MODEL].strip(),
            "serial": cells[_COL_SERIAL].strip(),
            "owner": cells[_COL_OWNER].strip(),
        })

    logger.info("Parsed %d ES- records.", len(records))
    return records


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict, icao_hex: str, registration: str) -> dict:
    """Build detail enrichment record from a parsed row."""
    aircraft_fields: dict = {}
    registrant_fields: dict = {}

    model = _WHITESPACE_RE.sub(" ", row.get("model", "").strip())
    if model:
        aircraft_fields["model"] = model

    serial = row.get("serial", "").strip()
    if serial:
        aircraft_fields["serial_number"] = serial

    owner = _WHITESPACE_RE.sub(" ", row.get("owner", "").strip())
    if owner:
        registrant_fields["names"] = [owner]

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "ee-transpordiamet",
    }
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
    if registrant_fields:
        record["registrant"] = registrant_fields

    return record


# ---------------------------------------------------------------------------
# RediSearch tag escaping
# ---------------------------------------------------------------------------

def _escape_tag(value: str) -> str:
    """Escape special characters for use in a RediSearch TagField query."""
    special = ',.<>{}[]"\':;!@#$%^&*()-+=~'
    result = []
    for char in value:
        if char in special:
            result.append("\\")
        result.append(char)
    return "".join(result)


# ---------------------------------------------------------------------------
# Registration → icao_hex lookup
# ---------------------------------------------------------------------------

def _build_registration_map(registrations: list[str], r: redis_lib.Redis) -> dict[str, str]:
    """Batch-query Redis simple search index for icao_hex by registration mark."""
    reg_map: dict[str, str] = {}
    total_batches = (len(registrations) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num, i in enumerate(range(0, len(registrations), BATCH_SIZE)):
        batch = registrations[i : i + BATCH_SIZE]
        escaped = [_escape_tag(reg) for reg in batch]
        query_str = f"@registration:{{{'|'.join(escaped)}}}"

        try:
            results = r.ft(AIRCRAFT_MICTRONICS_SEARCH_INDEX).search(
                Query(query_str).return_fields("registration").paging(0, BATCH_SIZE)
            )
            for doc in results.docs:
                icao_hex = doc.id.replace("aircraft:mictronics:", "")
                registration = getattr(doc, "registration", None)
                if registration:
                    reg_map[registration.strip()] = icao_hex
        except Exception as exc:
            logger.warning("RediSearch batch %d/%d failed: %s", batch_num + 1, total_batches, exc)

    return reg_map


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Write Transpordiamet data to aircraft:detail keys in Redis. Returns count written."""
    reg_row_map: dict[str, dict] = {}
    for row in rows:
        reg = row.get("registration", "").strip()
        if not reg:
            continue
        reg_row_map[reg] = row

    registrations = list(reg_row_map.keys())
    logger.info("Looking up %d registrations in Redis search index.", len(registrations))

    reg_icao_map = _build_registration_map(registrations, r)
    logger.info(
        "Found %d / %d registrations in Redis (remainder not yet in Mictronics).",
        len(reg_icao_map),
        len(registrations),
    )

    count = 0
    errors = 0
    pipe = r.pipeline()
    pipe_count = 0

    for registration, icao_hex in reg_icao_map.items():
        row = reg_row_map.get(registration)
        if row is None:
            continue
        record = _build_record(row, icao_hex, registration)
        key = aircraft_registry_key(icao_hex)
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
        "ids": "SkyFollower_runner_ee_transpordiamet",
        "name": "SkyFollower Estonia Transpordiamet Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Estonia Transpordiamet Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Estonia Transpordiamet Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Estonia Transpordiamet Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_ee_transpordiamet_{name}",
            "object_id": f"SkyFollower_runner_ee_transpordiamet_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_ee_transpordiamet_{name}/config",
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
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; P5Software SkyFollower)"})

    status = "failure"
    records_imported = 0

    try:
        rows = download_and_parse(session)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info(
            "Estonia Transpordiamet runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Estonia Transpordiamet runner failed: %s", exc, exc_info=True)

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
