#!/usr/bin/env python3
"""
SkyFollower Singapore CAAS Data Runner

Fetches the Singapore CAAS (Civil Aviation Authority of Singapore) aircraft
register from a monthly-updated xlsx file, looks up each 9V- registration in
the Redis simple search index to find the ICAO hex (provided by Mictronics),
writes enrichment data to aircraft:detail:{icao_hex}, publishes MQTT completion
stats, then exits.

The xlsx URL changes monthly (UUID component) and is discovered fresh each run
by scraping the CAAS certificate-of-registration page.  The CDN requires a
browser User-Agent and a Referer header pointing to the CAAS page.

Data source: https://www.caas.gov.sg/industry/aircraft-operators/certificate-of-registration/
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone

import openpyxl
import paho.mqtt.client as mqtt
import redis as redis_lib
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from shared.redis_keys import (
    AIRCRAFT_DETAIL_SEARCH_INDEX,
    AIRCRAFT_SIMPLE_SEARCH_INDEX,
    aircraft_detail_key,
)

logger = logging.getLogger("sg-caas")

_INDEX_URL = "https://www.caas.gov.sg/industry/aircraft-operators/certificate-of-registration/"
_CDN_HOST = "isomer-user-content.by.gov.sg"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/sg-caas"
BATCH_SIZE = 100

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Column header names in the xlsx (exact match)
_COL_REGISTRATION = "Aircraft Registration"
_COL_SERIAL = "Aircraft S/N"
_COL_MODEL = "Aircraft Model"
_COL_OPERATOR = "Operator"
_COL_MANUFACTURER = "Aircraft Manufacturer"
_COL_ENGINE_MODEL = "Engine Model"
_COL_ENGINE_MFR = "Engine Manufacturer"


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def _discover_xlsx_url(session: requests.Session) -> str:
    """Scrape the CAAS page and return the href of the aircraft register xlsx."""
    logger.info("Fetching CAAS register index from %s", _INDEX_URL)
    resp = session.get(_INDEX_URL, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"CAAS index page returned HTTP {resp.status_code}")

    soup = BeautifulSoup(resp.text, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if _CDN_HOST in href and href.lower().endswith(".xlsx"):
            return href

    raise RuntimeError("No xlsx link found on CAAS certificate-of-registration page.")


def download_and_parse(session: requests.Session) -> list[dict]:
    """Discover, download, and parse the Singapore CAAS aircraft register xlsx."""
    xlsx_url = _discover_xlsx_url(session)
    logger.info("Downloading Singapore CAAS aircraft register from %s", xlsx_url)
    resp = session.get(
        xlsx_url,
        headers={"Referer": _INDEX_URL},
        timeout=120,
    )
    if not resp.ok:
        raise RuntimeError(f"xlsx download failed with HTTP {resp.status_code}")

    wb = openpyxl.load_workbook(io.BytesIO(resp.content), read_only=True, data_only=True)
    ws = wb.active

    rows_iter = ws.iter_rows(values_only=True)
    headers: list[str] | None = None
    records: list[dict] = []

    for row in rows_iter:
        cells = [(str(v).strip() if v is not None else "") for v in row]
        if all(c == "" for c in cells):
            continue

        if headers is None:
            headers = cells
            continue

        if not cells or not cells[0].startswith("9V-"):
            continue

        record = dict(zip(headers, cells))
        records.append(record)

    wb.close()
    logger.info("Parsed %d 9V- rows from xlsx.", len(records))
    return records


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict, icao_hex: str, registration: str) -> dict:
    """Build detail enrichment record from a parsed xlsx row."""
    aircraft_fields: dict = {}
    powerplant_fields: dict = {}
    registrant_fields: dict = {}

    manufacturer = row.get(_COL_MANUFACTURER, "").strip()
    if manufacturer:
        aircraft_fields["manufacturer"] = manufacturer

    model = row.get(_COL_MODEL, "").strip()
    if model:
        aircraft_fields["model"] = model

    serial = row.get(_COL_SERIAL, "").strip()
    if serial:
        aircraft_fields["serial_number"] = serial

    engine_mfr = row.get(_COL_ENGINE_MFR, "").strip()
    if engine_mfr:
        powerplant_fields["manufacturer"] = engine_mfr

    engine_model = row.get(_COL_ENGINE_MODEL, "").strip()
    if engine_model:
        powerplant_fields["model"] = engine_model

    operator = row.get(_COL_OPERATOR, "").strip()
    if operator:
        registrant_fields["names"] = [operator]

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "sg-caas",
    }
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
    if powerplant_fields:
        aircraft_fields["powerplant"] = powerplant_fields
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
            result.append('\\')
        result.append(char)
    return ''.join(result)


# ---------------------------------------------------------------------------
# Search index
# ---------------------------------------------------------------------------

def _ensure_search_index(r: redis_lib.Redis) -> None:
    """Create the aircraft:detail JSON search index if it does not already exist."""
    try:
        r.ft(AIRCRAFT_DETAIL_SEARCH_INDEX).info()
    except Exception:
        r.ft(AIRCRAFT_DETAIL_SEARCH_INDEX).create_index(
            fields=[
                TagField("$.icao_hex", as_name="icao_hex"),
                TagField("$.registration", as_name="registration"),
            ],
            definition=IndexDefinition(prefix=["aircraft:detail:"], index_type=IndexType.JSON),
        )
        logger.info("Created search index %r.", AIRCRAFT_DETAIL_SEARCH_INDEX)


# ---------------------------------------------------------------------------
# Registration → icao_hex lookup
# ---------------------------------------------------------------------------

def _build_registration_map(registrations: list[str], r: redis_lib.Redis) -> dict[str, str]:
    """Batch-query Redis simple search index for icao_hex by registration mark."""
    reg_map: dict[str, str] = {}
    total_batches = (len(registrations) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num, i in enumerate(range(0, len(registrations), BATCH_SIZE)):
        batch = registrations[i:i + BATCH_SIZE]
        escaped = [_escape_tag(reg) for reg in batch]
        query_str = f"@registration:{{{'|'.join(escaped)}}}"

        try:
            results = r.ft(AIRCRAFT_SIMPLE_SEARCH_INDEX).search(
                Query(query_str)
                .return_fields("registration")
                .paging(0, BATCH_SIZE)
            )
            for doc in results.docs:
                icao_hex = doc.id.replace("aircraft:simple:", "")
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
    """Write Singapore CAAS data to aircraft:detail keys in Redis. Returns count written."""
    reg_row_map: dict[str, dict] = {}
    for row in rows:
        reg = row.get(_COL_REGISTRATION, "").strip()
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
        key = aircraft_detail_key(icao_hex)
        pipe.json().set(key, "$", record)
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
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
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
        "ids": "SkyFollower_runner_sg_caas",
        "name": "SkyFollower Singapore CAAS Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Singapore CAAS Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Singapore CAAS Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Singapore CAAS Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_sg_caas_{name}",
            "object_id": f"SkyFollower_runner_sg_caas_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_sg_caas_{name}/config",
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
    session.headers.update({
        "User-Agent": _BROWSER_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })

    status = "failure"
    records_imported = 0

    try:
        rows = download_and_parse(session)
        _ensure_search_index(r)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info(
            "Singapore CAAS runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Singapore CAAS runner failed: %s", exc, exc_info=True)

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
