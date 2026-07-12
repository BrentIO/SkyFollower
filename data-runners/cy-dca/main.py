#!/usr/bin/env python3
"""
SkyFollower Cyprus DCA Data Runner

Downloads the Cyprus Department of Civil Aviation aircraft register PDF from
a stable URL, parses all pages using pdfplumber extract_table(), filters to
5B-prefix rows, looks up each registration in the Redis simple search index
to find the ICAO hex (provided by Mictronics), writes enrichment data to
aircraft:registry:{icao_hex} with 14-day TTL, publishes MQTT completion stats,
then exits.

PDF columns (0-based):
  0: REGISTRATION MARK    → lookup key (5B-prefix)
  1: MANUFACTURER         → aircraft.manufacturer
  2: AIRCRAFT TYPE        → aircraft.model
  3: AIRCRAFT SERIAL NO   → aircraft.serial_number
  4: MTOW KGS             (not stored)
  5: AIRCRAFT OWNER/OPERATOR → registrant.names[0]

Data source: https://www.mcw.gov.cy/mcw/dca/dca.nsf/All/46E79D3B8B2B752AC2258D8D00384715/$file/Aircraft_Register_Extract.pdf
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

import paho.mqtt.client as mqtt
import pdfplumber
import redis as redis_lib
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.query import Query

from shared.redis_keys import (
    AIRCRAFT_MICTRONICS_SEARCH_INDEX,
    aircraft_registry_key,
)
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client
from shared.logging_setup import configure_logging

logger = logging.getLogger("cy-dca")

_PDF_URL = "https://www.mcw.gov.cy/mcw/dca/dca.nsf/All/46E79D3B8B2B752AC2258D8D00384715/$file/Aircraft_Register_Extract.pdf"

REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/cy-dca"
BATCH_SIZE = 100

_WHITESPACE_RE = re.compile(r"\s+")

_OWNER_HEADER_LABELS = {"OWNER", "TRUSTEE"}
_OWNER_REMOVE_RE = re.compile(r"LESS(?:OR|EE)", re.IGNORECASE)

_COL_REGISTRATION = 0
_COL_MANUFACTURER = 1
_COL_MODEL = 2
_COL_SERIAL = 3
_COL_OWNER = 5


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def download_and_parse(session: requests.Session) -> list[dict]:
    """Download the Cyprus DCA PDF and return parsed records."""
    logger.info("Downloading Cyprus DCA aircraft register from %s", _PDF_URL)
    resp = session.get(_PDF_URL, timeout=60, verify=False)
    if not resp.ok:
        raise RuntimeError(f"PDF request failed with HTTP {resp.status_code}")

    records = []
    with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue
            for row in table:
                if not row or len(row) <= _COL_OWNER:
                    continue
                registration = _WHITESPACE_RE.sub("", row[_COL_REGISTRATION] or "")
                if not registration.startswith("5B-"):
                    continue
                records.append({
                    "registration": registration,
                    "manufacturer": _clean(row[_COL_MANUFACTURER]),
                    "model": _clean(row[_COL_MODEL]),
                    "serial": _clean(row[_COL_SERIAL]),
                    "owner": _clean(row[_COL_OWNER]),
                })

    logger.info("Parsed %d 5B- records.", len(records))
    return records


def _clean(value) -> str:
    """Normalize whitespace in a cell value."""
    return _WHITESPACE_RE.sub(" ", (value or "").strip())


def _clean_owner_part(value: str) -> str:
    """Remove colons and LESSOR/LESSEE substrings, then normalize whitespace."""
    s = (value or "").replace(":", "")
    s = _OWNER_REMOVE_RE.sub("", s)
    return _WHITESPACE_RE.sub(" ", s).strip()


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict, icao_hex: str, registration: str) -> dict:
    """Build a Redis detail record from a parsed row."""
    aircraft_fields: dict = {}
    registrant_fields: dict = {}

    manufacturer = _clean(row.get("manufacturer", ""))
    if manufacturer:
        aircraft_fields["manufacturer"] = manufacturer

    model = _clean(row.get("model", ""))
    if model:
        aircraft_fields["model"] = model

    serial = _clean(row.get("serial", ""))
    if serial:
        aircraft_fields["serial_number"] = serial

    owner_raw = row.get("owner") or ""
    names = [p for part in owner_raw.split("/") if (p := _clean_owner_part(part)) and p.upper() not in _OWNER_HEADER_LABELS]
    if names:
        registrant_fields["names"] = names

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "cy-dca",
        "military": False,
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
    """Write Cyprus DCA data to aircraft:detail keys in Redis. Returns count written."""
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
        "ids": "SkyFollower_runner_cy_dca",
        "name": "SkyFollower Cyprus DCA Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Cyprus DCA Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Cyprus DCA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Cyprus DCA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_cy_dca_{name}",
            "object_id": f"SkyFollower_runner_cy_dca_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_cy_dca_{name}/config",
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
    try:
        cfg = _load_config()
    except FileNotFoundError as exc:
        configure_logging()
        logger.critical("Settings file not found: %s", exc)
        sys.exit(1)

    configure_logging(cfg.get("log_level"))

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
            "Cyprus DCA runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Cyprus DCA runner failed: %s", exc, exc_info=True)

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
