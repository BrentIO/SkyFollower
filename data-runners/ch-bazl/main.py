#!/usr/bin/env python3
"""
SkyFollower Switzerland BAZL Data Runner

Downloads the Switzerland Federal Office of Civil Aviation (FOCA/BAZL) aircraft
register as a single bulk CSV and writes enrichment data to Redis.

API: POST https://app02.bazl.admin.ch/web/bazl-backend/lfr/csv
Response: UTF-16 BE encoded, semicolon-delimited CSV; ~3,100 records.

Only rows with Status == "Registered" are written. No authentication required.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests
from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType

from shared.redis_keys import aircraft_detail_key, AIRCRAFT_DETAIL_SEARCH_INDEX

logger = logging.getLogger("ch-bazl")

API_URL = "https://app02.bazl.admin.ch/web/bazl-backend/lfr/csv"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/ch-bazl"

_AIRCRAFT_TYPE_MAP: dict[str, str] = {
    "Aeroplane": "Airplane",
    "Homebuilt Airplane": "Airplane",
    "Helicopter": "Helicopter",
    "Homebuilt Helicopter": "Helicopter",
    "Glider": "Glider",
    "Powered Glider": "Powered Glider",
    "Homebuild Glider": "Glider",
    "Balloon (Hot-air)": "Balloon",
    "Balloon (Gas)": "Balloon",
    "Airship (Hot-air)": "Airship",
    "Ultralight Gyrocopter": "Gyroplane",
    "Homebuilt Gyrocopter": "Gyroplane",
    "Ultralight (3-axis control)": "Microlight",
    "Ecolight": "Microlight",
    "Trike": "Weight-Shift-Control",
}

_ENGINE_TYPE_MAP: dict[str, str] = {
    "Piston Engine": "Piston",
    "Turboshaft Engine": "Turbo-shaft",
    "Turboprop Engine": "Turbo-prop",
    "Jet Engine": "Turbo-jet",
    "Electrical Engine": "Electric",
    "Hydrogen-Electric": "Hydrogen-Electric",
}

_CANTON_RE = re.compile(r"^[A-Z]{2}$")
_POSTAL_RE = re.compile(r"^\d{4} .+")


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_register(session: requests.Session) -> list[dict]:
    """Download the full BAZL register and return parsed rows."""
    payload = {
        "page_result_limit": 10000,
        "current_page_number": 1,
        "sort_list": "registration",
        "language": "en",
        "queryProperties": {
            "marketing": "",
            "aircraftStatus": ["Registered"],
        },
    }
    logger.info("Downloading from %s", API_URL)
    response = session.post(API_URL, json=payload)
    response.raise_for_status()

    text = response.content.decode("utf-16")
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    return list(reader)


# ---------------------------------------------------------------------------
# Decode helpers
# ---------------------------------------------------------------------------

def _decode_aircraft_type(raw: str) -> Optional[str]:
    val = raw.strip()
    return _AIRCRAFT_TYPE_MAP.get(val, val) if val else None


def _decode_engine_type(raw: str) -> Optional[str]:
    first = raw.split(",")[0].strip()
    if not first:
        return None
    return _ENGINE_TYPE_MAP.get(first, first)


def _parse_year(raw: str) -> Optional[str]:
    val = raw.strip()
    if val and val.isdigit() and len(val) == 4:
        return f"{val}-01-01"
    return None


def _parse_seats(raw: str) -> Optional[int]:
    val = raw.strip()
    try:
        return int(val) if val else None
    except ValueError:
        return None


def _parse_engine_model(raw: str) -> Optional[str]:
    first = raw.split(",")[0].strip()
    return first or None


def _parse_registrant(raw: str) -> Optional[dict]:
    """Best-effort parse of BAZL's unstructured owner/operator address string.

    Format: Name[, Canton]?, Street, PostalCode City, Switzerland
    When no street is present: Name[, Canton]?, PostalCode City, Switzerland
    """
    if not raw or raw.strip() in ("", "N/A", "-"):
        return None

    parts = [p.strip() for p in raw.split(",")]
    parts = [p for p in parts if p]

    if len(parts) < 2:
        return None

    # Last element is always "Switzerland"
    parts.pop()

    postal_code: Optional[str] = None
    city: Optional[str] = None
    street: Optional[str] = None

    if parts and _POSTAL_RE.match(parts[-1]):
        postal_city = parts.pop()
        postal_code = postal_city[:4]
        city = postal_city[5:].strip() or None

    # Strip canton abbreviations from remaining parts
    non_canton = [p for p in parts if not _CANTON_RE.match(p)]

    # Only treat the last part as street when 2+ non-canton parts remain;
    # if only 1 remains it is the name (no street line in this record)
    if len(non_canton) >= 2:
        street = non_canton[-1]
        name_parts = non_canton[:-1]
    else:
        name_parts = non_canton

    name = ", ".join(name_parts) if name_parts else None

    fields: dict = {}
    if name:
        fields["names"] = [name]
    if street:
        fields["street"] = [street]
    if city:
        fields["city"] = city
    if postal_code:
        fields["postal_code"] = postal_code
    fields["country"] = "CH"

    return fields or None


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict) -> Optional[dict]:
    """Build the icao_hex:{hex} enrichment record from a BAZL CSV row."""
    raw_hex = row.get(" Aircraft Address HEX", "").strip().upper()
    if len(raw_hex) != 6 or not all(c in "0123456789ABCDEF" for c in raw_hex):
        return None

    registration = row.get(" Registration", "").strip()
    if not registration:
        return None

    aircraft_fields: dict = {}
    aircraft_type = _decode_aircraft_type(row.get(" Aircraft Type", ""))
    if aircraft_type:
        aircraft_fields["type"] = aircraft_type
    manufacturer = row.get(" Manufacturer", "").strip()
    if manufacturer:
        aircraft_fields["manufacturer"] = manufacturer
    model = row.get(" Aicraft Model", "").strip()
    if model:
        aircraft_fields["model"] = model
    type_designator = row.get(" ICAO Aircraft Type", "").strip()
    if type_designator:
        aircraft_fields["type_designator"] = type_designator
    manufactured_date = _parse_year(row.get(" Year of Manufacture", ""))
    if manufactured_date:
        aircraft_fields["manufactured_date"] = manufactured_date
    serial_number = row.get(" Serial Number", "").strip()
    if serial_number:
        aircraft_fields["serial_number"] = serial_number
    mopsc = _parse_seats(row.get(" MOPSC", ""))
    crew = _parse_seats(row.get(" Minimum Crew", ""))
    seats = (mopsc or 0) + (crew or 0) if (mopsc is not None or crew is not None) else None
    if seats is not None:
        aircraft_fields["seats"] = seats

    powerplant_fields: dict = {}
    engine_type = _decode_engine_type(row.get(" Engine Category", ""))
    if engine_type:
        powerplant_fields["type"] = engine_type
    engine_manufacturer = row.get(" Engine manufacturer", "").strip()
    if engine_manufacturer:
        powerplant_fields["manufacturer"] = engine_manufacturer
    engine_model = _parse_engine_model(row.get(" Engine", ""))
    if engine_model:
        powerplant_fields["model"] = engine_model

    registrant = _parse_registrant(row.get(" Main Owner", ""))

    record: dict = {"icao_hex": raw_hex, "registration": registration}
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
    if powerplant_fields:
        aircraft_fields["powerplant"] = powerplant_fields
    if registrant:
        record["registrant"] = registrant

    return record


# ---------------------------------------------------------------------------
# Redis writer
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Build records from BAZL rows and write to Redis. Returns count written."""
    count = 0
    skipped = 0
    errors = 0

    for row in rows:
        status = row.get(" Status", "").strip()
        if status != "Registered":
            logger.debug("Skipping %s — Status is %r.", row.get(" Registration", "?"), status)
            skipped += 1
            continue

        raw_hex = row.get(" Aircraft Address HEX", "").strip().upper()
        if not raw_hex or len(raw_hex) != 6 or not all(c in "0123456789ABCDEF" for c in raw_hex):
            registration = row.get(" Registration", "?").strip()
            logger.warning("Skipping %s — no ICAO hex assigned.", registration)
            skipped += 1
            continue

        record = _build_record(row)
        if record is None:
            registration = row.get(" Registration", "?").strip()
            logger.warning("Could not build record for %s — skipping.", registration)
            errors += 1
            continue

        record["source"] = "ch-bazl"
        key = aircraft_detail_key(record["icao_hex"])
        try:
            r.json().set(key, "$", record)
            r.expire(key, ttl)
            count += 1
        except Exception as exc:
            logger.warning("Redis write failed for %s: %s", record["icao_hex"], exc)
            errors += 1
            continue

        if count % 500 == 0:
            logger.info("  ... %d records written (%d skipped, %d errors).", count, skipped, errors)

    logger.info("Finished: %d written, %d skipped, %d errors.", count, skipped, errors)
    return count


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
        "ids": "SkyFollower_runner_ch_bazl",
        "name": "SkyFollower Switzerland BAZL Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Switzerland BAZL Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Switzerland BAZL Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Switzerland BAZL Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_ch_bazl_{name}",
            "object_id": f"SkyFollower_runner_ch_bazl_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_ch_bazl_{name}/config",
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
    _ensure_search_index(r)

    ttl_days = cfg.get("redis_ttl_days", 14)
    ttl = ttl_days * 86400

    session = requests.Session()
    session.headers.update({"User-Agent": "P5Software SkyFollower"})

    status = "failure"
    records_imported = 0

    try:
        logger.info("Downloading Switzerland BAZL aircraft register...")
        rows = download_register(session)
        logger.info("Downloaded %d rows.", len(rows))
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info("Switzerland BAZL runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("Switzerland BAZL runner failed: %s", exc, exc_info=True)

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
