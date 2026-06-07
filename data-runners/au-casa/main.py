#!/usr/bin/env python3
"""
SkyFollower Australia CASA Data Runner

Downloads the Civil Aviation Safety Authority (CASA) aircraft register CSV,
looks up each VH- registration in the Redis search index to find the ICAO hex
(provided by Mictronics), deep-merges CASA enrichment data into existing Redis
records, publishes MQTT completion stats, then exits.

Important: the CASA register does not publish ICAO hex (Mode S) addresses.
This runner can only enrich records that already exist in Redis from Mictronics.
Schedule it AFTER the Mictronics runner.

Data source: https://www.casa.gov.au/files/acrftreg
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from shared.redis_keys import AIRCRAFT_SEARCH_INDEX, icao_hex_key

logger = logging.getLogger("au-casa")

DOWNLOAD_URL = "https://www.casa.gov.au/files/acrftreg"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/au-casa"
BATCH_SIZE = 100

# ---------------------------------------------------------------------------
# Decode tables
# ---------------------------------------------------------------------------

# Airframe → aircraft.type; pass through unknown values
_AIRCRAFT_TYPES: dict[str, str] = {
    "Power Driven Aeroplane": "Airplane",
    "Rotorcraft": "Helicopter",
    "Glider": "Glider",
    "Manned Free Balloon": "Balloon",
    "Motor-Glider": "Glider",
}

# Engtype → powerplant.type; None = omit field; pass through unknown values
_ENGINE_TYPES: dict[str, Optional[str]] = {
    "Piston": "Piston",
    "Turbofan": "Turbo-fan",
    "Turboprop": "Turbo-prop",
    "Turboshaft": "Turbo-shaft",
    "Turbojet": "Turbo-jet",
    "Electric": "Electric",
    "Diesel Engine": "Diesel",
    "Rotary": "Rotary",
    "Not Applicable": None,
}

# regholdCountry (full English name) → ISO 3166-1 alpha-2
_COUNTRY_NAMES: dict[str, str] = {
    "Australia": "AU",
    "Austria": "AT",
    "British Virgin Islands": "VG",
    "Canada": "CA",
    "Cayman Islands": "KY",
    "Czech Republic": "CZ",
    "Denmark": "DK",
    "Fiji": "FJ",
    "Finland": "FI",
    "France": "FR",
    "Germany": "DE",
    "Hong Kong": "HK",
    "Ireland": "IE",
    "Isle of Man": "IM",
    "Italy": "IT",
    "Japan": "JP",
    "Kiribati": "KI",
    "Lithuania": "LT",
    "Malaysia": "MY",
    "Malta": "MT",
    "New Zealand": "NZ",
    "Papua New Guinea": "PG",
    "People's Republic of China": "CN",
    "Singapore": "SG",
    "South Africa": "ZA",
    "Switzerland": "CH",
    "Timor-Leste": "TL",
    "United Kingdom": "GB",
    "United States of America": "US",
}


# ---------------------------------------------------------------------------
# Decode helpers
# ---------------------------------------------------------------------------

def _decode_aircraft_type(raw: str) -> Optional[str]:
    """Map CASA Airframe to canonical aircraft.type; pass through unknown values."""
    value = raw.strip()
    if not value:
        return None
    return _AIRCRAFT_TYPES.get(value, value)


def _decode_engine_type(raw: str) -> Optional[str]:
    """Map CASA Engtype to canonical powerplant.type; pass through unknown values."""
    value = raw.strip()
    if not value:
        return None
    if value in _ENGINE_TYPES:
        return _ENGINE_TYPES[value]
    return value


def _decode_country(raw: str) -> Optional[str]:
    """Map full English country name to ISO 3166-1 alpha-2; returns raw name if unmapped."""
    value = raw.strip()
    if not value:
        return None
    return _COUNTRY_NAMES.get(value, value)


def _parse_manufactured_date(year: str) -> Optional[str]:
    """Convert 4-digit manufacture year to ISO 8601 UTC datetime string."""
    value = year.strip()
    if not value:
        return None
    try:
        y = int(value)
        return f"{y:04d}-01-01T00:00:00Z"
    except ValueError:
        return None


def _parse_int(value: str) -> Optional[int]:
    """Parse integer, returning None on empty or zero."""
    value = value.strip()
    if not value:
        return None
    try:
        result = int(value)
        return result if result != 0 else None
    except ValueError:
        return None


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
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict, icao_hex: str, registration: str) -> dict:
    """Build the enrichment record from a CASA CSV row."""
    # aircraft sub-object
    aircraft_fields = {
        "type": _decode_aircraft_type(row.get("Airframe", "")),
        "manufacturer": row.get("Manu", "").strip() or None,
        "model": row.get("Model", "").strip() or None,
        "serial_number": row.get("Serial", "").strip() or None,
        "manufactured_date": _parse_manufactured_date(row.get("Yearmanu", "")),
        "type_designator": row.get("ICAOtypedesig", "").strip() or None,
    }
    aircraft = {k: v for k, v in aircraft_fields.items() if v is not None} or None

    # powerplant sub-object
    powerplant_fields = {
        "count": _parse_int(row.get("engnum", "")),
        "manufacturer": row.get("Engmanu", "").strip() or None,
        "type": _decode_engine_type(row.get("Engtype", "")),
        "model": row.get("Engmodel", "").strip() or None,
    }
    powerplant = {k: v for k, v in powerplant_fields.items() if v is not None} or None

    # registrant sub-object
    name = row.get("regholdname", "").strip() or None
    street = [s for s in [
        row.get("regholdadd1", "").strip() or None,
        row.get("regholdadd2", "").strip() or None,
    ] if s]
    registrant_fields = {
        "names": [name] if name else None,
        "street": street if street else None,
        "city": row.get("regholdSuburb", "").strip() or None,
        "administrative_area": row.get("regholdState", "").strip() or None,
        "postal_code": row.get("regholdPostcode", "").strip() or None,
        "country": _decode_country(row.get("regholdCountry", "")),
    }
    registrant = {k: v for k, v in registrant_fields.items() if v is not None} or None

    record: dict = {"icao_hex": icao_hex, "registration": registration}
    if aircraft:
        record["aircraft"] = aircraft
    if powerplant:
        record["powerplant"] = powerplant
    if registrant:
        record["registrant"] = registrant
    return record


# ---------------------------------------------------------------------------
# Record merge
# ---------------------------------------------------------------------------

def _deep_merge(base: dict, update: dict) -> dict:
    """Merge update into base. update values win; nested dicts are merged recursively."""
    result = dict(base)
    for k, v in update.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


# ---------------------------------------------------------------------------
# Search index
# ---------------------------------------------------------------------------

def _ensure_search_index(r: redis_lib.Redis) -> None:
    """Create the aircraft JSON search index if it does not already exist."""
    try:
        r.ft(AIRCRAFT_SEARCH_INDEX).info()
    except Exception:
        r.ft(AIRCRAFT_SEARCH_INDEX).create_index(
            fields=[
                TagField("$.icao_hex", as_name="icao_hex"),
                TagField("$.registration", as_name="registration"),
            ],
            definition=IndexDefinition(prefix=["icao_hex:"], index_type=IndexType.JSON),
        )
        logger.info("Created search index %r.", AIRCRAFT_SEARCH_INDEX)


# ---------------------------------------------------------------------------
# Registration → icao_hex lookup
# ---------------------------------------------------------------------------

def _build_registration_map(registrations: list[str], r: redis_lib.Redis) -> dict[str, str]:
    """Batch-query Redis search index for icao_hex by registration mark.

    Returns {registration → icao_hex} for registrations already in Redis.
    Registrations not found (aircraft not yet in Mictronics) are omitted.
    """
    reg_map: dict[str, str] = {}
    total_batches = (len(registrations) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num, i in enumerate(range(0, len(registrations), BATCH_SIZE)):
        batch = registrations[i:i + BATCH_SIZE]
        escaped = [_escape_tag(reg) for reg in batch]
        query_str = f"@registration:{{{'|'.join(escaped)}}}"

        try:
            results = r.ft(AIRCRAFT_SEARCH_INDEX).search(
                Query(query_str)
                .return_fields("registration")
                .paging(0, BATCH_SIZE)
            )
            for doc in results.docs:
                icao_hex = doc.id.replace("icao_hex:", "")
                registration = getattr(doc, "registration", None)
                if registration:
                    reg_map[registration] = icao_hex
        except Exception as exc:
            logger.warning("RediSearch batch %d/%d failed: %s", batch_num + 1, total_batches, exc)

        if (batch_num + 1) % 50 == 0:
            logger.info("  ... registration lookup %d/%d batches complete.", batch_num + 1, total_batches)

    return reg_map


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_registry(url: str) -> list[dict]:
    """Download the CASA aircraft register CSV and return rows as dicts."""
    logger.info("Downloading Australia CASA aircraft register from %s", url)
    response = requests.get(url, timeout=120, headers={"User-Agent": "P5Software SkyFollower"})
    if response.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {response.status_code}")
    logger.info("Download complete (%d bytes).", len(response.content))
    text = response.content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    rows = list(reader)
    logger.info("Parsed %d rows.", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Enrich existing Redis records with CASA data. Returns count of records written."""
    # Filter suspended records
    active_rows = [row for row in rows if row.get("REGO_STATUS", "").strip().lower() != "suspended"]
    suspended_count = len(rows) - len(active_rows)
    if suspended_count:
        logger.info("Filtered %d suspended records; %d active rows remain.", suspended_count, len(active_rows))

    # Build {VH-XXX → row} mapping
    reg_row_map: dict[str, dict] = {}
    for row in active_rows:
        mark = row.get("Mark", "").strip()
        if not mark:
            continue
        reg_row_map["VH-" + mark] = row

    registrations = list(reg_row_map.keys())
    logger.info("Looking up %d registrations in Redis search index.", len(registrations))

    reg_icao_map = _build_registration_map(registrations, r)
    logger.info(
        "Found %d / %d registrations in Redis (remainder not yet in Mictronics).",
        len(reg_icao_map),
        len(registrations),
    )

    count = 0
    batch: list[tuple[str, dict]] = []

    def _flush() -> None:
        keys = [k for k, _ in batch]
        existing_list = r.json().mget(keys, "$")
        pipe = r.pipeline()
        for (key, new_record), existing_raw in zip(batch, existing_list):
            merged = _deep_merge(existing_raw[0], new_record) if existing_raw else new_record
            pipe.json().set(key, "$", merged)
            pipe.expire(key, ttl)
        pipe.execute()

    for registration, icao_hex in reg_icao_map.items():
        row = reg_row_map[registration]
        record = _build_record(row, icao_hex, registration)
        batch.append((icao_hex_key(icao_hex), record))
        count += 1
        if len(batch) == 10000:
            _flush()
            batch.clear()
            logger.info("  ... %d records written.", count)

    if batch:
        _flush()

    logger.info("Finished writing %d records to Redis.", count)
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

        import time
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
        "ids": "SkyFollower_runner_au_casa",
        "name": "SkyFollower Australia CASA Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Australia CASA Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Australia CASA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Australia CASA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_au_casa_{name}",
            "object_id": f"SkyFollower_runner_au_casa_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_au_casa_{name}/config",
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

    status = "failure"
    records_imported = 0

    try:
        rows = download_registry(DOWNLOAD_URL)
        _ensure_search_index(r)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info("Australia CASA runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("Australia CASA runner failed: %s", exc, exc_info=True)

    finally:
        try:
            publish_completion_stats(cfg, records_imported, status)
        except Exception as exc:
            logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
