#!/usr/bin/env python3
"""
SkyFollower New Zealand CAA Data Runner

Downloads the NZ CAA aircraft register CSV, normalises fields into the
aircraft:detail:{hex} enrichment shape, writes to Redis with a 14-day TTL,
publishes MQTT completion stats, then exits.

Data source: https://www.aviation.govt.nz/assets/aircraft/aircraft-register/Aircraft-Register-for-website-.csv
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType

from shared.redis_keys import AIRCRAFT_DETAIL_SEARCH_INDEX, aircraft_detail_key

logger = logging.getLogger("nz-caa")

DOWNLOAD_URL = "https://www.aviation.govt.nz/assets/aircraft/aircraft-register/Aircraft-Register-for-website-.csv"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/nz-caa"

# ---------------------------------------------------------------------------
# Decode tables
# ---------------------------------------------------------------------------

_AIRCRAFT_TYPES: dict[str, str] = {
    "Aeroplane (Aircraft)": "Airplane",
    "Amateur Built Aeroplane": "Airplane",
    "Helicopter": "Helicopter",
    "Amateur Built Helicopter": "Helicopter",
    "Microlight Class 1": "Microlight",
    "Microlight Class 2": "Microlight",
    "Glider": "Glider",
    "Power Glider": "Glider",
    "Amateur Built Glider": "Glider",
    "Gyroplane (Gyrocopter)": "Gyroplane",
    "Balloon (hot-air)": "Balloon",
}

_COUNTRY_NAMES: dict[str, str] = {
    "New Zealand": "NZ",
    "Australia": "AU",
    "United Kingdom": "GB",
    "Slovenia": "SI",
    "Vanuatu": "VU",
    "Hong Kong": "HK",
}

_CITY_POSTAL_RE = re.compile(r"^(.+?)\s+(\d{3,6})$")


# ---------------------------------------------------------------------------
# Decode helpers
# ---------------------------------------------------------------------------

def _decode_aircraft_type(raw: str) -> Optional[str]:
    """Map NZ Model Category to canonical aircraft.type; pass through unknown values."""
    value = raw.strip()
    if not value:
        return None
    return _AIRCRAFT_TYPES.get(value, value)


def _decode_country(raw: str) -> Optional[str]:
    """Map country name to ISO 3166-1 alpha-2 code, returning raw name if unmapped."""
    value = raw.strip()
    if not value:
        return None
    return _COUNTRY_NAMES.get(value, value)


def _parse_address(raw: str) -> dict:
    """
    Parse a NZ CAA free-text address into registrant address sub-fields.

    Format: "Street 1[, Street 2, ...], City PostalCode, Country"

    Returns a dict with only the fields that could be extracted (no None values).
    """
    if not raw.strip():
        return {}

    parts = [p.strip() for p in raw.split(", ") if p.strip()]

    if len(parts) == 1:
        return {"street": parts}

    country = _decode_country(parts[-1])
    city_postal_segment = parts[-2]

    m = _CITY_POSTAL_RE.match(city_postal_segment)
    city_candidate = m.group(1).strip() if m else None
    if m and city_candidate and "box" not in city_candidate.lower():
        city = city_candidate
        postal_code = m.group(2) or None
        street = [p for p in parts[:-2] if p]
    else:
        city = None
        postal_code = None
        street = [p for p in parts[:-1] if p]

    result: dict = {}
    if street:
        result["street"] = street
    if city:
        result["city"] = city
    if postal_code:
        result["postal_code"] = postal_code
    if country:
        result["country"] = country
    return result


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict) -> Optional[dict]:
    """Build the aircraft:detail:{hex} JSON record from a CSV row. Returns None if no ICAO hex."""
    icao_hex = row.get("Mode S Code HEX", "").strip().upper()
    if not icao_hex:
        return None

    registration = row.get("Registration Mark", "").strip() or None

    # aircraft sub-object
    aircraft_type = _decode_aircraft_type(row.get("Model Category", ""))
    manufacturer = row.get("Manufacturer", "").strip() or None
    model = row.get("Model", "").strip() or None
    serial_number = row.get("Serial No.", "").strip() or None

    aircraft: Optional[dict] = None
    if any([aircraft_type, manufacturer, model, serial_number]):
        aircraft = {k: v for k, v in {
            "type": aircraft_type,
            "manufacturer": manufacturer,
            "model": model,
            "serial_number": serial_number,
        }.items() if v is not None}

    # registrant sub-object
    owner_name = row.get("Owner Name", "").strip() or None
    owner_address = row.get("Owner Address", "").strip()

    registrant: Optional[dict] = None
    if owner_name or owner_address:
        registrant = {}
        if owner_name:
            registrant["names"] = [owner_name]
        if owner_address:
            registrant.update(_parse_address(owner_address))

    record: dict = {"icao_hex": icao_hex, "registration": registration}
    if aircraft:
        record["aircraft"] = aircraft
    if registrant:
        record["registrant"] = registrant

    return record


# ---------------------------------------------------------------------------
# Search index
# ---------------------------------------------------------------------------

def _ensure_search_index(r: redis_lib.Redis) -> None:
    """Create the aircraft detail JSON search index if it does not already exist."""
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
# Download
# ---------------------------------------------------------------------------

def download_csv(url: str) -> list[dict]:
    """Download the NZ CAA aircraft register CSV and return a list of row dicts."""
    logger.info("Downloading NZ CAA aircraft register from %s", url)
    response = requests.get(url, timeout=120, headers={"User-Agent": "P5Software SkyFollower"})
    if response.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {response.status_code}")
    logger.info("Download complete (%d bytes).", len(response.content))
    content = response.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Build records from CSV rows and write to Redis. Returns count of records written."""
    count = 0
    batch: list[tuple[str, dict]] = []

    def _flush() -> None:
        pipe = r.pipeline()
        for key, record in batch:
            pipe.json().set(key, "$", record)
            pipe.expire(key, ttl)
        pipe.execute()

    for row in rows:
        record = _build_record(row)
        if record is None:
            continue
        record["source"] = "nz-caa"
        key = aircraft_detail_key(record["icao_hex"])
        batch.append((key, record))
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
        "ids": "SkyFollower_runner_nz_caa",
        "name": "SkyFollower New Zealand CAA Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "New Zealand CAA Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "New Zealand CAA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "New Zealand CAA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_nz_caa_{name}",
            "object_id": f"SkyFollower_runner_nz_caa_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_nz_caa_{name}/config",
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
        rows = download_csv(DOWNLOAD_URL)
        _ensure_search_index(r)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info("New Zealand CAA runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("New Zealand CAA runner failed: %s", exc, exc_info=True)

    finally:
        try:
            publish_completion_stats(cfg, records_imported, status)
        except Exception as exc:
            logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
