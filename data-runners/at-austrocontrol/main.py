#!/usr/bin/env python3
"""
SkyFollower Austria Austrocontrol Data Runner

Downloads the Austrian aircraft register from Austrocontrol, looks up each
OE- registration in the Redis search index to find the ICAO hex (provided by
Mictronics), deep-merges enrichment data into existing Redis records, publishes
MQTT completion stats, then exits.

Important: the Austrocontrol register does not publish ICAO hex (Mode S) addresses.
This runner can only enrich records that already exist in Redis from Mictronics.
Schedule it AFTER the Mictronics runner.

Data source: https://www.austrocontrol.at/lfa-publish-service/v2/oenfl/luftfahrzeuge
"""

from __future__ import annotations

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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from shared.redis_keys import AIRCRAFT_SEARCH_INDEX, icao_hex_key

logger = logging.getLogger("at-austrocontrol")

API_URL = "https://www.austrocontrol.at/lfa-publish-service/v2/oenfl/luftfahrzeuge?page=0&size=10000"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/at-austrocontrol"
BATCH_SIZE = 100

# ---------------------------------------------------------------------------
# Decode tables
# ---------------------------------------------------------------------------

_AIRCRAFT_TYPE_MAP: dict[str, str] = {
    "Flugzeug": "Airplane",
    "Hubschrauber": "Helicopter",
    "Tragschrauber": "Gyroplane",
    "Eigenstartfähiger Motorsegler": "Glider",
    "Nicht eigenstartfähiger Motorsegler": "Glider",
}

_COUNTRY_MAP: dict[str, str] = {
    "Belgien": "BE",
    "Bulgarien": "BG",
    "Deutschland": "DE",
    "Dänemark": "DK",
    "Estland": "EE",
    "Finnland": "FI",
    "Frankreich": "FR",
    "Griechenland": "GR",
    "Irland": "IE",
    "Italien": "IT",
    "Kroatien": "HR",
    "Litauen": "LT",
    "Luxemburg": "LU",
    "Malta": "MT",
    "Monaco": "MC",
    "Niederlande": "NL",
    "Norwegen": "NO",
    "Polen": "PL",
    "Portugal": "PT",
    "Rumänien": "RO",
    "Schweden": "SE",
    "Schweiz": "CH",
    "Slowakei": "SK",
    "Slowenien": "SI",
    "Spanien": "ES",
    "Tschechische Republik": "CZ",
    "Ungarn": "HU",
    "Zypern": "CY",
    "Österreich": "AT",
}

_STRIP_QUOTES_RE = re.compile(r'^"(.*)"$', re.DOTALL)


# ---------------------------------------------------------------------------
# Decode helpers
# ---------------------------------------------------------------------------

def _decode_aircraft_type(raw: str) -> Optional[str]:
    val = raw.strip()
    return _AIRCRAFT_TYPE_MAP.get(val, val) if val else None


def _decode_country(raw: str) -> Optional[str]:
    val = raw.strip()
    return _COUNTRY_MAP.get(val, val) if val else None


# ---------------------------------------------------------------------------
# Halter (owner) parsing
# ---------------------------------------------------------------------------

def _parse_halter(raw: str) -> Optional[dict]:
    """Parse the Austrocontrol halter field into a registrant sub-object.

    Format: Name\\r\\nPostalCode City, Street\\r\\nCountry
    Repeats for co-owners; only the first owner group is used.
    """
    if not raw or raw.strip() in ("", "N/A", "-"):
        return None

    lines = [ln.strip() for ln in raw.replace("\r\n", "\n").split("\n")]
    lines = [ln for ln in lines if ln]

    if len(lines) < 3:
        return None

    name_raw = lines[0]
    address_line = lines[1]
    country_raw = lines[2]

    m = _STRIP_QUOTES_RE.match(name_raw)
    name = m.group(1).strip() if m else name_raw.strip()

    country = _decode_country(country_raw)

    postal_code: Optional[str] = None
    city: Optional[str] = None
    street: Optional[str] = None

    if "," in address_line:
        left, _, right = address_line.partition(",")
        street = right.strip() or None
        left = left.strip()
        parts = left.split(None, 1)
        if len(parts) == 2 and parts[0].isdigit():
            postal_code = parts[0]
            city = parts[1].strip() or None
        elif parts:
            city = left
    else:
        parts = address_line.split(None, 1)
        if len(parts) == 2 and parts[0].isdigit():
            postal_code = parts[0]
            city = parts[1].strip() or None
        else:
            city = address_line.strip() or None

    fields: dict = {}
    if name:
        fields["names"] = [name]
    if street:
        fields["street"] = [street]
    if city:
        fields["city"] = city
    if postal_code:
        fields["postal_code"] = postal_code
    if country:
        fields["country"] = country

    return fields or None


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(item: dict, icao_hex: str, registration: str) -> dict:
    """Build the enrichment record from a single Austrocontrol API item."""
    aircraft_fields: dict = {}

    aircraft_type = _decode_aircraft_type(item.get("luftfahrzeugart") or "")
    if aircraft_type:
        aircraft_fields["type"] = aircraft_type

    manufacturer = (item.get("hersteller") or "").strip()
    if manufacturer:
        aircraft_fields["manufacturer"] = manufacturer

    model = (item.get("baumuster") or "").strip()
    if model:
        aircraft_fields["model"] = model

    serial_number = (item.get("seriennummer") or "").strip()
    if serial_number:
        aircraft_fields["serial_number"] = serial_number

    registrant = _parse_halter(item.get("halter") or "")

    record: dict = {"icao_hex": icao_hex, "registration": registration}
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
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

def download_register(session: requests.Session) -> list[dict]:
    """Download the Austrocontrol aircraft register and return as a list of dicts."""
    logger.info("Downloading Austria Austrocontrol aircraft register from %s", API_URL)
    response = session.get(API_URL, timeout=120)
    if response.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {response.status_code}")
    items = response.json()
    if not isinstance(items, list):
        raise RuntimeError(f"Unexpected response type: {type(items).__name__}")
    logger.info("Downloaded %d records.", len(items))
    return items


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(items: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Enrich existing Redis records with Austrocontrol data. Returns count written."""
    active: list[dict] = []
    deregistered = 0

    for item in items:
        if item.get("loeschung") is not None:
            logger.debug(
                "Skipping %s — loeschung is %r.",
                item.get("kennzeichen", "?"),
                item["loeschung"],
            )
            deregistered += 1
        else:
            active.append(item)

    if deregistered:
        logger.warning("Skipped %d deregistered aircraft (loeschung != null).", deregistered)

    reg_to_item: dict[str, dict] = {}
    for item in active:
        kennzeichen = (item.get("kennzeichen") or "").strip()
        if not kennzeichen:
            continue
        registration = f"OE-{kennzeichen}"
        reg_to_item[registration] = item

    registrations = list(reg_to_item.keys())
    logger.info("Looking up %d registrations in Redis search index.", len(registrations))

    reg_icao_map = _build_registration_map(registrations, r)
    logger.info(
        "Found %d / %d registrations in Redis (remainder not yet in Mictronics).",
        len(reg_icao_map),
        len(registrations),
    )

    count = 0
    errors = 0
    batch: list[tuple[str, dict]] = []

    def _flush() -> None:
        nonlocal errors
        keys = [k for k, _ in batch]
        try:
            existing_list = r.json().mget(keys, "$")
        except Exception as exc:
            logger.warning("Redis mget failed: %s", exc)
            errors += len(batch)
            return
        pipe = r.pipeline()
        for (key, new_record), existing_raw in zip(batch, existing_list):
            merged = _deep_merge(existing_raw[0], new_record) if existing_raw else new_record
            pipe.json().set(key, "$", merged)
            pipe.expire(key, ttl)
        try:
            pipe.execute()
        except Exception as exc:
            logger.warning("Redis pipeline failed: %s", exc)
            errors += len(batch)

    for registration, icao_hex in reg_icao_map.items():
        item = reg_to_item[registration]
        record = _build_record(item, icao_hex, registration)
        batch.append((icao_hex_key(icao_hex), record))
        count += 1
        if len(batch) == 10000:
            _flush()
            batch.clear()
            logger.info("  ... %d records written.", count)

    if batch:
        _flush()

    logger.info("Finished: %d written, %d skipped (deregistered), %d errors.", count, deregistered, errors)
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
        "ids": "SkyFollower_runner_at_austrocontrol",
        "name": "SkyFollower Austria Austrocontrol Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Austria Austrocontrol Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Austria Austrocontrol Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Austria Austrocontrol Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_at_austrocontrol_{name}",
            "object_id": f"SkyFollower_runner_at_austrocontrol_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_at_austrocontrol_{name}/config",
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
        items = download_register(session)
        _ensure_search_index(r)
        records_imported = write_to_redis(items, r, ttl)
        status = "success"
        logger.info("Austria Austrocontrol runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("Austria Austrocontrol runner failed: %s", exc, exc_info=True)

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
