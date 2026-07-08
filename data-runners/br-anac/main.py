#!/usr/bin/env python3
"""
SkyFollower Brazil ANAC Data Runner

Downloads the Registro Aeronáutico Brasileiro (RAB) aircraft register JSON
from ANAC, filters for active registrations, looks up each PP/PR/PT/PS/PU-
prefix registration in the Redis simple-aircraft search index (populated by
Mictronics) to find the ICAO hex, performs a type sanity check, then writes
enrichment data to aircraft:registry:{icao_hex} (fire-and-forget).

Important: the ANAC register does not publish ICAO hex (Mode S) addresses.
This runner can only enrich records already present in Redis from Mictronics.
Schedule it AFTER the Mictronics runner.

Data source: https://sistemas.anac.gov.br/dadosabertos/Aeronaves/RAB/dados_aeronaves.json
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

from shared.redis_keys import (
    AIRCRAFT_REGISTRY_SEARCH_INDEX,
    AIRCRAFT_MICTRONICS_SEARCH_INDEX,
    aircraft_registry_key,
    aircraft_mictronics_key,
)
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client

logger = logging.getLogger("br-anac")

DOWNLOAD_URL = "https://sistemas.anac.gov.br/dadosabertos/Aeronaves/RAB/dados_aeronaves.json"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/br-anac"
BATCH_SIZE = 100

# ---------------------------------------------------------------------------
# Decode tables
# ---------------------------------------------------------------------------

_AIRCRAFT_TYPE_MAP: dict[str, str] = {
    "L": "Airplane",
    "H": "Helicopter",
    "A": "Amphibian",
    "G": "Gyroplane",
    "S": "Seaplane",
}

_AIRCRAFT_CATEGORY_MAP: dict[str, str] = {
    "L": "Land",
    "H": "Land",
    "A": "Amphibian",
    "G": "Land",
    "S": "Sea",
}

_ENGINE_TYPE_MAP: dict[str, str] = {
    "P": "Piston",
    "J": "Turbo-jet",
    "E": "Electric",
}

# ---------------------------------------------------------------------------
# Type sanity check
# ---------------------------------------------------------------------------

_TYPE_TOKEN_RE = re.compile(r'[A-Z]{1,4}\d{2,4}')


def _type_tokens(model_str: str) -> set:
    return {t.split('-')[0] for t in _TYPE_TOKEN_RE.findall(model_str.upper())}


def _type_check_passes(simple_record: dict, detail_model_str: str) -> bool:
    if not detail_model_str:
        return True
    simple_tokens = _type_tokens(
        (simple_record.get("type_designator") or "") + " " +
        (simple_record.get("manufacturer_model") or "")
    )
    if not simple_tokens:
        return True
    detail_tokens = _type_tokens(detail_model_str)
    if not detail_tokens:
        return True
    return bool(simple_tokens & detail_tokens)


# ---------------------------------------------------------------------------
# Field parsers
# ---------------------------------------------------------------------------

def _format_registration(marca: str) -> Optional[str]:
    """Convert MARCA to hyphenated registration: PPAJH → PP-AJH."""
    marca = marca.strip().upper()
    if len(marca) < 3:
        return None
    return f"{marca[:2]}-{marca[2:]}"


def _is_active(row: dict) -> bool:
    """Return True if the record represents an active registration.

    Excludes rows where DTCANC is set or CDINTERDICAO starts with R or M.
    """
    if row.get("DTCANC"):
        return False
    cd = (row.get("CDINTERDICAO") or "").strip().upper()
    if cd.startswith("R") or cd.startswith("M"):
        return False
    return True


def _decode_cdcls(cdcls: Optional[str]) -> tuple[Optional[str], Optional[int], Optional[str]]:
    """Decode CDCLS into (aircraft_type, engine_count, engine_type).

    Format: {landing}{count}{propulsion} or RPA for drones.
    """
    if not cdcls:
        return None, None, None

    cdcls = cdcls.strip().upper()

    if cdcls == "RPA":
        return "Drone", None, None

    aircraft_type = _AIRCRAFT_TYPE_MAP.get(cdcls[0]) if len(cdcls) >= 1 else None

    engine_count: Optional[int] = None
    if len(cdcls) >= 2 and cdcls[1].isdigit() and cdcls[1] != '0':
        engine_count = int(cdcls[1])

    engine_type: Optional[str] = None
    if len(cdcls) >= 3 and cdcls[2] not in ('0', ''):
        prop = cdcls[2]
        if prop == 'T':
            engine_type = "Turbo-shaft" if cdcls[0] == 'H' else "Turbo-prop"
        else:
            engine_type = _ENGINE_TYPE_MAP.get(prop)

    return aircraft_type, engine_count, engine_type


def _parse_year(raw: Optional[str]) -> Optional[str]:
    val = (raw or "").strip()
    if val and val.isdigit() and len(val) == 4:
        return f"{val}-01-01T00:00:00Z"
    return None


def _parse_seats(raw: Optional[str]) -> Optional[int]:
    val = (raw or "").strip()
    try:
        return int(val) if val else None
    except ValueError:
        return None


def _parse_proprietarios(raw: Optional[str]) -> Optional[str]:
    """Extract first owner name from PROPRIETARIOSJSON field.

    The embedded JSON uses /""  as an escape for double-quotes.
    Returns None if NOME is missing, empty, or Indisponível.
    """
    if not raw:
        return None
    try:
        fixed = raw.replace('/""', '"')
        owners = json.loads(fixed)
        if not owners:
            return None
        nome = (owners[0].get("NOME") or "").strip()
        if not nome or nome == "Indisponível":
            return None
        return nome
    except (json.JSONDecodeError, TypeError, KeyError):
        return None


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(icao_hex: str, registration: str, row: dict) -> dict:
    """Build the aircraft:registry:{hex} enrichment record from a RAB row."""
    aircraft_type, engine_count, engine_type = _decode_cdcls(row.get("CDCLS"))

    cdcls_raw = (row.get("CDCLS") or "").strip().upper()
    if cdcls_raw == "RPA":
        aircraft_category: Optional[str] = "Land"
    else:
        aircraft_category = _AIRCRAFT_CATEGORY_MAP.get(cdcls_raw[:1]) if cdcls_raw else None

    aircraft_fields: dict = {}
    if aircraft_type:
        aircraft_fields["type"] = aircraft_type
    if aircraft_category:
        aircraft_fields["category"] = aircraft_category
    manufacturer = (row.get("NMFABRICANTE") or "").strip()
    if manufacturer:
        aircraft_fields["manufacturer"] = manufacturer
    model = (row.get("DSMODELO") or "").strip()
    if model:
        aircraft_fields["model"] = model
    type_designator = (row.get("CDTIPOICAO") or "").strip()
    if type_designator:
        aircraft_fields["type_designator"] = type_designator
    serial_number = (row.get("NRSERIE") or "").strip()
    if serial_number:
        aircraft_fields["serial_number"] = serial_number
    seats = _parse_seats(row.get("NRASSENTOS"))
    if seats is not None:
        aircraft_fields["seats"] = seats
    manufactured_date = _parse_year(row.get("NRANOFABRICACAO"))
    if manufactured_date:
        aircraft_fields["manufactured_date"] = manufactured_date

    powerplant_fields: dict = {}
    if engine_count is not None:
        powerplant_fields["count"] = engine_count
    if engine_type:
        powerplant_fields["type"] = engine_type

    nome = _parse_proprietarios(row.get("PROPRIETARIOSJSON"))

    record: dict = {"icao_hex": icao_hex, "registration": registration}
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
    if powerplant_fields:
        aircraft_fields["powerplant"] = powerplant_fields
    if nome:
        record["registrant"] = {"names": [nome]}
    return record


# ---------------------------------------------------------------------------
# Search index
# ---------------------------------------------------------------------------

def _ensure_search_index(r: redis_lib.Redis) -> None:
    """Create the aircraft:detail JSON search index if it does not already exist."""
    try:
        r.ft(AIRCRAFT_REGISTRY_SEARCH_INDEX).info()
    except Exception:
        r.ft(AIRCRAFT_REGISTRY_SEARCH_INDEX).create_index(
            fields=[
                TagField("$.icao_hex", as_name="icao_hex"),
                TagField("$.registration", as_name="registration"),
            ],
            definition=IndexDefinition(prefix=["aircraft:registry:"], index_type=IndexType.JSON),
        )
        logger.info("Created search index %r.", AIRCRAFT_REGISTRY_SEARCH_INDEX)


# ---------------------------------------------------------------------------
# Registration → icao_hex lookup
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


def _build_registration_map(registrations: list[str], r: redis_lib.Redis) -> dict[str, str]:
    """Batch-query Redis simple index for icao_hex by registration mark.

    Returns {registration → icao_hex} for registrations found in Redis.
    """
    reg_map: dict[str, str] = {}
    total_batches = (len(registrations) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num, i in enumerate(range(0, len(registrations), BATCH_SIZE)):
        batch = registrations[i:i + BATCH_SIZE]
        escaped = [_escape_tag(reg) for reg in batch]
        query_str = f"@registration:{{{'|'.join(escaped)}}}"

        try:
            results = r.ft(AIRCRAFT_MICTRONICS_SEARCH_INDEX).search(
                Query(query_str)
                .return_fields("registration")
                .paging(0, BATCH_SIZE)
            )
            for doc in results.docs:
                icao_hex = doc.id.replace("aircraft:mictronics:", "")
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
    """Download the ANAC RAB register and return parsed records."""
    logger.info("Downloading from %s", DOWNLOAD_URL)
    response = session.get(DOWNLOAD_URL, timeout=120)
    response.raise_for_status()
    logger.info("Download complete (%d bytes).", len(response.content))
    data = json.loads(response.content.decode("utf-8-sig"))
    logger.info("Parsed %d records.", len(data))
    return data


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(records: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Filter active records, resolve icao_hex via RediSearch, write to Redis.

    Returns the count of records successfully written.
    """
    active = [row for row in records if _is_active(row)]
    logger.info("%d active records (of %d total).", len(active), len(records))

    reg_map: dict[str, dict] = {}
    skipped_format = 0
    for row in active:
        registration = _format_registration(row.get("MARCA") or "")
        if not registration:
            skipped_format += 1
            continue
        reg_map[registration] = row

    if skipped_format:
        logger.warning("Skipped %d records with unparseable MARCA.", skipped_format)

    registrations = list(reg_map.keys())
    logger.info("Looking up %d registrations in Redis search index.", len(registrations))

    icao_map = _build_registration_map(registrations, r)
    logger.info(
        "Found %d / %d registrations in Redis (remainder not yet in Mictronics).",
        len(icao_map), len(registrations),
    )

    count = 0
    skipped_type = 0
    errors = 0

    for registration, icao_hex in icao_map.items():
        row = reg_map[registration]

        simple_raw = r.json().get(aircraft_mictronics_key(icao_hex))
        model_str = (row.get("DSMODELO") or "").strip()
        if simple_raw and not _type_check_passes(simple_raw, model_str):
            logger.debug(
                "%s: type mismatch (simple=%r, detail=%r) — skipping.",
                registration,
                simple_raw.get("type_designator"),
                model_str,
            )
            skipped_type += 1
            continue

        try:
            record = _build_record(icao_hex, registration, row)
            record["source"] = "br-anac"
            key = aircraft_registry_key(icao_hex)
            set_json(r, key, record)
            r.expire(key, ttl)
            count += 1
        except Exception as exc:
            logger.warning("Redis write failed for %s: %s", registration, exc)
            errors += 1
            continue

        if count % 500 == 0:
            logger.info("  ... %d records written (%d type mismatches, %d errors).", count, skipped_type, errors)

    logger.info("Finished: %d written, %d type mismatches, %d errors.", count, skipped_type, errors)
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
        "ids": "SkyFollower_runner_br_anac",
        "name": "SkyFollower Brazil ANAC Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Brazil ANAC Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Brazil ANAC Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Brazil ANAC Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_br_anac_{name}",
            "object_id": f"SkyFollower_runner_br_anac_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_br_anac_{name}/config",
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
        _ensure_search_index(r)
        records = download_register(session)
        records_imported = write_to_redis(records, r, ttl)
        status = "success"
        logger.info("Brazil ANAC runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("Brazil ANAC runner failed: %s", exc, exc_info=True)

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
