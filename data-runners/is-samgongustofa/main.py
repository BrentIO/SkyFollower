#!/usr/bin/env python3
"""
SkyFollower Iceland Samgöngustofa Data Runner

Fetches the Icelandic aircraft register from the Transport Authority
(Samgöngustofa) via Apollo Persisted Query, looks up each TF- registration
in the Redis simple search index to find the ICAO hex (provided by Mictronics),
then writes enrichment data to aircraft:registry:{icao_hex} and publishes MQTT
completion stats, then exits.

Important: the Samgöngustofa register does not publish ICAO hex (Mode S)
addresses. This runner can only enrich records that already exist in Redis from
Mictronics. Schedule it AFTER the Mictronics runner.

Data source: https://island.is/api/graphql (Apollo Persisted Query)
"""

from __future__ import annotations

import json
import logging
import os
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
)

logger = logging.getLogger("is-samgongustofa")

API_URL = (
    "https://island.is/api/graphql"
    "?operationName=GetAllAircrafts"
    "&variables=%7B%22input%22%3A%7B%22pageNumber%22%3A1%2C%22pageSize%22%3A10000%7D%7D"
    "&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22"
    "%3A%22e2fc04031e25ffd8532d5f917192aefc40a2e2407b113cc1b264c7ecb852a819%22%7D%7D"
)
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/is-samgongustofa"
BATCH_SIZE = 100

_COUNTRY_MAP: dict[str, str] = {
    "Ísland": "IS",
    "Iceland": "IS",
    "Ireland": "IE",
    "United Kingdom": "GB",
    "United States": "US",
    "Germany": "DE",
    "France": "FR",
    "Netherlands": "NL",
    "Denmark": "DK",
    "Norway": "NO",
    "Sweden": "SE",
    "Finland": "FI",
    "Switzerland": "CH",
    "Austria": "AT",
    "Belgium": "BE",
    "Luxembourg": "LU",
    "Canada": "CA",
    "Australia": "AU",
}


# ---------------------------------------------------------------------------
# Decode helpers
# ---------------------------------------------------------------------------

def _decode_country(raw: Optional[str]) -> Optional[str]:
    if not raw or not raw.strip():
        return None
    val = raw.strip()
    return _COUNTRY_MAP.get(val, val)


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(aircraft: dict, icao_hex: str, registration: str) -> dict:
    """Build the detail enrichment record from a Samgöngustofa API aircraft entry."""
    aircraft_fields: dict = {}

    model = (aircraft.get("type") or "").strip()
    if model:
        aircraft_fields["model"] = model

    serial = (aircraft.get("serialNumber") or "").strip()
    if serial:
        aircraft_fields["serial_number"] = serial

    year = aircraft.get("productionYear")
    if year and str(year).strip() and str(year) != "0":
        aircraft_fields["manufactured_date"] = f"{year}-01-01"

    registrant: dict = {}
    operator = aircraft.get("operator") or {}

    name = (operator.get("name") or "").strip()
    if name:
        registrant["names"] = [name]

    address = (operator.get("address") or "").strip()
    if address:
        registrant["street"] = [address]

    city = (operator.get("city") or "").strip()
    if city:
        registrant["city"] = city

    postal_code = (operator.get("postcode") or "").strip()
    if postal_code:
        registrant["postal_code"] = postal_code

    country = _decode_country(operator.get("country"))
    if country:
        registrant["country"] = country

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "is-samgongustofa",
    }
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
    if registrant:
        record["registrant"] = registrant

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

def _build_registration_map(registrations: list[str], r: redis_lib.Redis) -> dict[str, str]:
    """Batch-query Redis simple search index for icao_hex by registration mark.

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

        if (batch_num + 1) % 10 == 0:
            logger.info("  ... registration lookup %d/%d batches complete.", batch_num + 1, total_batches)

    return reg_map


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_register(session: requests.Session) -> list[dict]:
    """Fetch the Iceland aircraft register via Apollo Persisted Query."""
    logger.info("Downloading Iceland Samgöngustofa aircraft register from %s", API_URL)
    response = session.get(API_URL, timeout=60)
    if response.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {response.status_code}")

    data = response.json()
    try:
        aircrafts = data["data"]["aircraftRegistryAllAircrafts"]["aircrafts"]
    except (KeyError, TypeError) as exc:
        raise RuntimeError(f"Unexpected API response shape: {exc}") from exc

    if not isinstance(aircrafts, list):
        raise RuntimeError(f"Expected list of aircrafts, got {type(aircrafts).__name__}")

    logger.info("Downloaded %d records.", len(aircrafts))
    return aircrafts


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(aircrafts: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Enrich existing Redis records with Samgöngustofa data. Returns count written."""
    reg_to_aircraft: dict[str, dict] = {}
    skipped = 0

    for aircraft in aircrafts:
        registration = (aircraft.get("identifiers") or "").strip()
        if not registration:
            skipped += 1
            logger.debug("Skipping record with no identifiers field.")
            continue
        reg_to_aircraft[registration] = aircraft

    if skipped:
        logger.warning("Skipped %d records with missing registration.", skipped)

    registrations = list(reg_to_aircraft.keys())
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
        aircraft = reg_to_aircraft[registration]
        record = _build_record(aircraft, icao_hex, registration)
        key = aircraft_registry_key(icao_hex)
        pipe.json().set(key, "$", record)
        pipe.expire(key, ttl)
        pipe_count += 1
        count += 1

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

    logger.info(
        "Finished: %d written, %d skipped (no registration), %d errors.",
        count, skipped, errors,
    )
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
        "ids": "SkyFollower_runner_is_samgongustofa",
        "name": "SkyFollower Iceland Samgöngustofa Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Iceland Samgöngustofa Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Iceland Samgöngustofa Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Iceland Samgöngustofa Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_is_samgongustofa_{name}",
            "object_id": f"SkyFollower_runner_is_samgongustofa_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_is_samgongustofa_{name}/config",
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
        aircrafts = download_register(session)
        _ensure_search_index(r)
        records_imported = write_to_redis(aircrafts, r, ttl)
        status = "success"
        logger.info(
            "Iceland Samgöngustofa runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Iceland Samgöngustofa runner failed: %s", exc, exc_info=True)

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
