#!/usr/bin/env python3
"""
SkyFollower United Kingdom CAA Data Runner

Enumerates all G-registered aircraft via the UK CAA G-INFO REST API by
iterating every 2-letter suffix combination AA-ZZ (676 calls total).
For each aircraft returned with RegistrationStatus "R", calls the details
endpoint to fetch the full payload and writes enrichment data to Redis.

API base: https://ginfoapi.caa.co.uk  (no authentication required)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from itertools import product
from typing import Optional

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType

from shared.redis_keys import (
    AIRCRAFT_REGISTRY_SEARCH_INDEX,
    aircraft_registry_key,
)
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client

logger = logging.getLogger("uk-caa")

API_BASE = "https://ginfoapi.caa.co.uk"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/uk-caa"

_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

# ---------------------------------------------------------------------------
# Decode tables
# ---------------------------------------------------------------------------

_AIRCRAFT_CATEGORIES: dict[str, str] = {
    "FIXED-WING LANDPLANE": "Land",
    "FIXED-WING SEAPLANE": "Sea",
    "FIXED-WING AMPHIBIAN": "Amphibian",
}

_AIRCRAFT_CLASSES: dict[str, str] = {
    "FIXED-WING LANDPLANE": "Airplane",
    "FIXED-WING SEAPLANE": "Airplane",
    "FIXED-WING AMPHIBIAN": "Airplane",
    "ROTORCRAFT": "Helicopter",
    "GLIDER": "Glider",
    "POWERED GLIDER": "Powered Glider",
    "MOTOR GLIDER": "Powered Glider",
    "FREE BALLOON": "Balloon",
    "AIRSHIP": "Airship",
    "MICROLIGHT AEROPLANE": "Microlight",
    "GYROPLANE": "Gyroplane",
    "POWERED LIFT": "Powered Lift",
    "TILT ROTOR": "Powered Lift",
}

_COUNTRY_NAMES: dict[str, str] = {
    "UNITED KINGDOM": "GB",
    "ENGLAND": "GB",
    "SCOTLAND": "GB",
    "WALES": "GB",
    "NORTHERN IRELAND": "GB",
    "AUSTRALIA": "AU",
    "AUSTRIA": "AT",
    "BAHAMAS": "BS",
    "BELGIUM": "BE",
    "BERMUDA": "BM",
    "BRAZIL": "BR",
    "CANADA": "CA",
    "CAYMAN ISLANDS": "KY",
    "CHINA": "CN",
    "CYPRUS": "CY",
    "CZECH REPUBLIC": "CZ",
    "DENMARK": "DK",
    "FINLAND": "FI",
    "FRANCE": "FR",
    "GERMANY": "DE",
    "GREECE": "GR",
    "GUERNSEY": "GG",
    "HONG KONG": "HK",
    "HUNGARY": "HU",
    "ICELAND": "IS",
    "INDIA": "IN",
    "IRELAND": "IE",
    "ISLE OF MAN": "IM",
    "ISRAEL": "IL",
    "ITALY": "IT",
    "JAPAN": "JP",
    "JERSEY": "JE",
    "KENYA": "KE",
    "LUXEMBOURG": "LU",
    "MALAYSIA": "MY",
    "MALTA": "MT",
    "NETHERLANDS": "NL",
    "NEW ZEALAND": "NZ",
    "NIGERIA": "NG",
    "NORWAY": "NO",
    "POLAND": "PL",
    "PORTUGAL": "PT",
    "QATAR": "QA",
    "SAUDI ARABIA": "SA",
    "SINGAPORE": "SG",
    "SOUTH AFRICA": "ZA",
    "SPAIN": "ES",
    "SWEDEN": "SE",
    "SWITZERLAND": "CH",
    "TURKEY": "TR",
    "UNITED ARAB EMIRATES": "AE",
    "UNITED STATES": "US",
    "UNITED STATES OF AMERICA": "US",
}


# ---------------------------------------------------------------------------
# Decode helpers
# ---------------------------------------------------------------------------

def _decode_aircraft_class(raw: str) -> Optional[str]:
    """Map UK CAA AircraftClass to canonical aircraft.type; pass through unknown values."""
    value = raw.strip().upper() if raw else ""
    if not value:
        return None
    return _AIRCRAFT_CLASSES.get(value, raw.strip().title() or None)


def _decode_aircraft_category(raw: str) -> Optional[str]:
    """Map UK CAA AircraftClass to canonical aircraft.category; returns None for non-fixed-wing."""
    value = raw.strip().upper() if raw else ""
    return _AIRCRAFT_CATEGORIES.get(value)


def _decode_country(raw: str) -> Optional[str]:
    """Map full English country name to ISO 3166-1 alpha-2; returns raw name if unmapped."""
    value = raw.strip().upper() if raw else ""
    if not value:
        return None
    return _COUNTRY_NAMES.get(value, raw.strip() or None)


def _parse_year_built(year: Optional[int]) -> Optional[str]:
    """Convert integer manufacture year to ISO 8601 UTC datetime string."""
    if year is None:
        return None
    try:
        return f"{int(year):04d}-01-01T00:00:00Z"
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(details: dict) -> Optional[dict]:
    """Build the aircraft:registry:{hex} enrichment record from a G-INFO details payload."""
    aircraft_details = details.get("AircraftDetails", {})

    icao_addr = aircraft_details.get("ICAO24BitAircraftAddress") or {}
    icao_hex = (icao_addr.get("Hex") or "").strip().upper()
    if len(icao_hex) != 6:
        return None

    mark = (details.get("RegistrationDetails", {}).get("Mark") or "").strip()
    if not mark:
        return None
    registration = f"G-{mark}"

    # aircraft sub-object
    max_pax = aircraft_details.get("MaximumPassengers")
    aircraft_class_raw = aircraft_details.get("AircraftClass", "")
    aircraft_fields: dict = {
        "type": _decode_aircraft_class(aircraft_class_raw),
        "category": _decode_aircraft_category(aircraft_class_raw),
        "manufacturer": (aircraft_details.get("Manufacturer") or "").strip() or None,
        "model": (aircraft_details.get("Type") or "").strip() or None,
        "serial_number": (aircraft_details.get("SerialNumber") or "").strip() or None,
        "type_designator": (aircraft_details.get("ICAOAircraftTypeDesignator") or "").strip() or None,
        "manufactured_date": _parse_year_built(aircraft_details.get("YearBuild")),
        "seats": int(max_pax) + 1 if max_pax is not None else None,
    }

    # powerplant sub-object — use first engine entry
    engines = aircraft_details.get("Engines") or []
    powerplant: Optional[dict] = None
    if engines:
        eng = engines[0]
        count = eng.get("TotalNumberOfEngines")
        name = (eng.get("Name") or "").strip() or None
        pp_fields: dict = {}
        if count is not None:
            pp_fields["count"] = count
        if name:
            pp_fields["model"] = name
        powerplant = pp_fields or None
    if powerplant:
        aircraft_fields["powerplant"] = powerplant

    aircraft: Optional[dict] = {k: v for k, v in aircraft_fields.items() if v is not None} or None

    # registrant sub-object — use first registered owner. The details payload
    # also includes AircraftOperatedByAocHolder (the commercial operator flying
    # under an Air Operator Certificate, which can differ from the owner) —
    # intentionally not captured; only the registered owner is tracked.
    owners = details.get("RegisteredAircraftOwners") or []
    registrant: Optional[dict] = None
    if owners:
        owner = owners[0]
        name = (owner.get("RegisteredOwner") or "").strip() or None
        street = [s for s in [
            (owner.get("Address1") or "").strip() or None,
            (owner.get("Address2") or "").strip() or None,
        ] if s]
        reg_fields = {
            "names": [name] if name else None,
            "street": street if street else None,
            "city": (owner.get("Town") or "").strip() or None,
            "administrative_area": (owner.get("County") or "").strip() or None,
            "postal_code": (owner.get("PostCode") or "").strip() or None,
            "country": _decode_country(owner.get("Country") or ""),
        }
        registrant = {k: v for k, v in reg_fields.items() if v is not None} or None

    record: dict = {"icao_hex": icao_hex, "registration": registration, "military": False}
    if aircraft:
        record["aircraft"] = aircraft
    if registrant:
        record["registrant"] = registrant
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
# G-INFO API
# ---------------------------------------------------------------------------

def _search_by_prefix(session: requests.Session, prefix: str) -> list[dict]:
    """POST /api/aircraft/search with a 2-letter registration prefix.

    Returns the raw result list; may include non-registered aircraft.
    """
    resp = session.post(
        f"{API_BASE}/api/aircraft/search",
        json={"Registration": prefix},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json() or []


def _get_aircraft_details(session: requests.Session, aircraft_id: int) -> Optional[dict]:
    """GET /api/aircraft/details/{aircraft_id}. Returns the parsed JSON payload."""
    resp = session.get(
        f"{API_BASE}/api/aircraft/details/{aircraft_id}",
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def run_pipeline(
    session: requests.Session,
    r: redis_lib.Redis,
    ttl: int,
    request_interval: float,
) -> int:
    """Enumerate all G-registered aircraft via AA-ZZ search and write to Redis.

    For each prefix, immediately fetches details and writes records — no
    intermediate accumulation.  The search calls are not rate-limited; only
    details calls sleep for request_interval to be polite to the API.

    Returns the count of records successfully written.
    """
    count = 0
    skipped = 0
    errors = 0
    retry_queue: list[int] = []
    prefix_retry_queue: list[str] = []

    for c1, c2 in product(_LETTERS, _LETTERS):
        prefix = f"{c1}{c2}"
        try:
            results = _search_by_prefix(session, prefix)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 403:
                prefix_retry_queue.append(prefix)
            else:
                logger.warning("Search error for prefix %s: %s", prefix, exc)
            continue
        except Exception as exc:
            logger.warning("Search error for prefix %s: %s", prefix, exc)
            continue

        aircraft_ids = [
            item.get("AircraftID")
            for item in results
            if item.get("RegistrationStatus") == "R" and item.get("AircraftID") is not None
        ]
        logger.info("G-%s*: %d registered (written so far: %d).", prefix, len(aircraft_ids), count)

        for aircraft_id in aircraft_ids:
            try:
                time.sleep(request_interval)
                details = _get_aircraft_details(session, aircraft_id)
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 403:
                    retry_queue.append(aircraft_id)
                else:
                    logger.warning("Error fetching details for AircraftID=%d: %s", aircraft_id, exc)
                    errors += 1
                continue
            except Exception as exc:
                logger.warning("Error fetching details for AircraftID=%d: %s", aircraft_id, exc)
                errors += 1
                continue

            reg_status = (details.get("RegistrationDetails") or {}).get("Status", "")
            if reg_status != "Registered":
                logger.debug("AircraftID=%d: Status is %r — skipping.", aircraft_id, reg_status)
                skipped += 1
                continue

            record = _build_record(details)
            if record is None:
                logger.warning("Could not build record for AircraftID=%d.", aircraft_id)
                errors += 1
                continue

            record["source"] = "uk-caa"
            key = aircraft_registry_key(record["icao_hex"])
            set_json(r, key, record)
            r.expire(key, ttl)
            count += 1

    if prefix_retry_queue:
        logger.info("Retrying %d prefixes that returned 403 (500ms interval)...", len(prefix_retry_queue))
        for prefix in prefix_retry_queue:
            time.sleep(0.5)
            try:
                results = _search_by_prefix(session, prefix)
            except Exception as exc:
                logger.warning("Retry failed for prefix %s: %s", prefix, exc)
                errors += 1
                continue

            aircraft_ids = [
                item.get("AircraftID")
                for item in results
                if item.get("RegistrationStatus") == "R" and item.get("AircraftID") is not None
            ]
            logger.info("G-%s* (retry): %d registered.", prefix, len(aircraft_ids))

            for aircraft_id in aircraft_ids:
                try:
                    time.sleep(request_interval)
                    details = _get_aircraft_details(session, aircraft_id)
                except requests.HTTPError as exc:
                    if exc.response is not None and exc.response.status_code == 403:
                        retry_queue.append(aircraft_id)
                    else:
                        logger.warning("Error fetching details for AircraftID=%d: %s", aircraft_id, exc)
                        errors += 1
                    continue
                except Exception as exc:
                    logger.warning("Error fetching details for AircraftID=%d: %s", aircraft_id, exc)
                    errors += 1
                    continue

                reg_status = (details.get("RegistrationDetails") or {}).get("Status", "")
                if reg_status != "Registered":
                    skipped += 1
                    continue

                record = _build_record(details)
                if record is None:
                    logger.warning("Could not build record for AircraftID=%d.", aircraft_id)
                    errors += 1
                    continue

                record["source"] = "uk-caa"
                key = aircraft_registry_key(record["icao_hex"])
                set_json(r, key, record)
                r.expire(key, ttl)
                count += 1

    if retry_queue:
        logger.info("Retrying %d aircraft that returned 403 (500ms interval)...", len(retry_queue))
        for aircraft_id in retry_queue:
            time.sleep(0.5)
            try:
                details = _get_aircraft_details(session, aircraft_id)
            except Exception as exc:
                logger.warning("Retry failed for AircraftID=%d: %s", aircraft_id, exc)
                errors += 1
                continue

            reg_status = (details.get("RegistrationDetails") or {}).get("Status", "")
            if reg_status != "Registered":
                skipped += 1
                continue

            record = _build_record(details)
            if record is None:
                logger.warning("Could not build record for AircraftID=%d.", aircraft_id)
                errors += 1
                continue

            record["source"] = "uk-caa"
            key = aircraft_registry_key(record["icao_hex"])
            set_json(r, key, record)
            r.expire(key, ttl)
            count += 1

    logger.info("Finished: %d written, %d skipped, %d errors.", count, skipped, errors)
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
        "ids": "SkyFollower_runner_uk_caa",
        "name": "SkyFollower United Kingdom CAA Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "United Kingdom CAA Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "United Kingdom CAA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "United Kingdom CAA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_uk_caa_{name}",
            "object_id": f"SkyFollower_runner_uk_caa_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_uk_caa_{name}/config",
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
    request_interval = cfg.get("request_interval_seconds", 0.1)

    session = requests.Session()
    session.headers.update({"User-Agent": "P5Software SkyFollower"})

    status = "failure"
    records_imported = 0

    try:
        _ensure_search_index(r)
        logger.info("Enumerating and enriching G-registered aircraft via AA-ZZ search...")
        records_imported = run_pipeline(session, r, ttl, request_interval)
        status = "success"
        logger.info("UK CAA runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("UK CAA runner failed: %s", exc, exc_info=True)

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
