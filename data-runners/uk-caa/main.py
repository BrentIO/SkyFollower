#!/usr/bin/env python3
"""
SkyFollower United Kingdom CAA Data Runner

Uses the UK CAA G-INFO REST API to enrich aircraft records already loaded
by the Mictronics runner.  For each G-registered aircraft found in Redis
the runner:

  1. Scans Redis for icao_hex: keys in the UK ICAO hex range (400000-43FFFF).
  2. Filters for records whose registration field starts with "G-".
  3. POSTs to /api/aircraft/search with the registration suffix to resolve the
     internal AircraftID.
  4. GETs /api/aircraft/details/{AircraftID} for the full payload.
  5. Deep-merges the enrichment into the existing icao_hex:{hex} JSON key.

Important: this runner can only enrich records that already exist in Redis
(populated by the Mictronics runner).  Schedule it AFTER Mictronics.

API base: https://ginfoapi.caa.co.uk  (no authentication required)
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

from shared.redis_keys import AIRCRAFT_SEARCH_INDEX, icao_hex_key

logger = logging.getLogger("uk-caa")

API_BASE = "https://ginfoapi.caa.co.uk"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/uk-caa"

# Redis SCAN pattern covering UK ICAO hex range 400000-43FFFF.
# Second hex digit 0-3 uniquely identifies the UK allocation block.
_UK_ICAO_SCAN_PATTERN = "icao_hex:4[0123]*"

# Batch size for JSON mget calls when scanning Redis
_SCAN_BATCH_SIZE = 500

# ---------------------------------------------------------------------------
# Decode tables
# ---------------------------------------------------------------------------

_AIRCRAFT_CLASSES: dict[str, str] = {
    "FIXED-WING LANDPLANE": "Airplane",
    "FIXED-WING SEAPLANE": "Airplane",
    "FIXED-WING AMPHIBIAN": "Airplane",
    "ROTORCRAFT": "Helicopter",
    "GLIDER": "Glider",
    "POWERED GLIDER": "Glider",
    "MOTOR GLIDER": "Glider",
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
    """Build the icao_hex:{hex} enrichment record from a G-INFO details payload."""
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
    aircraft_fields = {
        "type": _decode_aircraft_class(aircraft_details.get("AircraftClass", "")),
        "manufacturer": (aircraft_details.get("Manufacturer") or "").strip() or None,
        "model": (aircraft_details.get("Type") or "").strip() or None,
        "serial_number": (aircraft_details.get("SerialNumber") or "").strip() or None,
        "type_designator": (aircraft_details.get("ICAOAircraftTypeDesignator") or "").strip() or None,
        "manufactured_date": _parse_year_built(aircraft_details.get("YearBuild")),
    }
    aircraft: Optional[dict] = {k: v for k, v in aircraft_fields.items() if v is not None} or None

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

    # registrant sub-object — use first registered owner
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
# Redis enumeration
# ---------------------------------------------------------------------------

def get_uk_registrations(r: redis_lib.Redis) -> list[tuple[str, str]]:
    """
    Return (registration, icao_hex) pairs for all G-registered aircraft in Redis.

    Scans icao_hex: keys in the UK ICAO hex range (400000-43FFFF) using the
    glob pattern icao_hex:4[0123]*.  Batch-fetches the registration field and
    filters for the G- prefix.
    """
    results: list[tuple[str, str]] = []
    batch: list[str] = []

    def _flush_batch() -> None:
        reg_lists = r.json().mget(batch, "$.registration")
        for key, reg_list in zip(batch, reg_lists):
            if not reg_list:
                continue
            reg = reg_list[0]
            if reg and str(reg).upper().startswith("G-"):
                hex_val = key[len("icao_hex:"):]
                results.append((str(reg).upper(), hex_val.upper()))
        batch.clear()

    for key in r.scan_iter(_UK_ICAO_SCAN_PATTERN):
        batch.append(key)
        if len(batch) >= _SCAN_BATCH_SIZE:
            _flush_batch()

    if batch:
        _flush_batch()

    logger.info("Found %d G-registered aircraft in Redis.", len(results))
    return results


# ---------------------------------------------------------------------------
# G-INFO API
# ---------------------------------------------------------------------------

def _search_aircraft(session: requests.Session, registration_suffix: str) -> Optional[int]:
    """
    POST /api/aircraft/search by registration suffix (without 'G-' prefix).
    Returns AircraftID of the first match, or None if not found.
    """
    payload = {
        "AircraftType": None,
        "AOCHolder": None,
        "ICAO24BitHex": None,
        "ICAOAircraftTypeDesignator": None,
        "MilitarySerialNumber": None,
        "RegisteredOwner": None,
        "Registration": registration_suffix,
        "SerialNumber": None,
        "IncludeDeregistered": False,
    }
    resp = session.post(
        f"{API_BASE}/api/aircraft/search",
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    results = resp.json()
    if not results:
        return None
    return results[0].get("AircraftID")


def _get_aircraft_details(session: requests.Session, aircraft_id: int) -> Optional[dict]:
    """GET /api/aircraft/details/{aircraft_id}. Returns the parsed JSON payload."""
    resp = session.get(
        f"{API_BASE}/api/aircraft/details/{aircraft_id}",
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(
    registrations: list[tuple[str, str]],
    r: redis_lib.Redis,
    session: requests.Session,
    ttl: int,
    request_interval: float,
) -> int:
    """
    Enrich Redis records with UK CAA G-INFO data.

    For each (registration, icao_hex) pair: searches the G-INFO API for the
    AircraftID, fetches the full details, builds the enrichment record, and
    deep-merges it into Redis.

    Returns the count of records successfully written.
    """
    count = 0
    not_found = 0
    errors = 0

    for i, (registration, _icao_hex_from_redis) in enumerate(registrations):
        if i > 0:
            time.sleep(request_interval)

        # Strip "G-" prefix for the search API
        suffix = registration[2:] if registration.upper().startswith("G-") else registration

        try:
            aircraft_id = _search_aircraft(session, suffix)
            if aircraft_id is None:
                logger.debug("No G-INFO result for %s — skipping.", registration)
                not_found += 1
                continue

            time.sleep(request_interval)
            details = _get_aircraft_details(session, aircraft_id)
            if details is None:
                logger.warning("Empty details response for %s (ID %d).", registration, aircraft_id)
                errors += 1
                continue

            record = _build_record(details)
            if record is None:
                logger.warning("Could not build record for %s (ID %d).", registration, aircraft_id)
                errors += 1
                continue

            key = icao_hex_key(record["icao_hex"])
            existing_list = r.json().mget([key], "$")
            existing_raw = existing_list[0] if existing_list else None
            merged = _deep_merge(existing_raw[0], record) if existing_raw else record
            r.json().set(key, "$", merged)
            r.expire(key, ttl)
            count += 1

            if count % 500 == 0:
                logger.info("  ... %d records written (%d not found, %d errors).", count, not_found, errors)

        except requests.HTTPError as exc:
            logger.warning("HTTP error for %s: %s", registration, exc)
            errors += 1
        except Exception as exc:
            logger.warning("Unexpected error for %s: %s", registration, exc)
            errors += 1

    logger.info(
        "Finished: %d written, %d not found in G-INFO, %d errors.",
        count, not_found, errors,
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
        registrations = get_uk_registrations(r)
        records_imported = write_to_redis(registrations, r, session, ttl, request_interval)
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
