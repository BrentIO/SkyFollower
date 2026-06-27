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

# Fields written by this runner that must be removed when a 404 confirms the
# aircraft record no longer exists in G-INFO.
_UK_CAA_AIRCRAFT_FIELDS = [
    "$.aircraft.type",
    "$.aircraft.manufacturer",
    "$.aircraft.model",
    "$.aircraft.serial_number",
    "$.aircraft.type_designator",
    "$.aircraft.manufactured_date",
    "$.aircraft.seats",
]
_UK_CAA_POWERPLANT_FIELDS = [
    "$.powerplant.count",
    "$.powerplant.model",
]
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
    max_pax = aircraft_details.get("MaximumPassengers")
    aircraft_fields = {
        "type": _decode_aircraft_class(aircraft_details.get("AircraftClass", "")),
        "manufacturer": (aircraft_details.get("Manufacturer") or "").strip() or None,
        "model": (aircraft_details.get("Type") or "").strip() or None,
        "serial_number": (aircraft_details.get("SerialNumber") or "").strip() or None,
        "type_designator": (aircraft_details.get("ICAOAircraftTypeDesignator") or "").strip() or None,
        "manufactured_date": _parse_year_built(aircraft_details.get("YearBuild")),
        "seats": int(max_pax) if max_pax is not None else None,
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

def get_uk_registrations(r: redis_lib.Redis) -> list[tuple[str, str, Optional[int]]]:
    """
    Return (registration, icao_hex, foreign_key) triples for all G-registered
    aircraft in Redis.

    Scans icao_hex: keys in the UK ICAO hex range (400000-43FFFF), batch-fetches
    the registration field and filters for the G- prefix, then fetches the cached
    G-INFO AircraftID (foreign_key) for each matched record.

    foreign_key is None when not yet cached.
    """
    results: list[tuple[str, str, Optional[int]]] = []
    batch: list[str] = []

    def _flush_batch() -> None:
        reg_lists = r.json().mget(batch, "$.registration")
        g_keys: list[str] = []
        g_regs: list[str] = []
        g_hexes: list[str] = []
        for key, reg_list in zip(batch, reg_lists):
            if not reg_list:
                continue
            reg = reg_list[0]
            if reg and str(reg).upper().startswith("G-"):
                hex_val = key[len("icao_hex:"):]
                g_keys.append(key)
                g_regs.append(str(reg).upper())
                g_hexes.append(hex_val.upper())

        if g_keys:
            fk_lists = r.json().mget(g_keys, '$["foreign_key"]')
            for reg, hex_val, fk_list in zip(g_regs, g_hexes, fk_lists):
                fk = fk_list[0] if fk_list else None
                results.append((reg, hex_val, fk))

        batch.clear()

    scan_start = time.monotonic()
    for key in r.scan_iter(_UK_ICAO_SCAN_PATTERN):
        batch.append(key)
        if len(batch) >= _SCAN_BATCH_SIZE:
            _flush_batch()

    if batch:
        _flush_batch()

    scan_elapsed = time.monotonic() - scan_start
    logger.info(
        "Found %d G-registered aircraft in Redis (scan completed in %.2fs).",
        len(results), scan_elapsed,
    )
    return results


# ---------------------------------------------------------------------------
# G-INFO API
# ---------------------------------------------------------------------------

def _search_aircraft(session: requests.Session, icao_hex: str, expected_mark: str) -> Optional[int]:
    """
    POST /api/aircraft/search by ICAO hex address.

    expected_mark is the registration suffix without 'G-' (e.g. 'VAHH').
    When the API returns multiple results, filters by Mark and logs a warning.
    Returns AircraftID of the matching result, or None if not found.
    """
    payload = {
        "AircraftType": None,
        "AOCHolder": None,
        "ICAO24BitHex": icao_hex,
        "ICAOAircraftTypeDesignator": None,
        "MilitarySerialNumber": None,
        "RegisteredOwner": None,
        "Registration": None,
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

    registered = [r for r in results if r.get("RegistrationStatus") == "R"]
    if not registered:
        logger.info("icao_hex=%s: all %d search result(s) have RegistrationStatus != R — skipping.",
                    icao_hex, len(results))
        return None

    if len(registered) > 1:
        logger.warning(
            "Search for icao_hex=%s returned %d registered results — filtering by mark %s.",
            icao_hex, len(registered), expected_mark,
        )
        matched = [r for r in registered if r.get("Mark", "").upper() == expected_mark.upper()]
        if not matched:
            logger.warning(
                "No registered result matching mark %s for icao_hex=%s after filtering.",
                expected_mark, icao_hex,
            )
            return None
        registered = matched

    return registered[0].get("AircraftID")


def _get_aircraft_details(session: requests.Session, aircraft_id: int) -> Optional[dict]:
    """GET /api/aircraft/details/{aircraft_id}. Returns the parsed JSON payload."""
    resp = session.get(
        f"{API_BASE}/api/aircraft/details/{aircraft_id}",
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Stale record cleanup
# ---------------------------------------------------------------------------

def _cleanup_stale_record(r: redis_lib.Redis, icao_hex: str) -> None:
    """
    Remove all fields written by this runner after a 404 confirms the aircraft
    no longer exists in G-INFO.  Parent objects (aircraft, powerplant) are
    deleted only if no other fields remain after cleanup.
    """
    key = icao_hex_key(icao_hex)
    for path in ["$.foreign_key", "$.registrant"] + _UK_CAA_AIRCRAFT_FIELDS + _UK_CAA_POWERPLANT_FIELDS:
        try:
            r.json().delete(key, path)
        except Exception:
            pass
    for obj_path in ["$.aircraft", "$.powerplant"]:
        try:
            result = r.json().get(key, obj_path)
            if result and result[0] == {}:
                r.json().delete(key, obj_path)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(
    registrations: list[tuple[str, str, Optional[int]]],
    r: redis_lib.Redis,
    session: requests.Session,
    ttl: int,
    request_interval: float,
) -> int:
    """
    Enrich Redis records with UK CAA G-INFO data.

    Fast path (foreign_key cached): calls GET /api/aircraft/details/{foreign_key}
    directly, verifies the returned ICAO hex matches, and writes.  On hex mismatch
    or HTTP 4xx the stale foreign_key is cleared and the slow path is used.

    Slow path (no cache): POST /api/aircraft/search by ICAO hex to resolve
    the AircraftID, then GET /api/aircraft/details/{id}.

    The resolved AircraftID is always written back to Redis as foreign_key so
    subsequent runs can use the fast path.

    Returns the count of records successfully written.
    """
    count = 0
    not_found = 0
    errors = 0

    for registration, icao_hex_from_redis, cached_fk in registrations:
        details = None
        aircraft_id = None
        need_search = cached_fk is None
        got_404 = False

        if cached_fk is not None:
            try:
                time.sleep(request_interval)
                details = _get_aircraft_details(session, cached_fk)
                reg_status = (details.get("RegistrationDetails") or {}).get("Status", "")
                if reg_status != "Registered":
                    logger.info(
                        "%s: RegistrationDetails.Status is %r — cleaning up stale uk-caa data.",
                        registration, reg_status,
                    )
                    try:
                        _cleanup_stale_record(r, icao_hex_from_redis)
                    except Exception as exc:
                        logger.warning("Cleanup failed for %s: %s", icao_hex_from_redis, exc)
                    continue
                aircraft_details = details.get("AircraftDetails", {})
                returned_hex = ((aircraft_details.get("ICAO24BitAircraftAddress") or {}).get("Hex") or "").strip().upper()
                if returned_hex != icao_hex_from_redis.upper():
                    logger.info(
                        "%s: foreign_key %d hex mismatch (got %s, expected %s) — re-searching.",
                        registration, cached_fk, returned_hex, icao_hex_from_redis.upper(),
                    )
                    details = None
                    need_search = True
                else:
                    aircraft_id = cached_fk
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status == 403:
                    logger.info("%s: details call returned 403 — retrying next run.", registration)
                    continue
                if status == 404:
                    got_404 = True
                logger.info("%s: details call failed (HTTP %s) — re-searching.", registration, status)
                details = None
                need_search = True
            except Exception as exc:
                logger.warning("Unexpected error fetching details for %s: %s", registration, exc)
                errors += 1
                continue

        if need_search:
            try:
                time.sleep(request_interval)
                aircraft_id = _search_aircraft(session, icao_hex_from_redis, registration[2:])
            except Exception as exc:
                logger.warning("Search error for icao_hex=%s registration=%s: %s",
                               icao_hex_from_redis, registration, exc)
                errors += 1
                continue

            if aircraft_id is None:
                not_found += 1
                if got_404 and cached_fk is not None:
                    logger.info(
                        "icao_hex=%s registration=%s: 404 confirmed, no replacement found"
                        " — cleaning up stale uk-caa data.",
                        icao_hex_from_redis, registration,
                    )
                    try:
                        _cleanup_stale_record(r, icao_hex_from_redis)
                    except Exception as exc:
                        logger.warning("Cleanup failed for %s: %s", icao_hex_from_redis, exc)
                elif cached_fk is not None:
                    try:
                        r.json().delete(icao_hex_key(icao_hex_from_redis), '$["foreign_key"]')
                    except Exception:
                        pass
                else:
                    logger.debug("No G-INFO result for %s — skipping.", registration)
                continue

            try:
                time.sleep(request_interval)
                details = _get_aircraft_details(session, aircraft_id)
            except requests.HTTPError as exc:
                logger.warning(
                    "HTTP error fetching details for icao_hex=%s registration=%s"
                    " (AircraftID=%d): %s — caching foreign_key for next run.",
                    icao_hex_from_redis, registration, aircraft_id, exc,
                )
                try:
                    r.json().set(icao_hex_key(icao_hex_from_redis), "$.foreign_key", aircraft_id)
                except Exception:
                    pass
                errors += 1
                continue
            except Exception as exc:
                logger.warning("Unexpected error fetching details for icao_hex=%s registration=%s: %s",
                               icao_hex_from_redis, registration, exc)
                errors += 1
                continue

        if details is None:
            errors += 1
            continue

        reg_status = (details.get("RegistrationDetails") or {}).get("Status", "")
        if reg_status != "Registered":
            logger.info(
                "%s: RegistrationDetails.Status is %r — skipping%s.",
                registration, reg_status,
                " and cleaning up stale uk-caa data" if cached_fk is not None else "",
            )
            if cached_fk is not None:
                try:
                    _cleanup_stale_record(r, icao_hex_from_redis)
                except Exception as exc:
                    logger.warning("Cleanup failed for %s: %s", icao_hex_from_redis, exc)
            continue

        record = _build_record(details)
        if record is None:
            logger.warning("Could not build record for %s.", registration)
            errors += 1
            continue

        record["foreign_key"] = aircraft_id

        key = icao_hex_key(icao_hex_from_redis)
        existing_list = r.json().mget([key], "$")
        existing_raw = existing_list[0] if existing_list else None
        merged = _deep_merge(existing_raw[0], record) if existing_raw else record
        r.json().set(key, "$", merged)
        r.expire(key, ttl)
        count += 1

        if count % 500 == 0:
            logger.info("  ... %d records written (%d not found, %d errors).", count, not_found, errors)

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
