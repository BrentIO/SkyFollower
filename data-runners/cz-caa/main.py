#!/usr/bin/env python3
"""
SkyFollower Czech CAA Data Runner

Two-step REST API:
  1. GET list endpoint → filter to active records (deletion_date = null)
  2. GET detail endpoint per record → extract ICAO hex from `transponder` field

ICAO hex is available directly (no RediSearch needed). Writes enrichment
data to aircraft:detail:{icao_hex} with 14-day TTL.

List endpoint:   https://lr.caa.gov.cz/api/avreg/filtered?start=0&length=10000
Detail endpoint: https://lr.caa.gov.cz/api/avreg/{id}

Field mapping:
  transponder       → aircraft:detail:{icao_hex} key (skip if null/empty)
  manufacturer      → aircraft.manufacturer
  model             → aircraft.model
  serial_number     → aircraft.serial_number
  manufacture_year  → aircraft.manufactured_date (integer year → YYYY-01-01)
  owners[0]         → registrant.names[0]
  operators[0]      → registrant.names[1] (omitted if identical to owner)
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.redis_keys import aircraft_detail_key

logger = logging.getLogger("cz-caa")

_LIST_URL = "https://lr.caa.gov.cz/api/avreg/filtered?start=0&length=10000"
_DETAIL_URL = "https://lr.caa.gov.cz/api/avreg/{id}"

REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/cz-caa"
REQUEST_DELAY = 0.25  # seconds between detail requests
_DETAIL_MAX_RETRIES = 3

_WHITESPACE_RE = re.compile(r"\s+")

_CATEGORY_MAP = {
    "AVREG_DATA.CATEGORIES.AIRPLANE": "Airplane",
    "AVREG_DATA.CATEGORIES.GAS_BALLOON": "Gas Balloon",
    "AVREG_DATA.CATEGORIES.GLIDER": "Glider",
    "AVREG_DATA.CATEGORIES.HELICOPTER": "Helicopter",
    "AVREG_DATA.CATEGORIES.HOT_AIR_AIRSHIP": "Hot Air Airship",
    "AVREG_DATA.CATEGORIES.HOT_AIR_BALLOON": "Hot Air Balloon",
    "AVREG_DATA.CATEGORIES.POWERED_GLIDER": "Powered Glider",
    "Bezpilotní letadlo": "Unmanned Aircraft",
}

_ENGINE_TYPE_MAP = {
    "AVREG_DATA.ENGINE_TYPES.ELECTRIC": "Electric",
    "AVREG_DATA.ENGINE_TYPES.PISTON": "Piston",
    "AVREG_DATA.ENGINE_TYPES.TURBINE": "Turbine",
    "AVREG_DATA.ENGINE_TYPES.TURBOSHAFT": "Turboshaft",
    # NO_ENGINE → omitted (no powerplant entry)
}


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def _fetch_active_ids(session: requests.Session) -> list[int]:
    """Fetch the aircraft list and return IDs of active (non-deleted) records."""
    logger.info("Downloading Czech CAA aircraft list from %s", _LIST_URL)
    resp = session.get(_LIST_URL, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"List request failed with HTTP {resp.status_code}")

    data = resp.json()
    records = data.get("rows", data) if isinstance(data, dict) else data
    active = [r["id"] for r in records if r.get("deletion_date") is None and r.get("id")]
    logger.info("Found %d active records (of %d total).", len(active), len(records))
    return active


def _fetch_detail(session: requests.Session, record_id: int) -> dict | None:
    """Fetch a single aircraft detail record with retry on connection errors."""
    url = _DETAIL_URL.format(id=record_id)
    logger.debug("Downloading Czech CAA detail from %s", url)

    for attempt in range(_DETAIL_MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=30)
            if not resp.ok:
                logger.warning("Detail request for id=%d failed with HTTP %d.", record_id, resp.status_code)
                return None
            return resp.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            if attempt < _DETAIL_MAX_RETRIES:
                wait = 2 ** attempt
                logger.warning(
                    "Detail request for id=%d failed (%s); retrying in %ds (attempt %d/%d).",
                    record_id, exc, wait, attempt + 1, _DETAIL_MAX_RETRIES,
                )
                time.sleep(wait)
            else:
                logger.warning(
                    "Detail request for id=%d failed after %d attempts: %s",
                    record_id, _DETAIL_MAX_RETRIES + 1, exc,
                )
                return None


def download_and_parse(session: requests.Session, delay: float = REQUEST_DELAY):
    """Yield enrichment-ready dicts one at a time as each detail record is fetched."""
    ids = _fetch_active_ids(session)

    for i, record_id in enumerate(ids):
        if i > 0:
            time.sleep(delay)

        detail = _fetch_detail(session, record_id)
        if detail is None:
            continue

        transponder = (detail.get("transponder") or "").strip().upper()
        if not transponder:
            continue

        yield {
            "icao_hex": transponder,
            "category": detail.get("category"),
            "manufacturer": (detail.get("manufacturer") or "").strip(),
            "model": (detail.get("model") or "").strip(),
            "serial": (detail.get("serial_number") or "").strip(),
            "manufacture_year": detail.get("manufacture_year"),
            "engine_type": detail.get("engine_type"),
            "engine_count": detail.get("engine_count"),
            "seats": detail.get("max_on_board"),
            "owners": _all_display_names(detail.get("owners")),
            "operator": _first_display_name(detail.get("operators")),
        }


def _first_display_name(entries) -> str:
    """Return display_name of the first entry in a list, or empty string."""
    if not entries:
        return ""
    return _WHITESPACE_RE.sub(" ", (entries[0].get("display_name") or "").strip())


def _all_display_names(entries) -> list[str]:
    """Return all non-empty display_names from a list of owner/operator dicts."""
    if not entries:
        return []
    names = []
    for entry in entries:
        name = _WHITESPACE_RE.sub(" ", (entry.get("display_name") or "").strip())
        if name:
            names.append(name)
    return names


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict) -> dict:
    """Build a Redis detail record from a parsed row."""
    icao_hex = row["icao_hex"]
    aircraft_fields: dict = {}
    registrant_fields: dict = {}

    category = _CATEGORY_MAP.get(row.get("category") or "")
    if category:
        aircraft_fields["category"] = category

    manufacturer = _WHITESPACE_RE.sub(" ", row.get("manufacturer", "").strip())
    if manufacturer:
        aircraft_fields["manufacturer"] = manufacturer

    model = _WHITESPACE_RE.sub(" ", row.get("model", "").strip())
    if model:
        aircraft_fields["model"] = model

    serial = row.get("serial", "").strip()
    if serial:
        aircraft_fields["serial_number"] = serial

    year = row.get("manufacture_year")
    if isinstance(year, int) and 1900 <= year <= 2100:
        aircraft_fields["manufactured_date"] = f"{year}-01-01"

    powerplant: dict = {}
    engine_type = _ENGINE_TYPE_MAP.get(row.get("engine_type") or "")
    if engine_type:
        powerplant["type"] = engine_type
    engine_count = row.get("engine_count")
    if isinstance(engine_count, int) and engine_count > 0:
        powerplant["count"] = engine_count
    if powerplant:
        aircraft_fields["powerplant"] = powerplant

    seats = row.get("seats")
    if isinstance(seats, int) and seats > 0:
        aircraft_fields["seats"] = seats

    owner_names = row.get("owners") or []
    operator = _WHITESPACE_RE.sub(" ", row.get("operator", "").strip())

    names: list[str] = list(owner_names)
    if operator and operator not in names:
        names.append(operator)
    if names:
        registrant_fields["names"] = names

    record: dict = {
        "icao_hex": icao_hex,
        "source": "cz-caa",
    }
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
    if registrant_fields:
        record["registrant"] = registrant_fields

    return record


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(row: dict, r: redis_lib.Redis, ttl: int) -> bool:
    """Write a single Czech CAA record to Redis. Returns True on success."""
    icao_hex = row.get("icao_hex", "").strip()
    if not icao_hex:
        return False
    record = _build_record(row)
    key = aircraft_detail_key(icao_hex)
    try:
        r.json().set(key, "$", record)
        r.expire(key, ttl)
        return True
    except Exception as exc:
        logger.warning("Redis write failed for %s: %s", icao_hex, exc)
        return False


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
        "ids": "SkyFollower_runner_cz_caa",
        "name": "SkyFollower Czech CAA Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Czech CAA Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Czech CAA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Czech CAA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_cz_caa_{name}",
            "object_id": f"SkyFollower_runner_cz_caa_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_cz_caa_{name}/config",
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
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; P5Software SkyFollower)"})

    status = "failure"
    records_imported = 0

    try:
        for row in download_and_parse(session):
            if write_to_redis(row, r, ttl):
                records_imported += 1
        status = "success"
        logger.info(
            "Czech CAA runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Czech CAA runner failed: %s", exc, exc_info=True)

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
