#!/usr/bin/env python3
"""
SkyFollower Netherlands ILT Data Runner

Downloads the Inspectie Leefomgeving en Transport (ILT) civil aircraft register
ODS file, parses aircraft data, and deep-merges enrichment records directly into
Redis using the ICAO hex (Mode S transponder code) from the X-Ponder column.

The ILT register publishes ICAO hex codes directly, so no RediSearch reverse-lookup
is required. Records can be written without Mictronics running first.

The download URL is date-stamped and changes monthly; the runner scrapes the ILT
index page to discover the current link automatically.

Data source: https://www.ilent.nl/documenten/lijsten/luchtvaart/databestanden/luchtvaartregister-data
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests
from odf.opendocument import load as ods_load
from odf.table import Table, TableCell, TableRow
from odf.text import P

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType

from shared.redis_keys import AIRCRAFT_REGISTRY_SEARCH_INDEX, aircraft_registry_key, aircraft_type_key
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client
from shared.logging_setup import configure_logging

logger = logging.getLogger("nl-ilt")

INDEX_URL = "https://www.ilent.nl/documenten/lijsten/luchtvaart/databestanden/luchtvaartregister-data"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/nl-ilt"
WRITE_BATCH_SIZE = 10000

# ---------------------------------------------------------------------------
# Decode tables
# ---------------------------------------------------------------------------

_GROUP_TO_TYPE: dict[str, str] = {
    "Small aeroplane": "Airplane",
    "Large aeroplane": "Airplane",
    "Sailplane": "Glider",
    "Glider": "Glider",
    "MLA, MLH": "Microlight",
    "Micro light aeroplane": "Microlight",
    "Balloon": "Balloon",
    "Rotorcraft": "Helicopter",
    "Drones": "Drone",
}

# None value → omit powerplant.type
_ENGKIND_TO_TYPE: dict[str, Optional[str]] = {
    "Reciprocating piston-driven engine": "Piston",
    "Reciprocating piston radial engine": "Piston",
    "Piston-driven shaft (rotorcraft) engine": "Piston",
    "Reciprocating piston-driven Diesel engine": "Diesel",
    "Electrical engine": "Electric",
    "Turbofan engine": "Turbo-fan",
    "Turbine-driven shaft (rotorcraft) engine": "Turbo-shaft",
    "Turbine-driven propeller engine": "Turbo-prop",
    "Turbojet engine": "Turbo-jet",
    "Wankel engine": "Rotary",
    "Engine - not defined": None,
}

_HEX_RE = re.compile(r"^[0-9A-Fa-f]{6}$")


# ---------------------------------------------------------------------------
# URL discovery
# ---------------------------------------------------------------------------

def _find_download_url() -> str:
    """Scrape the ILT index page to find the current .ods download URL."""
    logger.info("Fetching ILT index page to discover current ODS URL.")
    resp = requests.get(INDEX_URL, timeout=30, headers={"User-Agent": "P5Software SkyFollower"})
    if resp.status_code != 200:
        raise RuntimeError(f"ILT index page returned HTTP {resp.status_code}")

    # Full absolute URL containing the known filename stem
    matches = re.findall(
        r'https?://[^"\'>\s]*luchtvaartuigregister-ilt-datas2[^"\'>\s]*\.ods',
        resp.text,
    )
    if matches:
        return matches[0]

    # Relative or partial href
    matches = re.findall(
        r'href=["\']([^"\']*luchtvaartuigregister-ilt-datas2[^"\']*\.ods)["\']',
        resp.text,
    )
    if matches:
        url = matches[0]
        if not url.startswith("http"):
            url = "https://www.ilent.nl" + url
        return url

    raise RuntimeError("Could not find ODS download URL on ILT index page.")


# ---------------------------------------------------------------------------
# ODS parsing
# ---------------------------------------------------------------------------

def _cell_text(cell) -> str:
    parts = []
    for p in cell.getElementsByType(P):
        text = str(p)
        if text:
            parts.append(text)
    return " ".join(parts).strip()


def _read_row(row) -> list[str]:
    """Expand cells with numbercolumnsrepeated to avoid column misalignment."""
    cells = []
    for cell in row.getElementsByType(TableCell):
        text = _cell_text(cell)
        repeat = int(cell.getAttribute("numbercolumnsrepeated") or 1)
        # Guard: trailing empty cells can carry very large repeat counts
        if not text and repeat > 20:
            repeat = 1
        cells.extend([text] * repeat)
    return cells


def _clean_header(h: str) -> str:
    """Strip ILT bracket annotations (e.g. ' [details=n][kolom=j]') from header names."""
    return re.sub(r"\s*\[.*", "", h).strip()


def parse_ods(file_path: str) -> list[dict]:
    """Parse the ILT ODS file. Returns data rows as dicts keyed by clean column name."""
    doc = ods_load(file_path)
    sheets = doc.spreadsheet.getElementsByType(Table)
    if not sheets:
        raise RuntimeError("No sheets found in ODS file.")

    sheet = sheets[0]
    all_rows = sheet.getElementsByType(TableRow)

    if len(all_rows) < 3:
        raise RuntimeError(f"Expected at least 3 rows in ODS, got {len(all_rows)}.")

    # Row 0 = annotated headers; row 1 = informational text; row 2+ = data
    raw_headers = _read_row(all_rows[0])
    headers = [_clean_header(h) for h in raw_headers]

    rows = []
    for tr in all_rows[2:]:
        values = _read_row(tr)
        if not any(values):
            continue
        while len(values) < len(headers):
            values.append("")
        rows.append(dict(zip(headers, values)))

    logger.info("Parsed %d data rows from ODS.", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict) -> Optional[dict]:
    """Build enrichment record from an ODS row. Returns None if row should be skipped."""
    icao_hex = row.get("X-Ponder", "").strip().upper()
    if not _HEX_RE.match(icao_hex):
        return None

    registration = row.get("Registration", "").strip() or None
    if not registration:
        return None

    # aircraft sub-object
    aircraft_fields: dict = {}

    manufacturer = row.get("Manufacturer", "").strip() or None
    if manufacturer:
        aircraft_fields["manufacturer"] = manufacturer

    model = row.get("Model", "").strip() or None
    if model:
        aircraft_fields["model"] = model

    serial = row.get("Serial", "").strip() or None
    if serial:
        aircraft_fields["serial_number"] = serial

    built = row.get("Built", "").strip()
    if re.match(r"^\d{4}$", built):
        aircraft_fields["manufactured_date"] = f"{built}-01-01T00:00:00Z"

    group = row.get("Group", "").strip()
    if group:
        aircraft_fields["type"] = _GROUP_TO_TYPE.get(group, group)

    type_designator = row.get("ICAO-code", "").strip() or None
    if type_designator:
        aircraft_fields["type_designator"] = type_designator

    # powerplant sub-object
    powerplant_fields: dict = {}

    engines_raw = row.get("Engines", "").strip()
    if engines_raw.isdigit():
        powerplant_fields["count"] = int(engines_raw)

    eng_kind = row.get("EngKind", "").strip()
    if eng_kind:
        if eng_kind in _ENGKIND_TO_TYPE:
            eng_type = _ENGKIND_TO_TYPE[eng_kind]
            if eng_type is not None:
                powerplant_fields["type"] = eng_type
        else:
            powerplant_fields["type"] = eng_kind  # pass through unknown values

    eng_manufacturer = row.get("EngManufacturer", "").strip()
    if eng_manufacturer and eng_manufacturer.lower() != "unknown":
        powerplant_fields["manufacturer"] = eng_manufacturer

    eng_model = row.get("EngModel", "").strip()
    if eng_model and "not further defined" not in eng_model.lower():
        powerplant_fields["model"] = eng_model

    record: dict = {"icao_hex": icao_hex, "registration": registration, "military": False}
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
    if powerplant_fields:
        aircraft_fields["powerplant"] = powerplant_fields

    return record


def _apply_type_lookup(record: dict, r: redis_lib.Redis) -> None:
    """If the record has an aircraft.type_designator, look up aircraft:type:{designator}
    and set aircraft.manufacturer_model / aircraft.wake_turbulence_category when found.

    Unconditional: this runner's own type_designator is sourced directly from the
    ILT register and is authoritative, so the lookup happens regardless of whether
    Mictronics also has data for the same hex — merge_aircraft.lua's "registry wins
    over mictronics" precedence rule already guarantees this value takes priority at
    read time. The reference table is not a hard dependency: a lookup failure or a
    missing entry leaves the record exactly as _build_record produced it.
    """
    aircraft = record.get("aircraft")
    if not aircraft:
        return
    type_designator = aircraft.get("type_designator")
    if not type_designator:
        return
    try:
        type_doc = r.json().get(aircraft_type_key(type_designator))
    except Exception as exc:
        logger.warning("aircraft:type lookup failed for %s: %s", type_designator, exc)
        return
    if not type_doc:
        return
    manufacturer_model = (type_doc.get("manufacturer_model") or "").strip()
    if manufacturer_model:
        aircraft["manufacturer_model"] = manufacturer_model
    wake_turbulence_category = (type_doc.get("wake_turbulence_category") or "").strip()
    if wake_turbulence_category:
        aircraft["wake_turbulence_category"] = wake_turbulence_category


# ---------------------------------------------------------------------------
# Search index
# ---------------------------------------------------------------------------

def _ensure_search_index(r: redis_lib.Redis) -> None:
    """Create the aircraft detail JSON search index if it does not already exist."""
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
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Build records and write to Redis. Returns count of records written."""
    count = 0
    batch: list[tuple[str, dict]] = []  # (redis_key, record)

    def _flush() -> None:
        pipe = r.pipeline()
        for key, record in batch:
            set_json(pipe, key, record)
            pipe.expire(key, ttl)
        pipe.execute()

    for row in rows:
        record = _build_record(row)
        if record is None:
            continue
        _apply_type_lookup(record, r)
        record["source"] = "nl-ilt"
        batch.append((aircraft_registry_key(record["icao_hex"]), record))
        count += 1
        if len(batch) == WRITE_BATCH_SIZE:
            _flush()
            batch.clear()
            logger.info("  ... %d records written.", count)

    if batch:
        _flush()

    logger.info("Finished writing %d records to Redis.", count)
    return count


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_registry() -> str:
    """Download the ILT ODS file to a temp file. Returns the temp file path."""
    url = _find_download_url()
    logger.info("Downloading ILT aircraft register from %s", url)
    resp = requests.get(url, timeout=120, headers={"User-Agent": "P5Software SkyFollower"})
    if resp.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {resp.status_code}")
    logger.info("Download complete (%d bytes).", len(resp.content))

    tmp = tempfile.NamedTemporaryFile(suffix=".ods", delete=False)
    tmp.write(resp.content)
    tmp.close()
    return tmp.name


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
        "ids": "SkyFollower_runner_nl_ilt",
        "name": "SkyFollower Netherlands ILT Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Netherlands ILT Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Netherlands ILT Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Netherlands ILT Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_nl_ilt_{name}",
            "object_id": f"SkyFollower_runner_nl_ilt_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_nl_ilt_{name}/config",
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
    try:
        cfg = _load_config()
    except FileNotFoundError as exc:
        configure_logging()
        logger.critical("Settings file not found: %s", exc)
        sys.exit(1)

    configure_logging(cfg.get("log_level"))

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
    tmp_path: Optional[str] = None

    try:
        tmp_path = download_registry()
        rows = parse_ods(tmp_path)
        _ensure_search_index(r)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info("Netherlands ILT runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("Netherlands ILT runner failed: %s", exc, exc_info=True)

    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        try:
            publish_completion_stats(cfg, records_imported, status)
        except Exception as exc:
            logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
