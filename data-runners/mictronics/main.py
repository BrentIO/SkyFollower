#!/usr/bin/env python3
"""
SkyFollower Mictronics Data Runner

Downloads the Mictronics IndexedDB aircraft database, parses the aircraft,
operators, and types JSON files, stages records in local SQLite, writes
enrichment data to Redis with a 14-day TTL, publishes MQTT completion stats,
then exits.

types.json is used both internally (joined against aircrafts.json to enrich
each aircraft:mictronics:{icao_hex} record) and published standalone as
aircraft:type:{designator} — a type-designator reference table other runners
can look up directly for hexes Mictronics itself has no data for. Entries
with no manufacturer_model are skipped.

Data source: https://github.com/Mictronics/aircraft-database/raw/refs/heads/main/indexedDB.zip
"""

from __future__ import annotations

import io
import json
import logging
import os
import sqlite3
import sys
import zipfile
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType

from shared.redis_keys import (
    AIRCRAFT_MICTRONICS_SEARCH_INDEX,
    aircraft_mictronics_key,
    aircraft_type_key,
    operator_key,
)
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client

logger = logging.getLogger("mictronics")

DOWNLOAD_URL = "https://github.com/Mictronics/aircraft-database/raw/refs/heads/main/indexedDB.zip"
REDIS_TTL = 14 * 86400  # 14 days in seconds
MQTT_ROOT = "SkyFollower/runner/mictronics"

# ---------------------------------------------------------------------------
# SQLite schema for local staging
# ---------------------------------------------------------------------------
_SCHEMA = """
CREATE TABLE aircraft (
    icao_hex        TEXT PRIMARY KEY,
    registration    TEXT,
    type_designator TEXT,
    military        INTEGER,
    interesting     INTEGER
);
CREATE TABLE operators (
    airline_designator TEXT PRIMARY KEY,
    name               TEXT,
    country            TEXT,
    callsign           TEXT
);
CREATE TABLE types (
    type_designator        TEXT PRIMARY KEY,
    manufacturer_model     TEXT,
    powerplant             TEXT,
    category               TEXT,
    wake_turbulence_category TEXT
);
"""


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_and_extract(url: str) -> dict[str, bytes]:
    """Download the Mictronics ZIP and return a mapping of filename → bytes."""
    logger.info("Downloading Mictronics database from %s", url)
    response = requests.get(url, timeout=120)
    if response.status_code != 200:
        raise RuntimeError(
            f"Download failed with HTTP {response.status_code}"
        )
    logger.info("Download complete (%d bytes); extracting ZIP.", len(response.content))
    files: dict[str, bytes] = {}
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        for name in zf.namelist():
            files[name.lower()] = zf.read(name)
    logger.info("Extracted %d files: %s", len(files), list(files.keys()))
    return files


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _decode_wtc(wtc: str) -> Optional[str]:
    mapping = {
        "J": "Super",
        "H": "Heavy",
        "M": "Medium",
        "L": "Light",
        "M/L": "Medium/Light",
        "-": "Unknown/None",
    }
    return mapping.get(wtc, "Unknown" if wtc else None)


def _split_manufacturer_model(manufacturer_model: str) -> tuple[Optional[str], Optional[str]]:
    """
    Mictronics stores 'Manufacturer Model' as a single string like 'Boeing 737-800'.
    Split on first space to get manufacturer and model.
    """
    if not manufacturer_model:
        return None, None
    parts = manufacturer_model.split(" ", 1)
    manufacturer = parts[0].strip() or None
    model = parts[1].strip() if len(parts) > 1 else None
    return manufacturer, model


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
# SQLite staging
# ---------------------------------------------------------------------------

def stage_data(
    files: dict[str, bytes],
    db_path: str,
) -> sqlite3.Connection:
    """Parse all JSON files and stage rows into a SQLite database."""
    logger.info("Opening staging database at %s", db_path)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)

    # --- operators.json ---
    if "operators.json" in files:
        operators_data = json.loads(files["operators.json"].decode("utf-8"))
        logger.info("Staging %d operators.", len(operators_data))
        cur = conn.cursor()
        for designator, values in operators_data.items():
            # values: [name, country, callsign]
            cur.execute(
                "INSERT OR REPLACE INTO operators "
                "(airline_designator, name, country, callsign) VALUES (?,?,?,?)",
                (
                    str(designator).strip(),
                    str(values[0]).strip() if values[0] else None,
                    str(values[1]).strip() if values[1] else None,
                    str(values[2]).strip() if len(values) > 2 and values[2] else None,
                ),
            )
        conn.commit()
    else:
        logger.warning("operators.json not found in download.")

    # --- types.json ---
    if "types.json" in files:
        types_data = json.loads(files["types.json"].decode("utf-8"))
        logger.info("Staging %d types.", len(types_data))
        cur = conn.cursor()
        for designator, values in types_data.items():
            # values: [manufacturer_model, description, wtc]
            manufacturer_model = str(values[0]).strip() if values[0] else None
            wtc = _decode_wtc(str(values[2]).strip()) if len(values) > 2 and values[2] else None
            cur.execute(
                "INSERT OR REPLACE INTO types "
                "(type_designator, manufacturer_model, wake_turbulence_category) VALUES (?,?,?)",
                (
                    str(designator).strip(),
                    manufacturer_model,
                    wtc,
                ),
            )
        conn.commit()
    else:
        logger.warning("types.json not found in download.")

    # --- aircrafts.json ---
    if "aircrafts.json" in files:
        aircraft_data = json.loads(files["aircrafts.json"].decode("utf-8"))
        logger.info("Staging %d aircraft.", len(aircraft_data))
        cur = conn.cursor()
        for icao_hex, values in aircraft_data.items():
            # values: [registration, type_designator, flags]
            # flags is a 2-char string: flags[0]="1" → military, flags[1]="1" → interesting
            registration = str(values[0]).strip() if values[0] else None
            type_designator = str(values[1]).strip() if values[1] else None
            if type_designator == "":
                type_designator = None
            military = False
            interesting = False
            if len(values) > 2 and values[2]:
                flags = str(values[2])
                military = len(flags) > 0 and flags[0] == "1"
                interesting = len(flags) > 1 and flags[1] == "1"
            cur.execute(
                "INSERT OR REPLACE INTO aircraft "
                "(icao_hex, registration, type_designator, military, interesting) VALUES (?,?,?,?,?)",
                (
                    str(icao_hex).strip().upper(),
                    registration,
                    type_designator,
                    military,
                    interesting,
                ),
            )
        conn.commit()
    else:
        logger.warning("aircrafts.json not found in download.")

    return conn


# ---------------------------------------------------------------------------
# Build Redis record
# ---------------------------------------------------------------------------

def build_aircraft_record(row: sqlite3.Row, types_row: Optional[sqlite3.Row]) -> dict:
    """Build the aircraft:mictronics:{icao_hex} JSON record from staged rows."""
    icao_hex = row["icao_hex"]
    registration = row["registration"] or None
    type_designator = row["type_designator"] or None
    military = bool(row["military"])

    manufacturer: Optional[str] = None
    manufacturer_model: Optional[str] = None
    wake_turbulence_category: Optional[str] = None
    if types_row and types_row["manufacturer_model"]:
        manufacturer, _ = _split_manufacturer_model(types_row["manufacturer_model"])
        manufacturer_model = types_row["manufacturer_model"]
    if types_row:
        wake_turbulence_category = types_row["wake_turbulence_category"] or None

    aircraft_fields = {k: v for k, v in {
        "type_designator": type_designator,
        "manufacturer": manufacturer,
        "manufacturer_model": manufacturer_model,
        "wake_turbulence_category": wake_turbulence_category,
    }.items() if v is not None}

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "military": military,
        "source": "mictronics",
    }
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
    return record


def build_operator_record(row: sqlite3.Row) -> dict:
    """Build the operator:{designator} JSON record from a staged row."""
    record: dict = {"airline_designator": row["airline_designator"]}
    if row["name"]:
        record["name"] = row["name"]
    if row["country"]:
        record["country"] = row["country"]
    if row["callsign"]:
        record["callsign"] = row["callsign"]
    return record


def build_type_record(row: sqlite3.Row) -> dict:
    """Build the aircraft:type:{designator} JSON record from a staged types row."""
    record: dict = {
        "type_designator": row["type_designator"],
        "manufacturer_model": row["manufacturer_model"],
    }
    if row["wake_turbulence_category"]:
        record["wake_turbulence_category"] = row["wake_turbulence_category"]
    return record


# ---------------------------------------------------------------------------
# Search index
# ---------------------------------------------------------------------------

def _ensure_search_index(r: redis_lib.Redis) -> None:
    """Create the aircraft simple JSON search index if it does not already exist."""
    try:
        r.ft(AIRCRAFT_MICTRONICS_SEARCH_INDEX).info()
    except Exception:
        r.ft(AIRCRAFT_MICTRONICS_SEARCH_INDEX).create_index(
            fields=[
                TagField("$.icao_hex", as_name="icao_hex"),
                TagField("$.registration", as_name="registration"),
            ],
            definition=IndexDefinition(prefix=["aircraft:mictronics:"], index_type=IndexType.JSON),
        )
        logger.info("Created search index %r.", AIRCRAFT_MICTRONICS_SEARCH_INDEX)


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(conn: sqlite3.Connection, r: redis_lib.Redis, ttl: int) -> int:
    """Write all staged aircraft records to Redis. Returns count of records written."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT a.icao_hex, a.registration, a.type_designator, a.military, a.interesting,
               t.manufacturer_model, t.wake_turbulence_category
        FROM aircraft a
        LEFT JOIN types t ON a.type_designator = t.type_designator
        """
    )
    rows = cur.fetchall()
    logger.info("Writing %d aircraft records to Redis.", len(rows))

    count = 0
    batch: list[tuple[str, dict]] = []

    def _flush():
        keys = [k for k, _ in batch]
        existing_list = r.json().mget(keys, "$")
        pipe = r.pipeline()
        for (key, new_record), existing_raw in zip(batch, existing_list):
            merged = _deep_merge(existing_raw[0], new_record) if existing_raw else new_record
            set_json(pipe, key, merged)
            pipe.expire(key, ttl)
        pipe.execute()

    for row in rows:
        types_row = row if row["manufacturer_model"] is not None else None
        record = build_aircraft_record(row, types_row)
        key = aircraft_mictronics_key(record["icao_hex"])
        batch.append((key, record))
        count += 1
        if len(batch) == 10000:
            _flush()
            batch.clear()
            logger.info("  ... %d records written.", count)

    if batch:
        _flush()
    logger.info("Finished writing %d aircraft records to Redis.", count)
    return count


def write_operators_to_redis(conn: sqlite3.Connection, r: redis_lib.Redis, ttl: int) -> int:
    """Write all staged operator records to Redis. Returns count of records written."""
    cur = conn.cursor()
    cur.execute(
        "SELECT airline_designator, name, country, callsign FROM operators "
        "WHERE airline_designator IS NOT NULL AND airline_designator != ''"
    )
    rows = cur.fetchall()
    logger.info("Writing %d operator records to Redis.", len(rows))

    count = 0
    pipe = r.pipeline()
    for row in rows:
        record = build_operator_record(row)
        key = operator_key(record["airline_designator"])
        set_json(pipe, key, record)
        pipe.expire(key, ttl)
        count += 1
        if count % 10000 == 0:
            pipe.execute()
            pipe = r.pipeline()
            logger.info("  ... %d operator records written.", count)

    pipe.execute()
    logger.info("Finished writing %d operator records to Redis.", count)
    return count


def write_types_to_redis(conn: sqlite3.Connection, r: redis_lib.Redis, ttl: int) -> int:
    """Write all staged type-designator reference records to Redis. Returns count written."""
    cur = conn.cursor()
    cur.execute(
        "SELECT type_designator, manufacturer_model, wake_turbulence_category FROM types "
        "WHERE manufacturer_model IS NOT NULL AND manufacturer_model != ''"
    )
    rows = cur.fetchall()
    logger.info("Writing %d aircraft type reference records to Redis.", len(rows))

    count = 0
    pipe = r.pipeline()
    for row in rows:
        record = build_type_record(row)
        key = aircraft_type_key(row["type_designator"])
        set_json(pipe, key, record)
        pipe.expire(key, ttl)
        count += 1
        if count % 10000 == 0:
            pipe.execute()
            pipe = r.pipeline()
            logger.info("  ... %d type records written.", count)

    pipe.execute()
    logger.info("Finished writing %d type reference records to Redis.", count)
    return count


# ---------------------------------------------------------------------------
# MQTT
# ---------------------------------------------------------------------------

def publish_completion_stats(
    cfg: dict,
    records_imported: int,
    operators_imported: int,
    types_imported: int,
    status: str,
) -> None:
    """Publish completion statistics to MQTT and HA autodiscovery."""
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

        # Wait briefly for connection
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
        client.publish(f"{base}/operators_imported", str(operators_imported), retain=True)
        client.publish(f"{base}/types_imported", str(types_imported), retain=True)
        client.publish(f"{base}/last_run_at", run_at, retain=True)
        client.publish(f"{base}/last_run_status", status, retain=True)

        _publish_ha_autodiscovery(client)

        # Allow publishes to flush
        time.sleep(0.5)
        client.loop_stop()
        client.disconnect()
        logger.info(
            "MQTT stats published (status=%s, records=%d, operators=%d, types=%d).",
            status, records_imported, operators_imported, types_imported,
        )

    except Exception as exc:
        logger.warning("MQTT publish failed: %s", exc)
        try:
            client.loop_stop()
        except Exception:
            pass


def _publish_ha_autodiscovery(client: mqtt.Client) -> None:
    device = {
        "ids": "SkyFollower_runner_mictronics",
        "name": "SkyFollower Mictronics Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Mictronics Records Imported", "mdi:airplane", "total_increasing", None),
        ("operators_imported", "Mictronics Operators Imported", "mdi:account-group", "total_increasing", None),
        ("types_imported", "Mictronics Types Imported", "mdi:shape", "total_increasing", None),
        ("last_run_at", "Mictronics Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Mictronics Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_mictronics_{name}",
            "object_id": f"SkyFollower_runner_mictronics_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_mictronics_{name}/config",
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

    db_path = "/app/data/staging.db"

    status = "failure"
    records_imported = 0
    operators_imported = 0
    types_imported = 0

    try:
        # 1. Download and extract
        files = download_and_extract(DOWNLOAD_URL)

        # 2. Stage in SQLite
        conn = stage_data(files, db_path)

        # 3. Ensure search index exists, then write to Redis
        _ensure_search_index(r)
        records_imported = write_to_redis(conn, r, ttl)
        operators_imported = write_operators_to_redis(conn, r, ttl)
        types_imported = write_types_to_redis(conn, r, ttl)
        conn.close()

        status = "success"
        logger.info(
            "Mictronics runner completed successfully. Records imported: %d, Operators imported: %d, Types imported: %d",
            records_imported, operators_imported, types_imported,
        )

    except Exception as exc:
        logger.error("Mictronics runner failed: %s", exc, exc_info=True)
        status = "failure"

    finally:
        # 4. Publish MQTT stats regardless of success/failure
        try:
            publish_completion_stats(cfg, records_imported, operators_imported, types_imported, status)
        except Exception as exc:
            logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
