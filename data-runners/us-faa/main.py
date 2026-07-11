#!/usr/bin/env python3
"""
SkyFollower US FAA Data Runner

Downloads the FAA Releasable Aircraft Database, parses aircraft reference and
registration CSV files, stages records in local SQLite, writes enrichment data
to Redis with a 14-day TTL, publishes MQTT completion stats, then exits.

Data source: https://registry.faa.gov/database/ReleasableAircraft.zip
Supports 2017+ extracts (column layout changed in 2017).
"""

from __future__ import annotations

import csv
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

from shared.redis_keys import AIRCRAFT_REGISTRY_SEARCH_INDEX, aircraft_registry_key
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client

logger = logging.getLogger("us-faa")

DOWNLOAD_URL = "https://registry.faa.gov/database/ReleasableAircraft.zip"
REDIS_TTL = 14 * 86400  # 14 days in seconds
MQTT_ROOT = "SkyFollower/runner/us-faa"

# ---------------------------------------------------------------------------
# Decode helpers
# ---------------------------------------------------------------------------

_ENGINE_TYPES: dict[str, str] = {
    "0": "None",
    "1": "Piston",
    "2": "Turbo-prop",
    "3": "Turbo-shaft",
    "4": "Turbo-jet",
    "5": "Turbo-fan",
    "6": "Ramjet",
    "7": "2 Cycle",
    "8": "4 Cycle",
    "9": "Unknown",
    "10": "Electric",
    "11": "Rotary",
}

_THRUST_ENGINE_TYPES = frozenset({"Turbo-jet", "Turbo-fan", "Ramjet"})
_HP_ENGINE_TYPES = frozenset({"Piston", "Turbo-prop", "Turbo-shaft", "2 Cycle", "4 Cycle", "Rotary"})

_REGISTRANT_TYPES: dict[str, str] = {
    "1": "Individual",
    "2": "Partnership",
    "3": "Corporation",
    "4": "Co-Owned",
    "5": "Government",
    "7": "LLC",
    "8": "Non-Citizen Corporation",
    "9": "Non-Citizen Co-Owned",
    "": "None",
}


_AIRCRAFT_TYPES: dict[str, str] = {
    "1": "Glider",
    "2": "Balloon",
    "3": "Blimp/Dirigible",
    "4": "Airplane",
    "5": "Airplane",
    "6": "Rotorcraft",
    "7": "Weight-Shift-Control",
    "8": "Powered Parachute",
    "9": "Gyroplane",
}

_AIRCRAFT_CLASSES: dict[str, str] = {
    "1": "Land",
    "2": "Sea",
    "3": "Amphibian",
}


def _decode_engine_type(code: str) -> Optional[str]:
    """Map FAA engine type code to a human-readable string."""
    return _ENGINE_TYPES.get(code.strip())


def _decode_registrant_type(code: str) -> str:
    """Map FAA registrant type code to a human-readable string."""
    return _REGISTRANT_TYPES.get(code.strip(), "Unknown")


def _decode_aircraft_type(code: str) -> Optional[str]:
    """Map FAA aircraft type code to a human-readable string."""
    return _AIRCRAFT_TYPES.get(code.strip())


def _decode_aircraft_class(code: str) -> Optional[str]:
    """Map FAA aircraft class code to a human-readable string."""
    return _AIRCRAFT_CLASSES.get(code.strip())


# ---------------------------------------------------------------------------
# SQLite schema for local staging
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE aircraft (
    code          TEXT PRIMARY KEY,
    aircraft_type TEXT,
    manufacturer  TEXT,
    model         TEXT,
    seats         INTEGER,
    category      TEXT,
    engine_type   TEXT,
    engine_count  INTEGER
);
CREATE TABLE engines (
    code          TEXT PRIMARY KEY,
    manufacturer  TEXT,
    model         TEXT,
    engine_type   TEXT,
    horsepower    INTEGER,
    thrust        INTEGER
);
CREATE TABLE registrations (
    icao_hex            TEXT PRIMARY KEY,
    registration        TEXT NOT NULL,
    serial_number       TEXT,
    code_aircraft       TEXT,
    code_engine         TEXT,
    manufactured_year   TEXT,
    name_1              TEXT,
    name_2              TEXT,
    name_3              TEXT,
    name_4              TEXT,
    name_5              TEXT,
    name_6              TEXT,
    street_1            TEXT,
    street_2            TEXT,
    city                TEXT,
    administrative_area TEXT,
    postal_code         TEXT,
    country             TEXT,
    registrant_type     TEXT
);
"""


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_and_extract(url: str) -> dict[str, bytes]:
    """Download the FAA ZIP and return a mapping of normalised filename → bytes."""
    logger.info("Downloading FAA database from %s", url)
    response = requests.get(url, timeout=300, headers={"User-Agent": "P5Software AROI"})
    if response.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {response.status_code}")
    logger.info("Download complete (%d bytes); extracting ZIP.", len(response.content))
    files: dict[str, bytes] = {}
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        for name in zf.namelist():
            # Normalise: lowercase; strip duplicate .txt extensions that FAA
            # has occasionally produced (e.g. MASTER.txt.txt → master.txt)
            lower = name.lower()
            if lower.endswith(".txt.txt"):
                lower = lower[:-4]
            files[lower] = zf.read(name)
    logger.info("Extracted %d files: %s", len(files), list(files.keys()))
    return files


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _csv_rows(data: bytes):
    """Yield rows from a FAA CSV file (bytes, comma-delimited, UTF-8), skipping the header."""
    reader = csv.reader(io.StringIO(data.decode("utf-8", errors="replace")))
    next(reader, None)
    yield from reader


# ---------------------------------------------------------------------------
# SQLite staging
# ---------------------------------------------------------------------------

def stage_data(files: dict[str, bytes], db_path: str) -> sqlite3.Connection:
    """Parse acftref.txt and master.txt, stage rows into a SQLite database."""
    logger.info("Opening staging database at %s", db_path)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)

    # --- engine.txt (engine reference) ---
    # Columns: CODE, MFR, MODEL, TYPE, HORSEPOWER, THRUST
    engine_data = files.get("engine.txt")
    if engine_data:
        cur = conn.cursor()
        count = 0
        for row in _csv_rows(engine_data):
            if len(row) < 5:
                continue
            code = row[0].strip()
            if not code:
                continue
            manufacturer = row[1].strip() or None
            model = row[2].strip() or None
            try:
                engine_type = _decode_engine_type(str(int(row[3].strip())))
            except (ValueError, IndexError):
                engine_type = None
            try:
                horsepower = int(row[4].strip()) or None
            except (ValueError, IndexError):
                horsepower = None
            try:
                thrust = int(row[5].strip()) or None if len(row) > 5 and row[5].strip() else None
            except (ValueError, IndexError):
                thrust = None
            cur.execute(
                "INSERT OR REPLACE INTO engines (code, manufacturer, model, engine_type, horsepower, thrust) "
                "VALUES (?,?,?,?,?,?)",
                (code, manufacturer, model, engine_type, horsepower, thrust),
            )
            count += 1
        conn.commit()
        logger.info("Staged %d engine types.", count)
    else:
        logger.warning("engine.txt not found in download.")

    # --- acftref.txt (aircraft reference) ---
    # Columns: CODE, MFG, MODEL, TYPE-ACFT, TYPE-ENG, CLASS, RULES,
    #          NO-ENG, NO-SEATS, AC-WEIGHT, SPEED
    acft_data = files.get("acftref.txt")
    if acft_data:
        cur = conn.cursor()
        count = 0
        for row in _csv_rows(acft_data):
            if len(row) < 8:
                continue
            code = row[0].strip()
            if not code:
                continue
            manufacturer = row[1].strip() or None
            model = row[2].strip() or None
            try:
                aircraft_type = _decode_aircraft_type(str(int(row[3].strip())))
            except (ValueError, IndexError):
                aircraft_type = None
            try:
                engine_type = _decode_engine_type(str(int(row[4].strip())))
            except (ValueError, IndexError):
                engine_type = None
            try:
                category = _decode_aircraft_class(str(int(row[5].strip())))
            except (ValueError, IndexError):
                category = None
            try:
                engine_count = int(row[7].strip()) or None
            except (ValueError, IndexError):
                engine_count = None
            try:
                seats = int(row[8].strip()) or None
            except (ValueError, IndexError):
                seats = None
            cur.execute(
                "INSERT OR REPLACE INTO aircraft "
                "(code, aircraft_type, manufacturer, model, seats, category, engine_type, engine_count) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (code, aircraft_type, manufacturer, model, seats, category, engine_type, engine_count),
            )
            count += 1
        conn.commit()
        logger.info("Staged %d aircraft types.", count)
    else:
        logger.warning("acftref.txt not found in download.")

    # --- master.txt (registrations) ---
    # Key columns (2017+ layout):
    #  0  N-NUMBER   1  SERIAL NUMBER   2  MFR MDL CODE   3  ENG MFR MDL CODE
    #  4  YEAR MFR   5  TYPE REGISTRANT 6  NAME           7  STREET  8  STREET2
    #  9  CITY       10 STATE           11 ZIP CODE        14 COUNTRY
    # 24-28 OTHER NAMES(1-5)            33 MODE S CODE HEX
    master_data = files.get("master.txt")
    if master_data:
        cur = conn.cursor()
        count = 0
        for row in _csv_rows(master_data):
            if len(row) < 34:
                continue
            icao_hex = row[33].strip().upper()
            if not icao_hex or len(icao_hex) != 6:
                continue
            n_number = row[0].strip()
            if not n_number:
                continue
            registration = "N" + n_number
            serial_number = row[1].strip() or None
            code_aircraft = row[2].strip() or None
            code_engine = row[3].strip() or None
            manufactured_year = row[4].strip() or None
            registrant_type = _decode_registrant_type(row[5].strip())
            name_fields = [row[i].strip() if i < len(row) else "" for i in (6, 24, 25, 26, 27, 28)]
            street_1 = row[7].strip() or None if len(row) > 7 else None
            street_2 = row[8].strip() or None if len(row) > 8 else None
            city = row[9].strip() or None if len(row) > 9 else None
            administrative_area = row[10].strip() or None if len(row) > 10 else None
            postal_code = row[11].strip() or None if len(row) > 11 else None
            country = row[14].strip() or "US" if len(row) > 14 else "US"
            cur.execute(
                "INSERT OR REPLACE INTO registrations "
                "(icao_hex, registration, serial_number, code_aircraft, code_engine, manufactured_year, "
                "name_1, name_2, name_3, name_4, name_5, name_6, "
                "street_1, street_2, city, administrative_area, postal_code, country, registrant_type) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    icao_hex, registration, serial_number, code_aircraft, code_engine, manufactured_year,
                    name_fields[0] or None, name_fields[1] or None, name_fields[2] or None,
                    name_fields[3] or None, name_fields[4] or None, name_fields[5] or None,
                    street_1, street_2, city, administrative_area, postal_code, country, registrant_type,
                ),
            )
            count += 1
        conn.commit()
        logger.info("Staged %d registrations.", count)
    else:
        logger.warning("master.txt not found in download.")

    return conn


# ---------------------------------------------------------------------------
# Build Redis record
# ---------------------------------------------------------------------------

def build_aircraft_record(
    reg_row: sqlite3.Row,
    acft_row: Optional[sqlite3.Row],
    eng_row: Optional[sqlite3.Row] = None,
) -> dict:
    """Build the icao_hex:{hex} JSON record from staged rows."""
    # registrant
    names = [reg_row[f"name_{i}"] for i in range(1, 7) if reg_row[f"name_{i}"]]
    streets = [s for s in [reg_row["street_1"], reg_row["street_2"]] if s]
    registrant: Optional[dict] = None
    if names or streets or reg_row["city"]:
        registrant = {
            "names": names,
            "street": streets,
            "city": reg_row["city"] or None,
            "administrative_area": reg_row["administrative_area"] or None,
            "postal_code": reg_row["postal_code"] or None,
            "country": reg_row["country"] or "US",
            "type": reg_row["registrant_type"] or None,
        }

    # aircraft
    aircraft: Optional[dict] = None
    if acft_row:
        mfr_date = None
        if reg_row["manufactured_year"]:
            mfr_date = f"{reg_row['manufactured_year']}-01-01T00:00:00Z"
        aircraft = {
            "type": acft_row["aircraft_type"] or None,
            "model": acft_row["model"] or None,
            "seats": acft_row["seats"] or None,
            "category": acft_row["category"] or None,
            "manufacturer": acft_row["manufacturer"] or None,
            "serial_number": reg_row["serial_number"] or None,
            "manufactured_date": mfr_date,
        }

    # powerplant
    powerplant: Optional[dict] = None
    if acft_row or eng_row:
        count = acft_row["engine_count"] if acft_row else None
        eng_type = acft_row["engine_type"] if acft_row else None
        eng_model = eng_manufacturer = power_type = power_value = None
        if eng_row:
            eng_model = eng_row["model"] or None
            eng_manufacturer = eng_row["manufacturer"] or None
            et = eng_row["engine_type"]
            if et in _THRUST_ENGINE_TYPES:
                power_type = "Thrust"
                power_value = eng_row["thrust"] or None
            elif et in _HP_ENGINE_TYPES:
                power_type = "Horsepower"
                power_value = eng_row["horsepower"] or None
        if any([count, eng_type, eng_model, eng_manufacturer, power_type, power_value]):
            powerplant = {
                "count": count,
                "type": eng_type,
                "model": eng_model,
                "manufacturer": eng_manufacturer,
                "power_type": power_type,
                "power_value": power_value,
            }

    if powerplant is not None:
        if aircraft is None:
            aircraft = {}
        aircraft["powerplant"] = powerplant

    return {
        "icao_hex": reg_row["icao_hex"],
        "registration": reg_row["registration"],
        "military": False,
        "registrant": registrant,
        "aircraft": aircraft,
    }


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

def write_to_redis(conn: sqlite3.Connection, r: redis_lib.Redis, ttl: int) -> int:
    """Write all staged registration records to Redis. Returns count of records written."""
    eng_cur = conn.cursor()
    eng_cur.execute("SELECT code, manufacturer, model, engine_type, horsepower, thrust FROM engines")
    engines_by_code: dict[str, sqlite3.Row] = {row["code"]: row for row in eng_cur.fetchall()}

    cur = conn.cursor()
    cur.execute(
        """
        SELECT r.icao_hex, r.registration, r.serial_number, r.manufactured_year,
               r.registrant_type, r.code_engine,
               r.name_1, r.name_2, r.name_3, r.name_4, r.name_5, r.name_6,
               r.street_1, r.street_2, r.city, r.administrative_area,
               r.postal_code, r.country,
               a.aircraft_type, a.manufacturer, a.model, a.seats,
               a.category, a.engine_type, a.engine_count
        FROM registrations r
        LEFT JOIN aircraft a ON r.code_aircraft = a.code
        """
    )
    rows = cur.fetchall()
    logger.info("Writing %d registration records to Redis.", len(rows))

    count = 0
    batch: list[tuple[str, dict]] = []

    def _flush():
        pipe = r.pipeline()
        for key, record in batch:
            set_json(pipe, key, record)
            pipe.expire(key, ttl)
        pipe.execute()

    for row in rows:
        acft_row = row if row["manufacturer"] is not None else None
        eng_row = engines_by_code.get(row["code_engine"]) if row["code_engine"] else None
        record = build_aircraft_record(row, acft_row, eng_row)
        record["source"] = "us-faa"
        key = aircraft_registry_key(record["icao_hex"])
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
        "ids": "SkyFollower_runner_us_faa",
        "name": "SkyFollower US FAA Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "US FAA Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "US FAA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "US FAA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_us_faa_{name}",
            "object_id": f"SkyFollower_runner_us_faa_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_us_faa_{name}/config",
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

    try:
        files = download_and_extract(DOWNLOAD_URL)
        conn = stage_data(files, db_path)
        _ensure_search_index(r)
        records_imported = write_to_redis(conn, r, ttl)
        conn.close()
        status = "success"
        logger.info("US FAA runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("US FAA runner failed: %s", exc, exc_info=True)

    finally:
        try:
            publish_completion_stats(cfg, records_imported, status)
        except Exception as exc:
            logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
