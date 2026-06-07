#!/usr/bin/env python3
"""
SkyFollower Transport Canada Data Runner

Downloads the CCARCS database ZIP, parses aircraft (carscurr.txt) and owner
(carsownr.txt) CSV files, stages records in local SQLite, writes enrichment
data to Redis with a 14-day TTL, publishes MQTT completion stats, then exits.

Data source: https://wwwapps.tc.gc.ca/Saf-Sec-Sur/2/CCARCS-RIACC/download/ccarcsdb.zip

Column references follow the data dictionary (specs/data-dictionary.yaml).
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

from shared.redis_keys import AIRCRAFT_SEARCH_INDEX, icao_hex_key

logger = logging.getLogger("ca-transport-canada")

DOWNLOAD_URL = "https://wwwapps.tc.gc.ca/Saf-Sec-Sur/2/CCARCS-RIACC/download/ccarcsdb.zip"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/ca-transport-canada"

# ---------------------------------------------------------------------------
# Decode tables (data-dictionary.yaml)
# ---------------------------------------------------------------------------

# carscurr.txt col 10 AIRCRAFT_CATEGORY_E → aircraft.type
_AIRCRAFT_CATEGORIES: dict[str, str] = {
    "Aeroplane": "Airplane",
    "Helicopter": "Helicopter",
    "Glider": "Glider",
    "Balloon": "Balloon",
    "Gyroplane": "Gyroplane",
}

# carscurr.txt col 15 ENGINE_CATEGORY_E → powerplant.type
_ENGINE_CATEGORIES: dict[str, str] = {
    "Piston": "Piston",
    "Turbo Fan": "Turbo-fan",
    "Turbo Prop": "Turbo-prop",
    "Turbo Shaft": "Turbo-shaft",
    "Turbo Jet": "Turbo-jet",
    "Electric": "Electric",
    "Other": "Unknown",
}

# carsownr.txt col 9 COUNTRY_E (full English name) → ISO 3166-1 alpha-2
_COUNTRY_NAMES: dict[str, str] = {
    "CANADA": "CA",
    "UNITED STATES": "US",
    "UNITED KINGDOM": "GB",
    "AUSTRALIA": "AU",
    "NEW ZEALAND": "NZ",
    "FRANCE": "FR",
    "GERMANY": "DE",
    "ITALY": "IT",
    "SPAIN": "ES",
    "NETHERLANDS": "NL",
    "SWITZERLAND": "CH",
    "AUSTRIA": "AT",
    "BELGIUM": "BE",
    "DENMARK": "DK",
    "SWEDEN": "SE",
    "NORWAY": "NO",
    "FINLAND": "FI",
    "JAPAN": "JP",
    "CHINA": "CN",
    "BRAZIL": "BR",
    "MEXICO": "MX",
    "IRELAND": "IE",
}


# ---------------------------------------------------------------------------
# Decode helpers
# ---------------------------------------------------------------------------

def _parse_registration(raw_mark: str) -> Optional[str]:
    """Prepend 'C-' to the trimmed TC mark. Returns None for blank input."""
    value = raw_mark.strip()
    return ("C-" + value) if value else None


def _parse_icao_hex(binary_str: str) -> Optional[str]:
    """Convert TC 24-bit binary string to 6-char uppercase hex (data-dict col 42)."""
    value = binary_str.strip()
    if not value or len(value) != 24:
        return None
    try:
        return format(int(value, 2), "06X")
    except ValueError:
        return None


def _parse_date_yyyymmdd(value: str) -> Optional[str]:
    """Convert TC YYYY/MM/DD date to ISO 8601 UTC datetime string, or None."""
    value = value.strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y/%m/%d").strftime("%Y-%m-%dT00:00:00Z")
    except ValueError:
        return None


def _parse_int(value: str) -> Optional[int]:
    """Parse integer, returning None on empty or zero."""
    value = value.strip()
    if not value:
        return None
    try:
        result = int(value)
        return result if result != 0 else None
    except ValueError:
        return None


def _parse_float(value: str) -> Optional[float]:
    """Parse float, returning None on empty or zero."""
    value = value.strip()
    if not value:
        return None
    try:
        result = float(value)
        return result if result != 0.0 else None
    except ValueError:
        return None


def _decode_aircraft_type(raw: str) -> Optional[str]:
    """Map AIRCRAFT_CATEGORY_E to canonical aircraft.type string."""
    return _AIRCRAFT_CATEGORIES.get(raw.strip())


def _decode_engine_category(raw: str) -> Optional[str]:
    """Map ENGINE_CATEGORY_E to canonical powerplant.type string."""
    return _ENGINE_CATEGORIES.get(raw.strip())


def _decode_country(raw: str) -> str:
    """Map full English country name to ISO 3166-1 alpha-2 code, defaulting to 'CA'."""
    return _COUNTRY_NAMES.get(raw.strip().upper(), "CA")


# ---------------------------------------------------------------------------
# SQLite schema for local staging
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE aircraft (
    icao_hex                TEXT PRIMARY KEY,
    registration            TEXT NOT NULL,
    aircraft_type           TEXT,
    manufacturer_name       TEXT,
    model                   TEXT,
    serial_number           TEXT,
    engine_manufacturer     TEXT,
    engine_category         TEXT,
    engine_count            INTEGER,
    seat_count              INTEGER,
    manufactured_date       TEXT,
    ineffective_date        TEXT NOT NULL DEFAULT ''
);
CREATE TABLE owners (
    registration    TEXT NOT NULL,
    name            TEXT,
    trade_name      TEXT,
    street_1        TEXT,
    street_2        TEXT,
    city            TEXT,
    province        TEXT,
    postal_code     TEXT,
    country         TEXT,
    owner_type      TEXT,
    status          TEXT
);
CREATE INDEX idx_owners_registration ON owners(registration);
"""


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_and_extract(url: str) -> dict[str, bytes]:
    """Download the TC ZIP and return a mapping of lowercased filename → bytes."""
    logger.info("Downloading Transport Canada database from %s", url)
    response = requests.get(url, timeout=300, headers={"User-Agent": "P5Software SkyFollower"})
    if response.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {response.status_code}")
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

def _csv_rows(data: bytes):
    """Yield rows from a TC CSV file (ISO-8859-1, comma-delimited), skipping header.

    TC appends a blank row then a record count at the end — stop on first empty row.
    """
    reader = csv.reader(io.StringIO(data.decode("iso-8859-1", errors="replace")))
    next(reader, None)
    for row in reader:
        if not row:
            break
        yield row


# ---------------------------------------------------------------------------
# SQLite staging
# ---------------------------------------------------------------------------

def stage_data(files: dict[str, bytes], db_path: str) -> sqlite3.Connection:
    """Parse carscurr.txt and carsownr.txt, stage rows into SQLite."""
    logger.info("Opening staging database at %s", db_path)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)

    # --- carscurr.txt (aircraft records) ---
    # Key column indices (data-dictionary.yaml, ca-tc / carscurr.txt):
    #  0  MARK (raw letters; strip + prepend 'C-')
    #  4  MODEL_NAME
    #  5  MANUFACTURERS_SERIAL_NUMBER
    #  7  ID_PLATE_MANUFACTURERS_NAME
    # 10  AIRCRAFT_CATEGORY_E → decode via _AIRCRAFT_CATEGORIES
    # 13  ENGINE_MANUF_E
    # 15  ENGINE_CATEGORY_E   → decode via _ENGINE_CATEGORIES
    # 17  NUMBER_OF_ENGINES
    # 18  NUMBER_OF_SEATS
    # 23  ineffective date (TC-internal; empty = currently registered)
    # 31  DATE_MANUFACTURE_ASSEMBLY (YYYY/MM/DD)
    # 42  MODE_S_TRANSPONDER_BINARY
    # 46  TRIMMED_MARK (preferred; same as strip(col 0) when present)
    aircraft_data = files.get("carscurr.txt")
    if aircraft_data:
        cur = conn.cursor()
        count = 0
        for row in _csv_rows(aircraft_data):
            if len(row) < 43:
                continue
            icao_hex = _parse_icao_hex(row[42])
            if not icao_hex:
                continue
            # Use TRIMMED_MARK (col 46) when present; fall back to col 0 strip
            raw_mark = row[46].strip() if len(row) > 46 and row[46].strip() else row[0].strip()
            registration = _parse_registration(raw_mark)
            if not registration:
                continue
            ineffective_date = row[23].strip() if len(row) > 23 else ""
            cur.execute(
                "INSERT OR REPLACE INTO aircraft "
                "(icao_hex, registration, aircraft_type, manufacturer_name, model, serial_number, "
                "engine_manufacturer, engine_category, engine_count, seat_count, "
                "manufactured_date, ineffective_date) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    icao_hex,
                    registration,
                    _decode_aircraft_type(row[10]) if len(row) > 10 else None,
                    row[7].strip() or None,
                    row[4].strip() or None,
                    row[5].strip() or None,
                    row[13].strip() or None,
                    _decode_engine_category(row[15]) if len(row) > 15 else None,
                    _parse_int(row[17]) if len(row) > 17 else None,
                    _parse_int(row[18]) if len(row) > 18 else None,
                    _parse_date_yyyymmdd(row[31]) if len(row) > 31 else None,
                    ineffective_date,
                ),
            )
            count += 1
        conn.commit()
        logger.info("Staged %d aircraft records.", count)
    else:
        logger.warning("carscurr.txt not found in download.")

    # --- carsownr.txt (owner records) ---
    # Key column indices (data-dictionary.yaml, ca-tc / carsownr.txt):
    #  0  MARK_LINK (same format as carscurr.txt MARK)
    #  1  FULL_NAME
    #  2  TRADE_NAME
    #  3  STREET_NAME
    #  4  STREET_NAME2
    #  5  CITY
    #  6  PROVINCE_OR_STATE_E
    #  8  POSTAL_CODE
    #  9  COUNTRY_E (full English name → ISO code)
    # 11  TYPE_OF_OWNER_E
    # 13  ACTIVE_FLAG (A = active)
    owner_data = files.get("carsownr.txt")
    if owner_data:
        cur = conn.cursor()
        count = 0
        for row in _csv_rows(owner_data):
            if len(row) < 12:
                continue
            registration = _parse_registration(row[0])
            if not registration:
                continue
            status_code = row[13].strip() if len(row) > 13 else ""
            status = "Active" if status_code.upper() == "A" else "Inactive"
            country_raw = row[9].strip() if len(row) > 9 else ""
            cur.execute(
                "INSERT INTO owners "
                "(registration, name, trade_name, street_1, street_2, city, province, "
                "postal_code, country, owner_type, status) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    registration,
                    row[1].strip() or None,
                    row[2].strip() or None,
                    row[3].strip() or None,
                    row[4].strip() or None,
                    row[5].strip() or None,
                    row[6].strip() or None,
                    row[8].strip() or None if len(row) > 8 else None,
                    _decode_country(country_raw) if country_raw else "CA",
                    row[11].strip() or None if len(row) > 11 else None,
                    status,
                ),
            )
            count += 1
        conn.commit()
        logger.info("Staged %d owner records.", count)
    else:
        logger.warning("carsownr.txt not found in download.")

    return conn


# ---------------------------------------------------------------------------
# Build Redis record
# ---------------------------------------------------------------------------

def build_aircraft_record(acft_row: sqlite3.Row, owner_rows: list[sqlite3.Row]) -> dict:
    """Build the icao_hex:{hex} JSON record from staged rows."""
    # registrant — use first active owner record
    registrant: Optional[dict] = None
    if owner_rows:
        o = owner_rows[0]
        names = [n for n in [o["name"], o["trade_name"]] if n]
        streets = [s for s in [o["street_1"], o["street_2"]] if s]
        registrant = {
            "names": names,
            "street": streets,
            "city": o["city"] or None,
            "administrative_area": o["province"] or None,
            "postal_code": o["postal_code"] or None,
            "country": o["country"] or "CA",
            "type": o["owner_type"] or None,
        }

    # aircraft — type from decoded AIRCRAFT_CATEGORY_E; category null (FAA-only field)
    aircraft: Optional[dict] = None
    if acft_row["manufacturer_name"] or acft_row["model"] or acft_row["seat_count"] or acft_row["serial_number"]:
        aircraft = {
            "type": acft_row["aircraft_type"] or None,
            "model": acft_row["model"] or None,
            "seats": acft_row["seat_count"],
            "manufacturer": acft_row["manufacturer_name"] or None,
            "serial_number": acft_row["serial_number"] or None,
            "manufactured_date": acft_row["manufactured_date"] or None,
        }

    # powerplant
    powerplant: Optional[dict] = None
    if acft_row["engine_count"] or acft_row["engine_category"] or acft_row["engine_manufacturer"]:
        powerplant = {
            "count": acft_row["engine_count"],
            "type": acft_row["engine_category"] or None,
            "manufacturer": acft_row["engine_manufacturer"] or None,
        }

    return {
        "icao_hex": acft_row["icao_hex"],
        "registration": acft_row["registration"],
        "registrant": registrant,
        "aircraft": aircraft,
        "powerplant": powerplant,
    }


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
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(conn: sqlite3.Connection, r: redis_lib.Redis, ttl: int) -> int:
    """Write active aircraft records to Redis. Returns count of records written."""
    acft_cur = conn.cursor()
    acft_cur.execute(
        "SELECT icao_hex, registration, aircraft_type, manufacturer_name, model, serial_number, "
        "engine_manufacturer, engine_category, engine_count, seat_count, manufactured_date "
        "FROM aircraft WHERE ineffective_date = ''"
    )
    acft_rows = acft_cur.fetchall()
    logger.info("Writing %d active registration records to Redis.", len(acft_rows))

    owner_cur = conn.cursor()

    count = 0
    pipe = r.pipeline()
    for acft_row in acft_rows:
        owner_cur.execute(
            "SELECT name, trade_name, street_1, street_2, city, province, "
            "postal_code, country, owner_type FROM owners "
            "WHERE registration = ? AND status = 'Active' LIMIT 1",
            (acft_row["registration"],),
        )
        owner_rows = owner_cur.fetchall()
        record = build_aircraft_record(acft_row, owner_rows)
        key = icao_hex_key(record["icao_hex"])
        pipe.json().set(key, "$", record)
        pipe.expire(key, ttl)

        count += 1
        if count % 10000 == 0:
            pipe.execute()
            pipe = r.pipeline()
            logger.info("  ... %d records written.", count)

    pipe.execute()
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

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
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
        "ids": "SkyFollower_runner_ca_transport_canada",
        "name": "SkyFollower Transport Canada Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Transport Canada Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Transport Canada Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Transport Canada Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_ca_transport_canada_{name}",
            "object_id": f"SkyFollower_runner_ca_transport_canada_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_ca_transport_canada_{name}/config",
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
        logger.info("Transport Canada runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("Transport Canada runner failed: %s", exc, exc_info=True)

    finally:
        try:
            publish_completion_stats(cfg, records_imported, status)
        except Exception as exc:
            logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
