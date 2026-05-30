#!/usr/bin/env python3
"""
SkyFollower Mictronics Data Runner

Downloads the Mictronics IndexedDB aircraft database, parses the aircraft,
operators, and types JSON files, stages records in local SQLite, writes
enrichment data to Redis with a 14-day TTL, publishes MQTT completion stats,
then exits.

Data source: https://www.mictronics.de/aircraft-database/indexedDB.php
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

from shared.redis_keys import icao_hex_key, registration_key

logger = logging.getLogger("mictronics")

DOWNLOAD_URL = "https://www.mictronics.de/aircraft-database/indexedDB.php"
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
    military        INTEGER
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
            # values: [registration, type_designator, [military_flag, interesting_flag]]
            registration = str(values[0]).strip() if values[0] else None
            type_designator = str(values[1]).strip() if values[1] else None
            if type_designator == "":
                type_designator = None
            military = 0
            if len(values) > 2 and isinstance(values[2], list) and len(values[2]) > 0:
                try:
                    military = int(values[2][0])
                except (ValueError, TypeError):
                    military = 0
            cur.execute(
                "INSERT OR REPLACE INTO aircraft "
                "(icao_hex, registration, type_designator, military) VALUES (?,?,?,?)",
                (
                    str(icao_hex).strip().upper(),
                    registration,
                    type_designator,
                    military,
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
    """Build the icao_hex:{hex} JSON record from staged rows."""
    icao_hex = row["icao_hex"]
    registration = row["registration"] or None
    type_designator = row["type_designator"] or None
    military = bool(row["military"]) if row["military"] else False

    manufacturer: Optional[str] = None
    model: Optional[str] = None
    if types_row and types_row["manufacturer_model"]:
        manufacturer, model = _split_manufacturer_model(types_row["manufacturer_model"])

    return {
        "icao_hex": icao_hex,
        "registration": registration,
        "type_designator": type_designator,
        "manufacturer": manufacturer,
        "model": model,
        "is_private_operator": None,
        "operator": None,
        "airline_code": None,
        "serial_number": None,
        "year_built": None,
        "source": "mictronics",
    }


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(conn: sqlite3.Connection, r: redis_lib.Redis, ttl: int) -> int:
    """Write all staged aircraft records to Redis. Returns count of records written."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT a.icao_hex, a.registration, a.type_designator, a.military,
               t.manufacturer_model, t.wake_turbulence_category
        FROM aircraft a
        LEFT JOIN types t ON a.type_designator = t.type_designator
        """
    )
    rows = cur.fetchall()
    logger.info("Writing %d aircraft records to Redis.", len(rows))

    count = 0
    pipe = r.pipeline()
    for row in rows:
        types_row = None
        if row["manufacturer_model"] is not None:
            # Simulate a types row as a dict-like object
            types_row = row
        record = build_aircraft_record(row, types_row if row["manufacturer_model"] else None)
        key = icao_hex_key(record["icao_hex"])
        pipe.set(key, json.dumps(record), ex=ttl)

        # Write registration reverse-index (NX — don't overwrite more-authoritative source)
        if record["registration"]:
            reg_key = registration_key(record["registration"])
            pipe.set(reg_key, record["icao_hex"], nx=True, ex=ttl)

        count += 1
        if count % 10000 == 0:
            pipe.execute()
            pipe = r.pipeline()
            logger.info("  ... %d records written.", count)

    pipe.execute()
    logger.info("Finished writing %d aircraft records to Redis.", count)
    return count


# ---------------------------------------------------------------------------
# MQTT
# ---------------------------------------------------------------------------

def publish_completion_stats(
    cfg: dict,
    records_imported: int,
    status: str,
) -> None:
    """Publish completion statistics to MQTT and HA autodiscovery."""
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
        client.publish(f"{base}/last_run_at", run_at, retain=True)
        client.publish(f"{base}/last_run_status", status, retain=True)

        _publish_ha_autodiscovery(client)

        # Allow publishes to flush
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
        "ids": "SkyFollower_runner_mictronics",
        "name": "SkyFollower Mictronics Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Mictronics Records Imported", "mdi:airplane", "total_increasing", None),
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

    try:
        # 1. Download and extract
        files = download_and_extract(DOWNLOAD_URL)

        # 2. Stage in SQLite
        conn = stage_data(files, db_path)

        # 3. Write to Redis
        records_imported = write_to_redis(conn, r, ttl)
        conn.close()

        status = "success"
        logger.info("Mictronics runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("Mictronics runner failed: %s", exc, exc_info=True)
        status = "failure"

    finally:
        # 4. Publish MQTT stats regardless of success/failure
        try:
            publish_completion_stats(cfg, records_imported, status)
        except Exception as exc:
            logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
