#!/usr/bin/env python3
"""
SkyFollower OurAirports Data Runner

Downloads the OurAirports airports CSV, filters to 4-character ICAO codes,
stages records in local SQLite, writes airport enrichment data to Redis with
a 14-day TTL, publishes MQTT completion stats, then exits.

Data source: https://davidmegginson.github.io/ourairports-data/airports.csv
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.redis_keys import airport_key

logger = logging.getLogger("ourairports")

DOWNLOAD_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
REDIS_TTL = 14 * 86400  # 14 days in seconds
MQTT_ROOT = "SkyFollower/runner/ourairports"

# ---------------------------------------------------------------------------
# SQLite schema for local staging
# ---------------------------------------------------------------------------
_SCHEMA = """
CREATE TABLE airports (
    icao_code    TEXT PRIMARY KEY,
    name         TEXT,
    latitude     REAL,
    longitude    REAL,
    altitude_feet INTEGER,
    country      TEXT,
    municipality TEXT,
    type         TEXT
);
"""


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_csv(url: str) -> str:
    """Download the OurAirports CSV and return the text content."""
    logger.info("Downloading OurAirports CSV from %s", url)
    response = requests.get(url, timeout=120)
    if response.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {response.status_code}")
    logger.info("Download complete (%d bytes).", len(response.content))
    return response.text


# ---------------------------------------------------------------------------
# Parsing / filtering
# ---------------------------------------------------------------------------

def is_valid_icao(ident: str) -> bool:
    """Return True if ident is exactly 4 characters (ICAO airport code)."""
    return len(ident.strip()) == 4


def parse_altitude(elevation_ft: str) -> Optional[int]:
    """Parse elevation_ft string to int, returning None if empty or invalid."""
    val = elevation_ft.strip()
    if not val:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return None


def parse_coordinate(value: str) -> Optional[float]:
    """Parse a latitude or longitude string to float, returning None if invalid."""
    val = value.strip()
    if not val:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# SQLite staging
# ---------------------------------------------------------------------------

def stage_data(csv_text: str, db_path: str) -> sqlite3.Connection:
    """Parse the CSV and stage qualifying rows into a SQLite database."""
    logger.info("Opening staging database at %s", db_path)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)

    reader = csv.DictReader(io.StringIO(csv_text))
    count = 0
    skipped = 0

    cur = conn.cursor()
    for row in reader:
        ident = row.get("ident", "").strip()

        if not is_valid_icao(ident):
            skipped += 1
            continue

        airport_type = row.get("type", "").strip() or None
        name = row.get("name", "").strip() or None
        latitude = parse_coordinate(row.get("latitude_deg", ""))
        longitude = parse_coordinate(row.get("longitude_deg", ""))
        altitude_feet = parse_altitude(row.get("elevation_ft", ""))
        country = row.get("iso_country", "").strip() or None
        municipality = row.get("municipality", "").strip() or None

        cur.execute(
            "INSERT OR REPLACE INTO airports "
            "(icao_code, name, latitude, longitude, altitude_feet, country, municipality, type) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (ident.upper(), name, latitude, longitude, altitude_feet, country, municipality, airport_type),
        )
        count += 1

    conn.commit()
    logger.info(
        "Staged %d airports (%d skipped for non-4-char ident).", count, skipped
    )
    return conn


# ---------------------------------------------------------------------------
# Build Redis record
# ---------------------------------------------------------------------------

def build_airport_record(row: sqlite3.Row) -> dict:
    """Build the airport:{icao_code} JSON record from a staged row."""
    return {
        "icao_code": row["icao_code"],
        "name": row["name"],
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "altitude_feet": row["altitude_feet"],
        "country": row["country"],
        "municipality": row["municipality"],
        "type": row["type"],
    }


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(conn: sqlite3.Connection, r: redis_lib.Redis, ttl: int) -> int:
    """Write all staged airport records to Redis. Returns count of records written."""
    cur = conn.cursor()
    cur.execute(
        "SELECT icao_code, name, latitude, longitude, altitude_feet, "
        "country, municipality, type FROM airports"
    )
    rows = cur.fetchall()
    logger.info("Writing %d airport records to Redis.", len(rows))

    count = 0
    pipe = r.pipeline()
    for row in rows:
        record = build_airport_record(row)
        key = airport_key(record["icao_code"])
        pipe.set(key, json.dumps(record), ex=ttl)
        count += 1
        if count % 10000 == 0:
            pipe.execute()
            pipe = r.pipeline()
            logger.info("  ... %d records written.", count)

    pipe.execute()
    logger.info("Finished writing %d airport records to Redis.", count)
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
        "ids": "SkyFollower_runner_ourairports",
        "name": "SkyFollower OurAirports Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "OurAirports Records Imported", "mdi:airport", "total_increasing", None),
        ("last_run_at", "OurAirports Last Run At", "mdi:clock", None, None),
        ("last_run_status", "OurAirports Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_ourairports_{name}",
            "object_id": f"SkyFollower_runner_ourairports_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_ourairports_{name}/config",
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
        # 1. Download
        csv_text = download_csv(DOWNLOAD_URL)

        # 2. Stage in SQLite
        conn = stage_data(csv_text, db_path)

        # 3. Write to Redis
        records_imported = write_to_redis(conn, r, ttl)
        conn.close()

        status = "success"
        logger.info(
            "OurAirports runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("OurAirports runner failed: %s", exc, exc_info=True)
        status = "failure"

    finally:
        try:
            publish_completion_stats(cfg, records_imported, status)
        except Exception as exc:
            logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
