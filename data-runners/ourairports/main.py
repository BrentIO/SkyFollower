#!/usr/bin/env python3
"""
SkyFollower OurAirports Data Runner

Downloads the OurAirports airports CSV, filters to 4-character ICAO codes,
computes a voice-friendly phonic name for each airport, stages records in
local SQLite, writes airport enrichment data to Redis with a 14-day TTL,
publishes MQTT completion stats, then exits.

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
    icao_code TEXT PRIMARY KEY,
    name      TEXT,
    city      TEXT,
    region    TEXT,
    country   TEXT,
    phonic    TEXT
);
"""

# ---------------------------------------------------------------------------
# Phonic name computation
# ---------------------------------------------------------------------------

# Optional JSON file, co-located with this script, that maps ICAO codes to
# exact phonic strings. If a code is present, its value is returned verbatim
# by compute_phonic() — no "International"/"Airport" stripping or any other
# processing is applied. Format:
#
#   {
#       "KXXX": "Spoken name exactly as desired",
#       ...
#   }
#
# The file is loaded once at startup; adding entries requires a container
# restart. Missing file is silently ignored (empty overrides).
_OVERRIDES_PATH = os.path.join(os.path.dirname(__file__), "phonics_overrides.json")


def _load_phonics_overrides() -> dict:
    try:
        with open(_OVERRIDES_PATH) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


_PHONIC_OVERRIDES: dict = _load_phonics_overrides()


def _remove_international_airport(text: str) -> str:
    """Strip 'International' and 'Airport' and collapse extra spaces."""
    text = text.replace("International", "").replace("Airport", "")
    while "  " in text:
        text = text.replace("  ", " ")
    return text.strip()


def compute_phonic(icao_code: str, name: str, city: str) -> str:
    """
    Return a voice-friendly spoken name for an airport.

    If the ICAO code has an entry in phonics_overrides.json, that value is
    returned verbatim with no further processing.

    Otherwise the general algorithm applies, and "International" / "Airport"
    are stripped from the result as a final step on every path.
    """
    if not name:
        return ""

    # JSON override — returned exactly as written, no further processing.
    if icao_code in _PHONIC_OVERRIDES:
        return _PHONIC_OVERRIDES[icao_code]

    city = (city or "").strip()

    # "Greater …" airports and names containing " of " use the name as-is.
    if name.lower().startswith("greater") or " of " in name:
        phonic = name
    else:
        phonic = name
        if city and city.replace("/", " ").replace("-", " ") not in name:
            phonic = f"{city} {name}"

        # If phonic ends with city, move city to the front.
        if city and phonic.endswith(city):
            inner = name.replace(city, "").strip()
            phonic = f"{city} {inner}"

        # Strip trailing "/" or "-" artifacts (city-after-slash cases).
        if phonic.endswith("/") or phonic.endswith("-"):
            phonic = name

        # Normalise separators.
        phonic = phonic.replace("/", " ").replace("-", " ")
        while "  " in phonic:
            phonic = phonic.replace("  ", " ")

    # Always strip "International" and "Airport" as the final step.
    return _remove_international_airport(phonic)


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

        icao = ident.upper()
        name = row.get("name", "").strip() or None
        city = row.get("municipality", "").strip() or None
        region = row.get("iso_region", "").strip() or None
        country = row.get("iso_country", "").strip() or None
        phonic = compute_phonic(icao, name or "", city or "") or None

        cur.execute(
            "INSERT OR REPLACE INTO airports "
            "(icao_code, name, city, region, country, phonic) "
            "VALUES (?,?,?,?,?,?)",
            (icao, name, city, region, country, phonic),
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
        "city": row["city"],
        "region": row["region"],
        "country": row["country"],
        "phonic": row["phonic"],
    }


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(conn: sqlite3.Connection, r: redis_lib.Redis, ttl: int) -> int:
    """Write all staged airport records to Redis. Returns count of records written."""
    cur = conn.cursor()
    cur.execute(
        "SELECT icao_code, name, city, region, country, phonic FROM airports"
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
    """Publish completion statistics as a single JSON payload to MQTT."""
    mc = cfg.get("mqtt")
    if not mc:
        logger.info("No MQTT config; skipping stats publish.")
        return

    run_at = datetime.now(timezone.utc).isoformat()
    stats_payload = {
        "records_imported": records_imported,
        "last_run_at": run_at,
        "last_run_status": status,
    }
    stats_topic = f"{MQTT_ROOT}/statistics"

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

        client.publish(stats_topic, json.dumps(stats_payload))
        _publish_ha_autodiscovery(client, stats_topic)

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


def _publish_ha_autodiscovery(client: mqtt.Client, stats_topic: str) -> None:
    device = {
        "ids": "SkyFollower_runner_ourairports",
        "name": "SkyFollower OurAirports Runner",
        "manufacturer": "P5Software, LLC",
    }
    sensors = [
        ("records_imported", "OurAirports Records Imported", "mdi:airport",
         "total_increasing", None, "{{ value_json.records_imported }}"),
        ("last_run_at", "OurAirports Last Run At", "mdi:clock",
         None, None, "{{ value_json.last_run_at }}"),
        ("last_run_status", "OurAirports Last Run Status", "mdi:check-circle",
         None, None, "{{ value_json.last_run_status }}"),
    ]
    for name, friendly_name, icon, state_class, unit, tmpl in sensors:
        payload: dict = {
            "state_topic": stats_topic,
            "value_template": tmpl,
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
