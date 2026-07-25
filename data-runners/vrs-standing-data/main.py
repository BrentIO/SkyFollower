#!/usr/bin/env python3
"""
SkyFollower VRS Standing Data Runner (Virtual Radar Server Standing Data)

Downloads the VRS Standing Data Management (SDM) repository's route CSVs,
stages ident -> route rows in local SQLite, writes each as a plain Redis
string to route:{ident}, publishes MQTT completion stats, then exits.

Data source: https://github.com/vradarserver/standing-data (routes/schema-01/**/*.csv)

Scope is routes only -- aircraft, airline, and airport data from this same
repository are redundant with Mictronics/country-registry runners,
Mictronics' operators.json, and the ourairports runner respectively, so this
runner does not import them.

route:{ident} stores the source's AirportCodes column unmodified (e.g.
"KMIA-KJFK-KMIA" for a same-day out-and-back using one callsign) -- no
splitting, no filtering by leg count.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import sqlite3
import sys
import tarfile
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.redis_keys import route_key
from shared.mqtt import build_mqtt_client
from shared.logging_setup import configure_logging

logger = logging.getLogger("vrs-standing-data")

DOWNLOAD_URL = "https://codeload.github.com/vradarserver/standing-data/tar.gz/refs/heads/main"

# The upstream repo's "Standing data changes" commit lands daily around
# 03:49-03:51 UTC (verified against 30 days of commit history when this
# runner was built), unlike the weekly cadence of the registration sources
# this runner used to also cover. A 3-day TTL -- not the 14-day default
# every other (weekly) runner uses via redis_ttl_days in settings.json --
# keeps route data from silently going stale for over a week if a run or
# two is missed, without needing a runner-specific settings.json block for
# a single value.
REDIS_TTL = 3 * 86400  # 3 days in seconds

MQTT_ROOT = "SkyFollower/runner/vrs-standing-data"

_ROUTES_PATH_PREFIX = "routes/schema-01/"


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_and_extract_routes(url: str) -> dict[str, bytes]:
    """Download the standing-data repo tarball and return route CSVs as {path: bytes}."""
    logger.info("Downloading standing-data repository from %s", url)
    response = requests.get(url, timeout=300)
    if response.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {response.status_code}")
    logger.info("Download complete (%d bytes); extracting route CSVs.", len(response.content))

    files: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:gz") as tf:
        for member in tf.getmembers():
            if not member.isfile():
                continue
            # GitHub tarballs wrap everything in a single top-level
            # "standing-data-{ref}/" directory -- strip it before matching.
            parts = member.name.split("/", 1)
            if len(parts) != 2:
                continue
            relative = parts[1]
            if not relative.startswith(_ROUTES_PATH_PREFIX) or not relative.endswith(".csv"):
                continue
            extracted = tf.extractfile(member)
            if extracted is None:
                continue
            files[relative] = extracted.read()
    logger.info("Extracted %d route CSV files.", len(files))
    return files


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _csv_rows(data: bytes):
    """Yield rows from a route CSV (bytes, comma-delimited, UTF-8 with BOM), skipping the header."""
    reader = csv.reader(io.StringIO(data.decode("utf-8-sig", errors="replace")))
    next(reader, None)
    yield from reader


# ---------------------------------------------------------------------------
# SQLite staging
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE routes (
    ident TEXT PRIMARY KEY,
    route TEXT NOT NULL
);
"""


def stage_data(files: dict[str, bytes], db_path: str) -> sqlite3.Connection:
    """Parse every routes/schema-01/**/*.csv file, stage ident -> route rows."""
    logger.info("Opening staging database at %s", db_path)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)

    cur = conn.cursor()
    count = 0
    for data in files.values():
        # Columns: Callsign, Code, Number, AirlineCode, AirportCodes
        for row in _csv_rows(data):
            if len(row) < 5:
                continue
            ident = row[0].strip().upper()
            route = row[4].strip()
            if not ident or not route:
                continue
            cur.execute(
                "INSERT OR REPLACE INTO routes (ident, route) VALUES (?, ?)",
                (ident, route),
            )
            count += 1
    conn.commit()
    logger.info("Staged %d routes from %d files.", count, len(files))
    return conn


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(conn: sqlite3.Connection, r: redis_lib.Redis, ttl: int) -> int:
    """Write all staged ident -> route strings to Redis. Returns count of records written."""
    cur = conn.cursor()
    cur.execute("SELECT ident, route FROM routes")
    rows = cur.fetchall()
    logger.info("Writing %d route records to Redis.", len(rows))

    count = 0
    batch: list[tuple[str, str]] = []

    def _flush():
        pipe = r.pipeline()
        for key, value in batch:
            pipe.set(key, value, ex=ttl)
        pipe.execute()

    for row in rows:
        key = route_key(row["ident"])
        batch.append((key, row["route"]))
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
        "ids": "SkyFollower_runner_vrs_standing_data",
        "name": "SkyFollower Virtual Radar Server Standing Data Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Virtual Radar Server Standing Data Records Imported", "mdi:routes", "total_increasing", None),
        ("last_run_at", "Virtual Radar Server Standing Data Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Virtual Radar Server Standing Data Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_vrs_standing_data_{name}",
            "object_id": f"SkyFollower_runner_vrs_standing_data_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_vrs_standing_data_{name}/config",
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

    db_path = "/app/data/staging.db"

    status = "failure"
    records_imported = 0

    try:
        files = download_and_extract_routes(DOWNLOAD_URL)
        conn = stage_data(files, db_path)
        records_imported = write_to_redis(conn, r, REDIS_TTL)
        conn.close()
        status = "success"
        logger.info("VRS standing-data runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("VRS standing-data runner failed: %s", exc, exc_info=True)

    finally:
        try:
            publish_completion_stats(cfg, records_imported, status)
        except Exception as exc:
            logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
