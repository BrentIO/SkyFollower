#!/usr/bin/env python3
"""
SkyFollower VRS Standing Data Runner (Virtual Radar Server Standing Data)

Downloads the VRS Standing Data Management (SDM) repository's route,
code-block, and country CSVs, stages them in local SQLite, writes each to
Redis, publishes MQTT completion stats, then exits.

Data source: https://github.com/vradarserver/standing-data
  - routes/schema-01/**/*.csv       -> route:{ident} (plain string)
  - code-blocks/schema-01/*.csv     -> lookup:icao-code-blocks (RedisJSON array)
  - countries/schema-01/*.csv       -> lookup:icao-countries (RedisJSON object)

Scope was routes-only until #1848: aircraft, airline, and airport data from
this same repository are still redundant with Mictronics/country-registry
runners, Mictronics' operators.json, and the ourairports runner respectively,
so those remain unimported. code-blocks/countries were a distinct oversight
from that original scoping, not a deliberate exclusion -- they're the
ICAO 24-bit (Mode S) address allocation table used to resolve an aircraft's
country of registration for the ~140+ countries no runner scrapes a CAA for
(see shared/lua/merge_aircraft.lua, which is where the actual per-hex
resolution happens; this runner only stages the lookup tables it reads).

route:{ident} stores the source's AirportCodes column unmodified (e.g.
"KMIA-KJFK-KMIA" for a same-day out-and-back using one callsign) -- no
splitting, no filtering by leg count.

code-blocks.csv's Bitmask/SignificantBitmask columns are hex strings
converted to integers at import time; its Start/Finish/Count/IsMilitary
columns are not carried into Redis at all -- unused by the bitmask-match
lookup (Start always equals Bitmask in the source data; Finish/Count are
derivable from the mask; IsMilitary is a documented non-goal, see #1848).
Its CountryISO2 "ZZ" rows -- two entries that together cover the entire
24-bit address space at the lowest possible SignificantBitmask -- are the
source's synthetic "unknown/unassigned" catch-all, not a real country;
importing them would mean merge_aircraft.lua's linear scan always finds a
match, hiding the genuine no-match case behind a fake country, so they are
dropped here rather than in the Lua script.
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

from shared.config import ConfigError, load_config
from shared.timing import ROUTE_TTL_SECONDS, ENRICHMENT_TTL_SECONDS
from shared.redis_client import build_redis_client
from shared.ha_discovery import build_ha_device
from shared.redis_keys import normalize_flight_ident, route_key, icao_code_blocks_key, icao_countries_key
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client
from shared.mqtt_register import publish_register
from shared.logging_setup import configure_logging
from shared.sqlite_staging import open_staging_db

logger = logging.getLogger("vrs-standing-data")

DOWNLOAD_URL = "https://codeload.github.com/vradarserver/standing-data/tar.gz/refs/heads/main"

# The upstream repo's "Standing data changes" commit lands daily around
# 03:49-03:51 UTC (verified against 30 days of commit history when this
# runner was built), unlike the weekly cadence of the registration sources
# this runner used to also cover. Route keys therefore use ROUTE_TTL_SECONDS
# (shared/timing.py) -- deliberately shorter than the ENRICHMENT_TTL_SECONDS
# every other (weekly) runner uses -- so route data can't silently go stale
# for over a week if a run or two is missed.

MQTT_ROOT = "SkyFollower/runner/vrs-standing-data"

_ROUTES_PATH_PREFIX = "routes/schema-01/"
_CODE_BLOCKS_PATH_PREFIX = "code-blocks/schema-01/"
_COUNTRIES_PATH_PREFIX = "countries/schema-01/"

# The source's synthetic "unknown/unassigned country" catch-all -- see the
# module docstring for why this is excluded rather than imported.
_UNASSIGNED_COUNTRY_ISO2 = "ZZ"


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_tarball(url: str) -> bytes:
    """Download the standing-data repository tarball and return its raw bytes."""
    logger.info("Downloading standing-data repository from %s", url)
    response = requests.get(url, timeout=300)
    if response.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {response.status_code}")
    logger.info("Download complete (%d bytes).", len(response.content))
    return response.content


def extract_files(tarball: bytes, path_prefix: str, suffix: str = ".csv") -> dict[str, bytes]:
    """Return {path: bytes} for every file in `tarball` under `path_prefix`
    ending in `suffix`, with the GitHub tarball's single top-level
    "standing-data-{ref}/" directory stripped from each path. Called once per
    prefix (routes/code-blocks/countries) against the same downloaded bytes,
    so a single download serves all three imports."""
    files: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(tarball), mode="r:gz") as tf:
        for member in tf.getmembers():
            if not member.isfile():
                continue
            # GitHub tarballs wrap everything in a single top-level
            # "standing-data-{ref}/" directory -- strip it before matching.
            parts = member.name.split("/", 1)
            if len(parts) != 2:
                continue
            relative = parts[1]
            if not relative.startswith(path_prefix) or not relative.endswith(suffix):
                continue
            extracted = tf.extractfile(member)
            if extracted is None:
                continue
            files[relative] = extracted.read()
    return files


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _csv_rows(data: bytes):
    """Yield rows from a CSV (bytes, comma-delimited, UTF-8 with BOM), skipping the header."""
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
CREATE TABLE code_blocks (
    bitmask INTEGER NOT NULL,
    significant_bitmask INTEGER NOT NULL,
    country_code TEXT NOT NULL
);
CREATE TABLE countries (
    iso2 TEXT PRIMARY KEY,
    name TEXT NOT NULL
);
"""


def stage_routes(conn: sqlite3.Connection, files: dict[str, bytes]) -> int:
    """Parse every routes/schema-01/**/*.csv file, stage ident -> route rows. Returns row count staged."""
    cur = conn.cursor()
    count = 0
    for data in files.values():
        # Columns: Callsign, Code, Number, AirlineCode, AirportCodes
        for row in _csv_rows(data):
            if len(row) < 5:
                continue
            ident = normalize_flight_ident(row[0].strip().upper())
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
    return count


def stage_code_blocks(conn: sqlite3.Connection, files: dict[str, bytes]) -> int:
    """Parse code-blocks/schema-01/code-blocks.csv, stage
    (bitmask, significant_bitmask, country_code) rows -- Bitmask/
    SignificantBitmask converted from hex strings to integers, Start/Finish/
    Count/IsMilitary dropped (unused by the bitmask-match lookup), and the
    CountryISO2 "ZZ" unknown/unassigned catch-all rows excluded entirely
    (see module docstring). Returns row count staged."""
    cur = conn.cursor()
    count = 0
    for data in files.values():
        # Columns: Start, Finish, Count, Bitmask, SignificantBitmask, IsMilitary, CountryISO2
        for row in _csv_rows(data):
            if len(row) < 7:
                continue
            country_code = row[6].strip().upper()
            if not country_code or country_code == _UNASSIGNED_COUNTRY_ISO2:
                continue
            try:
                bitmask = int(row[3].strip(), 16)
                significant_bitmask = int(row[4].strip(), 16)
            except ValueError:
                continue
            cur.execute(
                "INSERT INTO code_blocks (bitmask, significant_bitmask, country_code) VALUES (?, ?, ?)",
                (bitmask, significant_bitmask, country_code),
            )
            count += 1
    conn.commit()
    logger.info("Staged %d code-block rows from %d files.", count, len(files))
    return count


def stage_countries(conn: sqlite3.Connection, files: dict[str, bytes]) -> int:
    """Parse countries/schema-01/countries.csv, stage iso2 -> name rows. Returns row count staged."""
    cur = conn.cursor()
    count = 0
    for data in files.values():
        # Columns: ISO, Name
        for row in _csv_rows(data):
            if len(row) < 2:
                continue
            iso2 = row[0].strip().upper()
            name = row[1].strip()
            if not iso2 or not name:
                continue
            cur.execute(
                "INSERT OR REPLACE INTO countries (iso2, name) VALUES (?, ?)",
                (iso2, name),
            )
            count += 1
    conn.commit()
    logger.info("Staged %d country rows from %d files.", count, len(files))
    return count


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_routes_to_redis(conn: sqlite3.Connection, r: redis_lib.Redis, ttl: int) -> int:
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


def build_code_blocks_array(conn: sqlite3.Connection) -> list[dict]:
    """Return every staged code_blocks row as a plain dict, sorted descending
    by significant_bitmask -- the order shared/lua/merge_aircraft.lua's
    linear scan depends on to take the first (most specific) match without
    re-sorting per call."""
    cur = conn.cursor()
    cur.execute(
        "SELECT bitmask, significant_bitmask, country_code FROM code_blocks "
        "ORDER BY significant_bitmask DESC"
    )
    return [
        {
            "bitmask": row["bitmask"],
            "significant_bitmask": row["significant_bitmask"],
            "country_code": row["country_code"],
        }
        for row in cur.fetchall()
    ]


def write_code_blocks_to_redis(conn: sqlite3.Connection, r: redis_lib.Redis, ttl: int) -> int:
    """Write the staged code-blocks table to Redis as a single RedisJSON
    array under icao_code_blocks_key(). Returns row count written."""
    blocks = build_code_blocks_array(conn)
    key = icao_code_blocks_key()
    set_json(r, key, blocks)
    r.expire(key, ttl)
    logger.info("Wrote %d code-block rows to %s.", len(blocks), key)
    return len(blocks)


def build_countries_object(conn: sqlite3.Connection) -> dict[str, str]:
    """Return every staged countries row as a plain {iso2: name} dict."""
    cur = conn.cursor()
    cur.execute("SELECT iso2, name FROM countries")
    return {row["iso2"]: row["name"] for row in cur.fetchall()}


def write_countries_to_redis(conn: sqlite3.Connection, r: redis_lib.Redis, ttl: int) -> int:
    """Write the staged countries table to Redis as a single RedisJSON
    object under icao_countries_key(). Returns row count written."""
    countries = build_countries_object(conn)
    key = icao_countries_key()
    set_json(r, key, countries)
    r.expire(key, ttl)
    logger.info("Wrote %d country rows to %s.", len(countries), key)
    return len(countries)


# ---------------------------------------------------------------------------
# MQTT
# ---------------------------------------------------------------------------

def publish_completion_stats(cfg: dict, records_imported: int, status: str) -> None:
    """Publish completion statistics to MQTT."""
    mc = cfg.get("mqtt")
    if not mc or not mc.get("host"):
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
        client.publish(f"{base}/last_run_status", status.capitalize(), retain=True)

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
    device = build_ha_device(
        identifier="SkyFollower_runner_vrs_standing_data",
        name="SkyFollower Virtual Radar Server Standing Data Runner",
        model="Virtual Radar Server Standing Data Runner",
        configuration_url="https://brentio.github.io/SkyFollower/runners/vrs-standing-data.html",
    )
    publish_register(client, device)
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
        if name == "last_run_at":
            payload["device_class"] = "timestamp"
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_vrs_standing_data_{name}/config",
            json.dumps(payload),
            retain=True,
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        cfg = load_config("redis", "mqtt")
    except ConfigError as exc:
        configure_logging()
        logger.critical("%s", exc)
        sys.exit(1)

    configure_logging(cfg.get("log_level"))

    rc = cfg["redis"]
    r = build_redis_client(rc)

    db_path = "/app/data/staging.db"

    status = "failure"
    records_imported = 0

    try:
        tarball = download_tarball(DOWNLOAD_URL)
        route_files = extract_files(tarball, _ROUTES_PATH_PREFIX)
        code_block_files = extract_files(tarball, _CODE_BLOCKS_PATH_PREFIX)
        country_files = extract_files(tarball, _COUNTRIES_PATH_PREFIX)
        logger.info(
            "Extracted %d route, %d code-block, and %d country CSV file(s).",
            len(route_files), len(code_block_files), len(country_files),
        )

        conn = open_staging_db(db_path, _SCHEMA)
        stage_routes(conn, route_files)
        stage_code_blocks(conn, code_block_files)
        stage_countries(conn, country_files)

        routes_written = write_routes_to_redis(conn, r, ROUTE_TTL_SECONDS)
        code_blocks_written = write_code_blocks_to_redis(conn, r, ENRICHMENT_TTL_SECONDS)
        countries_written = write_countries_to_redis(conn, r, ENRICHMENT_TTL_SECONDS)
        conn.close()

        # Reported as one combined total -- the existing "Records Imported"
        # HA sensor/MQTT stat predates code-blocks/countries and is a plain
        # totalizer, not scoped to routes specifically.
        records_imported = routes_written + code_blocks_written + countries_written
        status = "success"
        logger.info(
            "VRS standing-data runner completed successfully. "
            "Records imported: %d (routes=%d, code_blocks=%d, countries=%d)",
            records_imported, routes_written, code_blocks_written, countries_written,
        )

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
