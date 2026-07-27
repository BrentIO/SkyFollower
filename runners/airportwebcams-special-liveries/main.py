#!/usr/bin/env python3
"""
SkyFollower Airport Webcams Special Liveries Data Runner

Fetches the Airport Webcams Special Liveries table from a single static HTML
page (a TablePress-rendered table — the whole ~2,074-row table is present in
the initial response, no pagination/AJAX to crawl), looks up each
registration in the Redis simple search index to find the ICAO hex (provided
by Mictronics), then writes special_livery enrichment to
aircraft:livery:{icao_hex} and publishes MQTT completion stats, then exits.

Important: this source does not publish ICAO hex (Mode S) addresses.
This runner can only enrich records that already exist in Redis from
Mictronics. Schedule it AFTER the Mictronics runner.

special_livery holds the derived livery name, not the raw Description cell
verbatim — it is spoken by Home Assistant TTS on a matched-flight
notification, so it needs to read as a clean phrase. Presence of the field
is itself the flag (no separate boolean) — it's absent entirely for aircraft
with no special livery. The source Description cell sometimes packs more
than one livery name into a single cell separated by "/", and/or carries a
"(sticker...)" annotation and/or a "(#New at DD-Mon-YY)" site-freshness
marker. All parenthetical annotations containing "sticker" or "#new"
(case-insensitive) are stripped FIRST, before splitting on "/" — stripping
must happen before the split because at least one real annotation itself
contains a "/" (e.g. "(sticker; underside/belly)"), which would otherwise be
misread as an extra compound-description segment. The last "/"-separated
segment of what remains is then treated as the current/primary livery name.

Rows whose Registration is the literal string "Various" (a special livery
applied across an entire fleet, not a single tail) are skipped — they can't
be resolved to one aircraft.

Data source: https://airportwebcams.net/special-liveries/
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
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.query import Query

from shared.redis_keys import (
    AIRCRAFT_MICTRONICS_SEARCH_INDEX,
    aircraft_livery_key,
)
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client
from shared.logging_setup import configure_logging

logger = logging.getLogger("airportwebcams-special-liveries")

SOURCE_URL = "https://airportwebcams.net/special-liveries/"
TABLE_ID = "tablepress-8"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/airportwebcams-special-liveries"
BATCH_SIZE = 100

_ANNOTATION_RE = re.compile(r"\s*\([^()]*(?:sticker|#new)[^()]*\)", re.IGNORECASE)
_WHITESPACE_RE = re.compile(r"\s+")


# ---------------------------------------------------------------------------
# special_livery transform
# ---------------------------------------------------------------------------

def _derive_special_livery(description: str) -> str:
    """Derive a clean, TTS-ready livery name from a raw Description cell.

    Strips any parenthetical annotation containing "sticker" or "#new"
    (case-insensitive) first, then splits the remainder on "/" and takes the
    last segment — the last-listed livery is treated as the current/primary
    one for the ~7% of rows with a compound description. Order matters: at
    least one real annotation contains its own "/"
    (e.g. "(sticker; underside/belly)"), so splitting before stripping would
    misread part of an annotation as an extra livery segment.
    """
    stripped = _ANNOTATION_RE.sub("", description)
    segment = stripped.split("/")[-1]
    return _WHITESPACE_RE.sub(" ", segment).strip()


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def download_and_parse(session: requests.Session) -> list[dict]:
    """Fetch the special-liveries page and parse the TablePress table."""
    logger.info("Downloading Airport Webcams special liveries table from %s", SOURCE_URL)
    resp = session.get(SOURCE_URL, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {resp.status_code}")

    soup = BeautifulSoup(resp.text, "lxml")

    table = soup.find("table", id=TABLE_ID)
    if table is None:
        # Fall back to a header-text search in case the TablePress table ID
        # ever changes, mirroring the other HTML-scraping runners.
        for candidate in soup.find_all("table"):
            header_row = candidate.find("tr")
            if header_row and "Registration" in header_row.get_text() and "Description" in header_row.get_text():
                table = candidate
                break

    if table is None:
        raise RuntimeError("Could not find special liveries table on Airport Webcams page.")

    body = table.find("tbody") or table
    records: list[dict] = []
    for row in body.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        values = [cell.get_text(strip=True) for cell in cells]
        records.append({
            "country": values[0],
            "airline": values[1],
            "aircraft_type": values[2],
            "registration": values[3],
            "description": values[4],
        })

    logger.info("Parsed %d rows from special liveries table.", len(records))
    return records


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(icao_hex: str, registration: str, special_livery: str) -> dict:
    """Build the enrichment record for a single matched special livery."""
    return {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "airportwebcams-special-liveries",
        "special_livery": special_livery,
    }


# ---------------------------------------------------------------------------
# RediSearch tag escaping
# ---------------------------------------------------------------------------

def _escape_tag(value: str) -> str:
    """Escape special characters for use in a RediSearch TagField query."""
    special = ',.<>{}[]"\':;!@#$%^&*()-+=~'
    result = []
    for char in value:
        if char in special:
            result.append('\\')
        result.append(char)
    return ''.join(result)


# ---------------------------------------------------------------------------
# Registration → icao_hex lookup
# ---------------------------------------------------------------------------

def _build_registration_map(registrations: list[str], r: redis_lib.Redis) -> dict[str, str]:
    """Batch-query Redis simple search index for icao_hex by registration mark."""
    reg_map: dict[str, str] = {}
    if not registrations:
        return reg_map

    total_batches = (len(registrations) + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_num, i in enumerate(range(0, len(registrations), BATCH_SIZE)):
        batch = registrations[i:i + BATCH_SIZE]
        escaped = [_escape_tag(reg) for reg in batch]
        query_str = f"@registration:{{{'|'.join(escaped)}}}"

        try:
            results = r.ft(AIRCRAFT_MICTRONICS_SEARCH_INDEX).search(
                Query(query_str)
                .return_fields("registration")
                .paging(0, BATCH_SIZE)
            )
            for doc in results.docs:
                icao_hex = doc.id.replace("aircraft:mictronics:", "")
                registration = getattr(doc, "registration", None)
                if registration:
                    reg_map[registration] = icao_hex
        except Exception as exc:
            logger.warning("RediSearch batch %d/%d failed: %s", batch_num + 1, total_batches, exc)

    return reg_map


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Write Airport Webcams special livery data to aircraft:livery keys. Returns count written."""
    reg_row_map: dict[str, dict] = {}
    skipped_various = 0

    for row in rows:
        reg = (row.get("registration") or "").strip()
        if not reg:
            continue
        if reg.lower() == "various":
            skipped_various += 1
            continue
        reg_row_map[reg] = row

    if skipped_various:
        logger.info("Skipped %d row(s) with Registration='Various' (whole-fleet special liveries).", skipped_various)

    registrations = list(reg_row_map.keys())
    logger.info("Looking up %d registrations in Redis search index.", len(registrations))

    reg_icao_map = _build_registration_map(registrations, r)
    logger.info(
        "Found %d / %d registrations in Redis (remainder not yet in Mictronics).",
        len(reg_icao_map),
        len(registrations),
    )

    count = 0
    skipped_empty_name = 0
    errors = 0
    pipe = r.pipeline()
    pipe_count = 0

    for registration, icao_hex in reg_icao_map.items():
        row = reg_row_map[registration]
        special_livery = _derive_special_livery(row.get("description") or "")
        if not special_livery:
            logger.debug("Empty special_livery after transform for %s, skipping", registration)
            skipped_empty_name += 1
            continue

        record = _build_record(icao_hex, registration, special_livery)
        key = aircraft_livery_key(icao_hex)
        set_json(pipe, key, record)
        pipe.expire(key, ttl)
        count += 1
        pipe_count += 1

        if pipe_count >= 1000:
            try:
                pipe.execute()
            except Exception as exc:
                logger.warning("Redis pipeline failed: %s", exc)
                errors += pipe_count
            pipe = r.pipeline()
            pipe_count = 0

    if pipe_count:
        try:
            pipe.execute()
        except Exception as exc:
            logger.warning("Redis pipeline failed: %s", exc)
            errors += pipe_count

    logger.info(
        "Finished: %d written, %d skipped (empty special_livery), %d errors.",
        count, skipped_empty_name, errors,
    )
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
        "ids": "SkyFollower_runner_airportwebcams_special_liveries",
        "name": "SkyFollower Airport Webcams Special Liveries Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Airport Webcams Special Liveries Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Airport Webcams Special Liveries Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Airport Webcams Special Liveries Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_airportwebcams_special_liveries_{name}",
            "object_id": f"SkyFollower_runner_airportwebcams_special_liveries_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_airportwebcams_special_liveries_{name}/config",
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

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; P5Software SkyFollower)"})

    status = "failure"
    records_imported = 0

    try:
        rows = download_and_parse(session)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info(
            "Airport Webcams Special Liveries runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Airport Webcams Special Liveries runner failed: %s", exc, exc_info=True)

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
