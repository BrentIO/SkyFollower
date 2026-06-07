#!/usr/bin/env python3
"""
SkyFollower Bahamas CAA Data Runner

Downloads the Civil Aviation Authority of the Bahamas (CAA) aircraft register PDF,
parses aircraft data, and deep-merges enrichment records into Redis using the ICAO
hex resolved via RediSearch (Mode S address is not published in this register).

The Bahamas register contains only owner name, registration, combined make/model,
and serial number. No manufacturer, type designator, or powerplant data is available.
The download URL is date-stamped and changes with each update; the runner scrapes the
CAA registers page to discover the current link automatically.

Important: the Bahamas register does not publish ICAO hex (Mode S) addresses.
This runner can only enrich records that already exist in Redis from Mictronics.
Schedule it AFTER the Mictronics runner.

Data source: https://caabahamas.com/registers/
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
import pdfplumber
import redis as redis_lib
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from shared.redis_keys import AIRCRAFT_SEARCH_INDEX, icao_hex_key

logger = logging.getLogger("bs-caa")

INDEX_URL = "https://caabahamas.com/registers/"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/bs-caa"
BATCH_SIZE = 100


# ---------------------------------------------------------------------------
# URL discovery
# ---------------------------------------------------------------------------

def _find_download_url() -> str:
    """Scrape the Bahamas CAA registers page to find the current PDF URL."""
    logger.info("Fetching Bahamas CAA registers page to discover current PDF URL.")
    resp = requests.get(INDEX_URL, timeout=30, headers={"User-Agent": "P5Software SkyFollower"})
    if resp.status_code != 200:
        raise RuntimeError(f"Bahamas CAA index page returned HTTP {resp.status_code}")

    # Full absolute URL for the aircraft register PDF
    matches = re.findall(
        r'https?://[^"\'>\s]*Updated-THE-BAHAMAS-CIVIL-AIRCRAFT-REGISTER[^"\'>\s]*\.pdf',
        resp.text,
    )
    if matches:
        return matches[0]

    # Relative or partial href fallback
    matches = re.findall(
        r'href=["\']([^"\']*Updated-THE-BAHAMAS-CIVIL-AIRCRAFT-REGISTER[^"\']*\.pdf)["\']',
        resp.text,
    )
    if matches:
        url = matches[0]
        if not url.startswith("http"):
            url = "https://caabahamas.com" + url
        return url

    raise RuntimeError("Could not find aircraft register PDF URL on Bahamas CAA registers page.")


# ---------------------------------------------------------------------------
# PDF download
# ---------------------------------------------------------------------------

def download_registry() -> str:
    """Download the Bahamas CAA PDF to a temp file. Returns the temp file path."""
    url = _find_download_url()
    logger.info("Downloading Bahamas CAA aircraft register from %s", url)
    resp = requests.get(url, timeout=120, headers={"User-Agent": "P5Software SkyFollower"})
    if resp.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {resp.status_code}")
    logger.info("Download complete (%d bytes).", len(resp.content))

    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    tmp.write(resp.content)
    tmp.close()
    return tmp.name


# ---------------------------------------------------------------------------
# PDF parsing
# ---------------------------------------------------------------------------

def parse_pdf(file_path: str) -> list[dict]:
    """Parse the Bahamas CAA register PDF. Returns data rows as dicts keyed by column name."""
    headers: Optional[list[str]] = None
    rows = []

    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue

            for raw_row in table:
                cleaned = [str(c).strip() if c else "" for c in raw_row]

                if headers is None:
                    if any(cleaned):
                        headers = cleaned
                    continue

                # Excel-generated PDFs repeat the header row on each page
                if cleaned == headers:
                    continue

                if not any(cleaned):
                    continue

                rows.append(dict(zip(headers, cleaned)))

    if headers is None:
        raise RuntimeError("No table data found in PDF.")

    logger.info("Parsed %d data rows from PDF.", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(icao_hex: str, registration: str, row: dict) -> dict:
    """Build enrichment record from a PDF row."""
    owner = row.get("REGISTERED OWNER OF AIRCRAFT", "").strip() or None
    make_model = row.get("AIRCRAFT TYPE - MAKE/MODEL", "").strip() or None
    serial = row.get("SERIAL #", "").strip() or None

    aircraft_fields: dict = {}
    if make_model:
        aircraft_fields["model"] = make_model
    if serial:
        aircraft_fields["serial_number"] = serial

    registrant_fields: dict = {}
    if owner:
        registrant_fields["names"] = [owner]

    record: dict = {"icao_hex": icao_hex, "registration": registration}
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
    if registrant_fields:
        record["registrant"] = registrant_fields
    return record


# ---------------------------------------------------------------------------
# Deep merge
# ---------------------------------------------------------------------------

def _deep_merge(base: dict, update: dict) -> dict:
    """Merge update into base. update values win; nested dicts merged recursively."""
    result = dict(base)
    for k, v in update.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


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
# Registration → icao_hex lookup
# ---------------------------------------------------------------------------

def _build_registration_map(registrations: list[str], r: redis_lib.Redis) -> dict[str, str]:
    """Batch-query Redis search index for icao_hex by registration mark.

    Returns {registration → icao_hex} for registrations already in Redis.
    Registrations not found (aircraft not yet in Mictronics) are omitted.
    """
    reg_map: dict[str, str] = {}
    total_batches = (len(registrations) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num, i in enumerate(range(0, len(registrations), BATCH_SIZE)):
        batch = registrations[i:i + BATCH_SIZE]
        escaped = [_escape_tag(reg) for reg in batch]
        query_str = f"@registration:{{{'|'.join(escaped)}}}"

        try:
            results = r.ft(AIRCRAFT_SEARCH_INDEX).search(
                Query(query_str)
                .return_fields("registration")
                .paging(0, BATCH_SIZE)
            )
            for doc in results.docs:
                hex_val = doc.id.replace("icao_hex:", "")
                registration = getattr(doc, "registration", None)
                if registration:
                    reg_map[registration] = hex_val
        except Exception as exc:
            logger.warning("RediSearch batch %d/%d failed: %s", batch_num + 1, total_batches, exc)

    return reg_map


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Enrich existing Redis records with Bahamas CAA data. Returns count of records written."""
    reg_row_map: dict[str, dict] = {}
    for row in rows:
        reg = row.get("AIRCRAFT REGISTRATION NUMBER", "").strip()
        if not reg:
            continue
        reg_row_map[reg] = row

    registrations = list(reg_row_map.keys())
    logger.info("Looking up %d registrations in Redis search index.", len(registrations))

    reg_icao_map = _build_registration_map(registrations, r)
    logger.info(
        "Found %d / %d registrations in Redis (remainder not yet in Mictronics).",
        len(reg_icao_map),
        len(registrations),
    )

    count = 0
    batch: list[tuple[str, dict]] = []

    def _flush() -> None:
        keys = [k for k, _ in batch]
        existing_list = r.json().mget(keys, "$")
        pipe = r.pipeline()
        for (key, new_record), existing_raw in zip(batch, existing_list):
            merged = _deep_merge(existing_raw[0], new_record) if existing_raw else new_record
            pipe.json().set(key, "$", merged)
            pipe.expire(key, ttl)
        pipe.execute()

    for registration, hex_val in reg_icao_map.items():
        row = reg_row_map[registration]
        record = _build_record(hex_val, registration, row)
        batch.append((icao_hex_key(hex_val), record))
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
        "ids": "SkyFollower_runner_bs_caa",
        "name": "SkyFollower Bahamas CAA Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Bahamas CAA Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Bahamas CAA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Bahamas CAA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_bs_caa_{name}",
            "object_id": f"SkyFollower_runner_bs_caa_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_bs_caa_{name}/config",
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

    status = "failure"
    records_imported = 0
    tmp_path: Optional[str] = None

    try:
        tmp_path = download_registry()
        rows = parse_pdf(tmp_path)
        _ensure_search_index(r)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info("Bahamas CAA runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("Bahamas CAA runner failed: %s", exc, exc_info=True)

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
