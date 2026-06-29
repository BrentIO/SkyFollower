#!/usr/bin/env python3
"""
SkyFollower Luxembourg DAC Data Runner

Downloads the Luxembourg Directorate of Civil Aviation (DAC) aircraft register
PDF, parses it using pdfplumber word-position extraction (pdfplumber.extract_table()
does not correctly detect all columns in this PDF), looks up each LX- registration
in the Redis simple search index to find the ICAO hex (provided by Mictronics),
then writes enrichment data to aircraft:detail:{icao_hex} and publishes MQTT
completion stats, then exits.

Important: the Luxembourg DAC register does not publish ICAO hex (Mode S)
addresses. This runner can only enrich records that already exist in Redis from
Mictronics. Schedule it AFTER the Mictronics runner.

Data source: https://dac.gouvernement.lu/en/administration/departements/navigabilite/
             immatriculation-aeronefs/releve-immatriculations.html
"""

from __future__ import annotations

import io
import json
import logging
import os
import re
import sys
import time
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

from shared.redis_keys import (
    AIRCRAFT_DETAIL_SEARCH_INDEX,
    AIRCRAFT_SIMPLE_SEARCH_INDEX,
    aircraft_detail_key,
)

logger = logging.getLogger("lu-dac")

INDEX_URL = (
    "https://dac.gouvernement.lu/en/administration/departements/navigabilite"
    "/immatriculation-aeronefs/releve-immatriculations.html"
)
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/lu-dac"
BATCH_SIZE = 100

# x0 column boundaries — determined from PDF structure
# Columns: immat | constructeur | type | sn | proprietaire | exploitant
_COL_BOUNDS = [44, 98, 256, 421, 489, 639, 842]
_COL_NAMES = ["immat", "constructeur", "type", "sn", "proprietaire", "exploitant"]

# Multi-word strings that represent privacy placeholders — omit from names list
_PRIVATE_PLACEHOLDERS = {"EXPLOITANT PRIVÉ", "PROPRIÉTAIRE PRIVÉ", "COPROPRIÉTÉ"}

# Tolerance (points) for grouping words into the same row
_ROW_TOLERANCE = 5


# ---------------------------------------------------------------------------
# URL discovery
# ---------------------------------------------------------------------------

def _find_download_url(session: requests.Session) -> str:
    """Scrape the Luxembourg DAC index page to discover the current PDF URL."""
    logger.info("Fetching Luxembourg DAC index page to discover current PDF URL.")
    resp = session.get(INDEX_URL, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Luxembourg DAC index page returned HTTP {resp.status_code}")

    # Matches both URL-encoded (navigabilit%C3%A9) and plain (navigabilité) forms
    pattern = re.compile(
        r'href=["\']([^"\']*dam-assets[^"\']*relev[^"\']*\.pdf)["\']',
        re.IGNORECASE,
    )
    matches = pattern.findall(resp.text)
    if not matches:
        raise RuntimeError("Could not find PDF URL on Luxembourg DAC index page.")

    url = matches[0]
    if not url.startswith("http"):
        url = "https://dac.gouvernement.lu" + url
    logger.info("Discovered PDF URL: %s", url)
    return url


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_register(session: requests.Session) -> bytes:
    """Download the Luxembourg DAC PDF and return raw bytes."""
    url = _find_download_url(session)
    logger.info("Downloading Luxembourg DAC aircraft register PDF.")
    resp = session.get(url, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"PDF download failed with HTTP {resp.status_code}")
    logger.info("Download complete (%d bytes).", len(resp.content))
    return resp.content


# ---------------------------------------------------------------------------
# Word-position PDF parsing
# ---------------------------------------------------------------------------

def _assign_column(x0: float) -> Optional[str]:
    """Map an x0 coordinate to a column name using _COL_BOUNDS."""
    for i in range(len(_COL_BOUNDS) - 1):
        if _COL_BOUNDS[i] <= x0 < _COL_BOUNDS[i + 1]:
            return _COL_NAMES[i]
    return None


def _cluster_rows(words: list[dict]) -> list[dict[str, list[str]]]:
    """
    Group pdfplumber words into rows by `top` coordinate (5-point tolerance),
    then assign each word to a named column by x0.

    Returns rows as {col_name: [word, ...], ...} dicts.
    """
    if not words:
        return []

    # Sort by top then x0
    sorted_words = sorted(words, key=lambda w: (w["top"], w["x0"]))

    rows: list[dict[str, list[str]]] = []
    current_row: dict[str, list[str]] = {}
    current_top: Optional[float] = None

    for word in sorted_words:
        top = word["top"]
        text = word.get("text", "").strip()
        col = _assign_column(word["x0"])

        if col is None or not text:
            continue

        if current_top is None or abs(top - current_top) > _ROW_TOLERANCE:
            if current_row:
                rows.append(current_row)
            current_row = {}
            current_top = top

        current_row.setdefault(col, []).append(text)

    if current_row:
        rows.append(current_row)

    return rows


def _join_col(row: dict[str, list[str]], col: str) -> str:
    return " ".join(row.get(col, [])).strip()


def parse_pdf(pdf_bytes: bytes) -> list[dict]:
    """
    Parse the Luxembourg DAC PDF using word-position extraction.

    Returns a list of record dicts with keys:
      registration, manufacturer, model, serial_number, exploitant, proprietaire
    """
    all_rows: list[dict[str, list[str]]] = []

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            words = page.extract_words()
            all_rows.extend(_cluster_rows(words))

    # Merge multi-line cells: rows starting with LX- begin a new record;
    # rows without an LX- immat are continuation rows for the previous record.
    records: list[dict] = []
    current: Optional[dict] = None

    for row in all_rows:
        immat = _join_col(row, "immat")

        if immat.startswith("LX-"):
            if current is not None:
                records.append(current)
            current = {
                "registration": immat,
                "manufacturer": _join_col(row, "constructeur"),
                "model": _join_col(row, "type"),
                "serial_number": _join_col(row, "sn"),
                "proprietaire": _join_col(row, "proprietaire"),
                "exploitant": _join_col(row, "exploitant"),
            }
        elif current is not None:
            # Continuation: append to each populated column
            for col_name, field_key in [
                ("constructeur", "manufacturer"),
                ("type", "model"),
                ("sn", "serial_number"),
                ("proprietaire", "proprietaire"),
                ("exploitant", "exploitant"),
            ]:
                extra = _join_col(row, col_name)
                if extra:
                    current[field_key] = (current[field_key] + " " + extra).strip()

    if current is not None:
        records.append(current)

    logger.info("Parsed %d aircraft records from PDF.", len(records))
    return records


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_names(exploitant: str, proprietaire: str) -> list[str]:
    names: list[str] = []
    if exploitant and exploitant not in _PRIVATE_PLACEHOLDERS:
        names.append(exploitant)
    if proprietaire and proprietaire not in _PRIVATE_PLACEHOLDERS and proprietaire != exploitant:
        names.append(proprietaire)
    return names


def _build_record(parsed: dict, icao_hex: str) -> dict:
    """Build the detail enrichment record from a parsed PDF row."""
    aircraft_fields: dict = {}

    mfr = parsed.get("manufacturer", "").strip()
    if mfr:
        aircraft_fields["manufacturer"] = mfr

    model = parsed.get("model", "").strip()
    if model:
        aircraft_fields["model"] = model

    serial = parsed.get("serial_number", "").strip()
    if serial:
        aircraft_fields["serial_number"] = serial

    names = _build_names(
        parsed.get("exploitant", "").strip(),
        parsed.get("proprietaire", "").strip(),
    )

    record: dict = {
        "icao_hex": icao_hex,
        "registration": parsed["registration"],
        "source": "lu-dac",
    }
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
    if names:
        record["registrant"] = {"names": names}

    return record


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
    """Create the aircraft:detail JSON search index if it does not already exist."""
    try:
        r.ft(AIRCRAFT_DETAIL_SEARCH_INDEX).info()
    except Exception:
        r.ft(AIRCRAFT_DETAIL_SEARCH_INDEX).create_index(
            fields=[
                TagField("$.icao_hex", as_name="icao_hex"),
                TagField("$.registration", as_name="registration"),
            ],
            definition=IndexDefinition(prefix=["aircraft:detail:"], index_type=IndexType.JSON),
        )
        logger.info("Created search index %r.", AIRCRAFT_DETAIL_SEARCH_INDEX)


# ---------------------------------------------------------------------------
# Registration → icao_hex lookup
# ---------------------------------------------------------------------------

def _build_registration_map(registrations: list[str], r: redis_lib.Redis) -> dict[str, str]:
    """Batch-query Redis simple search index for icao_hex by registration mark."""
    reg_map: dict[str, str] = {}
    total_batches = (len(registrations) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num, i in enumerate(range(0, len(registrations), BATCH_SIZE)):
        batch = registrations[i:i + BATCH_SIZE]
        escaped = [_escape_tag(reg) for reg in batch]
        query_str = f"@registration:{{{'|'.join(escaped)}}}"

        try:
            results = r.ft(AIRCRAFT_SIMPLE_SEARCH_INDEX).search(
                Query(query_str)
                .return_fields("registration")
                .paging(0, BATCH_SIZE)
            )
            for doc in results.docs:
                icao_hex = doc.id.replace("aircraft:simple:", "")
                registration = getattr(doc, "registration", None)
                if registration:
                    reg_map[registration] = icao_hex
        except Exception as exc:
            logger.warning("RediSearch batch %d/%d failed: %s", batch_num + 1, total_batches, exc)

    return reg_map


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(records: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Write Luxembourg DAC data to aircraft:detail keys in Redis. Returns count written."""
    reg_record_map: dict[str, dict] = {}
    for rec in records:
        reg = rec.get("registration", "").strip()
        if not reg:
            continue
        reg_record_map[reg] = rec

    registrations = list(reg_record_map.keys())
    logger.info("Looking up %d registrations in Redis search index.", len(registrations))

    reg_icao_map = _build_registration_map(registrations, r)
    logger.info(
        "Found %d / %d registrations in Redis (remainder not yet in Mictronics).",
        len(reg_icao_map),
        len(registrations),
    )

    count = 0
    errors = 0
    pipe = r.pipeline()
    pipe_count = 0

    for registration, icao_hex in reg_icao_map.items():
        parsed = reg_record_map[registration]
        record = _build_record(parsed, icao_hex)
        key = aircraft_detail_key(icao_hex)
        pipe.json().set(key, "$", record)
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
        "Finished: %d written, %d errors.",
        count, errors,
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
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
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
        "ids": "SkyFollower_runner_lu_dac",
        "name": "SkyFollower Luxembourg DAC Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Luxembourg DAC Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Luxembourg DAC Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Luxembourg DAC Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_lu_dac_{name}",
            "object_id": f"SkyFollower_runner_lu_dac_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_lu_dac_{name}/config",
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

    session = requests.Session()
    session.headers.update({"User-Agent": "P5Software SkyFollower"})

    status = "failure"
    records_imported = 0

    try:
        pdf_bytes = download_register(session)
        records = parse_pdf(pdf_bytes)
        _ensure_search_index(r)
        records_imported = write_to_redis(records, r, ttl)
        status = "success"
        logger.info(
            "Luxembourg DAC runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Luxembourg DAC runner failed: %s", exc, exc_info=True)

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
