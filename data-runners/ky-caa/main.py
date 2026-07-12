#!/usr/bin/env python3
"""
SkyFollower Cayman Islands CAA Data Runner

Downloads the Cayman Islands Civil Aviation Authority (CAA) aircraft register
PDF, parses aircraft data, and writes enrichment records to Redis using
the ICAO hex resolved via RediSearch against the Mictronics data.

The register contains owner name, registration, combined make/model ("Series
Type"), and serial number. No manufacturer column, type designator, or
powerplant data is available. The PDF URL is static and does not change with
updates.

Important: the Cayman Islands register does not publish ICAO hex (Mode S)
addresses. This runner can only enrich records that already exist in Redis from
Mictronics. Schedule it AFTER the Mictronics runner.

Data source: https://www.caacayman.com/wp-content/uploads/Active-Aircraft-Register.pdf
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import tempfile
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
    aircraft_registry_key,
    aircraft_mictronics_key,
    AIRCRAFT_MICTRONICS_SEARCH_INDEX,
    AIRCRAFT_REGISTRY_SEARCH_INDEX,
)
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client
from shared.logging_setup import configure_logging

logger = logging.getLogger("ky-caa")

PDF_URL = "https://www.caacayman.com/wp-content/uploads/Active-Aircraft-Register.pdf"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/ky-caa"
BATCH_SIZE = 100

# Normalised column names (after stripping embedded newlines)
COL_REGISTRATION = "Aircraft Registration"
COL_OWNER = "Registered Owner"
COL_ADDRESS = "Registered Address"
COL_NATIONALITY = "Nationality"
COL_SERIES_TYPE = "Series Type"
COL_SERIAL = "Serial Number"

# Nationality column values → ISO 3166-1 alpha-2.  Keys are lowercased for
# case-insensitive matching ("Cayman islands" typo appears in the source PDF).
_NATIONALITY_TO_ISO: dict[str, str] = {
    "bermuda": "BM",
    "british virgin islands": "VG",
    "cayman islands": "KY",
    "germany": "DE",
    "ireland": "IE",
    "irish": "IE",
    "isle of man": "IM",
    "lithuania": "LT",
    "malaysia": "MY",
    "malta": "MT",
    "seychelles": "SC",
    "singapore": "SG",
    "united kingdom": "GB",
}


def _nationality_to_iso(raw: str) -> Optional[str]:
    """Map a Nationality column value to ISO 3166-1 alpha-2, or None if unmapped."""
    return _NATIONALITY_TO_ISO.get(raw.lower().strip())


# ---------------------------------------------------------------------------
# Type sanity check helpers
# ---------------------------------------------------------------------------

_TYPE_TOKEN_RE = re.compile(r'[A-Z]{1,4}\d{2,4}')


def _type_tokens(model_str: str) -> set:
    """Extract type designator tokens from a model string (e.g. 'AW139' from 'Leonardo AW139')."""
    return {t.split('-')[0] for t in _TYPE_TOKEN_RE.findall(model_str.upper())}


def _type_check_passes(simple_record: dict, detail_model_str: str) -> bool:
    """Return True if the ky-caa model string is consistent with the Mictronics record.

    Returns True when detail_model_str is empty, when neither record contains
    recognisable type tokens, or when the token sets overlap.
    """
    if not detail_model_str:
        return True
    simple_tokens = _type_tokens(
        (simple_record.get("type_designator") or "") + " " +
        (simple_record.get("manufacturer_model") or "")
    )
    if not simple_tokens:
        return True
    detail_tokens = _type_tokens(detail_model_str)
    if not detail_tokens:
        return True
    return bool(simple_tokens & detail_tokens)


# ---------------------------------------------------------------------------
# PDF download
# ---------------------------------------------------------------------------

def download_registry() -> str:
    """Download the Cayman Islands CAA PDF to a temp file. Returns the temp file path."""
    logger.info("Downloading Cayman Islands CAA aircraft register from %s", PDF_URL)
    resp = requests.get(PDF_URL, timeout=120, headers={"User-Agent": "P5Software SkyFollower"})
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

def _normalise_header(raw: str) -> str:
    """Replace embedded newlines with a space and strip whitespace."""
    return " ".join(raw.split())


def _normalise_cell(raw: Optional[str]) -> str:
    """Replace embedded newlines with a space and strip whitespace."""
    if raw is None:
        return ""
    return " ".join(raw.split())


def parse_pdf(file_path: str) -> list[dict]:
    """Parse the Cayman Islands CAA register PDF.

    Returns data rows as dicts keyed by normalised column name. Column names
    are normalised by collapsing embedded newlines to a single space.
    """
    headers: Optional[list[str]] = None
    normalised_headers: Optional[list[str]] = None
    rows = []

    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue

            for raw_row in table:
                cleaned = [_normalise_cell(c) for c in raw_row]

                if headers is None:
                    if any(cleaned):
                        headers = [str(c) for c in raw_row]
                        normalised_headers = [_normalise_header(h) for h in headers]
                    continue

                # The PDF repeats the header row on each page
                normalised_cleaned = [_normalise_cell(c) for c in raw_row]
                if normalised_cleaned == normalised_headers:
                    continue

                if not any(cleaned):
                    continue

                rows.append(dict(zip(normalised_headers, cleaned)))

    if headers is None:
        raise RuntimeError("No table data found in PDF.")

    logger.info("Parsed %d data rows from PDF.", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(icao_hex: str, registration: str, row: dict) -> dict:
    """Build enrichment record from a PDF row."""
    owner = row.get(COL_OWNER, "").strip() or None
    address = row.get(COL_ADDRESS, "").strip() or None
    nationality_raw = row.get(COL_NATIONALITY, "").strip() or None
    series_type = row.get(COL_SERIES_TYPE, "").strip() or None
    serial = row.get(COL_SERIAL, "").strip() or None

    # Strip the country name from the end of the address — the Nationality column
    # always matches the country suffix of Registered Address.
    if address and nationality_raw and address.lower().endswith(nationality_raw.lower()):
        address = address[:-len(nationality_raw)].strip()

    country_iso = _nationality_to_iso(nationality_raw) if nationality_raw else None

    aircraft_fields: dict = {}
    if series_type:
        aircraft_fields["model"] = series_type
    if serial:
        aircraft_fields["serial_number"] = serial

    street_parts = [p.strip() for p in address.split(",")] if address else []
    street_parts = [p for p in street_parts if p]

    registrant_fields: dict = {}
    if owner:
        registrant_fields["names"] = [owner]
    if street_parts:
        registrant_fields["street"] = street_parts
    if country_iso:
        registrant_fields["country"] = country_iso

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "ky-caa",
        "military": False,
    }
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
    if registrant_fields:
        record["registrant"] = registrant_fields
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
# Registration → icao_hex lookup
# ---------------------------------------------------------------------------

_MICTRONICS_KEY_PREFIX = "aircraft:mictronics:"


def _build_registration_map(registrations: list[str], r: redis_lib.Redis) -> dict[str, str]:
    """Batch-query the Mictronics search index for icao_hex by registration mark.

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
            results = r.ft(AIRCRAFT_MICTRONICS_SEARCH_INDEX).search(
                Query(query_str)
                .return_fields("registration")
                .paging(0, BATCH_SIZE)
            )
            for doc in results.docs:
                hex_val = doc.id[len(_MICTRONICS_KEY_PREFIX):]
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
    """Write Cayman Islands CAA data to aircraft detail keys. Returns count written.

    Looks up each registration in the Mictronics search index, runs a
    type sanity check against the simple record, then writes to
    aircraft:registry:{icao_hex} without reading the existing detail record
    (fire-and-forget).
    """
    reg_row_map: dict[str, dict] = {}
    for row in rows:
        reg = row.get(COL_REGISTRATION, "").strip()
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
    pending: list[tuple[str, str]] = []  # (registration, hex_val)

    def _flush_pending() -> None:
        nonlocal count
        simple_keys = [aircraft_mictronics_key(hex_val) for _, hex_val in pending]
        simple_records = r.json().mget(simple_keys, "$")

        pipe = r.pipeline()
        written = 0
        for (registration, hex_val), simple_raw_list in zip(pending, simple_records):
            simple_raw = simple_raw_list[0] if simple_raw_list else None
            row = reg_row_map[registration]
            series_type = row.get(COL_SERIES_TYPE, "").strip() or None
            if simple_raw is None or not _type_check_passes(simple_raw, series_type):
                logger.debug("Type sanity check failed for %s, skipping", registration)
                continue
            record = _build_record(hex_val, registration, row)
            key = aircraft_registry_key(hex_val)
            set_json(pipe, key, record)
            pipe.expire(key, ttl)
            written += 1

        if written:
            pipe.execute()
        count += written

    for registration, hex_val in reg_icao_map.items():
        pending.append((registration, hex_val))
        if len(pending) == 10000:
            _flush_pending()
            pending.clear()
            logger.info("  ... %d records written.", count)

    if pending:
        _flush_pending()

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
        "ids": "SkyFollower_runner_ky_caa",
        "name": "SkyFollower Cayman Islands CAA Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Cayman Islands CAA Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Cayman Islands CAA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Cayman Islands CAA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_ky_caa_{name}",
            "object_id": f"SkyFollower_runner_ky_caa_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_ky_caa_{name}/config",
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

    status = "failure"
    records_imported = 0
    tmp_path: Optional[str] = None

    try:
        tmp_path = download_registry()
        rows = parse_pdf(tmp_path)
        _ensure_search_index(r)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info("Cayman Islands CAA runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("Cayman Islands CAA runner failed: %s", exc, exc_info=True)

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
