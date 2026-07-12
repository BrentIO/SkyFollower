#!/usr/bin/env python3
"""
SkyFollower Guernsey (2-reg) Data Runner

Discovers the current aircraft register PDF from the 2-reg index page, parses
main-register pages (skipping the special sections at the end), looks up each
2-prefix registration in the Redis simple search index to find the ICAO hex
(provided by Mictronics), writes enrichment data to aircraft:registry:{icao_hex},
publishes MQTT completion stats, then exits.

PDF column layout (all pages; determined from word x-positions):
  x <  126 → Registration          (2-prefix; lookup key)
  x <  387 → Aircraft Manufacturer (stored as aircraft.manufacturer)
  x <  567 → Type                  (stored as aircraft.model)
  x <  648 → MSN                   (stored as aircraft.serial_number)
  x < 1015 → Registered Owner      (stored as registrant.names[0])
  x ≥ 1015 → Date of Registration  (not stored)

Special sections (pages with these first-line prefixes are skipped entirely):
  ALL NEW REGISTRATIONS IN …
  DEREGISTRATIONS IN …
  REGISTRATION OWNERSHIP CHANGES IN …
  REGISTRATION CHANGES IN …
  ALL CURRENT RESERVED OR UNAVAILABLE REGISTRATION MARKS

Data source: https://www.2-reg.com/legislation/register/
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

import paho.mqtt.client as mqtt
import pdfplumber
import redis as redis_lib
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from shared.redis_keys import (
    AIRCRAFT_REGISTRY_SEARCH_INDEX,
    AIRCRAFT_MICTRONICS_SEARCH_INDEX,
    aircraft_registry_key,
)
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client
from shared.logging_setup import configure_logging

logger = logging.getLogger("gg-2reg")

_INDEX_URL = "https://www.2-reg.com/legislation/register/"
_PDF_HREF_RE = re.compile(r"/wp-content/uploads/.+/Register_.+\.pdf", re.IGNORECASE)

REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/gg-2reg"
BATCH_SIZE = 100

_WHITESPACE_RE = re.compile(r"\s+")

# x-position thresholds separating the six PDF columns (see module docstring)
_COL_THRESHOLDS = (126, 387, 566, 648, 1015)

# First-line text that identifies special sections to skip
_SKIP_PREFIXES = (
    "ALL NEW REGISTRATIONS IN ",
    "DEREGISTRATIONS IN ",
    "REGISTRATION OWNERSHIP CHANGES IN ",
    "REGISTRATION CHANGES IN ",
    "ALL CURRENT RESERVED OR UNAVAILABLE REGISTRATION MARKS",
)

# Owner values that are privacy placeholders, not real names (matched case-insensitively)
_PRIVATE_PLACEHOLDERS = {"(PRIVATE)", "PRIVATE"}


# ---------------------------------------------------------------------------
# PDF word → column assignment
# ---------------------------------------------------------------------------

def _col_index(x0: float) -> int:
    """Return 0-based column index for a word at horizontal position x0."""
    for i, threshold in enumerate(_COL_THRESHOLDS):
        if x0 < threshold:
            return i
    return len(_COL_THRESHOLDS)


def _words_to_cols(words: list[dict]) -> list[str]:
    """Assemble pdfplumber word dicts into a list of 6 column strings."""
    cols: list[list[str]] = [[] for _ in range(len(_COL_THRESHOLDS) + 1)]
    for w in sorted(words, key=lambda w: w["x0"]):
        cols[_col_index(w["x0"])].append(w["text"])
    return [" ".join(c) for c in cols]


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def _find_pdf_url(session: requests.Session) -> str:
    """Scrape the 2-reg index page and return the URL of the current register PDF."""
    logger.info("Fetching 2-reg index page from %s", _INDEX_URL)
    resp = session.get(_INDEX_URL, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Index page request failed with HTTP {resp.status_code}")
    soup = BeautifulSoup(resp.text, "lxml")
    for a in soup.find_all("a", href=True):
        if _PDF_HREF_RE.search(a["href"]):
            href = a["href"]
            return href if href.startswith("http") else f"https://www.2-reg.com{href}"
    raise RuntimeError("No register PDF link found on 2-reg index page.")


def download_and_parse(session: requests.Session) -> list[dict]:
    """Discover, download, and parse the Guernsey aircraft register PDF."""
    pdf_url = _find_pdf_url(session)
    logger.info("Downloading Guernsey aircraft register from %s", pdf_url)
    resp = session.get(pdf_url, timeout=180)
    if not resp.ok:
        raise RuntimeError(f"PDF download failed with HTTP {resp.status_code}")

    records: list[dict] = []

    with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            page_text = page.extract_text() or ""
            first_line = page_text.strip().splitlines()[0] if page_text.strip() else ""

            if any(first_line.upper().startswith(p.upper()) for p in _SKIP_PREFIXES):
                logger.debug("Page %d: skipping special section (%s)", page_num, first_line[:50])
                continue

            # Group words by rounded y-position (same line)
            line_words: dict[int, list[dict]] = {}
            for w in page.extract_words():
                key = round(w["top"])
                line_words.setdefault(key, []).append(w)

            for top_key in sorted(line_words.keys()):
                cols = _words_to_cols(line_words[top_key])
                reg = cols[0].strip()
                if not reg.startswith("2-"):
                    continue
                records.append({
                    "registration": reg,
                    "manufacturer": cols[1].strip(),
                    "model": cols[2].strip(),
                    "serial": cols[3].strip(),
                    "owner": cols[4].strip(),
                })

    logger.info("Parsed %d 2-prefix records from PDF.", len(records))
    return records


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict, icao_hex: str, registration: str) -> dict:
    """Build detail enrichment record from a parsed PDF row."""
    aircraft_fields: dict = {}
    registrant_fields: dict = {}

    manufacturer = _WHITESPACE_RE.sub(" ", row.get("manufacturer", "").strip())
    if manufacturer:
        aircraft_fields["manufacturer"] = manufacturer

    model = _WHITESPACE_RE.sub(" ", row.get("model", "").strip())
    if model:
        aircraft_fields["model"] = model

    serial = _WHITESPACE_RE.sub(" ", row.get("serial", "").strip())
    if serial:
        aircraft_fields["serial_number"] = serial

    owner = _WHITESPACE_RE.sub(" ", row.get("owner", "").strip())
    if owner and owner.upper() not in _PRIVATE_PLACEHOLDERS:
        registrant_fields["names"] = [owner]

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "gg-2reg",
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
            result.append("\\")
        result.append(char)
    return "".join(result)


# ---------------------------------------------------------------------------
# Search index
# ---------------------------------------------------------------------------

def _ensure_search_index(r: redis_lib.Redis) -> None:
    """Create the aircraft:detail JSON search index if it does not already exist."""
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

def _build_registration_map(registrations: list[str], r: redis_lib.Redis) -> dict[str, str]:
    """Batch-query Redis simple search index for icao_hex by registration mark."""
    reg_map: dict[str, str] = {}
    total_batches = (len(registrations) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num, i in enumerate(range(0, len(registrations), BATCH_SIZE)):
        batch = registrations[i : i + BATCH_SIZE]
        escaped = [_escape_tag(reg) for reg in batch]
        query_str = f"@registration:{{{'|'.join(escaped)}}}"

        try:
            results = r.ft(AIRCRAFT_MICTRONICS_SEARCH_INDEX).search(
                Query(query_str).return_fields("registration").paging(0, BATCH_SIZE)
            )
            for doc in results.docs:
                icao_hex = doc.id.replace("aircraft:mictronics:", "")
                registration = getattr(doc, "registration", None)
                if registration:
                    reg_map[registration.strip()] = icao_hex
        except Exception as exc:
            logger.warning("RediSearch batch %d/%d failed: %s", batch_num + 1, total_batches, exc)

    return reg_map


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Write Guernsey 2-reg data to aircraft:detail keys in Redis. Returns count written."""
    reg_row_map: dict[str, dict] = {}
    for row in rows:
        reg = row.get("registration", "").strip()
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
    errors = 0
    pipe = r.pipeline()
    pipe_count = 0

    for registration, icao_hex in reg_icao_map.items():
        row = reg_row_map.get(registration)
        if row is None:
            continue
        record = _build_record(row, icao_hex, registration)
        key = aircraft_registry_key(icao_hex)
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

    logger.info("Finished: %d written, %d errors.", count, errors)
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
        "ids": "SkyFollower_runner_gg_2reg",
        "name": "SkyFollower Guernsey 2-reg Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Guernsey 2-reg Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Guernsey 2-reg Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Guernsey 2-reg Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_gg_2reg_{name}",
            "object_id": f"SkyFollower_runner_gg_2reg_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_gg_2reg_{name}/config",
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
    session.headers.update({"User-Agent": "P5Software SkyFollower"})

    status = "failure"
    records_imported = 0

    try:
        rows = download_and_parse(session)
        _ensure_search_index(r)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info(
            "Guernsey 2-reg runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Guernsey 2-reg runner failed: %s", exc, exc_info=True)

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
