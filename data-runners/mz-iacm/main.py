#!/usr/bin/env python3
"""
SkyFollower Mozambique IACM Data Runner

Fetches the Mozambique IACM (Instituto de Aviação Civil de Moçambique) aircraft
register from a date-stamped scanned PDF discovered by scraping the index page,
runs OCR on each page to extract C9- registration records, looks up each
registration in the Redis simple search index to find the ICAO hex (provided by
Mictronics), writes enrichment data to aircraft:detail:{icao_hex}, publishes
MQTT completion stats, then exits.

The PDF is a scanned printout — pdfplumber cannot extract text. OCR is performed
using pdf2image (Poppler) + pytesseract (Tesseract). Words are grouped into rows
by Y-coordinate proximity; column fields are split by horizontal gap width.

Expected column order (left to right after registration mark):
  Manufacturer, Aircraft Type/Model, Serial Number, Owner

Data source index: https://www.iacm.gov.mz/direccao-de-seguranca-de-voo/
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
import redis as redis_lib
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from shared.redis_keys import (
    AIRCRAFT_DETAIL_SEARCH_INDEX,
    AIRCRAFT_SIMPLE_SEARCH_INDEX,
    aircraft_detail_key,
)

logger = logging.getLogger("mz-iacm")

_INDEX_URL = "https://www.iacm.gov.mz/direccao-de-seguranca-de-voo/"
_PDF_HREF_PATTERN = re.compile(r"DIRECCAO-DE-SEGURANCA", re.IGNORECASE)
_REG_RE = re.compile(r"^C9-[A-Z0-9]+$")

REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/mz-iacm"
BATCH_SIZE = 100

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# OCR tuning constants
_OCR_DPI = 200
_ROW_TOLERANCE = 15   # pixels; words within this Y-range are in the same row
_COL_GAP = 40         # pixels; gaps wider than this between adjacent words = column boundary
_MIN_CONF = 30        # minimum tesseract confidence to include a word

# Field indices within the groups to the right of the C9- registration cell
_FIELD_MANUFACTURER = 0
_FIELD_MODEL = 1
_FIELD_SERIAL = 2
_FIELD_OWNER = 3
# Status is the 10th column from the left; with registration in the 2nd column
# that places it at field_groups index 8.  Tune if column layout differs.
_FIELD_STATUS = 8

_WHITESPACE_RE = re.compile(r"\s+")


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def _discover_pdf_url(session: requests.Session) -> str:
    """Scrape the IACM index page and return the PDF download URL."""
    logger.info("Fetching Mozambique IACM register index from %s", _INDEX_URL)
    resp = session.get(_INDEX_URL, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Index page returned HTTP {resp.status_code}")

    soup = BeautifulSoup(resp.text, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if _PDF_HREF_PATTERN.search(href) and href.lower().endswith(".pdf"):
            if not href.startswith("http"):
                from urllib.parse import urljoin
                href = urljoin(_INDEX_URL, href)
            return href

    raise RuntimeError(
        "No DIRECCAO-DE-SEGURANCA PDF link found on IACM index page."
    )


def download_and_parse(session: requests.Session) -> list[dict]:
    """Discover, download, OCR, and parse the Mozambique IACM aircraft register."""
    pdf_url = _discover_pdf_url(session)
    logger.info("Downloading Mozambique IACM aircraft register from %s", pdf_url)
    resp = session.get(pdf_url, timeout=180)
    if not resp.ok:
        raise RuntimeError(f"PDF download failed with HTTP {resp.status_code}")

    records = _ocr_pdf(resp.content)
    logger.info("Parsed %d C9- rows from PDF.", len(records))
    return records


# ---------------------------------------------------------------------------
# OCR
# ---------------------------------------------------------------------------

def _ocr_pdf(pdf_bytes: bytes) -> list[dict]:
    """Convert PDF pages to images, OCR each, and extract C9- records."""
    import pdf2image
    import pytesseract

    images = pdf2image.convert_from_bytes(pdf_bytes, dpi=_OCR_DPI)
    records: list[dict] = []

    for page_num, img in enumerate(images, start=1):
        logger.info("OCR processing page %d of %d.", page_num, len(images))
        data = pytesseract.image_to_data(
            img,
            output_type=pytesseract.Output.DICT,
            lang="por+eng",
            config="--psm 6",
        )
        row_records = _extract_records_from_ocr_data(data)
        valid = [r for r in row_records if r.get("status", "").lower() == "válido"]
        logger.info(
            "Page %d: extracted %d C9- rows, %d válido.",
            page_num, len(row_records), len(valid),
        )
        records.extend(valid)

    return records


def _extract_records_from_ocr_data(data: dict) -> list[dict]:
    """Group OCR word data into rows and extract records for rows containing C9- registrations."""
    words = []
    for i, text in enumerate(data["text"]):
        text = text.strip()
        if not text:
            continue
        try:
            conf = int(data["conf"][i])
        except (ValueError, TypeError):
            conf = -1
        if conf < _MIN_CONF:
            continue
        left = data["left"][i]
        top = data["top"][i]
        width = data["width"][i]
        words.append({
            "text": text,
            "left": left,
            "top": top,
            "right": left + width,
        })

    rows = _group_into_rows(words)
    records = []

    for row_words in rows:
        record = _parse_row(row_words)
        if record:
            records.append(record)

    return records


def _group_into_rows(words: list[dict]) -> list[list[dict]]:
    """Group words into rows based on Y-coordinate proximity."""
    buckets: list[tuple[int, list[dict]]] = []

    for word in words:
        top = word["top"]
        placed = False
        for i, (bucket_top, bucket_words) in enumerate(buckets):
            if abs(bucket_top - top) <= _ROW_TOLERANCE:
                bucket_words.append(word)
                placed = True
                break
        if not placed:
            buckets.append((top, [word]))

    # Sort each row by X position; sort rows by Y position
    result = []
    for _, row_words in sorted(buckets, key=lambda b: b[0]):
        result.append(sorted(row_words, key=lambda w: w["left"]))

    return result


def _split_by_gap(words: list[dict]) -> list[list[dict]]:
    """Split a sorted word list into groups wherever horizontal gap >= _COL_GAP pixels."""
    if not words:
        return []
    groups: list[list[dict]] = [[words[0]]]
    for word in words[1:]:
        gap = word["left"] - groups[-1][-1]["right"]
        if gap >= _COL_GAP:
            groups.append([word])
        else:
            groups[-1].append(word)
    return groups


def _parse_row(row_words: list[dict]) -> dict | None:
    """Return a parsed record dict if the row contains a C9- registration, else None."""
    reg_word = None
    reg_idx = -1
    for idx, w in enumerate(row_words):
        candidate = _normalize_registration(w["text"])
        if _REG_RE.match(candidate):
            reg_word = w
            reg_idx = idx
            break

    if reg_word is None:
        return None

    registration = reg_word["text"]

    # Words to the right of the registration cell
    right_words = [w for w in row_words[reg_idx + 1:]]
    groups = _split_by_gap(right_words)
    field_values = [" ".join(w["text"] for w in g).strip() for g in groups]

    status = field_values[_FIELD_STATUS].strip() if _FIELD_STATUS < len(field_values) else ""
    logger.debug("Row reg=%s status=%r groups=%s", registration, status, field_values)

    return {
        "registration": registration,
        "field_groups": field_values,
        "status": status,
    }


def _normalize_registration(raw: str) -> str:
    """Normalize OCR artifacts in registration marks (e.g. spaces, O→0)."""
    # Remove spaces within registration
    s = raw.replace(" ", "").upper()
    # OCR sometimes reads O as 0 or vice versa in the suffix — leave as-is
    return s


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict, icao_hex: str, registration: str) -> dict:
    """Build detail enrichment record from a parsed OCR row."""
    aircraft_fields: dict = {}
    registrant_fields: dict = {}

    groups = row.get("field_groups", [])

    def _get_group(idx: int) -> str:
        return groups[idx].strip() if idx < len(groups) else ""

    manufacturer = _WHITESPACE_RE.sub(" ", _get_group(_FIELD_MANUFACTURER))
    if manufacturer:
        aircraft_fields["manufacturer"] = manufacturer

    model = _WHITESPACE_RE.sub(" ", _get_group(_FIELD_MODEL))
    if model:
        aircraft_fields["model"] = model

    serial = _get_group(_FIELD_SERIAL)
    if serial:
        aircraft_fields["serial_number"] = serial

    owner = _WHITESPACE_RE.sub(" ", _get_group(_FIELD_OWNER))
    if owner:
        registrant_fields["names"] = [owner]

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "mz-iacm",
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
                    reg_map[registration.strip()] = icao_hex
        except Exception as exc:
            logger.warning("RediSearch batch %d/%d failed: %s", batch_num + 1, total_batches, exc)

    return reg_map


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Write Mozambique IACM data to aircraft:detail keys in Redis. Returns count written."""
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
        "ids": "SkyFollower_runner_mz_iacm",
        "name": "SkyFollower Mozambique IACM Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Mozambique IACM Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Mozambique IACM Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Mozambique IACM Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_mz_iacm_{name}",
            "object_id": f"SkyFollower_runner_mz_iacm_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_mz_iacm_{name}/config",
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
    session.headers.update({
        "User-Agent": _BROWSER_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })

    status = "failure"
    records_imported = 0

    try:
        rows = download_and_parse(session)
        _ensure_search_index(r)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info(
            "Mozambique IACM runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Mozambique IACM runner failed: %s", exc, exc_info=True)

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
