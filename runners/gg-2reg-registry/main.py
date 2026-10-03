#!/usr/bin/env python3
"""
SkyFollower Guernsey (2-reg) Data Runner

Discovers the current aircraft register PDF from the 2-reg index page, parses
main-register pages (skipping special sections at the end), resolves each
2-prefix registration to an ICAO hex via the Redis Mictronics search index,
and writes enrichment to aircraft:registry:{icao_hex}.

PDF column layout: each main-register page repeats a header row (Registration,
Aircraft Manufacturer, Type, MSN, Registered Owner/Charterer, Date of
registration). Column boundaries are derived per page from the x-position of
each header column's first word, so a change to the page size or column widths
does not require a code change. A page with no recognisable header row is
skipped with a warning. Columns, left to right:
  Registration          (2-prefix; lookup key)
  Aircraft Manufacturer (stored as aircraft.manufacturer)
  Type                  (stored as aircraft.model)
  MSN                   (stored as aircraft.serial_number)
  Registered Owner      (stored as registrant.names[0])
  Date of Registration  (not stored)

A run that parses no rows, or parses rows but matches none of them to the
Mictronics index, is treated as a failure.

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

from shared.config import ConfigError, load_config
from shared.timing import ENRICHMENT_TTL_SECONDS
from shared.redis_client import build_redis_client
from shared.ha_discovery import build_ha_device
from shared.redis_keys import (
    AIRCRAFT_REGISTRY_SEARCH_INDEX,
    AIRCRAFT_MICTRONICS_SEARCH_INDEX,
    aircraft_registry_key,
)
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client
from shared.mqtt_register import publish_register
from shared.logging_setup import configure_logging
from shared.country_flags import country_flag

logger = logging.getLogger("gg-2reg-registry")

_INDEX_URL = "https://www.2-reg.com/legislation/register/"
_PDF_HREF_RE = re.compile(r"/wp-content/uploads/.+/Register_.+\.pdf", re.IGNORECASE)

MQTT_ROOT = "SkyFollower/runner/gg-2reg-registry"
BATCH_SIZE = 100

_WHITESPACE_RE = re.compile(r"\s+")

# First word of each header column, left to right (see module docstring)
_HEADER_MARKERS = ("Registration", "Aircraft", "Type", "MSN", "Registered", "Date")

# Distance a column boundary sits left of its header word's x0
_COL_MARGIN = 2.0

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


def _find_column_thresholds(words: list[dict]) -> tuple[float, ...] | None:
    """Derive the five column boundaries from a page's header row.

    Returns None when no line contains every header marker in left-to-right order.
    """
    lines: dict[int, list[dict]] = {}
    for w in words:
        lines.setdefault(round(w["top"]), []).append(w)

    for top_key in sorted(lines):
        line = sorted(lines[top_key], key=lambda w: w["x0"])
        starts: list[float] = []
        pos = 0
        for marker in _HEADER_MARKERS:
            while pos < len(line) and line[pos]["text"] != marker:
                pos += 1
            if pos == len(line):
                break
            starts.append(line[pos]["x0"])
            pos += 1
        if len(starts) == len(_HEADER_MARKERS):
            return tuple(x - _COL_MARGIN for x in starts[1:])
    return None


def _col_index(x0: float, thresholds: tuple[float, ...]) -> int:
    """Return 0-based column index for a word at horizontal position x0."""
    for i, threshold in enumerate(thresholds):
        if x0 < threshold:
            return i
    return len(thresholds)


def _words_to_cols(words: list[dict], thresholds: tuple[float, ...]) -> list[str]:
    """Assemble pdfplumber word dicts into a list of 6 column strings."""
    cols: list[list[str]] = [[] for _ in range(len(thresholds) + 1)]
    for w in sorted(words, key=lambda w: w["x0"]):
        cols[_col_index(w["x0"], thresholds)].append(w["text"])
    return [" ".join(c) for c in cols]


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

            words = page.extract_words()
            thresholds = _find_column_thresholds(words)
            if thresholds is None:
                logger.warning("Page %d: no column header row found; skipping page.", page_num)
                continue

            # Group words by rounded y-position (same line)
            line_words: dict[int, list[dict]] = {}
            for w in words:
                key = round(w["top"])
                line_words.setdefault(key, []).append(w)

            for top_key in sorted(line_words.keys()):
                cols = _words_to_cols(line_words[top_key], thresholds)
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
    if not records:
        raise RuntimeError("No 2-prefix records parsed from the register PDF; its layout may have changed.")
    return records


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
        "source": "gg-2reg-registry",
        "country_code": "GG",
        "military": False,
    }
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
    if registrant_fields:
        record["registrant"] = registrant_fields

    return record


def _escape_tag(value: str) -> str:
    """Escape special characters for use in a RediSearch TagField query."""
    special = ',.<>{}[]"\':;!@#$%^&*()-+=~'
    result = []
    for char in value:
        if char in special:
            result.append("\\")
        result.append(char)
    return "".join(result)


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

    if registrations and not reg_icao_map:
        raise RuntimeError(
            f"None of {len(registrations)} parsed registrations matched the Mictronics index; "
            "the PDF layout may have changed or the Mictronics index is empty."
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
        identifier="SkyFollower_runner_gg_2reg_registry",
        name=f"SkyFollower Guernsey {country_flag('GG')} 2-reg Registry Runner",
        model=f"Guernsey {country_flag('GG')} 2-reg Registry Runner",
        configuration_url="https://brentio.github.io/SkyFollower/runners/gg-2reg-registry.html",
    )
    publish_register(client, device)
    stats = [
        ("records_imported", "Guernsey 2-reg Registry Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Guernsey 2-reg Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Guernsey 2-reg Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_gg_2reg_registry_{name}",
            "object_id": f"SkyFollower_runner_gg_2reg_registry_{name}",
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
            f"homeassistant/sensor/SkyFollower_runner_gg_2reg_registry_{name}/config",
            json.dumps(payload),
            retain=True,
        )


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

    ttl = ENRICHMENT_TTL_SECONDS

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
