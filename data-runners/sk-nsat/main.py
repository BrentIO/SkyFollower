#!/usr/bin/env python3
"""
SkyFollower Slovakia NSAT Data Runner

Fetches the Slovakia NSAT (Národný bezpečnostný úrad pre letectvo) aircraft
register PDF, parses all OM- registration rows using pdfplumber, looks up each
registration in the Redis simple search index to find the ICAO hex (provided by
Mictronics), writes enrichment data to aircraft:registry:{icao_hex}, and
publishes MQTT completion stats, then exits.

The PDF URL is date-stamped and discovered fresh each run by scraping the index
page — not hardcoded.

Important: the Slovakia register does not publish ICAO hex (Mode S) addresses.
This runner can only enrich records that already exist in Redis from Mictronics.
Schedule it AFTER the Mictronics runner.

Registration marks in the PDF use spaced format (e.g. "OM - 0101") and are
normalized to "OM-0101" before lookup.

Data source index: https://letectvo.nsat.sk/letova-sposobilost/register-lietadiel-slovenskej-republiky/zoznam-registra/
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

logger = logging.getLogger("sk-nsat")

INDEX_URL = "https://letectvo.nsat.sk/letova-sposobilost/register-lietadiel-slovenskej-republiky/zoznam-registra/"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/sk-nsat"
BATCH_SIZE = 100

# Column names in Slovak
_COL_REGISTRATION = "Poznávacia značka"
_COL_TYPE = "Typ lietadla"
_COL_SERIAL = "Výrobné číslo"
_COL_OWNER = "Vlastník"
_COL_OPERATOR = "Prevádzkovateľ"

_WHITESPACE_RE = re.compile(r"\s+")


# ---------------------------------------------------------------------------
# Registration normalization
# ---------------------------------------------------------------------------

def _normalize_registration(raw: str) -> str:
    """Strip all whitespace from a registration mark: 'OM - 0101' → 'OM-0101'."""
    return _WHITESPACE_RE.sub("", raw.strip())


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def _discover_pdf_url(session: requests.Session) -> str:
    """Scrape the NSAT index page and return the href of the first .pdf link."""
    logger.info("Fetching NSAT register index from %s", INDEX_URL)
    resp = session.get(INDEX_URL, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Index page returned HTTP {resp.status_code}")

    soup = BeautifulSoup(resp.text, "lxml")
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf"):
            return href

    raise RuntimeError("No PDF link found on NSAT index page.")


def download_and_parse(session: requests.Session) -> list[dict]:
    """Discover, download, and parse the Slovakia NSAT aircraft register PDF."""
    pdf_url = _discover_pdf_url(session)
    logger.info("Downloading Slovakia NSAT aircraft register from %s", pdf_url)
    resp = session.get(pdf_url, timeout=180)
    if resp.status_code != 200:
        raise RuntimeError(f"PDF download failed with HTTP {resp.status_code}")

    records: list[dict] = []
    headers: list[str] | None = None

    with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
        for page in pdf.pages:
            for table in page.extract_tables():
                for row in table:
                    if not row:
                        continue
                    cells = [(v or "").strip() for v in row]
                    # Registration is the second column (Poznávacia značka);
                    # data rows have a value normalizing to OM-XXXX there.
                    reg_cell = cells[1] if len(cells) > 1 else ""
                    normalized = _normalize_registration(reg_cell)
                    if normalized.startswith("OM-"):
                        if headers:
                            record = dict(zip(headers, cells))
                            # Force positional column values under our constant
                            # keys to avoid pdfplumber Unicode/newline encoding
                            # mismatches between the PDF header and our constants.
                            record[_COL_REGISTRATION] = reg_cell
                            record[_COL_TYPE] = cells[0] if len(cells) > 0 else ""
                            record[_COL_SERIAL] = cells[2] if len(cells) > 2 else ""
                            record[_COL_OWNER] = cells[3] if len(cells) > 3 else ""
                            record[_COL_OPERATOR] = cells[4] if len(cells) > 4 else ""
                            records.append(record)
                    else:
                        headers = [c.replace("\n", " ") for c in cells]

    logger.info("Parsed %d OM- rows from PDF.", len(records))
    return records


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict, icao_hex: str, registration: str) -> dict:
    """Build detail enrichment record from a parsed PDF row."""
    aircraft_fields: dict = {}
    registrant_fields: dict = {}

    model = _WHITESPACE_RE.sub(" ", row.get(_COL_TYPE, "").strip())
    if model:
        aircraft_fields["model"] = model

    serial = _WHITESPACE_RE.sub(" ", row.get(_COL_SERIAL, "").strip())
    if serial:
        aircraft_fields["serial_number"] = serial

    owner = _WHITESPACE_RE.sub(" ", row.get(_COL_OWNER, "").strip())
    names = [owner] if owner else []
    if names:
        registrant_fields["names"] = names

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "sk-nsat",
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
                    reg_map[_normalize_registration(registration)] = icao_hex
        except Exception as exc:
            logger.warning("RediSearch batch %d/%d failed: %s", batch_num + 1, total_batches, exc)

    return reg_map


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Write Slovakia NSAT data to aircraft:detail keys in Redis. Returns count written."""
    reg_row_map: dict[str, dict] = {}
    for row in rows:
        raw_reg = row.get(_COL_REGISTRATION, "").strip()
        reg = _normalize_registration(raw_reg)
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
        row = reg_row_map[registration]
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
        "ids": "SkyFollower_runner_sk_nsat",
        "name": "SkyFollower Slovakia NSAT Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Slovakia NSAT Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Slovakia NSAT Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Slovakia NSAT Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_sk_nsat_{name}",
            "object_id": f"SkyFollower_runner_sk_nsat_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_sk_nsat_{name}/config",
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
        rows = download_and_parse(session)
        _ensure_search_index(r)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info(
            "Slovakia NSAT runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Slovakia NSAT runner failed: %s", exc, exc_info=True)

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
