#!/usr/bin/env python3
"""
SkyFollower Bosnia and Herzegovina BHDCA Data Runner

Scrapes the Bosnia and Herzegovina Directorate of Civil Aviation (BHDCA)
airworthiness page to discover the current aircraft register PDF URL,
downloads and parses all pages with pdfplumber, looks up ICAO hex via the
Redis simple search index (Mictronics), writes enrichment data to
aircraft:registry:{icao_hex} with 14-day TTL, publishes MQTT completion
stats, then exits.

PDF columns (0-based):
  0: (row number)      (not stored)
  1: Registration mark → registration lookup key (E7-prefix)
  2: Designation       → aircraft.model
  3: Manifacturer      → aircraft.manufacturer (sic; typo in source PDF header)
  4: Serial number     → aircraft.serial_number
  5: Owner             → registrant.names[0]
  6: Registration date (not stored)

Data source: http://www.bhdca.gov.ba/index.php/en/regulations-and-areas/airworthiness
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
from redis.commands.search.query import Query

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.config import ConfigError, load_config
from shared.timing import ENRICHMENT_TTL_SECONDS
from shared.redis_client import build_redis_client
from shared.ha_discovery import build_ha_device
from shared.redis_keys import (
    AIRCRAFT_MICTRONICS_SEARCH_INDEX,
    aircraft_registry_key,
)
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client
from shared.logging_setup import configure_logging
from shared.country_flags import country_flag

logger = logging.getLogger("ba-bhdca-registry")

_INDEX_URL = "http://www.bhdca.gov.ba/index.php/en/regulations-and-areas/airworthiness"
_PDF_HREF_RE = re.compile(r"aircraft%20register[^\"']*\.pdf", re.IGNORECASE)
_REG_RE = re.compile(r"^E7-[A-Z0-9]{2,6}$")

MQTT_ROOT = "SkyFollower/runner/ba-bhdca-registry"
BATCH_SIZE = 100

_WHITESPACE_RE = re.compile(r"\s+")

_COL_REGISTRATION = 1
_COL_MODEL = 2
_COL_MANUFACTURER = 3
_COL_SERIAL = 4
_COL_OWNER = 5


# ---------------------------------------------------------------------------
# PDF URL discovery
# ---------------------------------------------------------------------------

def _discover_pdf_url(session: requests.Session) -> str:
    """Scrape the airworthiness index page for the Aircraft Register PDF link."""
    logger.info("Downloading BHDCA airworthiness index page from %s", _INDEX_URL)
    resp = session.get(_INDEX_URL, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Index page request failed with HTTP {resp.status_code}")
    soup = BeautifulSoup(resp.text, "lxml")
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if _PDF_HREF_RE.search(href):
            if href.startswith("http"):
                return href
            return f"http://www.bhdca.gov.ba{href}"
    raise RuntimeError("No Aircraft Register PDF link found on BHDCA airworthiness page")


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def download_and_parse(session: requests.Session) -> list[dict]:
    """Discover PDF URL, download and return parsed records."""
    pdf_url = _discover_pdf_url(session)
    logger.info("Downloading BHDCA aircraft register from %s", pdf_url)
    resp = session.get(pdf_url, timeout=120)
    if not resp.ok:
        raise RuntimeError(f"PDF request failed with HTTP {resp.status_code}")

    records = []
    with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue
            for row in table:
                if not row or len(row) <= _COL_OWNER:
                    continue
                # Source PDF has a handful of rows with a stray space after
                # the hyphen (e.g. "E7- NEL" instead of "E7-NEL") — normalize
                # before validating so these aren't silently dropped.
                registration = re.sub(r"-\s+", "-", _clean(row[_COL_REGISTRATION]))
                if not _REG_RE.match(registration):
                    continue
                records.append({
                    "registration": registration,
                    "model": _clean(row[_COL_MODEL]),
                    "manufacturer": _clean(row[_COL_MANUFACTURER]),
                    "serial": _clean(row[_COL_SERIAL]),
                    "owner": _clean(row[_COL_OWNER]),
                })

    logger.info("Parsed %d E7- records.", len(records))
    return records


def _clean(value) -> str:
    """Normalize whitespace (including newlines) in a cell value."""
    return _WHITESPACE_RE.sub(" ", (value or "").strip())


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict, icao_hex: str, registration: str) -> dict:
    """Build a Redis detail record from a parsed row."""
    aircraft_fields: dict = {}
    registrant_fields: dict = {}

    model = _clean(row.get("model", ""))
    if model:
        aircraft_fields["model"] = model

    manufacturer = _clean(row.get("manufacturer", ""))
    if manufacturer:
        aircraft_fields["manufacturer"] = manufacturer

    serial = _clean(row.get("serial", ""))
    if serial:
        aircraft_fields["serial_number"] = serial

    owner = _clean(row.get("owner", ""))
    if owner:
        registrant_fields["names"] = [owner]

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "ba-bhdca-registry",
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
# Registration → icao_hex lookup
# ---------------------------------------------------------------------------

def _build_registration_map(registrations: list[str], r: redis_lib.Redis) -> dict[str, str]:
    """Batch-query Redis simple search index for icao_hex by registration mark."""
    reg_map: dict[str, str] = {}
    if not registrations:
        return reg_map

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
    """Write BHDCA data to aircraft:detail keys in Redis. Returns count written."""
    reg_row_map: dict[str, dict] = {}
    for row in rows:
        reg = row.get("registration", "").strip()
        if reg:
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
        identifier="SkyFollower_runner_ba_bhdca_registry",
        name=f"SkyFollower Bosnia and Herzegovina {country_flag('BA')} BHDCA Registry Runner",
        model=f"Bosnia and Herzegovina {country_flag('BA')} BHDCA Registry Runner",
        configuration_url="https://brentio.github.io/SkyFollower/runners/ba-bhdca-registry.html",
    )
    stats = [
        ("records_imported", "Bosnia and Herzegovina BHDCA Registry Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Bosnia and Herzegovina BHDCA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Bosnia and Herzegovina BHDCA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_ba_bhdca_registry_{name}",
            "object_id": f"SkyFollower_runner_ba_bhdca_registry_{name}",
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
            f"homeassistant/sensor/SkyFollower_runner_ba_bhdca_registry_{name}/config",
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

    ttl = ENRICHMENT_TTL_SECONDS

    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; P5Software SkyFollower)"})

    status = "failure"
    records_imported = 0

    try:
        rows = download_and_parse(session)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info(
            "Bosnia and Herzegovina BHDCA runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Bosnia and Herzegovina BHDCA runner failed: %s", exc, exc_info=True)

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
