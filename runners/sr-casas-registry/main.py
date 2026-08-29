#!/usr/bin/env python3
"""
SkyFollower Suriname CASAS Data Runner

Scrapes the Civil Aviation Safety Authority Suriname (CASAS) registry page to
discover the current Civil Aircraft Register xlsx URL (distinct from the
separate UAS/drone registry xlsx also linked on the same page), downloads and
parses it with openpyxl, looks up ICAO hex via the Redis simple search index
(Mictronics), writes enrichment data to aircraft:registry:{icao_hex} with
14-day TTL, publishes MQTT completion stats, then exits.

Xlsx columns (named header row 0; data from row 1):
  #                                   (not stored)
  MAKE                                → aircraft.manufacturer
  MODEL                               → aircraft.model (+ SERIES appended if present)
  SERIES                              → appended to aircraft.model; may be absent
  MANUFACTURER                        (not stored -- licensed-builder detail,
                                       distinct from MAKE for ~half of rows,
                                       e.g. Grumman G164 built by Schweizer;
                                       AircraftRecord has only one
                                       manufacturer field, so MAKE -- the
                                       type's brand -- wins)
  SERIAL_NUMBER                       → aircraft.serial_number (mixed int/str
                                       in source, cast to str)
  NATIONALITY MARK OR COMMON MARK     → registration prefix (always "PZ" seen,
                                       read dynamically rather than hardcoded)
  REGISTRATION MARK                   → registration suffix; concatenated
                                       with the nationality mark as the
                                       lookup key, e.g. "PZ" + "UBD" → "PZ-UBD"
  OWNER_NAME                          → registrant.names
  OPERATOR (*)                        (not stored -- matches the established
                                       convention across every other runner
                                       in this repo with both an owner and an
                                       operator column, e.g. lu-dac-registry,
                                       sk-nsat-registry: only owner becomes
                                       registrant.names, operator is present
                                       in source but intentionally not read.
                                       No runner in this codebase writes
                                       AircraftRecord's top-level `operator`
                                       field)

Data source: https://www.casas.sr/registry/
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

import openpyxl
import paho.mqtt.client as mqtt
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

logger = logging.getLogger("sr-casas-registry")

_INDEX_URL = "https://www.casas.sr/registry/"
_XLSX_HREF_RE = re.compile(r'href="([^"]*/REGISTER-\d{2}-\d{4}\.xlsx)"', re.IGNORECASE)

MQTT_ROOT = "SkyFollower/runner/sr-casas-registry"
BATCH_SIZE = 100

_WHITESPACE_RE = re.compile(r"\s+")
_TRAILING_STAR_RE = re.compile(r"\*+$")


# ---------------------------------------------------------------------------
# Xlsx URL discovery
# ---------------------------------------------------------------------------

def _discover_xlsx_url(session: requests.Session) -> str:
    """Scrape the registry page for the Civil Aircraft Register xlsx link.

    The page also links a separate UAS/drone registry xlsx
    (CASAS-UAS-REGISTRY-*.xlsx) -- the REGISTER-{MM}-{YYYY}.xlsx pattern is
    specific enough to avoid matching that one.
    """
    logger.info("Downloading Suriname CASAS registry page from %s", _INDEX_URL)
    resp = session.get(_INDEX_URL, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Registry page request failed with HTTP {resp.status_code}")
    match = _XLSX_HREF_RE.search(resp.text)
    if not match:
        raise RuntimeError("No Civil Aircraft Register xlsx link found on Suriname CASAS registry page")
    return match.group(1)


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def download_and_parse(session: requests.Session) -> list[dict]:
    """Discover xlsx URL, download and return parsed records."""
    xlsx_url = _discover_xlsx_url(session)
    logger.info("Downloading Suriname CASAS civil aircraft register from %s", xlsx_url)
    resp = session.get(xlsx_url, timeout=120)
    if not resp.ok:
        raise RuntimeError(f"Xlsx request failed with HTTP {resp.status_code}")

    wb = openpyxl.load_workbook(io.BytesIO(resp.content), read_only=True, data_only=True)
    ws = wb.active

    records = []
    headers: list[str] | None = None
    for row in ws.iter_rows(values_only=True):
        if headers is None:
            headers = [_clean(c) for c in row]
            continue
        if not any(row):
            continue
        record = dict(zip(headers, row))
        nationality = _clean(record.get("NATIONALITY MARK OR COMMON MARK"))
        suffix = _clean(record.get("REGISTRATION MARK"))
        if not nationality or not suffix:
            continue
        records.append({
            "registration": f"{nationality}-{suffix}",
            "make": _clean(record.get("MAKE")),
            "model": _clean(record.get("MODEL")),
            "series": _clean(record.get("SERIES")),
            "serial": _clean(record.get("SERIAL_NUMBER")),
            "owner": _clean(record.get("OWNER_NAME")),
        })

    wb.close()
    logger.info("Parsed %d records.", len(records))
    return records


def _clean(value) -> str:
    """Normalize whitespace in a cell value, coercing non-string types (e.g.
    a numeric-looking serial number) to str first."""
    if value is None:
        return ""
    return _WHITESPACE_RE.sub(" ", str(value).strip())


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict, icao_hex: str, registration: str) -> dict:
    """Build a Redis detail record from a parsed row."""
    aircraft_fields: dict = {}
    registrant_fields: dict = {}

    make = row.get("make", "")
    if make:
        aircraft_fields["manufacturer"] = make

    model = _TRAILING_STAR_RE.sub("", row.get("model", "")).strip()
    series = row.get("series", "")
    if model and series:
        model = f"{model}{series}"
    if model:
        aircraft_fields["model"] = model

    serial = row.get("serial", "")
    if serial:
        aircraft_fields["serial_number"] = serial

    owner = row.get("owner", "")
    if owner:
        registrant_fields["names"] = [owner]

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "sr-casas-registry",
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
    """Write Suriname CASAS data to aircraft:detail keys in Redis. Returns count written."""
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
        identifier="SkyFollower_runner_sr_casas_registry",
        name=f"SkyFollower Suriname {country_flag('SR')} CASAS Registry Runner",
        model=f"Suriname {country_flag('SR')} CASAS Registry Runner",
        configuration_url="https://brentio.github.io/SkyFollower/runners/sr-casas-registry.html",
    )
    stats = [
        ("records_imported", "Suriname CASAS Registry Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Suriname CASAS Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Suriname CASAS Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_sr_casas_registry_{name}",
            "object_id": f"SkyFollower_runner_sr_casas_registry_{name}",
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
            f"homeassistant/sensor/SkyFollower_runner_sr_casas_registry_{name}/config",
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
            "Suriname CASAS runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Suriname CASAS runner failed: %s", exc, exc_info=True)

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
