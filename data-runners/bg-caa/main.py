#!/usr/bin/env python3
"""
SkyFollower Bulgaria CAA Data Runner

Scrapes the Bulgaria CAA aircraft register page to discover the current
date-encoded xlsx URL, downloads and parses it with openpyxl, filters to
LZ-prefix rows, looks up ICAO hex via the Redis simple search index
(Mictronics), writes enrichment data to aircraft:registry:{icao_hex} with
14-day TTL, publishes MQTT completion stats, then exits.

Xlsx columns (0-based; row 0 = info text, row 1 = header, data from row 2):
  0: Рег. №           (not stored)
  1: Дата             (Excel serial date, not stored)
  2: Модел            → aircraft.model
  3: Сериен №         → aircraft.serial_number
  4: Рег. знак        → registration lookup key (LZ-prefix)
  5: Категория ВС     (not stored)
  6: Основание        (not stored)

Data source: https://www.caa.bg/bg/category/300/17238
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

from shared.redis_keys import (
    AIRCRAFT_MICTRONICS_SEARCH_INDEX,
    aircraft_registry_key,
)
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client
from shared.logging_setup import configure_logging

logger = logging.getLogger("bg-caa")

_INDEX_URL = "https://www.caa.bg/bg/category/300/17238"
_XLSX_PATTERN = re.compile(r"Aircraft_Register_\d+\.xlsx", re.IGNORECASE)

REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/bg-caa"
BATCH_SIZE = 100

_WHITESPACE_RE = re.compile(r"\s+")

_CATEGORY_MAP = {
    "glider": "Glider",
    "gyroplane": "Gyroplane",
    "hot-air balloon": "Balloon",
    "large aeroplane": "Airplane",
    "motor-hanglider": "Weight-Shift-Control",
    "paramotor-trike": "Paramotor Trike",
    "powered sailplane": "Powered Glider",
    "rotorcraft": "Rotorcraft",
    "sailplane": "Glider",
    "small aeroplane": "Airplane",
    "small rotorcraft": "Rotorcraft",
    "very light aeroplane": "Airplane",
}

_COL_MODEL = 2
_COL_SERIAL = 3
_COL_REGISTRATION = 4
_COL_CATEGORY = 5
_HEADER_ROWS = 2


# ---------------------------------------------------------------------------
# Xlsx URL discovery
# ---------------------------------------------------------------------------

def _discover_xlsx_url(session: requests.Session) -> str:
    """Scrape the index page and return the href of the Aircraft_Register xlsx."""
    logger.info("Downloading Bulgaria CAA index page from %s", _INDEX_URL)
    resp = session.get(_INDEX_URL, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Index page request failed with HTTP {resp.status_code}")
    soup = BeautifulSoup(resp.text, "lxml")
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if _XLSX_PATTERN.search(href):
            if href.startswith("http"):
                return href
            return f"https://www.caa.bg{href}"
    raise RuntimeError("No Aircraft_Register xlsx link found on Bulgaria CAA index page")


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def download_and_parse(session: requests.Session) -> list[dict]:
    """Discover xlsx URL, download and return parsed records."""
    xlsx_url = _discover_xlsx_url(session)
    logger.info("Downloading Bulgaria CAA aircraft register from %s", xlsx_url)
    resp = session.get(xlsx_url, timeout=120, headers={"Referer": "https://www.caa.bg/"})
    if not resp.ok:
        raise RuntimeError(f"Xlsx request failed with HTTP {resp.status_code}")

    wb = openpyxl.load_workbook(io.BytesIO(resp.content), read_only=True, data_only=True)
    ws = wb.active

    records = []
    for row_idx, row in enumerate(ws.iter_rows(values_only=True)):
        if row_idx < _HEADER_ROWS:
            continue
        if not row or len(row) <= _COL_REGISTRATION:
            continue
        registration = _clean(row[_COL_REGISTRATION])
        if not registration.startswith("LZ-"):
            continue
        records.append({
            "registration": registration,
            "model": _clean(row[_COL_MODEL]),
            "serial": _clean(row[_COL_SERIAL]),
            "category": _clean(row[_COL_CATEGORY]) if len(row) > _COL_CATEGORY else "",
        })

    wb.close()
    logger.info("Parsed %d LZ- records.", len(records))
    return records


def _clean(value) -> str:
    """Normalize whitespace in a cell value."""
    if value is None:
        return ""
    return _WHITESPACE_RE.sub(" ", str(value).strip())


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict, icao_hex: str, registration: str) -> dict:
    """Build a Redis detail record from a parsed row."""
    aircraft_fields: dict = {}

    category_raw = _clean(row.get("category", ""))
    if category_raw:
        aircraft_type = _CATEGORY_MAP.get(category_raw.lower())
        if aircraft_type:
            aircraft_fields["type"] = aircraft_type

    model = _clean(row.get("model", ""))
    if model:
        aircraft_fields["model"] = model

    serial = _clean(row.get("serial", ""))
    if serial:
        aircraft_fields["serial_number"] = serial

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "bg-caa",
        "military": False,
    }
    if aircraft_fields:
        record["aircraft"] = aircraft_fields

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
    """Write Bulgaria CAA data to aircraft:detail keys in Redis. Returns count written."""
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
        "ids": "SkyFollower_runner_bg_caa",
        "name": "SkyFollower Bulgaria CAA Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Bulgaria CAA Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Bulgaria CAA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Bulgaria CAA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_bg_caa_{name}",
            "object_id": f"SkyFollower_runner_bg_caa_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_bg_caa_{name}/config",
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
            "Bulgaria CAA runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Bulgaria CAA runner failed: %s", exc, exc_info=True)

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
