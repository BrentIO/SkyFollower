#!/usr/bin/env python3
"""
SkyFollower Kyrgyzstan CAA Data Runner

Fetches the Kyrgyzstan CAA aircraft register from a static HTML page, handles
rowspan-merged operator cells, looks up each EX- registration in the Redis
simple search index to find the ICAO hex (provided by Mictronics), writes
enrichment data to aircraft:detail:{icao_hex}, publishes MQTT completion
stats, then exits.

Table columns (0-based, after rowspan expansion):
  0: № п/п — always empty in data rows (visual row-number placeholder)
  1: Эксплуатант/Operator + Собственник/Owner (rowspan; carried forward)
  2: Тип ВС/Type of Aircraft (rowspan; carried forward; stored as aircraft.model)
  3: Регистрац. номер/Registration (EX-prefix; used as lookup key)
  4: Дата регистрации/Date of Registration (not stored)
  5: Серийный №/Serial Number (stored as aircraft.serial_number)
  6: Дата производства/Date of Manufacture (stored as aircraft.manufactured_date)

Date of Manufacture formats:
  DD.MM.YYYY  → YYYY-MM-DD
  Month YYYY  → YYYY-01-01  (month names may be in Russian or English)

Data source: https://caa.kg/en/node/46
"""

from __future__ import annotations

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

logger = logging.getLogger("kg-caa")

_PAGE_URL = "https://caa.kg/en/node/46"

REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/kg-caa"
BATCH_SIZE = 100

_WHITESPACE_RE = re.compile(r"\s+")

# Russian and English month names → month number
_MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "январь": 1, "февраль": 2, "март": 3, "апрель": 4,
    "май": 5, "июнь": 6, "июль": 7, "август": 8,
    "сентябрь": 9, "октябрь": 10, "ноябрь": 11, "декабрь": 12,
}

_DMY_RE = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$")
_MONTH_YEAR_RE = re.compile(r"^([^\d]+)\s+(\d{4})$")

# Cyrillic homoglyphs that appear in place of Latin in some registration cells
_CYRILLIC_SUBSTITUTIONS = str.maketrans("ЕХех", "EXex")


# ---------------------------------------------------------------------------
# Date normalisation
# ---------------------------------------------------------------------------

def _parse_manufacture_date(raw: str) -> str:
    """Normalise a manufacture date string to YYYY-MM-DD or YYYY-01-01."""
    raw = _WHITESPACE_RE.sub(" ", raw).strip()

    m = _DMY_RE.match(raw)
    if m:
        day, month, year = m.group(1), m.group(2), m.group(3)
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    m = _MONTH_YEAR_RE.match(raw)
    if m:
        month_name = m.group(1).strip().lower()
        year = m.group(2)
        month_num = _MONTH_MAP.get(month_name)
        if month_num:
            return f"{year}-{str(month_num).zfill(2)}-01"

    # Year only
    if raw.isdigit() and len(raw) == 4:
        return f"{raw}-01-01"

    return ""


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def _expand_table(table) -> list[list[str]]:
    """Expand HTML rowspan attributes into a uniform grid of text cells."""
    grid: list[list[str]] = []
    pending: dict[int, tuple[str, int]] = {}  # col → (text, remaining_rows)

    for tr in table.find_all("tr"):
        cells = list(tr.find_all(["td", "th"]))
        row: list[str] = []
        col = 0
        ci = 0

        while ci < len(cells) or any(k >= col for k in pending):
            if col in pending:
                text, rem = pending[col]
                row.append(text)
                if rem > 1:
                    pending[col] = (text, rem - 1)
                else:
                    del pending[col]
                col += 1
            elif ci < len(cells):
                cell = cells[ci]
                ci += 1
                text = _WHITESPACE_RE.sub(" ", cell.get_text(separator=" ", strip=True))
                span = int(cell.get("rowspan") or 1)
                if span > 1:
                    pending[col] = (text, span - 1)
                row.append(text)
                col += 1
            else:
                col += 1  # skip gap to reach next pending column

        if row:
            grid.append(row)

    return grid


def download_and_parse(session: requests.Session) -> list[dict]:
    """Fetch and parse the Kyrgyzstan CAA aircraft register HTML page."""
    logger.info("Downloading Kyrgyzstan CAA aircraft register from %s", _PAGE_URL)
    resp = session.get(_PAGE_URL, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Page request failed with HTTP {resp.status_code}")

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table")
    if not table:
        raise RuntimeError("No table found on Kyrgyzstan CAA page.")

    # Columns (0-based, after rowspan expansion):
    #   0: № п/п (empty placeholder)  1: Operator/Owner  2: Type/Model
    #   3: Registration  4: Date registered (skip)  5: Serial  6: Date manufactured
    records: list[dict] = []
    for row in _expand_table(table):
        if len(row) < 7:
            continue
        # Normalise: remove all whitespace, map Cyrillic homoglyphs to Latin
        raw_reg = re.sub(r"\s+", "", row[3]).translate(_CYRILLIC_SUBSTITUTIONS)
        if not raw_reg.startswith("EX-"):
            continue
        records.append({
            "registration": raw_reg,
            "operator": row[1].strip(),
            "model": row[2].strip(),
            "serial": re.sub(r"\s+", "", row[5]).translate(_CYRILLIC_SUBSTITUTIONS),
            "manufacture_date_raw": row[6].strip(),
        })

    logger.info("Parsed %d EX- records.", len(records))
    return records


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict, icao_hex: str, registration: str) -> dict:
    """Build detail enrichment record from a parsed row."""
    aircraft_fields: dict = {}
    registrant_fields: dict = {}

    model = _WHITESPACE_RE.sub(" ", row.get("model", "").strip())
    if model:
        aircraft_fields["model"] = model

    serial = row.get("serial", "").strip()
    if serial:
        aircraft_fields["serial_number"] = serial

    mfr_date = _parse_manufacture_date(row.get("manufacture_date_raw", ""))
    if mfr_date:
        aircraft_fields["manufactured_date"] = mfr_date

    operator = _WHITESPACE_RE.sub(" ", row.get("operator", "").strip())
    if operator:
        registrant_fields["names"] = [operator]

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "kg-caa",
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
    """Write Kyrgyzstan CAA data to aircraft:detail keys in Redis. Returns count written."""
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
        "ids": "SkyFollower_runner_kg_caa",
        "name": "SkyFollower Kyrgyzstan CAA Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Kyrgyzstan CAA Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Kyrgyzstan CAA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Kyrgyzstan CAA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_kg_caa_{name}",
            "object_id": f"SkyFollower_runner_kg_caa_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_kg_caa_{name}/config",
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
            "Kyrgyzstan CAA runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Kyrgyzstan CAA runner failed: %s", exc, exc_info=True)

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
