#!/usr/bin/env python3
"""
SkyFollower Georgia GCAA Data Runner

Fetches the Georgia Civil Aviation Agency aircraft register page, parses two
pre-rendered HTML tables (table_1: operator data; table_2: owner data), merges
them by registration mark, looks up each 4L- registration in the Redis simple
search index to find the ICAO hex (provided by Mictronics), writes enrichment
data to aircraft:registry:{icao_hex}, publishes MQTT completion stats, then exits.

Table columns (0-based; identical layout in both tables):
  0: Operator / Owner (Georgian + English text; stored differently per table)
  1: Aircraft type   (stored as aircraft.model)
  2: Registration    (4L-prefix; used as lookup key)
  3: Registration date (not stored)
  4: Serial number   (stored as aircraft.serial_number)
  5: Year of manufacture (4-digit year → stored as aircraft.manufactured_date YYYY-01-01)

Merge logic:
  - aircraft.model and aircraft.serial_number: from whichever table provides them
  - aircraft.manufactured_date: from whichever table provides the year
  - registrant.names[0]: operator (table_1 col 0)
  - registrant.names[1]: owner (table_2 col 0), omitted if identical to operator

Data source: https://gcaa.ge/civil-aircraft-register/
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
    AIRCRAFT_REGISTRY_SEARCH_INDEX,
    AIRCRAFT_MICTRONICS_SEARCH_INDEX,
    aircraft_registry_key,
)
from shared.redis_json import set_json

logger = logging.getLogger("ge-gcaa")

_PAGE_URL = "https://gcaa.ge/civil-aircraft-register/"

REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/ge-gcaa"
BATCH_SIZE = 100

_WHITESPACE_RE = re.compile(r"\s+")

_COL_PARTY = 0       # Operator (table_1) / Owner (table_2)
_COL_MODEL = 1
_COL_REGISTRATION = 2
_COL_SERIAL = 4
_COL_YEAR = 5


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def _parse_table(table_tag) -> list[dict]:
    """Extract rows from a BeautifulSoup table element, skipping the header."""
    rows = table_tag.find_all("tr")
    records = []
    for tr in rows[1:]:  # skip header
        cells = [_WHITESPACE_RE.sub(" ", td.get_text(strip=True)) for td in tr.find_all("td")]
        if len(cells) < 6:
            continue
        reg = cells[_COL_REGISTRATION].strip()
        if not reg.startswith("4L-"):
            continue
        records.append({
            "registration": reg,
            "party": cells[_COL_PARTY].strip(),
            "model": cells[_COL_MODEL].strip(),
            "serial": cells[_COL_SERIAL].strip(),
            "year": cells[_COL_YEAR].strip(),
        })
    return records


def download_and_parse(session: requests.Session) -> list[dict]:
    """Fetch the Georgia GCAA page and return merged records keyed by registration."""
    logger.info("Downloading Georgia GCAA aircraft register from %s", _PAGE_URL)
    resp = session.get(_PAGE_URL, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Page request failed with HTTP {resp.status_code}")

    soup = BeautifulSoup(resp.text, "lxml")

    table_1 = soup.find(id="table_1")
    table_2 = soup.find(id="table_2")

    if not table_1:
        raise RuntimeError("table_1 not found on Georgia GCAA page.")
    if not table_2:
        raise RuntimeError("table_2 not found on Georgia GCAA page.")

    operator_rows = _parse_table(table_1)
    owner_rows = _parse_table(table_2)

    # Index by registration
    merged: dict[str, dict] = {}

    for row in operator_rows:
        reg = row["registration"]
        merged[reg] = {
            "registration": reg,
            "model": row["model"],
            "serial": row["serial"],
            "year": row["year"],
            "operator": row["party"],
            "owner": "",
        }

    for row in owner_rows:
        reg = row["registration"]
        if reg in merged:
            entry = merged[reg]
            if not entry["model"]:
                entry["model"] = row["model"]
            if not entry["serial"]:
                entry["serial"] = row["serial"]
            if not entry["year"]:
                entry["year"] = row["year"]
            entry["owner"] = row["party"]
        else:
            merged[reg] = {
                "registration": reg,
                "model": row["model"],
                "serial": row["serial"],
                "year": row["year"],
                "operator": "",
                "owner": row["party"],
            }

    records = list(merged.values())
    logger.info("Parsed %d unique 4L- records.", len(records))
    return records


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict, icao_hex: str, registration: str) -> dict:
    """Build detail enrichment record from a merged row."""
    aircraft_fields: dict = {}
    registrant_fields: dict = {}

    model = _WHITESPACE_RE.sub(" ", row.get("model", "").strip())
    if model:
        aircraft_fields["model"] = model

    serial = row.get("serial", "").strip()
    if serial:
        aircraft_fields["serial_number"] = serial

    year = row.get("year", "").strip()
    if year.isdigit() and len(year) == 4:
        y = int(year)
        if 1900 <= y <= 2100:
            aircraft_fields["manufactured_date"] = f"{year}-01-01"

    operator = _WHITESPACE_RE.sub(" ", row.get("operator", "").strip())
    owner = _WHITESPACE_RE.sub(" ", row.get("owner", "").strip())

    names: list[str] = []
    if operator:
        names.append(operator)
    if owner and owner != operator:
        names.append(owner)
    if names:
        registrant_fields["names"] = names

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "ge-gcaa",
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
    """Write Georgia GCAA data to aircraft:detail keys in Redis. Returns count written."""
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
        "ids": "SkyFollower_runner_ge_gcaa",
        "name": "SkyFollower Georgia GCAA Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Georgia GCAA Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Georgia GCAA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Georgia GCAA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_ge_gcaa_{name}",
            "object_id": f"SkyFollower_runner_ge_gcaa_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_ge_gcaa_{name}/config",
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
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; P5Software SkyFollower)"})

    status = "failure"
    records_imported = 0

    try:
        rows = download_and_parse(session)
        _ensure_search_index(r)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info(
            "Georgia GCAA runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Georgia GCAA runner failed: %s", exc, exc_info=True)

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
