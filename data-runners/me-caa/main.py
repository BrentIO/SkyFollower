#!/usr/bin/env python3
"""
SkyFollower Montenegro CAA Data Runner

Fetches the Montenegro CAA aircraft register from a paginated HTML list at
https://www.caa.me/en/registri, filters out deregistered aircraft (Ime ==
"Ispisan iz registra - Deregistered"), fetches each active aircraft's detail
page to confirm Dereg == "No" and to collect all enrichment fields, looks up
each 4O- registration in the Redis simple search index to find the ICAO hex
(provided by Mictronics), writes enrichment data to aircraft:detail:{icao_hex},
publishes MQTT completion stats, then exits.

List page columns: Registarska oznaka (registration), Redni broj u registru
(sequence, not stored), Ime (deregistered marker or operator — used only for
filtering), Tip (short type code — overridden by detail page model).

Detail page fields collected:
  Aircraft section: Manufacturer, Year Built, Category, S/N,
                    Aircraft model/type, ARC expiry date (not stored)
  Registration Details: Dereg ("No" = active; other = skip)
  Operator details: Name, Address, Zip code, town, Country

Data source: https://www.caa.me/en/registri
"""

from __future__ import annotations

import json
import logging
import os
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

logger = logging.getLogger("me-caa")

_LIST_URL = "https://www.caa.me/en/registri"
_DETAIL_BASE_URL = "https://www.caa.me/en/"
_DEREGISTERED_MARKER = "Ispisan iz registra - Deregistered"

REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/me-caa"
BATCH_SIZE = 100

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

_WHITESPACE_RE = __import__("re").compile(r"\s+")


# ---------------------------------------------------------------------------
# Category → aircraft.type decoder
# ---------------------------------------------------------------------------

def _decode_category(raw: str) -> str:
    """Extract aircraft type from a category string like 'Transport – Airplane'."""
    for sep in ("–", "-"):  # em-dash then hyphen
        if sep in raw:
            return raw.split(sep, 1)[1].strip()
    return raw.strip()


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def _fetch_list_page(session: requests.Session, page: int) -> list[dict]:
    """Fetch one page of the registry list. Returns rows; empty list = no more pages."""
    url = f"{_LIST_URL}?page={page}"
    logger.info("Fetching Montenegro CAA registry list from %s", url)
    resp = session.get(url, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"List page {page} returned HTTP {resp.status_code}")

    soup = BeautifulSoup(resp.text, "lxml")
    rows = []

    table = soup.find("table")
    if not table:
        return []

    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    for tr in table.find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        row = dict(zip(headers, [td.get_text(strip=True) for td in cells]))
        rows.append(row)

    return rows


def _fetch_detail_page(session: requests.Session, registration: str) -> dict:
    """Fetch the detail page for one aircraft and return extracted fields."""
    slug = registration.lower()
    url = f"{_DETAIL_BASE_URL}{slug}"
    logger.info("Fetching Montenegro CAA detail page from %s", url)
    resp = session.get(url, timeout=30)
    if not resp.ok:
        logger.warning("Detail page for %s returned HTTP %s; skipping.", registration, resp.status_code)
        return {}

    soup = BeautifulSoup(resp.text, "lxml")
    fields: dict = {}

    # Detail pages use a definition list or table of label/value pairs.
    # Try <table> first, then <dl>.
    table = soup.find("table")
    if table:
        for tr in table.find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                fields[key] = value
    else:
        dl = soup.find("dl")
        if dl:
            terms = dl.find_all("dt")
            defs = dl.find_all("dd")
            for dt, dd in zip(terms, defs):
                fields[dt.get_text(strip=True)] = dd.get_text(strip=True)

    return fields


def download_and_parse(session: requests.Session) -> list[dict]:
    """Paginate the registry list, fetch detail pages for active aircraft, and return records."""
    active_rows: list[dict] = []
    page = 0

    while True:
        rows = _fetch_list_page(session, page)
        if not rows:
            break
        for row in rows:
            ime = row.get("Ime", "").strip()
            if ime == _DEREGISTERED_MARKER:
                continue
            active_rows.append(row)
        page += 1

    logger.info("Found %d active aircraft on list pages.", len(active_rows))

    records: list[dict] = []
    for row in active_rows:
        registration = row.get("Registarska oznaka", "").strip()
        if not registration.startswith("4O-"):
            continue

        detail = _fetch_detail_page(session, registration)

        # Secondary deregistration check — Dereg is "No" for active aircraft
        dereg = detail.get("Dereg", "").strip()
        if dereg.lower() != "no":
            logger.info("Skipping %s — Dereg is %r (not 'No').", registration, dereg)
            continue

        records.append({
            "registration": registration,
            # Prefer full model name from detail page over short Tip from list
            "model": detail.get("Aircraft model/type", "").strip() or row.get("Tip", "").strip(),
            "category": detail.get("Category", "").strip(),
            "serial_number": detail.get("S/N", "").strip(),
            "manufacturer": detail.get("Manufacturer", "").strip(),
            "year_built": detail.get("Year Built", "").strip(),
            "operator_name": detail.get("Name", "").strip(),
            "operator_address": detail.get("Address", "").strip(),
            "operator_zip": detail.get("Zip code, town", "").strip(),
            "operator_country": detail.get("Country", "").strip(),
        })

    logger.info("Parsed %d active 4O- records.", len(records))
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

    raw_category = row.get("category", "").strip()
    if raw_category:
        aircraft_fields["type"] = _decode_category(raw_category)

    serial = row.get("serial_number", "").strip()
    if serial:
        aircraft_fields["serial_number"] = serial

    manufacturer = _WHITESPACE_RE.sub(" ", row.get("manufacturer", "").strip())
    if manufacturer:
        aircraft_fields["manufacturer"] = manufacturer

    year = row.get("year_built", "").strip()
    if year and year.isdigit() and len(year) == 4:
        aircraft_fields["manufactured_date"] = f"{year}-01-01"

    operator_name = _WHITESPACE_RE.sub(" ", row.get("operator_name", "").strip())
    if operator_name:
        registrant_fields["names"] = [operator_name]

    operator_address = _WHITESPACE_RE.sub(" ", row.get("operator_address", "").strip())
    if operator_address:
        registrant_fields["street"] = operator_address

    operator_zip = row.get("operator_zip", "").strip()
    if operator_zip:
        registrant_fields["postal_code"] = operator_zip

    operator_country = _WHITESPACE_RE.sub(" ", row.get("operator_country", "").strip())
    if operator_country:
        registrant_fields["country"] = operator_country

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "me-caa",
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
    """Write Montenegro CAA data to aircraft:detail keys in Redis. Returns count written."""
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
        "ids": "SkyFollower_runner_me_caa",
        "name": "SkyFollower Montenegro CAA Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Montenegro CAA Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Montenegro CAA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Montenegro CAA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_me_caa_{name}",
            "object_id": f"SkyFollower_runner_me_caa_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_me_caa_{name}/config",
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
            "Montenegro CAA runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Montenegro CAA runner failed: %s", exc, exc_info=True)

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
