#!/usr/bin/env python3
"""
SkyFollower Macau AACM Data Runner

Fetches the AACM (Civil Aviation Authority of Macau) registered-aircraft
page and parses the single embedded HTML table with BeautifulSoup (single
request, no discovery step, no file to download), expanding the table's
rowspan-merged Operator column into a uniform grid first, looks up ICAO
hex via the Redis simple search index (Mictronics), writes enrichment
data to aircraft:registry:{icao_hex} with 14-day TTL, publishes MQTT
completion stats, then exits.

Only the Traditional Chinese page (zh-hant) actually renders a table on
plain HTTP fetch -- the /en-us/ path returns the same page shell with
unrendered Vue.js template placeholders and no table content, so the
Operator column is genuine Chinese text (e.g. 澳門航空股份有限公司),
stored as-is rather than transliterated.

The Operator column uses rowspan to span every aircraft belonging to the
same operator (e.g. rowspan="23" for the first operator) rather than
repeating the operator name on every row -- table is expanded to a full
grid before parsing so column indices are always fixed, the same
approach kg-caa-registry uses for its own rowspan-merged operator column.

Table columns (0-based, after rowspan expansion):
  0: Operator (經營人)             → registrant.names -- no separate owner
                                     column exists in this source, same
                                     approach sg-caas-registry/tt-caa-registry/
                                     jo-carc-registry take for their own
                                     operator-only sources
  1: Registration Number (註冊編號) → registration lookup key (B-M-prefix)
  2: Aircraft Type (型號)          → aircraft.model -- combined
                                     manufacturer+model string (e.g.
                                     "空中巴士 A321-231" / "Airbus A321-231"),
                                     no reliable delimiter to split
                                     manufacturer out, same approach
                                     bs-caa-registry/tt-caa-registry take

No serial number, address, or registration date columns exist in this
source -- a minimal register (operator/registration/type only).

Data source: https://www.aacm.gov.mo/zh-hant/industry-page/RegisteredAircraft
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

logger = logging.getLogger("mo-aacm-registry")

_INDEX_URL = "https://www.aacm.gov.mo/zh-hant/industry-page/RegisteredAircraft"
_REG_RE = re.compile(r"^B-M[A-Z0-9]{2,4}$")

MQTT_ROOT = "SkyFollower/runner/mo-aacm-registry"
BATCH_SIZE = 100

_WHITESPACE_RE = re.compile(r"\s+")

_COL_OPERATOR = 0
_COL_REGISTRATION = 1
_COL_MODEL = 2


# ---------------------------------------------------------------------------
# Rowspan expansion
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


# ---------------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------------

def download_and_parse(session: requests.Session) -> list[dict]:
    """Fetch the AACM register page and return parsed records."""
    logger.info("Downloading Macau AACM aircraft register from %s", _INDEX_URL)
    resp = session.get(_INDEX_URL, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Register page request failed with HTTP {resp.status_code}")

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("No table found on Macau AACM register page")

    records = []
    for row in _expand_table(table):
        if len(row) <= _COL_MODEL:
            continue
        # A handful of registration cells have the mark split across
        # multiple text nodes in the source HTML (e.g. "B-MBU" rendering
        # as "B-MB U") -- strip all internal whitespace, not just collapse
        # runs of it, before validating.
        registration = row[_COL_REGISTRATION].replace(" ", "")
        if not _REG_RE.match(registration):
            continue
        records.append({
            "registration": registration,
            "operator": row[_COL_OPERATOR],
            "model": row[_COL_MODEL],
        })

    logger.info("Parsed %d B-M records.", len(records))
    return records


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict, icao_hex: str, registration: str) -> dict:
    """Build a Redis detail record from a parsed row."""
    aircraft_fields: dict = {}
    registrant_fields: dict = {}

    model = row.get("model", "")
    if model:
        aircraft_fields["model"] = model

    operator = row.get("operator", "")
    if operator:
        registrant_fields["names"] = [operator]

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "mo-aacm-registry",
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
    """Write AACM data to aircraft:detail keys in Redis. Returns count written."""
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
        identifier="SkyFollower_runner_mo_aacm_registry",
        name=f"SkyFollower Macau {country_flag('MO')} AACM Registry Runner",
        model=f"Macau {country_flag('MO')} AACM Registry Runner",
        configuration_url="https://brentio.github.io/SkyFollower/runners/mo-aacm-registry.html",
    )
    stats = [
        ("records_imported", "Macau AACM Registry Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Macau AACM Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Macau AACM Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_mo_aacm_registry_{name}",
            "object_id": f"SkyFollower_runner_mo_aacm_registry_{name}",
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
            f"homeassistant/sensor/SkyFollower_runner_mo_aacm_registry_{name}/config",
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
            "Macau AACM runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Macau AACM runner failed: %s", exc, exc_info=True)

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
