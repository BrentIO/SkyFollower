#!/usr/bin/env python3
"""
SkyFollower Togo ANAC Data Runner

Fetches the ANAC Togo civil aircraft register page and parses the single
embedded HTML table with BeautifulSoup (single request, no file to download
or index page to discover), looks up ICAO hex via the Redis simple search
index (Mictronics), writes enrichment data to aircraft:registry:{icao_hex}
with 14-day TTL, publishes MQTT completion stats, then exits.

The table's "Radiation" column ("deregistered?": OUI/NON) is a real,
majority filter, not an edge case -- about 70% of rows are OUI
(deregistered) at any given time; only Radiation == NON rows are current
and get written. Despite its header, column 1 ("type") holds model
designations (e.g. "DC8 62", "PA 31T"), not the aircraft.type category --
this is the source's own misleading naming, not a transcription error.

Table columns (0-based):
  0: N° Ordre             (not stored)
  1: type                 → aircraft.model (see note above)
  2: Immatriculation      → registration lookup key (5V-prefix)
  3: Constructeur         → aircraft.manufacturer
  4: N° de serie          → aircraft.serial_number
  5: Radiation            → filter only, not stored (NON = active, keep)
  6: Nom propriétaire     → registrant.names
  7: Adresse propriétaire → registrant.street (comma-split)

Data source: http://www.anac-togo.tg/espace-professionnel/aeronefs/consultation-du-registre-dimmatriculation/
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

logger = logging.getLogger("tg-anac-registry")

_INDEX_URL = "http://www.anac-togo.tg/espace-professionnel/aeronefs/consultation-du-registre-dimmatriculation/"
_REG_RE = re.compile(r"^5V-[A-Z0-9]{2,6}$")

MQTT_ROOT = "SkyFollower/runner/tg-anac-registry"
BATCH_SIZE = 100

_WHITESPACE_RE = re.compile(r"\s+")

_COL_MODEL = 1
_COL_REGISTRATION = 2
_COL_MANUFACTURER = 3
_COL_SERIAL = 4
_COL_RADIATION = 5
_COL_OWNER = 6
_COL_ADDRESS = 7


# ---------------------------------------------------------------------------
# Fetch + parse
# ---------------------------------------------------------------------------

def download_and_parse(session: requests.Session) -> list[dict]:
    """Fetch the ANAC Togo register page and return parsed active records."""
    logger.info("Downloading Togo ANAC aircraft register from %s", _INDEX_URL)
    resp = session.get(_INDEX_URL, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"Register page request failed with HTTP {resp.status_code}")

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("No table found on Togo ANAC register page")

    records = []
    deregistered_count = 0
    for tr in table.find_all("tr"):
        cells = [_clean(td.get_text()) for td in tr.find_all(["td", "th"])]
        if len(cells) <= _COL_ADDRESS:
            continue
        registration = cells[_COL_REGISTRATION]
        if not _REG_RE.match(registration):
            continue
        if cells[_COL_RADIATION].upper() != "NON":
            deregistered_count += 1
            continue
        records.append({
            "registration": registration,
            "model": cells[_COL_MODEL],
            "manufacturer": cells[_COL_MANUFACTURER],
            "serial": cells[_COL_SERIAL],
            "owner": cells[_COL_OWNER],
            "address": cells[_COL_ADDRESS],
        })

    logger.info(
        "Parsed %d active 5V- records (%d deregistered rows skipped).",
        len(records), deregistered_count,
    )
    return records


def _clean(value) -> str:
    """Normalize whitespace in a cell value."""
    return _WHITESPACE_RE.sub(" ", (value or "").strip())


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

    manufacturer = row.get("manufacturer", "")
    if manufacturer:
        aircraft_fields["manufacturer"] = manufacturer

    serial = row.get("serial", "")
    if serial:
        aircraft_fields["serial_number"] = serial

    owner = row.get("owner", "")
    if owner:
        registrant_fields["names"] = [owner]

    address_raw = row.get("address") or ""
    address_parts = [p for part in address_raw.split(",") if (p := _clean(part))]
    if address_parts:
        registrant_fields["street"] = address_parts

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "tg-anac-registry",
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
    """Write Togo ANAC data to aircraft:detail keys in Redis. Returns count written."""
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
        identifier="SkyFollower_runner_tg_anac_registry",
        name=f"SkyFollower Togo {country_flag('TG')} ANAC Registry Runner",
        model=f"Togo {country_flag('TG')} ANAC Registry Runner",
        configuration_url="https://brentio.github.io/SkyFollower/runners/tg-anac-registry.html",
    )
    stats = [
        ("records_imported", "Togo ANAC Registry Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Togo ANAC Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Togo ANAC Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_tg_anac_registry_{name}",
            "object_id": f"SkyFollower_runner_tg_anac_registry_{name}",
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
            f"homeassistant/sensor/SkyFollower_runner_tg_anac_registry_{name}/config",
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
            "Togo ANAC runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Togo ANAC runner failed: %s", exc, exc_info=True)

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
