#!/usr/bin/env python3
"""
SkyFollower Spain AESA Data Runner

Downloads the AESA (Agencia Estatal de Seguridad Aérea) aircraft register PDF,
extracts all EC- registration rows using pdfplumber, looks up each registration
in the Redis simple search index to find the ICAO hex (provided by Mictronics),
writes enrichment data to aircraft:detail:{icao_hex}, and publishes MQTT
completion stats, then exits.

No registrant data is available in this register.

Important: no ICAO hex column — resolved via RediSearch reverse-lookup against
Mictronics records. Must run after the Mictronics runner.

Data source: https://www.seguridadaerea.gob.es/sites/default/files/aeronaves_inscritas.pdf
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import pdfplumber
import redis as redis_lib
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from shared.redis_keys import (
    AIRCRAFT_DETAIL_SEARCH_INDEX,
    AIRCRAFT_SIMPLE_SEARCH_INDEX,
    aircraft_detail_key,
)

logger = logging.getLogger("es-aesa")

PDF_URL = "https://www.seguridadaerea.gob.es/sites/default/files/aeronaves_inscritas.pdf"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/es-aesa"
BATCH_SIZE = 100

_NO_SERIAL = "NO\nDISPONIBLE"

# pdfplumber sometimes extracts Unicode hyphen variants instead of ASCII '-'.
# Normalize them all so EC- prefix checks and RediSearch lookups work correctly.
_UNICODE_HYPHENS = str.maketrans(
    "‐‑‒–—−­",
    "-------",
)

_CLASE_MAP = {
    "AVION": "Airplane",
    "HELICOPTERO (VTOL)": "Helicopter",
    "AUTOGIRO": "Gyroplane",
    "GLOBO": "Balloon",
    "PLANEADOR/MOTOPL\nANEADOR": "Glider",
    "ULM-AVION": "Airplane",
    "ULM-AUTOGIRO": "Gyroplane",
    "AFI-AVION": "Airplane",
}


# ---------------------------------------------------------------------------
# Download + parse
# ---------------------------------------------------------------------------

def _cell(value) -> str:
    """Normalize a PDF cell value: strip whitespace and unify Unicode hyphens."""
    return (value or "").strip().translate(_UNICODE_HYPHENS)


def download_and_parse(session: requests.Session) -> list[dict]:
    """Download the AESA PDF and extract all EC- registration rows."""
    logger.info("Downloading Spain AESA aircraft register from %s", PDF_URL)
    resp = session.get(PDF_URL, timeout=180)
    if resp.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {resp.status_code}")

    records: list[dict] = []
    headers: list[str] | None = None
    pages_no_table = 0

    with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()
            if not tables:
                pages_no_table += 1
                continue
            for table in tables:
                for row in table:
                    if not row:
                        continue
                    cells = [_cell(v) for v in row]
                    first = cells[0]
                    if first.startswith("EC-"):
                        if headers:
                            records.append(dict(zip(headers, cells)))
                    else:
                        # Normalize header newlines to spaces so field lookups match
                        headers = [c.replace("\n", " ") for c in cells]

    if pages_no_table:
        logger.warning("%d pages yielded no table from extract_tables().", pages_no_table)
    logger.info("Parsed %d EC- rows from PDF.", len(records))
    if records:
        logger.info("PDF column keys: %s", list(records[0].keys()))
        registrations = [r.get("Matrícula", "") for r in records]
        logger.info("First 5 registrations: %s", registrations[:5])
        if "EC-FTR" in registrations:
            logger.info("EC-FTR found in parsed rows.")
        else:
            logger.warning("EC-FTR NOT found in parsed rows — may be absent from PDF.")
    return records


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _decode_clase(value: str) -> str | None:
    """Map the Spanish Clase column value to a canonical English aircraft type."""
    return _CLASE_MAP.get(value)


def _build_record(row: dict, icao_hex: str, registration: str) -> dict:
    """Build detail enrichment record from a parsed PDF row."""
    aircraft_fields: dict = {}

    manufacturer = row.get("Fabricante", "").strip()
    if manufacturer:
        aircraft_fields["manufacturer"] = manufacturer

    model = row.get("Modelo", "").strip()
    if model:
        aircraft_fields["model"] = model

    serial = row.get("Nº serie", "").strip()
    if serial and serial != _NO_SERIAL:
        aircraft_fields["serial_number"] = serial

    year_str = row.get("Año cons.", "").strip()
    if year_str:
        try:
            year = int(year_str)
            if year != 1900:
                aircraft_fields["manufactured_date"] = f"{year}-01-01"
        except ValueError:
            pass

    powerplant_fields: dict = {}

    powerplant_manufacturer = row.get("Marca Motor", "").strip()
    if powerplant_manufacturer:
        powerplant_fields["manufacturer"] = powerplant_manufacturer

    powerplant_model = row.get("Modelo Motor", "").strip()
    if powerplant_model:
        powerplant_fields["model"] = powerplant_model

    num_motors = row.get("Nº mot.", "").strip()
    if num_motors:
        try:
            powerplant_fields["count"] = int(num_motors)
        except ValueError:
            pass

    clase = row.get("Clase", "").strip()
    decoded_clase = _decode_clase(clase)
    if decoded_clase:
        aircraft_fields["type"] = decoded_clase

    record: dict = {
        "icao_hex": icao_hex,
        "registration": registration,
        "source": "es-aesa",
    }
    if aircraft_fields:
        record["aircraft"] = aircraft_fields
    if powerplant_fields:
        aircraft_fields["powerplant"] = powerplant_fields

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
                    reg_map[registration] = icao_hex
        except Exception as exc:
            logger.warning("RediSearch batch %d/%d failed: %s", batch_num + 1, total_batches, exc)

    return reg_map


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Write Spain AESA data to aircraft:detail keys in Redis. Returns count written."""
    reg_row_map: dict[str, dict] = {}
    skipped = 0
    for row in rows:
        reg = row.get("Matrícula", "").strip()
        if not reg:
            skipped += 1
            continue
        reg_row_map[reg] = row
    if skipped:
        logger.warning("%d rows skipped due to empty Matrícula — possible column key mismatch.", skipped)

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
        "ids": "SkyFollower_runner_es_aesa",
        "name": "SkyFollower Spain AESA Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Spain AESA Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Spain AESA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Spain AESA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_es_aesa_{name}",
            "object_id": f"SkyFollower_runner_es_aesa_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_es_aesa_{name}/config",
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
            "Spain AESA runner completed successfully. Records imported: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("Spain AESA runner failed: %s", exc, exc_info=True)

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
