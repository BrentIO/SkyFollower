#!/usr/bin/env python3
"""
SkyFollower France DGAC Data Runner

Downloads the Direction Générale de l'Aviation Civile (DGAC) aircraft register
CSV, groups rows by registration to handle co-ownership, looks up each F-
registration in the Redis search index to find the ICAO hex (provided by
Mictronics), deep-merges DGAC enrichment data into existing Redis records,
publishes MQTT completion stats, then exits.

Important: the DGAC register does not publish ICAO hex (Mode S) addresses.
This runner can only enrich records that already exist in Redis from Mictronics.
Schedule it AFTER the Mictronics runner.

Data source: https://immat.aviation-civile.gouv.fr/immat/servlet/static/upload/export.csv
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from shared.redis_keys import AIRCRAFT_SEARCH_INDEX, icao_hex_key

logger = logging.getLogger("fr-dgac")

DOWNLOAD_URL = "https://immat.aviation-civile.gouv.fr/immat/servlet/static/upload/export.csv"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/fr-dgac"
BATCH_SIZE = 100

# ---------------------------------------------------------------------------
# Country decode tables (French names → ISO 3166-1 alpha-2)
# Multi-word entries must be checked before single-word entries.
# ---------------------------------------------------------------------------

# Checked by suffix match against the end of the address string (uppercased).
# Ordered longest-first within this list so more specific matches win.
_COUNTRY_MULTI_WORD: list[tuple[str, str]] = [
    ("ETATS UNIS D'AMERIQUE", "US"),
    ("ÉTATS-UNIS D'AMÉRIQUE", "US"),
    ("REPUBLIQUE HONGROISE", "HU"),
    ("PAYS BAS", "NL"),
    ("PAYS-BAS", "NL"),
]

_COUNTRY_SINGLE_WORD: dict[str, str] = {
    "FRANCE": "FR",
    "IRLANDE": "IE",
    "JAPON": "JP",
    "BELGIQUE": "BE",
    "LUXEMBOURG": "LU",
    "ALLEMAGNE": "DE",
    "ITALIE": "IT",
    "ESPAGNE": "ES",
    "MONACO": "MC",
    "SUISSE": "CH",
    "SEYCHELLES": "SC",
    "AUTRICHE": "AT",
    "MALTE": "MT",
    "ANDORRE": "AD",
    "PORTUGAL": "PT",
    "CHYPRE": "CY",
    "ROUMANIE": "RO",
    "ROYAUME-UNI": "GB",
    "LETTONIE": "LV",
    "SLOVAQUIE": "SK",
    "POLOGNE": "PL",
    "FINLANDE": "FI",
    "DANEMARK": "DK",
    "SUEDE": "SE",
    "NORVEGE": "NO",
    "GRECE": "GR",
    "BULGARIE": "BG",
    "CROATIE": "HR",
    "SLOVENIE": "SI",
    "LITUANIE": "LT",
    "ESTONIE": "EE",
    "TCHEQUE": "CZ",
    "CANADA": "CA",
    "AUSTRALIE": "AU",
    "CHINE": "CN",
    "BRESIL": "BR",
    "MAROC": "MA",
    "TUNISIE": "TN",
    "SENEGAL": "SN",
    "CAMEROUN": "CM",
    "NIGERIA": "NG",
    "COTE": "CI",
}


# ---------------------------------------------------------------------------
# Address parsing
# ---------------------------------------------------------------------------

def _parse_address(raw: str) -> tuple[Optional[list[str]], Optional[str], Optional[str], Optional[str]]:
    """Parse ADRESSE_PROPRIETAIRE into (street, city, postal_code, country).

    Algorithm:
    1. Strip known French country name from the end (multi-word first).
    2. If a 5-digit postal code is found, extract street/postal_code/city.
    3. Fallback: record country only; skip street/city/postal_code.

    Returns (street_lines, city, postal_code, country_iso).
    """
    addr = raw.strip()
    if not addr:
        return None, None, None, None

    addr_upper = addr.upper()
    country_iso: Optional[str] = None
    remainder = addr

    # Try multi-word country names first
    for french, iso in _COUNTRY_MULTI_WORD:
        if addr_upper.endswith(french.upper()):
            country_iso = iso
            remainder = addr[:len(addr) - len(french)].strip().rstrip(",").strip()
            break

    # Try single-word country name
    if not country_iso:
        last_word = addr_upper.rsplit(None, 1)[-1]
        if last_word in _COUNTRY_SINGLE_WORD:
            country_iso = _COUNTRY_SINGLE_WORD[last_word]
            remainder = addr.rsplit(None, 1)[0].strip().rstrip(",").strip()

    # Try to find 5-digit postal code in remainder
    m = re.search(r'^(.*?)\b(\d{5})\b\s*(.*?)\s*$', remainder)
    if m:
        street_raw = m.group(1).strip().rstrip(",").strip()
        postal_code = m.group(2)
        city = m.group(3).strip() or None
        street = [street_raw] if street_raw else None
        return street, city, postal_code, country_iso

    # No postal code — record country only
    return None, None, None, country_iso


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
# Record builder
# ---------------------------------------------------------------------------

def _build_record(
    icao_hex: str,
    registration: str,
    first_row: dict,
    names: list[str],
    address_raw: Optional[str],
) -> dict:
    """Build the enrichment record from grouped DGAC rows."""
    # aircraft sub-object
    aircraft_fields = {
        "manufacturer": first_row.get("CONSTRUCTEUR", "").strip() or None,
        "model": first_row.get("MODELE", "").strip() or None,
        "serial_number": first_row.get("NUMERO_SERIE", "").strip() or None,
    }
    aircraft = {k: v for k, v in aircraft_fields.items() if v is not None} or None

    # registrant sub-object
    registrant: Optional[dict] = None
    street, city, postal_code, country = _parse_address(address_raw or "")
    registrant_fields: dict = {}
    if names:
        registrant_fields["names"] = names
    if street:
        registrant_fields["street"] = street
    if city:
        registrant_fields["city"] = city
    if postal_code:
        registrant_fields["postal_code"] = postal_code
    if country:
        registrant_fields["country"] = country
    if registrant_fields:
        registrant = registrant_fields

    record: dict = {"icao_hex": icao_hex, "registration": registration}
    if aircraft:
        record["aircraft"] = aircraft
    if registrant:
        record["registrant"] = registrant
    return record


# ---------------------------------------------------------------------------
# Record merge
# ---------------------------------------------------------------------------

def _deep_merge(base: dict, update: dict) -> dict:
    """Merge update into base. update values win; nested dicts are merged recursively."""
    result = dict(base)
    for k, v in update.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


# ---------------------------------------------------------------------------
# Search index
# ---------------------------------------------------------------------------

def _ensure_search_index(r: redis_lib.Redis) -> None:
    """Create the aircraft JSON search index if it does not already exist."""
    try:
        r.ft(AIRCRAFT_SEARCH_INDEX).info()
    except Exception:
        r.ft(AIRCRAFT_SEARCH_INDEX).create_index(
            fields=[
                TagField("$.icao_hex", as_name="icao_hex"),
                TagField("$.registration", as_name="registration"),
            ],
            definition=IndexDefinition(prefix=["icao_hex:"], index_type=IndexType.JSON),
        )
        logger.info("Created search index %r.", AIRCRAFT_SEARCH_INDEX)


# ---------------------------------------------------------------------------
# Registration → icao_hex lookup
# ---------------------------------------------------------------------------

def _build_registration_map(registrations: list[str], r: redis_lib.Redis) -> dict[str, str]:
    """Batch-query Redis search index for icao_hex by registration mark.

    Returns {registration → icao_hex} for registrations already in Redis.
    """
    reg_map: dict[str, str] = {}
    total_batches = (len(registrations) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_num, i in enumerate(range(0, len(registrations), BATCH_SIZE)):
        batch = registrations[i:i + BATCH_SIZE]
        escaped = [_escape_tag(reg) for reg in batch]
        query_str = f"@registration:{{{'|'.join(escaped)}}}"

        try:
            results = r.ft(AIRCRAFT_SEARCH_INDEX).search(
                Query(query_str)
                .return_fields("registration")
                .paging(0, BATCH_SIZE)
            )
            for doc in results.docs:
                icao_hex = doc.id.replace("icao_hex:", "")
                registration = getattr(doc, "registration", None)
                if registration:
                    reg_map[registration] = icao_hex
        except Exception as exc:
            logger.warning("RediSearch batch %d/%d failed: %s", batch_num + 1, total_batches, exc)

        if (batch_num + 1) % 50 == 0:
            logger.info("  ... registration lookup %d/%d batches complete.", batch_num + 1, total_batches)

    return reg_map


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_registry(url: str) -> list[dict]:
    """Download the DGAC aircraft register CSV and return rows as dicts."""
    logger.info("Downloading France DGAC aircraft register from %s", url)
    response = requests.get(url, timeout=120, headers={"User-Agent": "P5Software SkyFollower"})
    if response.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {response.status_code}")
    logger.info("Download complete (%d bytes).", len(response.content))
    text = response.content.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    rows = list(reader)
    logger.info("Parsed %d rows.", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Group rows by registration (co-ownership)
# ---------------------------------------------------------------------------

def _group_by_registration(rows: list[dict]) -> dict[str, dict]:
    """Group co-ownership rows by IMMATRICULATION.

    Returns {registration → {first_row, names, address_raw}} where:
    - first_row: the first CSV row (aircraft fields come from here)
    - names: deduplicated list of non-blank PROPRIETAIRE values in order
    - address_raw: first non-blank ADRESSE_PROPRIETAIRE across the group
    """
    groups: dict[str, dict] = {}
    seen_names: dict[str, list[str]] = defaultdict(list)
    seen_name_sets: dict[str, set] = defaultdict(set)

    for row in rows:
        reg = row.get("IMMATRICULATION", "").strip()
        if not reg:
            continue

        if reg not in groups:
            groups[reg] = {
                "first_row": row,
                "names": [],
                "address_raw": None,
            }

        name = row.get("PROPRIETAIRE", "").strip()
        if name and name not in seen_name_sets[reg]:
            seen_name_sets[reg].add(name)
            groups[reg]["names"].append(name)

        if not groups[reg]["address_raw"]:
            addr = row.get("ADRESSE_PROPRIETAIRE", "").strip()
            if addr:
                groups[reg]["address_raw"] = addr

    return groups


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Enrich existing Redis records with DGAC data. Returns count of records written."""
    groups = _group_by_registration(rows)
    registrations = list(groups.keys())
    logger.info("Looking up %d registrations in Redis search index.", len(registrations))

    reg_icao_map = _build_registration_map(registrations, r)
    logger.info(
        "Found %d / %d registrations in Redis (remainder not yet in Mictronics).",
        len(reg_icao_map),
        len(registrations),
    )

    count = 0
    batch: list[tuple[str, dict]] = []

    def _flush() -> None:
        keys = [k for k, _ in batch]
        existing_list = r.json().mget(keys, "$")
        pipe = r.pipeline()
        for (key, new_record), existing_raw in zip(batch, existing_list):
            merged = _deep_merge(existing_raw[0], new_record) if existing_raw else new_record
            pipe.json().set(key, "$", merged)
            pipe.expire(key, ttl)
        pipe.execute()

    for registration, icao_hex in reg_icao_map.items():
        group = groups[registration]
        record = _build_record(
            icao_hex=icao_hex,
            registration=registration,
            first_row=group["first_row"],
            names=group["names"],
            address_raw=group["address_raw"],
        )
        batch.append((icao_hex_key(icao_hex), record))
        count += 1
        if len(batch) == 10000:
            _flush()
            batch.clear()
            logger.info("  ... %d records written.", count)

    if batch:
        _flush()

    logger.info("Finished writing %d records to Redis.", count)
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

        import time
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
        "ids": "SkyFollower_runner_fr_dgac",
        "name": "SkyFollower France DGAC Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "France DGAC Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "France DGAC Last Run At", "mdi:clock", None, None),
        ("last_run_status", "France DGAC Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_fr_dgac_{name}",
            "object_id": f"SkyFollower_runner_fr_dgac_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_fr_dgac_{name}/config",
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

    status = "failure"
    records_imported = 0

    try:
        rows = download_registry(DOWNLOAD_URL)
        _ensure_search_index(r)
        records_imported = write_to_redis(rows, r, ttl)
        status = "success"
        logger.info("France DGAC runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("France DGAC runner failed: %s", exc, exc_info=True)

    finally:
        try:
            publish_completion_stats(cfg, records_imported, status)
        except Exception as exc:
            logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
