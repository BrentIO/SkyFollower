#!/usr/bin/env python3
"""
SkyFollower Norway CAA Data Runner

Downloads the Norwegian Civil Aviation Authority aircraft register JSON,
normalises fields into the aircraft:registry:{hex} enrichment shape, writes
to Redis with a 14-day TTL, publishes MQTT completion stats, then exits.

Data source: https://data.caa.no/nlr/norgesluftfartoyregister.json
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType

from shared.redis_keys import AIRCRAFT_REGISTRY_SEARCH_INDEX, aircraft_registry_key

logger = logging.getLogger("no-caa")

DOWNLOAD_URL = "https://data.caa.no/nlr/norgesluftfartoyregister.json"
REDIS_TTL = 14 * 86400
MQTT_ROOT = "SkyFollower/runner/no-caa"

# ---------------------------------------------------------------------------
# Decode tables
# ---------------------------------------------------------------------------

# Kategori → aircraft.type
_AIRCRAFT_TYPES: dict[str, str] = {
    "Fly": "Airplane",
    "Helikopter": "Helicopter",
    "Seilfly": "Glider",
    "Ballong": "Balloon",
}

# Land (owner country) → ISO 3166-1 alpha-2
_COUNTRY_NAMES: dict[str, str] = {
    # Norwegian names
    "Norge": "NO",
    "Sverige": "SE",
    "Danmark": "DK",
    # English names present in the dataset
    "Afghanistan": "AF",
    "Belgium": "BE",
    "Canada": "CA",
    "Cayman Islands": "KY",
    "Czech Republic": "CZ",
    "Estonia": "EE",
    "Finland": "FI",
    "France": "FR",
    "Georgia": "GE",
    "Germany": "DE",
    "Ireland": "IE",
    "Italy": "IT",
    "Japan": "JP",
    "Latvia": "LV",
    "Lithuania": "LT",
    "Luxembourg": "LU",
    "Netherlands": "NL",
    "Poland": "PL",
    "Portugal": "PT",
    "Romania": "RO",
    "Singapore": "SG",
    "Slovenia": "SI",
    "Spain": "ES",
    "Switzerland": "CH",
    "United Kingdom": "GB",
    "United States of America": "US",
}


# ---------------------------------------------------------------------------
# Decode helpers
# ---------------------------------------------------------------------------

def _decode_aircraft_type(raw: str) -> Optional[str]:
    """Map Norwegian Kategori to canonical aircraft.type; pass through unknown values."""
    value = raw.strip()
    if not value:
        return None
    return _AIRCRAFT_TYPES.get(value, value)


def _decode_country(raw: str) -> Optional[str]:
    """Map country name to ISO 3166-1 alpha-2 code, returning raw name if unmapped."""
    value = raw.strip()
    if not value:
        return None
    return _COUNTRY_NAMES.get(value, value)


def _extract_icao_hex(icao_array: list) -> Optional[str]:
    """Extract the Heksadesimal value from the ICAO 24-bits adresse array."""
    for entry in icao_array:
        if "Heksadesimal" in entry:
            value = entry["Heksadesimal"].strip().upper()
            return value if value else None
    return None


def _parse_manufactured_date(byggeaar: str) -> Optional[str]:
    """Convert a 4-digit build year to ISO 8601 UTC datetime string."""
    value = byggeaar.strip()
    if not value:
        return None
    try:
        year = int(value)
        return f"{year:04d}-01-01T00:00:00Z"
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(row: dict) -> Optional[dict]:
    """Build the aircraft:registry:{hex} JSON record from a registry entry. Returns None if no ICAO hex."""
    icao_hex = _extract_icao_hex(row.get("ICAO 24-bits adresse") or [])
    if not icao_hex:
        return None

    registration = row.get("Registreringsmerke", "").strip() or None

    # aircraft sub-object
    aircraft_type = _decode_aircraft_type(row.get("Kategori", ""))
    manufacturer = row.get("Produsent", "").strip() or None
    model = row.get("Type", "").strip() or None
    serial_number = row.get("Serienummer", "").strip() or None
    manufactured_date = _parse_manufactured_date(row.get("Byggeår", ""))

    aircraft: Optional[dict] = None
    if any([aircraft_type, manufacturer, model, serial_number, manufactured_date]):
        aircraft = {k: v for k, v in {
            "type": aircraft_type,
            "manufacturer": manufacturer,
            "model": model,
            "serial_number": serial_number,
            "manufactured_date": manufactured_date,
        }.items() if v is not None}

    # registrant sub-object — use Eier/Kontakt entry for address; collect all names
    owners = row.get("Eier(e)") or []
    contact = next((o for o in owners if o.get("Eier type") == "Eier/Kontakt"), None)

    # Names: Eier/Kontakt first, then all other Eier entries
    names: list[str] = []
    if contact and contact.get("Navn"):
        names.append(contact["Navn"].strip())
    for o in owners:
        if o.get("Eier type") != "Eier/Kontakt" and o.get("Navn"):
            name = o["Navn"].strip()
            if name not in names:
                names.append(name)

    registrant: Optional[dict] = None
    if contact or names:
        registrant = {}
        if names:
            registrant["names"] = names
        if contact:
            street = contact.get("Gateadresse", "").strip()
            if street:
                registrant["street"] = [street]
            city = contact.get("Poststed", "").strip() or None
            if city:
                registrant["city"] = city
            postal_code = contact.get("Postnummer", "").strip() or None
            if postal_code:
                registrant["postal_code"] = postal_code
            country = _decode_country(contact.get("Land", ""))
            if country:
                registrant["country"] = country

    record: dict = {"icao_hex": icao_hex, "registration": registration}
    if aircraft:
        record["aircraft"] = aircraft
    if registrant:
        record["registrant"] = registrant

    return record


# ---------------------------------------------------------------------------
# Search index
# ---------------------------------------------------------------------------

def _ensure_search_index(r: redis_lib.Redis) -> None:
    """Create the aircraft detail JSON search index if it does not already exist."""
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
# Download
# ---------------------------------------------------------------------------

def download_registry(url: str) -> list[dict]:
    """Download the Norway CAA aircraft register JSON and return the data array."""
    logger.info("Downloading Norway CAA aircraft register from %s", url)
    response = requests.get(url, timeout=120, headers={"User-Agent": "P5Software SkyFollower"})
    if response.status_code != 200:
        raise RuntimeError(f"Download failed with HTTP {response.status_code}")
    logger.info("Download complete (%d bytes).", len(response.content))
    payload = response.json()
    records = payload.get("data", [])
    logger.info("Parsed %d records.", len(records))
    return records


# ---------------------------------------------------------------------------
# Write to Redis
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis, ttl: int) -> int:
    """Build records from registry rows and write to Redis. Returns count of records written."""
    count = 0
    batch: list[tuple[str, dict]] = []

    def _flush() -> None:
        pipe = r.pipeline()
        for key, record in batch:
            pipe.json().set(key, "$", record)
            pipe.expire(key, ttl)
        pipe.execute()

    for row in rows:
        record = _build_record(row)
        if record is None:
            continue
        record["source"] = "no-caa"
        key = aircraft_registry_key(record["icao_hex"])
        batch.append((key, record))
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
        "ids": "SkyFollower_runner_no_caa",
        "name": "SkyFollower Norway CAA Runner",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("records_imported", "Norway CAA Records Imported", "mdi:airplane", "total_increasing", None),
        ("last_run_at", "Norway CAA Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Norway CAA Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_no_caa_{name}",
            "object_id": f"SkyFollower_runner_no_caa_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_no_caa_{name}/config",
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
        logger.info("Norway CAA runner completed successfully. Records imported: %d", records_imported)

    except Exception as exc:
        logger.error("Norway CAA runner failed: %s", exc, exc_info=True)

    finally:
        try:
            publish_completion_stats(cfg, records_imported, status)
        except Exception as exc:
            logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
