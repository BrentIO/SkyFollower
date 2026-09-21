#!/usr/bin/env python3
"""
SkyFollower US FAA Telephony Designators Data Runner

Backfills operator:{designator} records (operator/airline name, country,
radio telephony callsign) for designators FAA Order 7340.2 assigns that
Mictronics' operators.json does not cover -- e.g. KM Malta Airlines (KMM),
AJet (TKJ), Bermudair (BMA), or a purely US special-use callsign like NASA
or FEMA. Mictronics stays the primary/preferred source (better name
capitalization, actively maintained); this runner only ever fills a gap,
never corrects or overrides an existing entry.

Two independent FAA sources, both a single fixed-URL HTML page (no
discovery step, no pagination):

  Section 3 -- "Three-Letter Designator/Aircraft Company/Telephony
  Decode": the standard ICAO 3-letter airline company designator table.
  The page paginates this table into ~26 separate <table> elements (same
  repeated header row), not one -- every matching table must be parsed and
  combined, not just the first found.

  Section 4 -- "U.S. Special Telephony/Call Signs": government/agency/
  special-use callsigns. The "Identifier" column (the actual callsign
  prefix a flight files under) is NOT always 3 letters (e.g. ARSIX, NASA,
  FEMA) -- message-processor's _enrich_operator() already extracts the
  ident prefix as "letters before the first digit" with no fixed-length
  assumption, so this needs no downstream change. Not a column on this
  table: country -- every entry is by definition US-issued, so it's
  defaulted to "United States" here. Some rows carry a real Expiration
  Date; a row whose expiration has already passed is skipped at runtime
  (checked against the actual date each run, not a hardcoded cutoff).

Additive-only, enforced by Redis itself: every write uses
shared.redis_json.set_json(..., nx=True), i.e. Redis JSON.SET's native NX
option -- "set only if the key does not already exist" as a single atomic
command, not a check-then-set race. This runner never updates or removes
an existing operator:{designator} record, regardless of which runner (or
a prior run of this one) wrote it. Schedule this runner AFTER mictronics
(same dependency convention as bz-bdca-registry/is-samgongustofa-registry)
-- correctness falls out of ordering, not a merge script: mictronics
already overwrites operator:{designator} unconditionally on every run, so
if it ever gains a designator this runner backfilled, mictronics' own
next scheduled run simply replaces it.

Deliberately no TTL on the records this runner writes, unlike most other
enrichment keys. A given designator is written at most once ever -- every
run after that first write finds the key already present and NX skips it,
so the key's TTL clock (if it had one) would never get refreshed. With
the usual ENRICHMENT_TTL_SECONDS (14 days) and this runner's weekly
cadence, the record would still expire and vanish from Redis between
runs, then reappear on the next run -- a real, avoidable gap this
additive-only design has no reason to accept for what is a static,
FAA-published reference table, not perishable per-run enrichment data.

Data sources:
  https://www.faa.gov/air_traffic/publications/atpubs/cnt_html/chap3_section_3.html
  https://www.faa.gov/air_traffic/publications/atpubs/cnt_html/chap3_section_4.html
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.config import ConfigError, load_config
from shared.redis_client import build_redis_client
from shared.ha_discovery import build_ha_device
from shared.redis_keys import operator_key
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client
from shared.mqtt_register import publish_register
from shared.logging_setup import configure_logging
from shared.country_flags import country_flag

logger = logging.getLogger("us-faa-telephony-designators")

SECTION_3_URL = "https://www.faa.gov/air_traffic/publications/atpubs/cnt_html/chap3_section_3.html"
SECTION_4_URL = "https://www.faa.gov/air_traffic/publications/atpubs/cnt_html/chap3_section_4.html"
MQTT_ROOT = "SkyFollower/runner/us-faa-telephony-designators"

# Soft hyphen (U+00AD) shows up inside Section 4's wrapped header cell text
# (e.g. "Expiration\xadDate") -- stripped before matching header text.
_SOFT_HYPHEN = "­"


# ---------------------------------------------------------------------------
# Download + parse -- Section 3
# ---------------------------------------------------------------------------

def download_section_3(session: requests.Session) -> list[dict]:
    """Fetch and parse every 3-letter-designator table on the Section 3
    page. The page splits the one logical table across ~26 <table>
    elements (each with the same repeated header row) -- every matching
    table is parsed and combined, not just the first found."""
    logger.info("Downloading FAA 7340.2 Chapter 3 Section 3 from %s", SECTION_3_URL)
    resp = session.get(SECTION_3_URL, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Section 3 download failed with HTTP {resp.status_code}")

    soup = BeautifulSoup(resp.text, "lxml")

    rows: list[dict] = []
    matched_tables = 0
    for table in soup.find_all("table"):
        first_row = table.find("tr")
        if not first_row:
            continue
        header_text = first_row.get_text(" ", strip=True)
        if "Telephony" not in header_text or "Company" not in header_text:
            continue
        matched_tables += 1
        for tr in table.find_all("tr")[1:]:
            cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
            if len(cells) != 4:
                continue
            designator, company, country, callsign = cells
            if not designator or not company:
                continue
            rows.append({
                "designator": designator,
                "company": company,
                "country": country or None,
                "callsign": callsign or None,
            })

    if matched_tables == 0:
        raise RuntimeError("Could not find any Section 3 designator table on the FAA page.")

    logger.info("Parsed %d rows across %d Section 3 tables.", len(rows), matched_tables)
    return rows


# ---------------------------------------------------------------------------
# Download + parse -- Section 4
# ---------------------------------------------------------------------------

def download_section_4(session: requests.Session) -> list[dict]:
    """Fetch and parse the Section 4 special-telephony/call-sign table."""
    logger.info("Downloading FAA 7340.2 Chapter 3 Section 4 from %s", SECTION_4_URL)
    resp = session.get(SECTION_4_URL, timeout=60)
    if resp.status_code != 200:
        raise RuntimeError(f"Section 4 download failed with HTTP {resp.status_code}")

    soup = BeautifulSoup(resp.text, "lxml")

    table = None
    for candidate in soup.find_all("table"):
        first_row = candidate.find("tr")
        if not first_row:
            continue
        header_text = first_row.get_text(" ", strip=True).replace(_SOFT_HYPHEN, "")
        if "Identifier" in header_text and "Telephony" in header_text:
            table = candidate
            break

    if table is None:
        raise RuntimeError("Could not find the Section 4 special-telephony table on the FAA page.")

    rows: list[dict] = []
    for tr in table.find_all("tr")[1:]:
        cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
        if len(cells) != 4:
            continue
        callsign, identifier, company, expiration = cells
        if not identifier or not company:
            continue
        rows.append({
            "designator": identifier,
            "company": company,
            "callsign": callsign or None,
            "expiration": expiration,
        })

    logger.info("Parsed %d rows from Section 4.", len(rows))
    return rows


# ---------------------------------------------------------------------------
# Expiration handling (Section 4 only)
# ---------------------------------------------------------------------------

def is_expired(expiration: str, today: Optional[datetime] = None) -> bool:
    """True if `expiration` (FAA's "D-Mon-YYYY" format, or "N/A" for
    permanent) names a date strictly before `today` (defaults to the
    real current UTC date)."""
    if not expiration or expiration.strip().upper() == "N/A":
        return False
    try:
        expiry_date = datetime.strptime(expiration.strip(), "%d-%b-%Y").replace(tzinfo=timezone.utc)
    except ValueError:
        logger.warning("Could not parse expiration date %r; treating as not expired.", expiration)
        return False
    reference = today or datetime.now(timezone.utc)
    return expiry_date.date() < reference.date()


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def build_record(row: dict, *, default_country: Optional[str] = None) -> dict:
    """Build an operator:{designator} record from a parsed Section 3 or
    Section 4 row. `default_country` is used only when the row itself
    carries no country (Section 4 has no country column at all)."""
    record: dict = {"airline_designator": row["designator"].strip().upper()}
    company = row.get("company", "").strip()
    if company:
        record["name"] = company
    country = (row.get("country") or default_country)
    if country:
        record["country"] = country.strip() if isinstance(country, str) else country
    callsign = row.get("callsign")
    if callsign:
        record["callsign"] = callsign.strip()
    return record


# ---------------------------------------------------------------------------
# Write to Redis (additive-only)
# ---------------------------------------------------------------------------

def write_to_redis(rows: list[dict], r: redis_lib.Redis) -> int:
    """Write operator:{designator} for every row, only when that key does
    not already exist. Returns the count of designators actually newly
    written (not the count attempted -- a row skipped because the key
    already exists doesn't count)."""
    written = 0
    skipped_existing = 0
    for row in rows:
        key = operator_key(row["airline_designator"])
        result = set_json(r, key, row, nx=True)
        if result:
            written += 1
        else:
            skipped_existing += 1

    logger.info(
        "Finished: %d designators newly written, %d already present (left untouched).",
        written,
        skipped_existing,
    )
    return written


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
        identifier="SkyFollower_runner_us_faa_telephony_designators",
        name=f"SkyFollower US {country_flag('US')} FAA Telephony Designators Runner",
        model=f"US {country_flag('US')} FAA Telephony Designators Runner",
        configuration_url="https://brentio.github.io/SkyFollower/runners/us-faa-telephony-designators.html",
    )
    publish_register(client, device)
    stats = [
        ("records_imported", "US FAA Telephony Designators Records Imported", "mdi:radio-handheld", "total_increasing", None),
        ("last_run_at", "US FAA Telephony Designators Last Run At", "mdi:clock", None, None),
        ("last_run_status", "US FAA Telephony Designators Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_runner_us_faa_telephony_designators_{name}",
            "object_id": f"SkyFollower_runner_us_faa_telephony_designators_{name}",
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
            f"homeassistant/sensor/SkyFollower_runner_us_faa_telephony_designators_{name}/config",
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

    session = requests.Session()
    session.headers.update({"User-Agent": "P5Software SkyFollower"})

    status = "failure"
    records_imported = 0

    try:
        section_3_rows = download_section_3(session)
        section_4_rows_raw = download_section_4(session)

        now = datetime.now(timezone.utc)
        skipped_expired = 0
        section_4_rows = []
        for row in section_4_rows_raw:
            if is_expired(row["expiration"], now):
                skipped_expired += 1
                continue
            section_4_rows.append(row)
        if skipped_expired:
            logger.info("Skipped %d expired Section 4 row(s).", skipped_expired)

        records = [build_record(row) for row in section_3_rows]
        records += [build_record(row, default_country="United States") for row in section_4_rows]

        records_imported = write_to_redis(records, r)
        status = "success"
        logger.info(
            "US FAA telephony designators runner completed successfully. Newly written: %d",
            records_imported,
        )

    except Exception as exc:
        logger.error("US FAA telephony designators runner failed: %s", exc, exc_info=True)

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
