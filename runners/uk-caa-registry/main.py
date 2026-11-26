#!/usr/bin/env python3
"""
SkyFollower United Kingdom CAA Data Runner

Two jobs against the UK CAA G-INFO REST API, run as separate commands:

  search   searches every 2-letter registration prefix AA-ZZ and keeps one
           Redis queue record per registered aircraft
  details  fetches the aircraft whose queue records are due and writes
           their enrichment records to Redis

API base: https://ginfoapi.caa.co.uk  (no authentication required)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import signal
import sys
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from itertools import product
from typing import Optional

import paho.mqtt.client as mqtt
import redis as redis_lib
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from redis.commands.search.field import NumericField, TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from shared.config import ConfigError, ConfigLoader, load_config
from shared.timing import (
    UK_CAA_DETAILS_RUN_TIMEOUT_SECONDS,
    UK_CAA_DETAILS_REFRESH_INTERVAL_SECONDS,
    UK_CAA_DETAILS_RETRY_BACKOFF_SECONDS,
    UK_CAA_LOCK_POLL_INTERVAL_SECONDS,
    UK_CAA_LOCK_REFRESH_INTERVAL_SECONDS,
    UK_CAA_LOCK_TTL_SECONDS,
    UK_CAA_QUEUE_TTL_SECONDS,
    UK_CAA_REGISTRY_TTL_SECONDS,
    UK_CAA_SEARCH_RUN_TIMEOUT_SECONDS,
)
from shared.redis_client import build_redis_client
from shared.ha_discovery import build_ha_device
from shared.redis_keys import (
    AIRCRAFT_REGISTRY_SEARCH_INDEX,
    UK_CAA_LOCK_KEY,
    UK_CAA_QUEUE_KEY_PREFIX,
    UK_CAA_QUEUE_SEARCH_INDEX,
    UK_CAA_SEARCH_PROGRESS_KEY,
    aircraft_registry_key,
    aircraft_type_key,
    uk_caa_queue_key,
)
from shared.redis_json import set_json
from shared.mqtt import build_mqtt_client
from shared.mqtt_register import publish_register
from shared.logging_setup import configure_logging
from shared.country_flags import country_flag

logger = logging.getLogger("uk-caa-registry")

API_BASE = "https://ginfoapi.caa.co.uk"
MQTT_ROOT = "SkyFollower/runner/uk-caa-registry"

_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

DEFAULT_REQUEST_INTERVAL_SECONDS = 1.0
DEFAULT_DETAILS_LIMIT = 1000
_QUEUE_BATCH_SIZE = 100
# At most this many request attempts, refused ones and retries included, in
# any window of this many seconds.
_REQUEST_WINDOW_LIMIT = 25
_REQUEST_WINDOW_SECONDS = 65
_REFUSAL_STATUSES = (403, 429)
# The first wait runs a little past a minute so a per-minute limit has reset.
_REFUSAL_WAITS = (65, 120, 300, 600, 900)
_MAX_RETRY_AFTER_SECONDS = 3600
_TRANSIENT_WAITS = (2, 5, 15)
_MAX_DETAIL_FAILURE_RATIO = 0.01

# ---------------------------------------------------------------------------
# Decode tables
# ---------------------------------------------------------------------------

_AIRCRAFT_CATEGORIES: dict[str, str] = {
    "FIXED-WING LANDPLANE": "Land",
    "FIXED-WING SEAPLANE": "Sea",
    "FIXED-WING AMPHIBIAN": "Amphibian",
}

_AIRCRAFT_CLASSES: dict[str, str] = {
    "FIXED-WING LANDPLANE": "Airplane",
    "FIXED-WING SEAPLANE": "Airplane",
    "FIXED-WING AMPHIBIAN": "Airplane",
    "ROTORCRAFT": "Helicopter",
    "GLIDER": "Glider",
    "POWERED GLIDER": "Powered Glider",
    "MOTOR GLIDER": "Powered Glider",
    "FREE BALLOON": "Balloon",
    "AIRSHIP": "Airship",
    "MICROLIGHT AEROPLANE": "Microlight",
    "GYROPLANE": "Gyroplane",
    "POWERED LIFT": "Powered Lift",
    "TILT ROTOR": "Powered Lift",
}

_COUNTRY_NAMES: dict[str, str] = {
    "UNITED KINGDOM": "GB",
    "ENGLAND": "GB",
    "SCOTLAND": "GB",
    "WALES": "GB",
    "NORTHERN IRELAND": "GB",
    "AUSTRALIA": "AU",
    "AUSTRIA": "AT",
    "BAHAMAS": "BS",
    "BELGIUM": "BE",
    "BERMUDA": "BM",
    "BRAZIL": "BR",
    "CANADA": "CA",
    "CAYMAN ISLANDS": "KY",
    "CHINA": "CN",
    "CYPRUS": "CY",
    "CZECH REPUBLIC": "CZ",
    "DENMARK": "DK",
    "FINLAND": "FI",
    "FRANCE": "FR",
    "GERMANY": "DE",
    "GREECE": "GR",
    "GUERNSEY": "GG",
    "HONG KONG": "HK",
    "HUNGARY": "HU",
    "ICELAND": "IS",
    "INDIA": "IN",
    "IRELAND": "IE",
    "ISLE OF MAN": "IM",
    "ISRAEL": "IL",
    "ITALY": "IT",
    "JAPAN": "JP",
    "JERSEY": "JE",
    "KENYA": "KE",
    "LUXEMBOURG": "LU",
    "MALAYSIA": "MY",
    "MALTA": "MT",
    "NETHERLANDS": "NL",
    "NEW ZEALAND": "NZ",
    "NIGERIA": "NG",
    "NORWAY": "NO",
    "POLAND": "PL",
    "PORTUGAL": "PT",
    "QATAR": "QA",
    "SAUDI ARABIA": "SA",
    "SINGAPORE": "SG",
    "SOUTH AFRICA": "ZA",
    "SPAIN": "ES",
    "SWEDEN": "SE",
    "SWITZERLAND": "CH",
    "TURKEY": "TR",
    "UNITED ARAB EMIRATES": "AE",
    "UNITED STATES": "US",
    "UNITED STATES OF AMERICA": "US",
}


# ---------------------------------------------------------------------------
# Decode helpers
# ---------------------------------------------------------------------------

def _decode_aircraft_class(raw: str) -> Optional[str]:
    """Map UK CAA AircraftClass to canonical aircraft.type; pass through unknown values."""
    value = raw.strip().upper() if raw else ""
    if not value:
        return None
    return _AIRCRAFT_CLASSES.get(value, raw.strip().title() or None)


def _decode_aircraft_category(raw: str) -> Optional[str]:
    """Map UK CAA AircraftClass to canonical aircraft.category; returns None for non-fixed-wing."""
    value = raw.strip().upper() if raw else ""
    return _AIRCRAFT_CATEGORIES.get(value)


def _decode_country(raw: str) -> Optional[str]:
    """Map full English country name to ISO 3166-1 alpha-2; returns raw name if unmapped."""
    value = raw.strip().upper() if raw else ""
    if not value:
        return None
    return _COUNTRY_NAMES.get(value, raw.strip() or None)


def _parse_year_built(year: Optional[int]) -> Optional[str]:
    """Convert integer manufacture year to ISO 8601 UTC datetime string."""
    if year is None:
        return None
    try:
        return f"{int(year):04d}-01-01T00:00:00Z"
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Record builder
# ---------------------------------------------------------------------------

def _build_record(details: dict) -> Optional[dict]:
    """Build the aircraft:registry:{hex} enrichment record from a G-INFO details payload."""
    aircraft_details = details.get("AircraftDetails", {})

    icao_addr = aircraft_details.get("ICAO24BitAircraftAddress") or {}
    icao_hex = (icao_addr.get("Hex") or "").strip().upper()
    if len(icao_hex) != 6:
        return None

    mark = (details.get("RegistrationDetails", {}).get("Mark") or "").strip()
    if not mark:
        return None
    registration = f"G-{mark}"

    # aircraft sub-object
    max_pax = aircraft_details.get("MaximumPassengers")
    aircraft_class_raw = aircraft_details.get("AircraftClass", "")
    aircraft_fields: dict = {
        "type": _decode_aircraft_class(aircraft_class_raw),
        "category": _decode_aircraft_category(aircraft_class_raw),
        "manufacturer": (aircraft_details.get("Manufacturer") or "").strip() or None,
        "model": (aircraft_details.get("Type") or "").strip() or None,
        "serial_number": (aircraft_details.get("SerialNumber") or "").strip() or None,
        "type_designator": (aircraft_details.get("ICAOAircraftTypeDesignator") or "").strip() or None,
        "manufactured_date": _parse_year_built(aircraft_details.get("YearBuild")),
        "seats": int(max_pax) + 1 if max_pax is not None else None,
    }

    # powerplant sub-object — use first engine entry
    engines = aircraft_details.get("Engines") or []
    powerplant: Optional[dict] = None
    if engines:
        eng = engines[0]
        count = eng.get("TotalNumberOfEngines")
        name = (eng.get("Name") or "").strip() or None
        pp_fields: dict = {}
        if count is not None:
            pp_fields["count"] = count
        if name:
            pp_fields["model"] = name
        powerplant = pp_fields or None
    if powerplant:
        aircraft_fields["powerplant"] = powerplant

    aircraft: Optional[dict] = {k: v for k, v in aircraft_fields.items() if v is not None} or None

    # Uses the first registered owner only; AircraftOperatedByAocHolder (the
    # commercial AOC operator, which can differ from the owner) is not captured.
    owners = details.get("RegisteredAircraftOwners") or []
    registrant: Optional[dict] = None
    if owners:
        owner = owners[0]
        name = (owner.get("RegisteredOwner") or "").strip() or None
        street = [s for s in [
            (owner.get("Address1") or "").strip() or None,
            (owner.get("Address2") or "").strip() or None,
        ] if s]
        reg_fields = {
            "names": [name] if name else None,
            "street": street if street else None,
            "city": (owner.get("Town") or "").strip() or None,
            "administrative_area": (owner.get("County") or "").strip() or None,
            "postal_code": (owner.get("PostCode") or "").strip() or None,
            "country": _decode_country(owner.get("Country") or ""),
        }
        registrant = {k: v for k, v in reg_fields.items() if v is not None} or None

    record: dict = {"icao_hex": icao_hex, "registration": registration, "military": False}
    if aircraft:
        record["aircraft"] = aircraft
    if registrant:
        record["registrant"] = registrant
    return record


def _apply_type_lookup(record: dict, r: redis_lib.Redis) -> None:
    """Fill aircraft.manufacturer_model/description_code from aircraft:type:{type_designator}
    when found. Runs unconditionally since this runner's type_designator is
    authoritative regardless of Mictronics data; a failed or missing lookup leaves
    the record unchanged."""
    aircraft = record.get("aircraft")
    if not aircraft:
        return
    type_designator = aircraft.get("type_designator")
    if not type_designator:
        return
    try:
        type_doc = r.json().get(aircraft_type_key(type_designator))
    except Exception as exc:
        logger.warning("aircraft:type lookup failed for %s: %s", type_designator, exc)
        return
    if not type_doc:
        return
    manufacturer_model = (type_doc.get("manufacturer_model") or "").strip()
    if manufacturer_model:
        aircraft["manufacturer_model"] = manufacturer_model
    description_code = (type_doc.get("description_code") or "").strip()
    if description_code:
        aircraft["description_code"] = description_code


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


def _ensure_queue_index(r: redis_lib.Redis) -> None:
    """Create the details queue search index if it does not already exist."""
    try:
        r.ft(UK_CAA_QUEUE_SEARCH_INDEX).info()
    except Exception:
        r.ft(UK_CAA_QUEUE_SEARCH_INDEX).create_index(
            fields=[NumericField("$.next_fetch_at", as_name="next_fetch_at", sortable=True)],
            definition=IndexDefinition(prefix=[UK_CAA_QUEUE_KEY_PREFIX], index_type=IndexType.JSON),
        )
        logger.info("Created search index %r.", UK_CAA_QUEUE_SEARCH_INDEX)


# ---------------------------------------------------------------------------
# G-INFO API
# ---------------------------------------------------------------------------

class IncompleteRunError(Exception):
    """The job did not do all it was expected to."""


class RunStoppedError(IncompleteRunError):
    """No further CAA requests can be made in this run."""


class RateLimitedError(RunStoppedError):
    """CAA kept refusing a request after every wait."""


class LockLostError(RunStoppedError):
    """The other job may now be calling CAA."""


class DeadlineReached(RunStoppedError):
    """The run's maximum time is up."""


class _RunLock:
    """Redis lock held while a job calls CAA, refreshed from a thread so a
    long backoff cannot let it expire."""

    _REFRESH = (
        "if redis.call('GET', KEYS[1]) == ARGV[1] then "
        "return redis.call('EXPIRE', KEYS[1], ARGV[2]) else return 0 end"
    )
    _RELEASE = (
        "if redis.call('GET', KEYS[1]) == ARGV[1] then "
        "return redis.call('DEL', KEYS[1]) else return 0 end"
    )

    def __init__(self, r: redis_lib.Redis):
        self._r = r
        self._token = uuid.uuid4().hex
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.lost = False

    def acquire(self, deadline: float) -> bool:
        """Wait for the lock until `deadline`. False if it never came free."""
        waiting = False
        while not self._r.set(UK_CAA_LOCK_KEY, self._token, nx=True, ex=UK_CAA_LOCK_TTL_SECONDS):
            if time.monotonic() + UK_CAA_LOCK_POLL_INTERVAL_SECONDS >= deadline:
                return False
            if not waiting:
                logger.info("Waiting for the other uk-caa-registry job to finish.")
                waiting = True
            time.sleep(UK_CAA_LOCK_POLL_INTERVAL_SECONDS)
        self._thread = threading.Thread(target=self._keep_alive, daemon=True)
        self._thread.start()
        return True

    def _keep_alive(self) -> None:
        refreshed_at = time.monotonic()
        while not self._stop.wait(UK_CAA_LOCK_REFRESH_INTERVAL_SECONDS):
            try:
                if not self._r.eval(
                    self._REFRESH, 1, UK_CAA_LOCK_KEY, self._token, UK_CAA_LOCK_TTL_SECONDS
                ):
                    self.lost = True
                    return
                refreshed_at = time.monotonic()
            except Exception as exc:
                logger.warning("Could not refresh the run lock: %s", exc)
                if time.monotonic() - refreshed_at >= UK_CAA_LOCK_TTL_SECONDS:
                    self.lost = True
                    return

    def release(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        try:
            self._r.eval(self._RELEASE, 1, UK_CAA_LOCK_KEY, self._token)
        except Exception as exc:
            logger.warning("Could not release the run lock: %s", exc)


class _Client:
    """Paces every G-INFO request and waits out refusals, within the run's
    deadline."""

    def __init__(
        self,
        session: requests.Session,
        interval: float,
        deadline: float = float("inf"),
        lock: Optional[_RunLock] = None,
    ):
        self._session = session
        self._interval = interval
        self._deadline = deadline
        self._lock = lock
        self._accepted = 0
        self._sent: deque[float] = deque(maxlen=_REQUEST_WINDOW_LIMIT)

    def _wait_for_slot(self) -> None:
        """Wait until one more attempt keeps within the request window."""
        if len(self._sent) == self._sent.maxlen:
            remaining = self._sent[0] + _REQUEST_WINDOW_SECONDS - time.monotonic()
            if remaining > 0:
                self._sleep(remaining)

    def _sleep(self, seconds: float) -> None:
        if time.monotonic() + seconds >= self._deadline:
            raise DeadlineReached("Maximum run time reached")
        time.sleep(seconds)
        if self._lock is not None and self._lock.lost:
            raise LockLostError("Lost the run lock")

    def request(self, method: str, url: str, **kwargs):
        refusals = 0
        transients = 0
        waited = 0
        while True:
            self._sleep(self._interval)
            self._wait_for_slot()
            self._sent.append(time.monotonic())
            try:
                resp = getattr(self._session, method)(url, timeout=30, **kwargs)
                resp.raise_for_status()
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status in _REFUSAL_STATUSES:
                    if refusals == len(_REFUSAL_WAITS):
                        raise RateLimitedError(
                            f"Still refused with HTTP {status} after waiting {waited}s: {url}"
                        ) from exc
                    wait = max(_REFUSAL_WAITS[refusals], _retry_after_seconds(exc.response))
                    _log_refusal(exc.response, url, self._accepted, wait)
                    self._accepted = 0
                    refusals += 1
                    waited += wait
                elif status is not None and status >= 500 and transients < len(_TRANSIENT_WAITS):
                    wait = _TRANSIENT_WAITS[transients]
                    transients += 1
                else:
                    raise
            except (requests.ConnectionError, requests.Timeout):
                if transients == len(_TRANSIENT_WAITS):
                    raise
                wait = _TRANSIENT_WAITS[transients]
                transients += 1
            else:
                if refusals:
                    logger.info("Accepted again after waiting %ds.", waited)
                self._accepted += 1
                return resp
            self._sleep(wait)


def _retry_after_seconds(response) -> int:
    try:
        seconds = int(response.headers.get("Retry-After"))
    except (TypeError, ValueError):
        return 0
    return min(max(seconds, 0), _MAX_RETRY_AFTER_SECONDS)


def _log_refusal(response, url: str, accepted: int, wait: int) -> None:
    logger.warning(
        "HTTP %d from %s after %d accepted requests; waiting %ds. "
        "Retry-After=%s x-azure-ref=%s body=%r",
        response.status_code,
        url,
        accepted,
        wait,
        response.headers.get("Retry-After"),
        response.headers.get("x-azure-ref"),
        _page_text(str(response.text))[:300],
    )


def _page_text(body: str) -> str:
    body = re.sub(r"(?is)<(style|script)\b.*?</\1>|<!DOCTYPE[^>]*>|<[^>]+>", " ", body)
    return " ".join(body.split())


def _search_by_prefix(client: _Client, prefix: str) -> list[dict]:
    """POST /api/aircraft/search with a 2-letter registration prefix.

    Returns the raw result list; may include non-registered aircraft.
    """
    url = f"{API_BASE}/api/aircraft/search"
    logger.debug("Searching UK CAA G-INFO for prefix=%s at %s", prefix, url)
    results = client.request("post", url, json={"Registration": prefix}).json() or []
    if not isinstance(results, list):
        raise ValueError(f"Unexpected search response: {type(results).__name__}")
    return results


def _get_aircraft_details(client: _Client, aircraft_id: int) -> Optional[dict]:
    """GET /api/aircraft/details/{aircraft_id}. Returns the parsed JSON payload."""
    url = f"{API_BASE}/api/aircraft/details/{aircraft_id}"
    logger.debug("Fetching UK CAA aircraft details from %s", url)
    return client.request("get", url).json()


# ---------------------------------------------------------------------------
# Search job
# ---------------------------------------------------------------------------

def _prefixes_longest_waiting_first(r: redis_lib.Redis) -> list[str]:
    progress = r.hgetall(UK_CAA_SEARCH_PROGRESS_KEY)
    prefixes = ["".join(pair) for pair in product(_LETTERS, _LETTERS)]
    return sorted(prefixes, key=lambda p: (int(progress.get(p, 0)), p))


# One AircraftID is listed under every mark the aircraft has held, so only a
# non-registered result for the queued mark means it has been deregistered.
_DELETE_IF_SAME_MARK = (
    "local mark = redis.call('JSON.GET', KEYS[1], '.mark') "
    "if mark and cjson.decode(mark) == ARGV[1] then "
    "return redis.call('DEL', KEYS[1]) end return 0"
)


def _apply_search_results(r: redis_lib.Redis, results: list[dict], stats: dict) -> None:
    """Create or renew a queue record for each registered aircraft, and delete
    one whose own mark is no longer registered. Absence from the results
    changes nothing."""
    # Rows for one mark share a prefix, so they are always in the same result.
    registered = {
        (item.get("AircraftID"), (item.get("Mark") or "").strip())
        for item in results if item.get("RegistrationStatus") == "R"
    }
    pipe = r.pipeline()
    operations: list[Optional[str]] = []
    for item in results:
        aircraft_id = item.get("AircraftID")
        status = item.get("RegistrationStatus")
        if aircraft_id is None or not status:
            continue
        key = uk_caa_queue_key(aircraft_id)
        mark = (item.get("Mark") or "").strip()
        if status != "R":
            if (aircraft_id, mark) in registered:
                continue
            pipe.eval(_DELETE_IF_SAME_MARK, 1, key, mark)
            operations.append("aircraft_removed")
            continue
        record = {"aircraft_id": aircraft_id, "mark": mark, "next_fetch_at": 0}
        # NX keeps an existing record's next_fetch_at.
        set_json(pipe, key, record, nx=True)
        operations.append("aircraft_added")
        pipe.json().set(key, "$.mark", mark, xx=True)
        operations.append(None)
        pipe.expire(key, UK_CAA_QUEUE_TTL_SECONDS)
        operations.append(None)
        stats["aircraft_found"] += 1
    for operation, result in zip(operations, pipe.execute()):
        if operation and result:
            stats[operation] += 1


def run_search(client: _Client, r: redis_lib.Redis, stats: dict) -> None:
    """Search every prefix, updating the details queue. Raises
    IncompleteRunError unless every prefix succeeds."""
    for prefix in _prefixes_longest_waiting_first(r):
        try:
            results = _search_by_prefix(client, prefix)
        except RunStoppedError:
            raise
        except Exception as exc:
            logger.warning("Search error for prefix %s: %s", prefix, exc)
            stats["prefixes_failed"] += 1
            continue

        _apply_search_results(r, results, stats)
        r.hset(UK_CAA_SEARCH_PROGRESS_KEY, prefix, int(time.time()))
        stats["prefixes_searched"] += 1
        logger.info(
            "G-%s*: %d results. Run totals: %d prefixes searched, "
            "%d aircraft added, %d removed.",
            prefix, len(results), stats["prefixes_searched"],
            stats["aircraft_added"], stats["aircraft_removed"],
        )

    if stats["prefixes_failed"]:
        raise IncompleteRunError(f"{stats['prefixes_failed']} prefix searches failed")


# ---------------------------------------------------------------------------
# Details job
# ---------------------------------------------------------------------------

def _queue_query(minimum, maximum, offset: int, limit: int) -> Query:
    return (
        Query(f"@next_fetch_at:[{minimum} {maximum}]")
        .sort_by("next_fetch_at", asc=True)
        .paging(offset, limit)
    )


def _reschedule(r: redis_lib.Redis, key: str, delay: int) -> None:
    """Move a queue record's next fetch. A path update keeps the TTL, and XX
    leaves a deleted or expired record gone."""
    r.json().set(key, "$.next_fetch_at", int(time.time()) + delay, xx=True)


def _write_aircraft(r: redis_lib.Redis, record: dict) -> None:
    _apply_type_lookup(record, r)
    record["source"] = "uk-caa-registry"
    record["country_code"] = "GB"
    key = aircraft_registry_key(record["icao_hex"])
    pipe = r.pipeline()
    set_json(pipe, key, record)
    pipe.expire(key, UK_CAA_REGISTRY_TTL_SECONDS)
    pipe.execute()


def _process_aircraft(client: _Client, r: redis_lib.Redis, key: str, stats: dict) -> None:
    aircraft_id = int(key.rsplit(":", 1)[1])
    try:
        details = _get_aircraft_details(client, aircraft_id)
        status = (details.get("RegistrationDetails") or {}).get("Status", "")
        record = _build_record(details) if status == "Registered" else None
    except RunStoppedError:
        raise
    except Exception as exc:
        logger.warning("Error fetching details for AircraftID=%d: %s", aircraft_id, exc)
        stats["requests_attempted"] += 1
        stats["requests_failed"] += 1
        _reschedule(r, key, UK_CAA_DETAILS_RETRY_BACKOFF_SECONDS)
        return

    stats["requests_attempted"] += 1
    if record is not None:
        _write_aircraft(r, record)
        stats["records_imported"] += 1
    else:
        logger.info(
            "AircraftID=%d: no record written (status %r%s).", aircraft_id, status,
            ", no ICAO address or mark" if status == "Registered" else "",
        )
    _reschedule(r, key, UK_CAA_DETAILS_REFRESH_INTERVAL_SECONDS)


def run_details(client: _Client, r: redis_lib.Redis, limit: int, stats: dict) -> None:
    """Fetch up to `limit` due aircraft, longest-due first. Raises
    IncompleteRunError if nothing due could be fetched or too many requests
    failed."""
    cutoff = int(time.time())
    seen: set[str] = set()
    try:
        while len(seen) < limit:
            # Offset stays 0: each processed record leaves the filter.
            query = _queue_query(
                "-inf", cutoff, 0, min(_QUEUE_BATCH_SIZE, limit - len(seen))
            ).no_content()
            docs = r.ft(UK_CAA_QUEUE_SEARCH_INDEX).search(query).docs
            keys = [doc.id for doc in docs if doc.id not in seen]
            if not keys:
                break
            for key in keys:
                _process_aircraft(client, r, key, stats)
                seen.add(key)
                if len(seen) % _QUEUE_BATCH_SIZE == 0:
                    logger.info(
                        "%d aircraft processed (%d written, %d failed).",
                        len(seen), stats["records_imported"], stats["requests_failed"],
                    )
    except DeadlineReached:
        if stats["requests_attempted"] == stats["requests_failed"]:
            raise
        logger.info("Maximum run time reached; the rest stay queued.")

    attempted, failed = stats["requests_attempted"], stats["requests_failed"]
    if attempted and failed == attempted:
        raise IncompleteRunError(f"All {attempted} requests failed")
    if failed > attempted * _MAX_DETAIL_FAILURE_RATIO:
        raise IncompleteRunError(f"{failed} of {attempted} requests failed")


def queue_statistics(r: redis_lib.Redis) -> dict:
    """Queue counts for reporting, computed by Redis without returning the
    records. Failed fetches are rescheduled, so oldest_overdue_days measures
    a backlog the limit or rate cannot clear."""
    index = r.ft(UK_CAA_QUEUE_SEARCH_INDEX)
    now = int(time.time())

    def _count(query: str) -> int:
        return index.search(Query(query).paging(0, 0)).total

    oldest = index.search(
        _queue_query(1, "+inf", 0, 1).return_field("next_fetch_at")
    ).docs
    overdue = max(0, now - int(float(oldest[0].next_fetch_at))) if oldest else 0
    return {
        "queue_size": _count("*"),
        "aircraft_due": _count(f"@next_fetch_at:[-inf {now}]"),
        "aircraft_never_fetched": _count("@next_fetch_at:[0 0]"),
        "oldest_overdue_days": overdue // 86400,
    }


# ---------------------------------------------------------------------------
# MQTT
# ---------------------------------------------------------------------------

# job -> (statistic, friendly name, icon, state_class)
_STATISTICS: dict[str, tuple] = {
    "search": (
        ("prefixes_searched", "Prefixes Searched", "mdi:magnify", "measurement"),
        ("prefixes_failed", "Prefixes Failed", "mdi:alert-circle", "measurement"),
        ("aircraft_found", "Aircraft Found", "mdi:airplane-search", "measurement"),
        ("aircraft_added", "Aircraft Added", "mdi:airplane-plus", "measurement"),
        ("aircraft_removed", "Aircraft Removed", "mdi:airplane-minus", "measurement"),
    ),
    "details": (
        ("records_imported", "Records Imported", "mdi:airplane", "measurement"),
        ("requests_failed", "Requests Failed", "mdi:alert-circle", "measurement"),
        ("queue_size", "Queue Size", "mdi:tray-full", "measurement"),
        ("aircraft_due", "Aircraft Due", "mdi:timer-sand", "measurement"),
        ("aircraft_never_fetched", "Aircraft Never Fetched", "mdi:help-circle", "measurement"),
        ("oldest_overdue_days", "Oldest Overdue Days", "mdi:calendar-alert", "measurement"),
    ),
}
_RUN_STATISTICS = (
    ("last_run_at", "Last Run At", "mdi:clock", None),
    ("last_run_status", "Last Run Status", "mdi:check-circle", None),
)
# Published by the single weekly job these two jobs replaced.
_RETIRED_STATISTICS = ("records_imported", "last_run_at", "last_run_status")


def publish_completion_stats(cfg: dict, job: str, stats: dict, status: str) -> None:
    """Publish completion statistics to MQTT."""
    mc = cfg.get("mqtt")
    if not mc or not mc.get("host"):
        logger.info("No MQTT config; skipping stats publish.")
        return

    run_at = datetime.now(timezone.utc).isoformat()

    client = build_mqtt_client(mc)
    connected = False

    refused = None

    def _on_connect(c, userdata, flags, reason_code, properties):
        nonlocal connected, refused
        if getattr(reason_code, "is_failure", False):
            refused = reason_code
        else:
            connected = True

    client.on_connect = _on_connect

    try:
        client.connect(mc["host"], port=mc.get("port", 1883), keepalive=60)
        client.loop_start()

        deadline = time.monotonic() + 5
        while not connected and refused is None and time.monotonic() < deadline:
            time.sleep(0.05)

        if not connected:
            if refused is not None:
                logger.warning("MQTT connection refused (%s); skipping stats publish.", refused)
            else:
                logger.warning("MQTT connect timed out; skipping stats publish.")
            client.loop_stop()
            return

        base = f"{MQTT_ROOT}/{job}/statistic"
        for name, *_ in _STATISTICS[job]:
            if name in stats:
                client.publish(f"{base}/{name}", str(stats[name]), retain=True)
        client.publish(f"{base}/last_run_at", run_at, retain=True)
        client.publish(f"{base}/last_run_status", status.capitalize(), retain=True)

        _publish_ha_autodiscovery(client, job)
        _clear_retired_topics(client)

        time.sleep(0.5)
        client.loop_stop()
        client.disconnect()
        logger.info("MQTT stats published (job=%s, status=%s).", job, status)

    except Exception as exc:
        logger.warning("MQTT publish failed: %s", exc)
        try:
            client.loop_stop()
        except Exception:
            pass


def _publish_ha_autodiscovery(client: mqtt.Client, job: str) -> None:
    device = build_ha_device(
        identifier="SkyFollower_runner_uk_caa_registry",
        name=f"SkyFollower United Kingdom {country_flag('GB')} CAA Registry Runner",
        model=f"United Kingdom {country_flag('GB')} CAA Registry Runner",
        configuration_url="https://brentio.github.io/SkyFollower/runners/uk-caa-registry.html",
    )
    publish_register(client, device)
    for name, label, icon, state_class in _STATISTICS[job] + _RUN_STATISTICS:
        unique_id = f"SkyFollower_runner_uk_caa_registry_{job}_{name}"
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/{job}/statistic/{name}",
            "name": f"United Kingdom CAA {job.title()} {label}",
            "unique_id": unique_id,
            "object_id": unique_id,
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if name == "last_run_at":
            payload["device_class"] = "timestamp"
        client.publish(
            f"homeassistant/sensor/{unique_id}/config", json.dumps(payload), retain=True
        )


def _clear_retired_topics(client: mqtt.Client) -> None:
    """An empty retained payload removes the topic, and with it the Home
    Assistant entity."""
    for name in _RETIRED_STATISTICS:
        client.publish(
            f"homeassistant/sensor/SkyFollower_runner_uk_caa_registry_{name}/config",
            "", retain=True,
        )
        client.publish(f"{MQTT_ROOT}/statistic/{name}", "", retain=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_job(
    job: str,
    session: requests.Session,
    r: redis_lib.Redis,
    request_interval: float,
    details_limit: int,
) -> tuple[str, dict]:
    """Run one job under the lock. Returns (status, statistics)."""
    max_runtime = (
        UK_CAA_SEARCH_RUN_TIMEOUT_SECONDS if job == "search"
        else UK_CAA_DETAILS_RUN_TIMEOUT_SECONDS
    )
    deadline = time.monotonic() + max_runtime
    stats = {name: 0 for name, *_ in _STATISTICS[job]}
    if job == "details":
        stats = {"records_imported": 0, "requests_failed": 0, "requests_attempted": 0}
    status = "failure"
    try:
        _ensure_search_index(r)
        _ensure_queue_index(r)
        lock = _RunLock(r)
        if not lock.acquire(deadline):
            raise IncompleteRunError("The other uk-caa-registry job did not finish in time")
        try:
            client = _Client(session, request_interval, deadline, lock)
            if job == "search":
                run_search(client, r, stats)
            else:
                run_details(client, r, details_limit, stats)
        finally:
            lock.release()
        status = "success"
        logger.info("UK CAA %s completed successfully: %s", job, stats)
    except IncompleteRunError as exc:
        logger.error("UK CAA %s incomplete: %s (%s)", job, exc, stats)
    except Exception as exc:
        logger.error("UK CAA %s failed: %s", job, exc, exc_info=True)

    if job == "details":
        try:
            stats.update(queue_statistics(r))
            logger.info("Queue: %s", {name: stats[name] for name in (
                "queue_size", "aircraft_due", "aircraft_never_fetched", "oldest_overdue_days",
            )})
        except Exception as exc:
            logger.warning("Could not read queue statistics: %s", exc)
    return status, stats


def _exit_on_sigterm(signum, frame) -> None:
    # Unwinds the stack so a stopped run releases its lock.
    sys.exit(128 + signum)


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="UK CAA registry runner")
    parser.add_argument("job", choices=sorted(_STATISTICS))
    job = parser.parse_args(argv).job

    try:
        cfg = load_config("redis", "mqtt")
    except ConfigError as exc:
        configure_logging()
        logger.critical("%s", exc)
        sys.exit(1)

    configure_logging(cfg.get("log_level"))

    loader = ConfigLoader()
    request_interval = loader.number("REQUEST_INTERVAL_SECONDS", DEFAULT_REQUEST_INTERVAL_SECONDS)
    details_limit = loader.integer("DETAILS_LIMIT", DEFAULT_DETAILS_LIMIT)
    try:
        loader.raise_for_problems()
    except ConfigError as exc:
        logger.critical("%s", exc)
        sys.exit(1)

    r = build_redis_client(cfg["redis"])
    signal.signal(signal.SIGTERM, _exit_on_sigterm)
    session = requests.Session()
    session.headers.update({"User-Agent": "P5Software SkyFollower"})

    try:
        status, stats = run_job(job, session, r, request_interval, details_limit)
    finally:
        session.close()

    try:
        publish_completion_stats(cfg, job, stats, status)
    except Exception as exc:
        logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
