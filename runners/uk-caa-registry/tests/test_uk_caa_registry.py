"""
Tests for the UK CAA data runner.

Covers:
- Decode helpers (aircraft class, country, year built)
- Record builder (_build_record)
- Search index helper (_ensure_search_index) — mocked
- API helpers (_search_by_prefix, _get_aircraft_details) — mocked
- Request pacing and refusal handling — mocked
- Search and details jobs, queue statistics — against real Redis Stack
  (skipped when none is reachable on REDIS_HOST/REDIS_PORT)
- MQTT completion stats — mocked
"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import sys
import time
from unittest.mock import MagicMock, patch

import redis
import requests

import pytest

# ---------------------------------------------------------------------------
# Module import helper
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
_RUNNER_DIR = os.path.dirname(_HERE)        # runners/uk-caa-registry/
_REPO_ROOT = os.path.abspath(os.path.join(_RUNNER_DIR, "..", ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "uk_caa_registry_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["uk_caa_registry_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()


_decode_aircraft_class = _mod._decode_aircraft_class
_decode_aircraft_category = _mod._decode_aircraft_category
_decode_country = _mod._decode_country
_parse_year_built = _mod._parse_year_built
_build_record = _mod._build_record
_apply_type_lookup = _mod._apply_type_lookup
_search_by_prefix = _mod._search_by_prefix
_get_aircraft_details = _mod._get_aircraft_details
run_search = _mod.run_search
run_details = _mod.run_details
DeadlineReached = _mod.DeadlineReached
IncompleteRunError = _mod.IncompleteRunError
RateLimitedError = _mod.RateLimitedError


def _client(session):
    return _mod._Client(session, 0)


def _ok(payload):
    resp = MagicMock()
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def _http_error(status, headers=None):
    error = MagicMock(status_code=status, headers=headers or {}, text="blocked")
    resp = MagicMock()
    resp.raise_for_status.side_effect = requests.HTTPError(response=error)
    return resp
publish_completion_stats = _mod.publish_completion_stats
uk_caa_queue_key = _mod.uk_caa_queue_key
UK_CAA_QUEUE_SEARCH_INDEX = _mod.UK_CAA_QUEUE_SEARCH_INDEX
UK_CAA_SEARCH_PROGRESS_KEY = _mod.UK_CAA_SEARCH_PROGRESS_KEY
UK_CAA_LOCK_KEY = _mod.UK_CAA_LOCK_KEY
_QUEUE_TTL = _mod.UK_CAA_QUEUE_TTL_SECONDS
_REGISTRY_TTL = _mod.UK_CAA_REGISTRY_TTL_SECONDS
_REFRESH = _mod.UK_CAA_DETAILS_REFRESH_INTERVAL_SECONDS
MQTT_ROOT = _mod.MQTT_ROOT


# ---------------------------------------------------------------------------
# Sample API fixtures
# ---------------------------------------------------------------------------

def _make_details(
    mark="VAHH",
    icao_hex="406B48",
    manufacturer="BOEING COMPANY",
    aircraft_type="BOEING 787-9",
    serial="37967",
    type_designator="B789",
    aircraft_class="FIXED-WING LANDPLANE",
    year_built=2014,
    maximum_passengers=296,
    engine_count=2,
    engine_name="ROLLS-ROYCE Trent 1000-K2",
    owner_name="VIRGIN ATLANTIC AIRWAYS LTD",
    address1="THE VHQ",
    address2="FLEMING WAY",
    town="CRAWLEY",
    county=None,
    postcode="RH10 9DF",
    country="UNITED KINGDOM",
    reg_status="Registered",
) -> dict:
    return {
        "RegistrationDetails": {
            "Mark": mark,
            "Status": reg_status,
        },
        "AircraftDetails": {
            "Manufacturer": manufacturer,
            "Type": aircraft_type,
            "SerialNumber": serial,
            "ICAO24BitAircraftAddress": {
                "Binary": "0100_0000_0110_1011_0100_1000",
                "Hex": icao_hex,
                "Octal": "20065510",
            },
            "ICAOAircraftTypeDesignator": type_designator,
            "AircraftClass": aircraft_class,
            "YearBuild": year_built,
            "MaximumPassengers": maximum_passengers,
            "Engines": [
                {
                    "NumberOfEngines": engine_count,
                    "Name": engine_name,
                    "TotalNumberOfEngines": engine_count,
                    "IsPropeller": False,
                }
            ],
        },
        "RegisteredAircraftOwners": [
            {
                "RegisteredOwner": owner_name,
                "Address1": address1,
                "Address2": address2,
                "Town": town,
                "County": county,
                "PostCode": postcode,
                "Country": country,
            }
        ],
    }


_SEARCH_RESULT = [{"AircraftID": 66819, "Mark": "VAHH", "RegistrationStatus": "R"}]


# ---------------------------------------------------------------------------
# Tests: _decode_aircraft_class
# ---------------------------------------------------------------------------

class TestDecodeAircraftClass:
    def test_fixed_wing_landplane(self):
        assert _decode_aircraft_class("FIXED-WING LANDPLANE") == "Airplane"

    def test_fixed_wing_seaplane(self):
        assert _decode_aircraft_class("FIXED-WING SEAPLANE") == "Airplane"

    def test_rotorcraft(self):
        assert _decode_aircraft_class("ROTORCRAFT") == "Helicopter"

    def test_glider(self):
        assert _decode_aircraft_class("GLIDER") == "Glider"

    def test_free_balloon(self):
        assert _decode_aircraft_class("FREE BALLOON") == "Balloon"

    def test_airship(self):
        assert _decode_aircraft_class("AIRSHIP") == "Airship"

    def test_microlight(self):
        assert _decode_aircraft_class("MICROLIGHT AEROPLANE") == "Microlight"

    def test_gyroplane(self):
        assert _decode_aircraft_class("GYROPLANE") == "Gyroplane"

    def test_case_insensitive(self):
        assert _decode_aircraft_class("fixed-wing landplane") == "Airplane"

    def test_empty_returns_none(self):
        assert _decode_aircraft_class("") is None

    def test_none_returns_none(self):
        assert _decode_aircraft_class(None) is None


# ---------------------------------------------------------------------------
# Tests: _decode_aircraft_category
# ---------------------------------------------------------------------------

class TestDecodeAircraftCategory:
    def test_landplane(self):
        assert _decode_aircraft_category("FIXED-WING LANDPLANE") == "Land"

    def test_seaplane(self):
        assert _decode_aircraft_category("FIXED-WING SEAPLANE") == "Sea"

    def test_amphibian(self):
        assert _decode_aircraft_category("FIXED-WING AMPHIBIAN") == "Amphibian"

    def test_rotorcraft_returns_none(self):
        assert _decode_aircraft_category("ROTORCRAFT") is None

    def test_empty_returns_none(self):
        assert _decode_aircraft_category("") is None

    def test_case_insensitive(self):
        assert _decode_aircraft_category("fixed-wing landplane") == "Land"

    def test_unknown_passes_through(self):
        result = _decode_aircraft_class("SOME UNKNOWN CLASS")
        assert result is not None


# ---------------------------------------------------------------------------
# Tests: _decode_country
# ---------------------------------------------------------------------------

class TestDecodeCountry:
    def test_united_kingdom(self):
        assert _decode_country("UNITED KINGDOM") == "GB"

    def test_case_insensitive(self):
        assert _decode_country("united kingdom") == "GB"

    def test_australia(self):
        assert _decode_country("AUSTRALIA") == "AU"

    def test_united_states(self):
        assert _decode_country("UNITED STATES") == "US"

    def test_isle_of_man(self):
        assert _decode_country("ISLE OF MAN") == "IM"

    def test_unknown_passes_through(self):
        result = _decode_country("RURITANIA")
        assert result == "RURITANIA"

    def test_empty_returns_none(self):
        assert _decode_country("") is None

    def test_strips_whitespace(self):
        assert _decode_country("  UNITED KINGDOM  ") == "GB"


# ---------------------------------------------------------------------------
# Tests: _parse_year_built
# ---------------------------------------------------------------------------

class TestParseYearBuilt:
    def test_valid_year(self):
        assert _parse_year_built(2014) == "2014-01-01T00:00:00Z"

    def test_none_returns_none(self):
        assert _parse_year_built(None) is None

    def test_old_year(self):
        assert _parse_year_built(1955) == "1955-01-01T00:00:00Z"

    def test_string_year(self):
        assert _parse_year_built("2020") == "2020-01-01T00:00:00Z"

    def test_invalid_returns_none(self):
        assert _parse_year_built("not-a-year") is None


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_top_level_fields(self):
        record = _build_record(_make_details())
        assert record["icao_hex"] == "406B48"
        assert record["registration"] == "G-VAHH"
        assert record["military"] is False

    def test_icao_hex_uppercased(self):
        record = _build_record(_make_details(icao_hex="406b48"))
        assert record["icao_hex"] == "406B48"

    def test_registration_has_g_prefix(self):
        record = _build_record(_make_details(mark="ABCD"))
        assert record["registration"] == "G-ABCD"

    def test_aircraft_type_decoded(self):
        record = _build_record(_make_details(aircraft_class="FIXED-WING LANDPLANE"))
        assert record["aircraft"]["type"] == "Airplane"

    def test_aircraft_manufacturer(self):
        record = _build_record(_make_details())
        assert record["aircraft"]["manufacturer"] == "BOEING COMPANY"

    def test_aircraft_model(self):
        record = _build_record(_make_details())
        assert record["aircraft"]["model"] == "BOEING 787-9"

    def test_aircraft_serial_number(self):
        record = _build_record(_make_details())
        assert record["aircraft"]["serial_number"] == "37967"

    def test_aircraft_type_designator(self):
        record = _build_record(_make_details())
        assert record["aircraft"]["type_designator"] == "B789"

    def test_aircraft_manufactured_date(self):
        record = _build_record(_make_details(year_built=2014))
        assert record["aircraft"]["manufactured_date"] == "2014-01-01T00:00:00Z"

    def test_aircraft_seats(self):
        record = _build_record(_make_details(maximum_passengers=296))
        assert record["aircraft"]["seats"] == 297

    def test_aircraft_seats_none_when_absent(self):
        details = _make_details()
        details["AircraftDetails"]["MaximumPassengers"] = None
        assert "seats" not in _build_record(details)["aircraft"]

    def test_powerplant_count(self):
        record = _build_record(_make_details(engine_count=2))
        assert record["aircraft"]["powerplant"]["count"] == 2

    def test_powerplant_model(self):
        record = _build_record(_make_details(engine_name="ROLLS-ROYCE Trent 1000-K2"))
        assert record["aircraft"]["powerplant"]["model"] == "ROLLS-ROYCE Trent 1000-K2"

    def test_registrant_name(self):
        record = _build_record(_make_details())
        assert "VIRGIN ATLANTIC AIRWAYS LTD" in record["registrant"]["names"]

    def test_registrant_street(self):
        record = _build_record(_make_details())
        assert "THE VHQ" in record["registrant"]["street"]
        assert "FLEMING WAY" in record["registrant"]["street"]

    def test_registrant_city(self):
        record = _build_record(_make_details())
        assert record["registrant"]["city"] == "CRAWLEY"

    def test_registrant_postal_code(self):
        record = _build_record(_make_details())
        assert record["registrant"]["postal_code"] == "RH10 9DF"

    def test_registrant_country_decoded(self):
        record = _build_record(_make_details(country="UNITED KINGDOM"))
        assert record["registrant"]["country"] == "GB"

    def test_no_county_omitted(self):
        record = _build_record(_make_details(county=None))
        assert "administrative_area" not in record.get("registrant", {})

    def test_missing_icao_hex_returns_none(self):
        details = _make_details()
        details["AircraftDetails"]["ICAO24BitAircraftAddress"]["Hex"] = ""
        assert _build_record(details) is None

    def test_missing_mark_returns_none(self):
        details = _make_details()
        details["RegistrationDetails"]["Mark"] = ""
        assert _build_record(details) is None

    def test_no_engines_omits_powerplant(self):
        details = _make_details()
        details["AircraftDetails"]["Engines"] = []
        record = _build_record(details)
        assert "powerplant" not in record.get("aircraft", {})

    def test_no_owners_omits_registrant(self):
        details = _make_details()
        details["RegisteredAircraftOwners"] = []
        record = _build_record(details)
        assert "registrant" not in record


# ---------------------------------------------------------------------------
# Tests: _apply_type_lookup
# ---------------------------------------------------------------------------

class TestApplyTypeLookup:
    def _make_redis(self, type_doc=None) -> MagicMock:
        r = MagicMock()
        r.json.return_value.get.return_value = type_doc
        return r

    def test_designator_resolves_sets_manufacturer_model(self):
        record = {"aircraft": {"type_designator": "SV4"}}
        r = self._make_redis({"type_designator": "SV4", "manufacturer_model": "NORD SV-4"})
        _apply_type_lookup(record, r)
        assert record["aircraft"]["manufacturer_model"] == "NORD SV-4"

    def test_designator_resolves_sets_description_code(self):
        record = {"aircraft": {"type_designator": "B763"}}
        r = self._make_redis({"manufacturer_model": "BOEING 767-332ER", "description_code": "L2J"})
        _apply_type_lookup(record, r)
        assert record["aircraft"]["description_code"] == "L2J"

    def test_description_code_not_in_type_doc_leaves_field_unset(self):
        record = {"aircraft": {"type_designator": "SV4"}}
        r = self._make_redis({"manufacturer_model": "NORD SV-4"})
        _apply_type_lookup(record, r)
        assert "description_code" not in record["aircraft"]

    def test_lookup_key_uses_type_designator(self):
        record = {"aircraft": {"type_designator": "SV4"}}
        r = self._make_redis({"manufacturer_model": "NORD SV-4"})
        _apply_type_lookup(record, r)
        r.json.return_value.get.assert_called_once_with("aircraft:type:SV4")

    def test_write_happens_even_if_mictronics_manufacturer_model_already_present(self):
        """This runner's own type_designator is authoritative — the lookup and
        write happen regardless of whether Mictronics also has data for the
        same hex; merge_aircraft.lua's registry-wins precedence handles it."""
        record = {"aircraft": {"type_designator": "B763", "manufacturer_model": "STALE VALUE"}}
        r = self._make_redis({"manufacturer_model": "BOEING 767-332ER"})
        _apply_type_lookup(record, r)
        assert record["aircraft"]["manufacturer_model"] == "BOEING 767-332ER"

    def test_designator_not_found_leaves_fields_unset(self):
        record = {"aircraft": {"type_designator": "ULAC"}}
        r = self._make_redis(None)
        _apply_type_lookup(record, r)
        assert "manufacturer_model" not in record["aircraft"]

    def test_no_type_designator_skips_lookup_entirely(self):
        record = {"aircraft": {"model": "Some Model"}}
        r = self._make_redis({"manufacturer_model": "SHOULD NOT BE USED"})
        _apply_type_lookup(record, r)
        assert "manufacturer_model" not in record["aircraft"]
        r.json.return_value.get.assert_not_called()

    def test_no_aircraft_object_does_not_crash(self):
        record = {"icao_hex": "406B48"}
        r = self._make_redis({"manufacturer_model": "SHOULD NOT BE USED"})
        _apply_type_lookup(record, r)
        assert "aircraft" not in record

    def test_redis_lookup_failure_degrades_gracefully(self):
        """Reference table lookup failure is not a hard dependency — the
        runner still completes successfully without it."""
        record = {"aircraft": {"type_designator": "B789"}}
        r = MagicMock()
        r.json.return_value.get.side_effect = Exception("connection refused")
        _apply_type_lookup(record, r)
        assert "manufacturer_model" not in record["aircraft"]

    def test_blank_manufacturer_model_in_type_doc_leaves_field_unset(self):
        record = {"aircraft": {"type_designator": "XXXX"}}
        r = self._make_redis({"manufacturer_model": "   "})
        _apply_type_lookup(record, r)
        assert "manufacturer_model" not in record["aircraft"]


# ---------------------------------------------------------------------------
# Tests: _search_by_prefix
# ---------------------------------------------------------------------------

class TestSearchByPrefix:
    def _make_session(self, results: list) -> MagicMock:
        session = MagicMock()
        resp = MagicMock()
        resp.json.return_value = results
        resp.raise_for_status.return_value = None
        session.post.return_value = resp
        return session

    def test_posts_to_correct_url(self):
        session = self._make_session(_SEARCH_RESULT)
        _search_by_prefix(_client(session), "VA")
        url = session.post.call_args.args[0]
        assert "/api/aircraft/search" in url

    def test_sends_registration_prefix(self):
        session = self._make_session(_SEARCH_RESULT)
        _search_by_prefix(_client(session), "VA")
        payload = session.post.call_args.kwargs["json"]
        assert payload == {"Registration": "VA"}

    def test_returns_results_list(self):
        session = self._make_session(_SEARCH_RESULT)
        results = _search_by_prefix(_client(session), "VA")
        assert results == _SEARCH_RESULT

    def test_empty_response_returns_empty_list(self):
        session = self._make_session([])
        results = _search_by_prefix(_client(session), "ZZ")
        assert results == []

    def test_none_response_returns_empty_list(self):
        session = self._make_session(None)
        results = _search_by_prefix(_client(session), "ZZ")
        assert results == []

    def test_logs_url_and_prefix_at_debug(self, caplog):
        session = self._make_session(_SEARCH_RESULT)
        with caplog.at_level(logging.DEBUG, logger="uk-caa-registry"):
            _search_by_prefix(_client(session), "VA")
        assert any(
            "/api/aircraft/search" in r.message and "VA" in r.message
            for r in caplog.records
        )


# ---------------------------------------------------------------------------
# Tests: _get_aircraft_details
# ---------------------------------------------------------------------------

class TestGetAircraftDetails:
    def _make_session(self, details: dict) -> MagicMock:
        session = MagicMock()
        resp = MagicMock()
        resp.json.return_value = details
        resp.raise_for_status.return_value = None
        session.get.return_value = resp
        return session

    def test_gets_correct_url(self):
        session = self._make_session(_make_details())
        _get_aircraft_details(_client(session), 66819)
        url = session.get.call_args.args[0]
        assert "/api/aircraft/details/66819" in url

    def test_returns_parsed_json(self):
        details = _make_details()
        session = self._make_session(details)
        result = _get_aircraft_details(_client(session), 66819)
        assert result == details

    def test_logs_url_at_debug(self, caplog):
        session = self._make_session(_make_details())
        with caplog.at_level(logging.DEBUG, logger="uk-caa-registry"):
            _get_aircraft_details(_client(session), 66819)
        assert any(
            "/api/aircraft/details/66819" in r.message for r in caplog.records
        )


# ---------------------------------------------------------------------------
# Tests: request pacing and refusals
# ---------------------------------------------------------------------------

class TestClient:
    def _request(self, responses, interval=1.0, deadline=float("inf")):
        session = MagicMock()
        session.get.side_effect = responses
        sleeps = []
        client = _mod._Client(session, interval, deadline)
        with patch("time.sleep", side_effect=sleeps.append):
            client.request("get", "https://example.invalid/x")
        return session, sleeps

    def test_every_request_is_paced(self):
        _, sleeps = self._request([_ok([])], interval=0.25)
        assert sleeps == [0.25]

    @pytest.mark.parametrize("status", [403, 429])
    def test_refusal_is_retried_after_waiting(self, status):
        session, sleeps = self._request([_http_error(status), _ok([])])
        assert sleeps == [1.0, 65, 1.0]
        assert session.get.call_count == 2

    def test_waits_increase_then_request_gives_up(self):
        session = MagicMock()
        session.get.return_value = _http_error(403)
        sleeps = []
        with patch("time.sleep", side_effect=sleeps.append):
            with pytest.raises(RateLimitedError):
                _mod._Client(session, 1.0).request("get", "u")
        assert [s for s in sleeps if s != 1.0] == [65, 120, 300, 600, 900]
        assert session.get.call_count == 6

    @pytest.mark.parametrize("header, expected", [
        ("200", 200), ("5", 65), ("999999", 3600), ("soon", 65),
    ])
    def test_retry_after_header(self, header, expected):
        _, sleeps = self._request([_http_error(429, {"Retry-After": header}), _ok([])])
        assert sleeps[1] == expected

    def test_refusal_is_logged_with_diagnostics(self, caplog):
        with caplog.at_level(logging.INFO, logger="uk-caa-registry"):
            self._request([_http_error(403, {"x-azure-ref": "abc123"}), _ok([])])
        assert "after 0 accepted requests" in caplog.text
        assert "x-azure-ref=abc123" in caplog.text
        assert "blocked" in caplog.text
        assert "Accepted again after waiting 65s" in caplog.text

    def test_error_page_is_logged_as_plain_text(self):
        page = "<!DOCTYPE html><html><style>body { }</style>\n<h2>Request  blocked</h2></html>"
        assert _mod._page_text(page) == "Request blocked"

    @pytest.mark.parametrize("failure", [_http_error(503), requests.ConnectionError()])
    def test_transient_error_is_retried(self, failure):
        _, sleeps = self._request([failure, _ok([])])
        assert sleeps[1] == 2

    def test_wait_past_the_deadline_stops_without_sleeping(self):
        with patch.object(_mod.time, "monotonic", return_value=1000.0):
            with pytest.raises(DeadlineReached):
                self._request([_http_error(403)], deadline=1030.0)

    def test_lost_lock_stops_requests(self):
        lock = MagicMock(lost=True)
        session = MagicMock()
        with patch("time.sleep"), pytest.raises(_mod.LockLostError):
            _mod._Client(session, 0, lock=lock).request("get", "u")
        session.get.assert_not_called()


class TestRequestWindow:
    def _send_times(self, count, interval, responses=None):
        clock = [1000.0]
        sent = []
        answers = iter(responses or [])

        def _get(url, **kwargs):
            sent.append(clock[0])
            return next(answers, _ok([]))

        session = MagicMock()
        session.get.side_effect = _get

        def _sleep(seconds):
            clock[0] += seconds

        with patch.object(_mod.time, "monotonic", side_effect=lambda: clock[0]), \
                patch("time.sleep", side_effect=_sleep):
            client = _mod._Client(session, interval)
            for _ in range(count):
                client.request("get", "u")
        return sent

    def test_never_more_than_25_attempts_in_65_seconds(self):
        sent = self._send_times(80, 1.0)
        assert all(later - earlier >= 65 for earlier, later in zip(sent, sent[25:]))

    def test_first_25_are_only_paced_and_the_26th_waits_for_the_window(self):
        sent = self._send_times(27, 1.0)
        assert sent[24] - sent[0] == 24
        assert sent[25] - sent[0] == 65
        assert sent[26] - sent[1] == 65

    def test_slow_requests_are_not_delayed_further(self):
        sent = self._send_times(30, 3.0)
        assert all(later - earlier == 3 for earlier, later in zip(sent, sent[1:]))

    def test_refused_attempts_count_toward_the_window(self):
        sent = self._send_times(40, 0, responses=[_http_error(403)])
        assert len(sent) == 41
        assert all(later - earlier >= 65 for earlier, later in zip(sent, sent[25:]))


# ---------------------------------------------------------------------------
# Real Redis Stack
# ---------------------------------------------------------------------------

@pytest.fixture
def r():
    client = redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", "6379")),
        password=os.environ.get("REDIS_PASSWORD") or None,
        decode_responses=True,
        socket_connect_timeout=1,
    )
    try:
        client.ft(UK_CAA_QUEUE_SEARCH_INDEX)
        client.execute_command("FT._LIST")
    except redis.exceptions.RedisError:
        pytest.skip("Redis Stack is not reachable")

    def _clean():
        keys = [k for pattern in ("uk_caa:*", "aircraft:registry:FF*", "aircraft:type:ZZ*")
                for k in client.scan_iter(pattern)]
        if keys:
            client.delete(*keys)

    _clean()
    _mod._ensure_queue_index(client)
    yield client
    _clean()


def _queue(r, aircraft_id, next_fetch_at=0, ttl=1000):
    key = uk_caa_queue_key(aircraft_id)
    r.json().set(key, "$", {"aircraft_id": aircraft_id, "mark": "VAHH",
                            "next_fetch_at": next_fetch_at})
    r.expire(key, ttl)
    return key


def _next_fetch(r, key):
    return r.json().get(key)["next_fetch_at"]


class _Caa:
    """Stands in for _Client: answers searches by prefix, details by id."""

    def __init__(self, searches=None, details=None):
        self.searches = searches or {}
        self.details = details or {}
        self.requested = []

    def request(self, method, url, **kwargs):
        if method == "post":
            target = kwargs["json"]["Registration"]
            answer = self.searches.get(target, [])
        else:
            target = int(url.rsplit("/", 1)[1])
            answer = self.details.get(target, _make_details(icao_hex=f"FF{target:04X}"))
        self.requested.append(target)
        if isinstance(answer, Exception):
            raise answer
        return _ok(answer)


def _search_stats():
    return {name: 0 for name, *_ in _mod._STATISTICS["search"]}


def _details_stats():
    return {"records_imported": 0, "requests_failed": 0, "requests_attempted": 0}


def _result(aircraft_id, status="R", mark="VAHH"):
    return {"AircraftID": aircraft_id, "Mark": mark, "RegistrationStatus": status}


# ---------------------------------------------------------------------------
# Tests: search job
# ---------------------------------------------------------------------------

@pytest.mark.xdist_group("uk_caa_redis")
class TestRunSearch:
    def test_creates_a_due_record_only_for_registered_aircraft(self, r):
        stats = _search_stats()
        run_search(_Caa({"AA": [_result(1), _result(2, "D")]}), r, stats)
        assert r.json().get(uk_caa_queue_key(1)) == {
            "aircraft_id": 1, "mark": "VAHH", "next_fetch_at": 0,
        }
        assert not r.exists(uk_caa_queue_key(2))
        assert _QUEUE_TTL - 60 < r.ttl(uk_caa_queue_key(1)) <= _QUEUE_TTL
        assert (stats["aircraft_found"], stats["aircraft_added"], stats["aircraft_removed"]) == (1, 1, 0)

    def test_known_aircraft_is_renewed_without_resetting_next_fetch_at(self, r):
        key = _queue(r, 1, next_fetch_at=12345, ttl=50)
        stats = _search_stats()
        run_search(_Caa({"AA": [_result(1, mark="VNEW")]}), r, stats)
        assert r.json().get(key) == {"aircraft_id": 1, "mark": "VNEW", "next_fetch_at": 12345}
        assert r.ttl(key) > 50
        assert stats["aircraft_added"] == 0

    def test_non_registered_result_deletes_only_the_queue_record(self, r):
        key = _queue(r, 1)
        r.json().set("aircraft:registry:FF0001", "$", {"source": "uk-caa-registry"})
        stats = _search_stats()
        run_search(_Caa({"AA": [_result(1, "D")]}), r, stats)
        assert not r.exists(key)
        assert r.exists("aircraft:registry:FF0001")
        assert stats["aircraft_removed"] == 1

    @pytest.mark.parametrize("results", [
        [_result(1, "R", "AFRZ"), _result(1, "D", "AIDE")],
        [_result(1, "D", "AIDE"), _result(1, "R", "AFRZ")],
    ])
    def test_old_mark_does_not_remove_a_registered_aircraft(self, r, results):
        stats = _search_stats()
        run_search(_Caa({"AA": results[:1], "AB": results[1:]}), r, stats)
        assert r.json().get(uk_caa_queue_key(1))["mark"] == "AFRZ"
        assert stats["aircraft_removed"] == 0

    @pytest.mark.parametrize("statuses", [("R", "D"), ("D", "R")])
    def test_history_row_for_the_same_mark_does_not_remove_it(self, r, statuses):
        key = _queue(r, 1, next_fetch_at=12345)
        run_search(_Caa({"AA": [_result(1, status) for status in statuses]}), r, _search_stats())
        assert _next_fetch(r, key) == 12345

    def test_old_mark_leaves_the_schedule_of_a_known_aircraft(self, r):
        key = _queue(r, 1, next_fetch_at=12345)
        run_search(_Caa({"AA": [_result(1, "D", "AOLD")]}), r, _search_stats())
        assert _next_fetch(r, key) == 12345

    def test_missing_and_failed_results_delete_nothing(self, r):
        key = _queue(r, 1, ttl=500)
        with pytest.raises(IncompleteRunError):
            run_search(_Caa({"AB": ValueError("boom")}), r, _search_stats())
        assert r.exists(key)
        assert r.ttl(key) <= 500

    def test_every_prefix_is_searched_and_failures_are_not_marked(self, r):
        caa = _Caa({"AB": ValueError("boom")})
        stats = _search_stats()
        with pytest.raises(IncompleteRunError):
            run_search(caa, r, stats)
        assert len(caa.requested) == 676
        assert (stats["prefixes_searched"], stats["prefixes_failed"]) == (675, 1)
        progress = r.hgetall(UK_CAA_SEARCH_PROGRESS_KEY)
        assert len(progress) == 675 and "AB" not in progress
        assert r.ttl(UK_CAA_SEARCH_PROGRESS_KEY) == -1

    def test_longest_waiting_prefixes_go_first(self, r):
        every = [a + b for a in _mod._LETTERS for b in _mod._LETTERS]
        r.hset(UK_CAA_SEARCH_PROGRESS_KEY, mapping={p: 1000 for p in every})
        r.hset(UK_CAA_SEARCH_PROGRESS_KEY, "ZZ", 1)
        r.hdel(UK_CAA_SEARCH_PROGRESS_KEY, "MM")
        caa = _Caa()
        run_search(caa, r, _search_stats())
        assert caa.requested[:3] == ["MM", "ZZ", "AA"]

    def test_blocked_run_resumes_with_the_unsearched_prefixes(self, r):
        with pytest.raises(RateLimitedError):
            run_search(_Caa({"AC": RateLimitedError("refused")}), r, _search_stats())
        caa = _Caa()
        run_search(caa, r, _search_stats())
        assert caa.requested[0] == "AC"
        assert caa.requested[-2:] == ["AA", "AB"]


# ---------------------------------------------------------------------------
# Tests: details job
# ---------------------------------------------------------------------------

@pytest.mark.xdist_group("uk_caa_redis")
class TestRunDetails:
    def test_fetches_only_due_aircraft_longest_due_first_up_to_limit(self, r):
        now = int(time.time())
        _queue(r, 1, now - 10)
        _queue(r, 2, 0)
        _queue(r, 3, now - 500)
        _queue(r, 4, now + 500)
        caa = _Caa()
        run_details(caa, r, 2, _details_stats())
        assert caa.requested == [2, 3]

    def test_reads_in_batches_without_skipping(self, r):
        for aircraft_id in range(1, 8):
            _queue(r, aircraft_id, aircraft_id)
        caa = _Caa()
        with patch.object(_mod, "_QUEUE_BATCH_SIZE", 3):
            run_details(caa, r, 100, _details_stats())
        assert caa.requested == list(range(1, 8))

    def test_success_writes_record_and_schedules_next_fetch(self, r):
        key = _queue(r, 1, ttl=900)
        r.json().set("aircraft:type:ZZZZ", "$", {"manufacturer_model": "TEST MODEL"})
        stats = _details_stats()
        run_details(_Caa(details={1: _make_details(icao_hex="FF0001", type_designator="ZZZZ")}),
                    r, 10, stats)
        record = r.json().get("aircraft:registry:FF0001")
        assert record["source"] == "uk-caa-registry"
        assert record["country_code"] == "GB"
        assert record["aircraft"]["manufacturer_model"] == "TEST MODEL"
        assert _REGISTRY_TTL - 60 < r.ttl("aircraft:registry:FF0001") <= _REGISTRY_TTL
        assert _next_fetch(r, key) == pytest.approx(time.time() + _REFRESH, abs=60)
        assert 800 < r.ttl(key) <= 900
        assert stats["records_imported"] == 1

    def test_failed_request_retries_in_a_day_and_renews_nothing(self, r):
        key = _queue(r, 1, ttl=900)
        r.json().set("aircraft:registry:FF0001", "$", {"source": "uk-caa-registry"})
        r.expire("aircraft:registry:FF0001", 100)
        with pytest.raises(IncompleteRunError):
            run_details(_Caa(details={1: ValueError("boom")}), r, 10, _details_stats())
        assert _next_fetch(r, key) == pytest.approx(time.time() + 86400, abs=60)
        assert r.ttl(key) <= 900
        assert r.ttl("aircraft:registry:FF0001") <= 100

    @pytest.mark.parametrize("payload", [
        _make_details(icao_hex="FF0001", reg_status="De-registered"),
        _make_details(icao_hex=""),
    ])
    def test_unusable_answer_waits_a_full_cycle_and_is_not_a_failure(self, r, payload):
        key = _queue(r, 1)
        stats = _details_stats()
        run_details(_Caa(details={1: payload}), r, 10, stats)
        assert r.exists(key)
        assert _next_fetch(r, key) == pytest.approx(time.time() + _REFRESH, abs=60)
        assert not r.exists("aircraft:registry:FF0001")
        assert (stats["records_imported"], stats["requests_failed"]) == (0, 0)

    def test_record_removed_during_the_fetch_is_not_recreated(self, r):
        key = _queue(r, 1)

        class Removing(_Caa):
            def request(self, *args, **kwargs):
                r.delete(key)
                return super().request(*args, **kwargs)

        run_details(Removing(), r, 10, _details_stats())
        assert not r.exists(key)

    def test_few_failures_still_succeed(self, r):
        for aircraft_id in range(1, 101):
            _queue(r, aircraft_id, aircraft_id)
        stats = _details_stats()
        run_details(_Caa(details={5: ValueError("boom")}), r, 100, stats)
        assert (stats["records_imported"], stats["requests_failed"]) == (99, 1)

    def test_more_than_one_percent_failures_is_incomplete(self, r):
        for aircraft_id in range(1, 101):
            _queue(r, aircraft_id, aircraft_id)
        with pytest.raises(IncompleteRunError):
            run_details(_Caa(details={5: ValueError("a"), 6: ValueError("b")}),
                        r, 100, _details_stats())

    def test_refusal_stops_the_run_and_leaves_the_aircraft_due(self, r):
        _queue(r, 1, 1)
        key = _queue(r, 2, 2)
        stats = _details_stats()
        with pytest.raises(RateLimitedError):
            run_details(_Caa(details={2: RateLimitedError("refused")}), r, 10, stats)
        assert _next_fetch(r, key) == 2
        assert stats["records_imported"] == 1

    def test_deadline_after_progress_is_success(self, r):
        _queue(r, 1, 1)
        _queue(r, 2, 2)
        stats = _details_stats()
        run_details(_Caa(details={2: DeadlineReached("time")}), r, 10, stats)
        assert stats["records_imported"] == 1

    def test_deadline_without_progress_is_incomplete(self, r):
        _queue(r, 1)
        with pytest.raises(DeadlineReached):
            run_details(_Caa(details={1: DeadlineReached("time")}), r, 10, _details_stats())

    def test_nothing_due_succeeds(self, r):
        _queue(r, 1, int(time.time()) + 500)
        caa = _Caa()
        run_details(caa, r, 10, _details_stats())
        assert caa.requested == []

    def test_queue_statistics(self, r):
        now = int(time.time())
        _queue(r, 1, 0)
        _queue(r, 2, now - 3 * 86400 - 60)
        _queue(r, 3, now - 60)
        _queue(r, 4, now + 500)
        assert _mod.queue_statistics(r) == {
            "queue_size": 4, "aircraft_due": 3,
            "aircraft_never_fetched": 1, "oldest_overdue_days": 3,
        }


# ---------------------------------------------------------------------------
# Tests: run lock and job runner
# ---------------------------------------------------------------------------

@pytest.mark.xdist_group("uk_caa_redis")
class TestRunLock:
    def test_second_holder_waits_until_released(self, r):
        first = _mod._RunLock(r)
        assert first.acquire(float("inf"))
        with patch("time.sleep", side_effect=lambda _: first.release()):
            second = _mod._RunLock(r)
            assert second.acquire(float("inf"))
        second.release()
        assert not r.exists(UK_CAA_LOCK_KEY)

    def test_gives_up_at_the_deadline_and_leaves_the_holder_alone(self, r):
        r.set(UK_CAA_LOCK_KEY, "other")
        assert not _mod._RunLock(r).acquire(time.monotonic() + 1)
        _mod._RunLock(r).release()
        assert r.get(UK_CAA_LOCK_KEY) == "other"

    def test_job_releases_the_lock_after_a_failure(self, r):
        _queue(r, 1)
        with patch.object(_mod, "_get_aircraft_details", side_effect=ValueError("boom")):
            status, stats = _mod.run_job("details", MagicMock(), r, 0, 10)
        assert status == "failure"
        assert stats["requests_failed"] == 1 and stats["queue_size"] == 1
        assert not r.exists(UK_CAA_LOCK_KEY)


# ---------------------------------------------------------------------------
# Tests: main
# ---------------------------------------------------------------------------

class TestMain:
    def _run_main(self, monkeypatch, job, status="success"):
        monkeypatch.setenv("REDIS_HOST", "localhost")
        monkeypatch.setenv("REDIS_PASSWORD", "x")
        with patch.object(_mod, "build_redis_client"), \
                patch.object(_mod, "configure_logging"), \
                patch.object(_mod, "run_job", return_value=(status, {"a": 1})) as run, \
                patch.object(_mod, "publish_completion_stats") as publish:
            _mod.main([job])
        return run, publish

    def test_defaults(self, monkeypatch):
        monkeypatch.delenv("REQUEST_INTERVAL_SECONDS", raising=False)
        monkeypatch.delenv("DETAILS_LIMIT", raising=False)
        run, publish = self._run_main(monkeypatch, "details")
        assert run.call_args.args[0] == "details"
        assert run.call_args.args[3:] == (1.0, 1000)
        assert publish.call_args.args[1:] == ("details", {"a": 1}, "success")

    def test_settings_from_environment(self, monkeypatch):
        monkeypatch.setenv("REQUEST_INTERVAL_SECONDS", "2.5")
        monkeypatch.setenv("DETAILS_LIMIT", "7")
        run, _ = self._run_main(monkeypatch, "details")
        assert run.call_args.args[3:] == (2.5, 7)

    def test_sigterm_unwinds_so_the_lock_is_released(self):
        with pytest.raises(SystemExit):
            _mod._exit_on_sigterm(15, None)

    def test_failure_exits_nonzero(self, monkeypatch):
        with patch.object(_mod.sys, "exit") as exit_:
            self._run_main(monkeypatch, "search", status="failure")
        exit_.assert_called_once_with(1)

    @pytest.mark.parametrize("argv", [[], ["both"]])
    def test_a_known_job_is_required(self, argv):
        with pytest.raises(SystemExit):
            _mod.main(argv)


# ---------------------------------------------------------------------------
# Tests: MQTT completion stats (mocked)
# ---------------------------------------------------------------------------

class TestMqttCompletionStats:
    def _publish(self, job, stats, status="success", cfg=None):
        client = MagicMock()
        client.connect.side_effect = lambda *a, **k: client.on_connect(client, None, None, 0, None)
        with patch("uk_caa_registry_main.mqtt.Client", return_value=client), patch("time.sleep"):
            publish_completion_stats(
                cfg if cfg is not None else {"mqtt": {"host": "localhost", "port": 1883}},
                job, stats, status,
            )
        return client, {c.args[0]: c.args[1] for c in client.publish.call_args_list}

    def test_each_job_publishes_under_its_own_topic(self):
        _, search = self._publish("search", {"prefixes_searched": 676})
        _, details = self._publish("details", {"records_imported": 42}, "failure")
        assert search[f"{MQTT_ROOT}/search/statistic/prefixes_searched"] == "676"
        assert search[f"{MQTT_ROOT}/search/statistic/last_run_status"] == "Success"
        assert details[f"{MQTT_ROOT}/details/statistic/records_imported"] == "42"
        assert details[f"{MQTT_ROOT}/details/statistic/last_run_status"] == "Failure"
        assert f"{MQTT_ROOT}/details/statistic/last_run_at" in details

    def test_unavailable_statistics_are_not_published(self):
        _, published = self._publish("details", {"records_imported": 1})
        assert f"{MQTT_ROOT}/details/statistic/queue_size" not in published

    @pytest.mark.parametrize("job, sensors", [("search", 7), ("details", 8)])
    def test_ha_discovery_and_retired_topics(self, job, sensors):
        client, published = self._publish(job, {})
        configs = {t: p for t, p in published.items() if t.startswith("homeassistant/") and p}
        assert len(configs) == sensors
        assert all(f"_{job}_" in t for t in configs)
        assert not any("total_increasing" in p for p in configs.values())
        for name in ("records_imported", "last_run_at", "last_run_status"):
            for topic in (
                f"homeassistant/sensor/SkyFollower_runner_uk_caa_registry_{name}/config",
                f"{MQTT_ROOT}/statistic/{name}",
            ):
                assert published[topic] == ""
                client.publish.assert_any_call(topic, "", retain=True)

    @pytest.mark.parametrize("cfg", [{}, {"mqtt": {"host": "", "port": 1883}}])
    def test_no_mqtt_host_skips(self, cfg):
        client, _ = self._publish("search", {}, cfg=cfg)
        client.connect.assert_not_called()
