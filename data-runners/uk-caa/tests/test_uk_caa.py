"""
Tests for the UK CAA data runner.

Covers:
- Decode helpers (aircraft class, country, year built)
- Record builder (_build_record)
- Deep merge
- Redis enumeration (get_uk_registrations)
- API helpers (_search_aircraft, _get_aircraft_details) — mocked
- Redis write logic (write_to_redis) — mocked
- MQTT completion stats — mocked
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, call, patch

import requests

import pytest

# ---------------------------------------------------------------------------
# Module import helper
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
_RUNNER_DIR = os.path.dirname(_HERE)        # data-runners/uk-caa/
_REPO_ROOT = os.path.abspath(os.path.join(_RUNNER_DIR, "..", ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "uk_caa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["uk_caa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_decode_aircraft_class = _mod._decode_aircraft_class
_decode_country = _mod._decode_country
_parse_year_built = _mod._parse_year_built
_build_record = _mod._build_record
_deep_merge = _mod._deep_merge
_search_aircraft = _mod._search_aircraft
_get_aircraft_details = _mod._get_aircraft_details
get_uk_registrations = _mod.get_uk_registrations
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
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
        assert record["aircraft"]["seats"] == 296

    def test_aircraft_seats_none_when_absent(self):
        details = _make_details()
        details["AircraftDetails"]["MaximumPassengers"] = None
        assert "seats" not in _build_record(details)["aircraft"]

    def test_powerplant_count(self):
        record = _build_record(_make_details(engine_count=2))
        assert record["powerplant"]["count"] == 2

    def test_powerplant_model(self):
        record = _build_record(_make_details(engine_name="ROLLS-ROYCE Trent 1000-K2"))
        assert record["powerplant"]["model"] == "ROLLS-ROYCE Trent 1000-K2"

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
        assert "powerplant" not in record

    def test_no_owners_omits_registrant(self):
        details = _make_details()
        details["RegisteredAircraftOwners"] = []
        record = _build_record(details)
        assert "registrant" not in record


# ---------------------------------------------------------------------------
# Tests: _deep_merge
# ---------------------------------------------------------------------------

class TestDeepMerge:
    def test_update_wins_on_conflict(self):
        result = _deep_merge({"a": 1}, {"a": 2})
        assert result["a"] == 2

    def test_nested_dicts_merged_recursively(self):
        base = {"aircraft": {"type": "Airplane", "manufacturer": "OLD"}}
        update = {"aircraft": {"manufacturer": "NEW", "model": "B737"}}
        result = _deep_merge(base, update)
        assert result["aircraft"]["type"] == "Airplane"
        assert result["aircraft"]["manufacturer"] == "NEW"
        assert result["aircraft"]["model"] == "B737"

    def test_base_keys_preserved(self):
        result = _deep_merge({"a": 1, "b": 2}, {"a": 99})
        assert result["b"] == 2

    def test_new_keys_added(self):
        result = _deep_merge({}, {"x": 42})
        assert result["x"] == 42


# ---------------------------------------------------------------------------
# Tests: get_uk_registrations
# ---------------------------------------------------------------------------

class TestGetUkRegistrations:
    def _make_redis(
        self,
        scan_keys: list[str],
        reg_map: dict[str, str],
        fk_map: dict[str, int] = None,
    ) -> MagicMock:
        r = MagicMock()
        r.scan_iter.return_value = iter(scan_keys)
        _fk_map = fk_map or {}

        def mget_side_effect(keys, path):
            if path == "$.registration":
                return [[reg_map.get(k)] if k in reg_map else None for k in keys]
            # $["foreign_key"]
            return [[_fk_map.get(k)] if k in _fk_map else None for k in keys]

        r.json.return_value.mget.side_effect = mget_side_effect
        return r

    def test_returns_g_registrations(self):
        r = self._make_redis(
            ["icao_hex:406B48"],
            {"icao_hex:406B48": "G-VAHH"},
        )
        results = get_uk_registrations(r)
        regs = [reg for reg, _, _ in results]
        assert "G-VAHH" in regs

    def test_filters_non_g_registrations(self):
        r = self._make_redis(
            ["icao_hex:406B48", "icao_hex:400001"],
            {"icao_hex:406B48": "N12345", "icao_hex:400001": "G-ABCD"},
        )
        results = get_uk_registrations(r)
        regs = [reg for reg, _, _ in results]
        assert "N12345" not in regs
        assert "G-ABCD" in regs

    def test_icao_hex_uppercased(self):
        r = self._make_redis(
            ["icao_hex:406b48"],
            {"icao_hex:406b48": "G-VAHH"},
        )
        results = get_uk_registrations(r)
        assert results[0][1] == "406B48"

    def test_registration_uppercased(self):
        r = self._make_redis(
            ["icao_hex:406B48"],
            {"icao_hex:406B48": "g-vahh"},
        )
        results = get_uk_registrations(r)
        assert results[0][0] == "G-VAHH"

    def test_missing_registration_skipped(self):
        r = self._make_redis(["icao_hex:406B48"], {})
        results = get_uk_registrations(r)
        assert results == []

    def test_uses_uk_icao_scan_pattern(self):
        r = self._make_redis([], {})
        get_uk_registrations(r)
        r.scan_iter.assert_called_once_with("icao_hex:4[0123]*")

    def test_returns_cached_foreign_key(self):
        r = self._make_redis(
            ["icao_hex:406B48"],
            {"icao_hex:406B48": "G-VAHH"},
            fk_map={"icao_hex:406B48": 66819},
        )
        results = get_uk_registrations(r)
        assert results[0][2] == 66819

    def test_returns_none_foreign_key_when_absent(self):
        r = self._make_redis(
            ["icao_hex:406B48"],
            {"icao_hex:406B48": "G-VAHH"},
        )
        results = get_uk_registrations(r)
        assert results[0][2] is None


# ---------------------------------------------------------------------------
# Tests: API helpers (mocked session)
# ---------------------------------------------------------------------------

class TestApiHelpers:
    def _make_session(self, search_json, details_json=None) -> MagicMock:
        session = MagicMock()

        search_resp = MagicMock()
        search_resp.json.return_value = search_json
        search_resp.raise_for_status.return_value = None

        details_resp = MagicMock()
        details_resp.json.return_value = details_json
        details_resp.raise_for_status.return_value = None

        session.post.return_value = search_resp
        session.get.return_value = details_resp
        return session

    def test_search_returns_aircraft_id(self):
        session = self._make_session(_SEARCH_RESULT)
        result = _search_aircraft(session, "406B48", "VAHH")
        assert result == 66819

    def test_search_empty_result_returns_none(self):
        session = self._make_session([])
        result = _search_aircraft(session, "406B48", "VAHH")
        assert result is None

    def test_search_posts_to_correct_url(self):
        session = self._make_session(_SEARCH_RESULT)
        _search_aircraft(session, "406B48", "VAHH")
        url = session.post.call_args.args[0]
        assert "/api/aircraft/search" in url

    def test_search_payload_uses_icao_hex(self):
        session = self._make_session(_SEARCH_RESULT)
        _search_aircraft(session, "406B48", "VAHH")
        payload = session.post.call_args.kwargs["json"]
        assert payload["ICAO24BitHex"] == "406B48"
        assert payload["Registration"] is None
        assert payload["IncludeDeregistered"] is False

    def test_search_multiple_results_filters_by_mark(self):
        multi = [
            {"AircraftID": 11111, "Mark": "ZZZZ", "RegistrationStatus": "R"},
            {"AircraftID": 66819, "Mark": "VAHH", "RegistrationStatus": "R"},
        ]
        session = self._make_session(multi)
        result = _search_aircraft(session, "406B48", "VAHH")
        assert result == 66819

    def test_search_multiple_results_no_mark_match_returns_none(self):
        multi = [
            {"AircraftID": 11111, "Mark": "AAAA", "RegistrationStatus": "R"},
            {"AircraftID": 22222, "Mark": "BBBB", "RegistrationStatus": "R"},
        ]
        session = self._make_session(multi)
        result = _search_aircraft(session, "406B48", "VAHH")
        assert result is None

    def test_search_multiple_results_case_insensitive(self):
        multi = [
            {"AircraftID": 11111, "Mark": "ZZZZ", "RegistrationStatus": "R"},
            {"AircraftID": 66819, "Mark": "vahh", "RegistrationStatus": "R"},
        ]
        session = self._make_session(multi)
        result = _search_aircraft(session, "406B48", "VAHH")
        assert result == 66819

    def test_search_non_registered_status_returns_none(self):
        session = self._make_session([{"AircraftID": 48843, "Mark": "VAHH", "RegistrationStatus": "D"}])
        result = _search_aircraft(session, "406B48", "VAHH")
        assert result is None

    def test_search_filters_out_non_registered_before_mark(self):
        multi = [
            {"AircraftID": 11111, "Mark": "VAHH", "RegistrationStatus": "D"},
            {"AircraftID": 66819, "Mark": "VAHH", "RegistrationStatus": "R"},
        ]
        session = self._make_session(multi)
        result = _search_aircraft(session, "406B48", "VAHH")
        assert result == 66819

    def test_details_gets_correct_url(self):
        session = self._make_session([], _make_details())
        _get_aircraft_details(session, 66819)
        url = session.get.call_args.args[0]
        assert "/api/aircraft/details/66819" in url

    def test_details_returns_parsed_json(self):
        details = _make_details()
        session = self._make_session([], details)
        result = _get_aircraft_details(session, 66819)
        assert result == details


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def _make_redis(self, existing: dict = None) -> MagicMock:
        r = MagicMock()
        r_json = MagicMock()
        r.json.return_value = r_json
        r_json.mget.return_value = [[existing]] if existing else [None]
        return r

    def _make_session(self, details: dict) -> MagicMock:
        session = MagicMock()

        search_resp = MagicMock()
        search_resp.json.return_value = _SEARCH_RESULT
        search_resp.raise_for_status.return_value = None

        details_resp = MagicMock()
        details_resp.json.return_value = details
        details_resp.raise_for_status.return_value = None

        session.post.return_value = search_resp
        session.get.return_value = details_resp
        return session

    # ------------------------------------------------------------------
    # Slow path (no cached foreign_key)
    # ------------------------------------------------------------------

    def test_slow_path_returns_count(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            count = write_to_redis([("G-VAHH", "406B48", None)], r, session, REDIS_TTL, 0.1)
        assert count == 1

    def test_slow_path_json_set_correct_key(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([("G-VAHH", "406B48", None)], r, session, REDIS_TTL, 0.1)
        key_arg = r.json.return_value.set.call_args.args[0]
        assert key_arg == "icao_hex:406B48"

    def test_slow_path_json_set_uses_root_path(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([("G-VAHH", "406B48", None)], r, session, REDIS_TTL, 0.1)
        assert r.json.return_value.set.call_args.args[1] == "$"

    def test_slow_path_expire_correct_ttl(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([("G-VAHH", "406B48", None)], r, session, REDIS_TTL, 0.1)
        r.expire.assert_called_once_with("icao_hex:406B48", REDIS_TTL)

    def test_slow_path_merges_with_existing(self):
        existing = {"icao_hex": "406B48", "military": False, "aircraft": {"wake_turbulence_category": "Heavy"}}
        r = self._make_redis(existing=existing)
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([("G-VAHH", "406B48", None)], r, session, REDIS_TTL, 0.1)
        written = r.json.return_value.set.call_args.args[2]
        assert written["military"] is False
        assert written["aircraft"]["wake_turbulence_category"] == "Heavy"
        assert written["aircraft"]["manufacturer"] == "BOEING COMPANY"

    def test_slow_path_searches_by_icao_hex(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([("G-VAHH", "406B48", None)], r, session, REDIS_TTL, 0.1)
        payload = session.post.call_args.kwargs["json"]
        assert payload["ICAO24BitHex"] == "406B48"
        assert payload["Registration"] is None

    def test_slow_path_writes_foreign_key(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([("G-VAHH", "406B48", None)], r, session, REDIS_TTL, 0.1)
        written = r.json.return_value.set.call_args.args[2]
        assert written["foreign_key"] == 66819

    def test_slow_path_search_not_found_skips(self):
        r = self._make_redis()
        session = MagicMock()
        search_resp = MagicMock()
        search_resp.json.return_value = []
        search_resp.raise_for_status.return_value = None
        session.post.return_value = search_resp
        with patch("time.sleep"):
            count = write_to_redis([("G-VAHH", "406B48", None)], r, session, REDIS_TTL, 0.1)
        assert count == 0
        r.json.return_value.set.assert_not_called()

    def test_slow_path_http_error_skips_and_continues(self):
        r = self._make_redis()
        session = MagicMock()
        search_resp = MagicMock()
        search_resp.raise_for_status.side_effect = Exception("HTTP 503")
        session.post.return_value = search_resp

        details = _make_details(mark="BCDE", icao_hex="406B49")
        search_resp2 = MagicMock()
        search_resp2.json.return_value = [{"AircraftID": 66820, "Mark": "BCDE", "RegistrationStatus": "R"}]
        search_resp2.raise_for_status.return_value = None
        details_resp = MagicMock()
        details_resp.json.return_value = details
        details_resp.raise_for_status.return_value = None

        session.post.side_effect = [search_resp, search_resp2]
        session.get.return_value = details_resp

        with patch("time.sleep"):
            count = write_to_redis(
                [("G-VAHH", "406B48", None), ("G-BCDE", "406B49", None)],
                r, session, REDIS_TTL, 0.1,
            )
        assert count == 1

    def test_slow_path_details_403_caches_foreign_key(self):
        r = self._make_redis()
        session = MagicMock()

        search_resp = MagicMock()
        search_resp.json.return_value = _SEARCH_RESULT
        search_resp.raise_for_status.return_value = None

        error_resp = MagicMock()
        error_resp.status_code = 403
        details_resp = MagicMock()
        details_resp.raise_for_status.side_effect = requests.HTTPError(response=error_resp)

        session.post.return_value = search_resp
        session.get.return_value = details_resp

        with patch("time.sleep"):
            count = write_to_redis([("G-VAHH", "406B48", None)], r, session, REDIS_TTL, 0.1)

        assert count == 0
        r.json.return_value.set.assert_called_once_with("icao_hex:406B48", "$.foreign_key", 66819)

    # ------------------------------------------------------------------
    # Fast path (cached foreign_key)
    # ------------------------------------------------------------------

    def test_fast_path_skips_search(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([("G-VAHH", "406B48", 66819)], r, session, REDIS_TTL, 0.1)
        session.post.assert_not_called()

    def test_fast_path_calls_details_directly(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([("G-VAHH", "406B48", 66819)], r, session, REDIS_TTL, 0.1)
        url = session.get.call_args.args[0]
        assert "/api/aircraft/details/66819" in url

    def test_fast_path_returns_count(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            count = write_to_redis([("G-VAHH", "406B48", 66819)], r, session, REDIS_TTL, 0.1)
        assert count == 1

    def test_fast_path_preserves_foreign_key(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([("G-VAHH", "406B48", 66819)], r, session, REDIS_TTL, 0.1)
        written = r.json.return_value.set.call_args.args[2]
        assert written["foreign_key"] == 66819

    def test_fast_path_hex_mismatch_triggers_cleanup(self):
        r = self._make_redis()
        r.json.return_value.get.return_value = [{}]
        session = MagicMock()

        details_stale = _make_details(icao_hex="407FFF")
        details_resp_stale = MagicMock()
        details_resp_stale.json.return_value = details_stale
        details_resp_stale.raise_for_status.return_value = None

        session.get.return_value = details_resp_stale

        with patch("time.sleep"):
            count = write_to_redis([("G-VAHH", "406B48", 99999)], r, session, REDIS_TTL, 0.1)

        assert count == 0
        session.post.assert_not_called()
        deleted_paths = [c.args[1] for c in r.json.return_value.delete.call_args_list]
        assert "$.foreign_key" in deleted_paths
        assert "$.registrant" in deleted_paths

    def test_fast_path_http_error_triggers_cleanup(self):
        r = self._make_redis()
        r.json.return_value.get.return_value = [{}]
        session = MagicMock()

        error_resp = MagicMock()
        error_resp.status_code = 404
        details_resp_error = MagicMock()
        details_resp_error.raise_for_status.side_effect = requests.HTTPError(response=error_resp)

        session.get.return_value = details_resp_error

        with patch("time.sleep"):
            count = write_to_redis([("G-VAHH", "406B48", 99999)], r, session, REDIS_TTL, 0.1)

        assert count == 0
        session.post.assert_not_called()
        deleted_paths = [c.args[1] for c in r.json.return_value.delete.call_args_list]
        assert "$.foreign_key" in deleted_paths
        assert "$.registrant" in deleted_paths

    def test_fast_path_403_skips_without_search_or_cleanup(self):
        r = self._make_redis()
        session = MagicMock()

        error_resp = MagicMock()
        error_resp.status_code = 403
        details_resp_error = MagicMock()
        details_resp_error.raise_for_status.side_effect = requests.HTTPError(response=error_resp)

        session.get.return_value = details_resp_error

        with patch("time.sleep"):
            count = write_to_redis([("G-VAHH", "406B48", 99999)], r, session, REDIS_TTL, 0.1)

        assert count == 0
        session.post.assert_not_called()
        r.json.return_value.delete.assert_not_called()

    def test_fast_path_non_registered_status_triggers_cleanup(self):
        r = self._make_redis()
        session = self._make_session(_make_details(reg_status="De-registered"))
        with patch("time.sleep"):
            count = write_to_redis([("G-VAHH", "406B48", 66819)], r, session, REDIS_TTL, 0.1)
        assert count == 0
        deleted_paths = [c.args[1] for c in r.json.return_value.delete.call_args_list]
        assert "$.registrant" in deleted_paths

    def test_slow_path_non_registered_details_skips_without_cleanup(self):
        r = self._make_redis()
        session = MagicMock()

        search_resp = MagicMock()
        search_resp.json.return_value = _SEARCH_RESULT
        search_resp.raise_for_status.return_value = None

        details_resp = MagicMock()
        details_resp.json.return_value = _make_details(reg_status="De-registered")
        details_resp.raise_for_status.return_value = None

        session.post.return_value = search_resp
        session.get.return_value = details_resp

        with patch("time.sleep"):
            count = write_to_redis([("G-VAHH", "406B48", None)], r, session, REDIS_TTL, 0.1)

        assert count == 0
        r.json.return_value.delete.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: MQTT completion stats (mocked)
# ---------------------------------------------------------------------------

class TestMqttCompletionStats:
    def _setup_mock_client(self):
        mock_client = MagicMock()

        def fake_connect(host, port, keepalive):
            mock_client.on_connect(mock_client, None, None, 0, None)

        mock_client.connect.side_effect = fake_connect
        return mock_client

    def test_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("uk_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("uk_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 99, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "99"

    def test_publishes_last_run_at(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("uk_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/last_run_at" in topics

    def test_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("uk_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_no_mqtt_config_skips(self):
        cfg = {}
        mc = self._setup_mock_client()
        with patch("uk_caa_main.mqtt.Client", return_value=mc):
            publish_completion_stats(cfg, 0, "success")
        mc.connect.assert_not_called()

    def test_ha_autodiscovery_three_sensors(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("uk_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 100, "success")
        ha_topics = [
            c.args[0] for c in mc.publish.call_args_list
            if c.args[0].startswith("homeassistant/")
        ]
        assert len(ha_topics) == 3
        assert "homeassistant/sensor/SkyFollower_runner_uk_caa_records_imported/config" in ha_topics
        assert "homeassistant/sensor/SkyFollower_runner_uk_caa_last_run_at/config" in ha_topics
        assert "homeassistant/sensor/SkyFollower_runner_uk_caa_last_run_status/config" in ha_topics
