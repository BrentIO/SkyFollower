"""
Tests for the UK CAA data runner.

Covers:
- Decode helpers (aircraft class, country, year built)
- Record builder (_build_record)
- Search index helper (_ensure_search_index) — mocked
- API helpers (_search_by_prefix, _get_aircraft_details) — mocked
- Registration enumeration (enumerate_registrations) — mocked
- Redis write logic (write_to_redis) — mocked
- MQTT completion stats — mocked
"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import sys
from unittest.mock import MagicMock, patch

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
run_pipeline = _mod.run_pipeline
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
REDIS_TTL = _mod.ENRICHMENT_TTL_SECONDS
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
# Tests: run_pipeline
# ---------------------------------------------------------------------------

class TestRunPipeline:
    def _make_redis(self) -> MagicMock:
        r = MagicMock()
        r.json.return_value = MagicMock()
        # No aircraft:type match by default, so existing assertions on the
        # written record aren't polluted by a MagicMock manufacturer_model
        # value; tests that exercise the lookup override this explicitly.
        r.json.return_value.get.return_value = None
        return r

    def _make_session(self, search_results: list, details: dict) -> MagicMock:
        session = MagicMock()

        search_resp = MagicMock()
        search_resp.json.return_value = search_results
        search_resp.raise_for_status.return_value = None
        session.post.return_value = search_resp

        details_resp = MagicMock()
        details_resp.json.return_value = details
        details_resp.raise_for_status.return_value = None
        session.get.return_value = details_resp

        return session

    def test_searches_all_676_prefixes(self):
        r = self._make_redis()
        session = self._make_session([], _make_details())
        with patch("time.sleep"):
            run_pipeline(session, r, REDIS_TTL, 0.1)
        assert session.post.call_count == 676

    def test_manufacturer_model_set_from_type_lookup_end_to_end(self):
        r = self._make_redis()
        r.json.return_value.get.return_value = {
            "manufacturer_model": "BOEING 787-9 (Dreamliner)",
        }
        session = self._make_session(_SEARCH_RESULT, _make_details(type_designator="B789"))
        with patch("time.sleep"):
            run_pipeline(session, r, REDIS_TTL, 0.1)
        written = r.json.return_value.set.call_args.args[2]
        assert written["aircraft"]["manufacturer_model"] == "BOEING 787-9 (Dreamliner)"

    def test_includes_first_prefix_aa(self):
        r = self._make_redis()
        session = self._make_session([], _make_details())
        with patch("time.sleep"):
            run_pipeline(session, r, REDIS_TTL, 0.1)
        called_prefixes = [c.kwargs["json"]["Registration"] for c in session.post.call_args_list]
        assert "AA" in called_prefixes

    def test_includes_last_prefix_zz(self):
        r = self._make_redis()
        session = self._make_session([], _make_details())
        with patch("time.sleep"):
            run_pipeline(session, r, REDIS_TTL, 0.1)
        called_prefixes = [c.kwargs["json"]["Registration"] for c in session.post.call_args_list]
        assert "ZZ" in called_prefixes

    def test_writes_record_for_registered_result(self):
        r = self._make_redis()
        session = self._make_session(_SEARCH_RESULT, _make_details())
        with patch("time.sleep"):
            count = run_pipeline(session, r, REDIS_TTL, 0.1)
        assert count == 676  # one registered result per prefix

    def test_filters_non_registered_status(self):
        r = self._make_redis()
        search_results = [{"AircraftID": 1, "RegistrationStatus": "D"}]
        session = self._make_session(search_results, _make_details())
        with patch("time.sleep"):
            count = run_pipeline(session, r, REDIS_TTL, 0.1)
        assert count == 0
        r.json.return_value.set.assert_not_called()

    def test_writes_to_correct_key(self):
        r = self._make_redis()
        session = self._make_session(_SEARCH_RESULT, _make_details())
        with patch("time.sleep"):
            run_pipeline(session, r, REDIS_TTL, 0.1)
        key_arg = r.json.return_value.set.call_args.args[0]
        assert key_arg == "aircraft:registry:406B48"

    def test_writes_at_root_path(self):
        r = self._make_redis()
        session = self._make_session(_SEARCH_RESULT, _make_details())
        with patch("time.sleep"):
            run_pipeline(session, r, REDIS_TTL, 0.1)
        assert r.json.return_value.set.call_args.args[1] == "$"

    def test_writes_source_field(self):
        r = self._make_redis()
        session = self._make_session(_SEARCH_RESULT, _make_details())
        with patch("time.sleep"):
            run_pipeline(session, r, REDIS_TTL, 0.1)
        written = r.json.return_value.set.call_args.args[2]
        assert written["source"] == "uk-caa-registry"

    def test_null_fields_omitted_from_written_record(self):
        r = self._make_redis()
        session = self._make_session(_SEARCH_RESULT, _make_details(county=None))
        with patch("time.sleep"):
            run_pipeline(session, r, REDIS_TTL, 0.1)
        written = r.json.return_value.set.call_args.args[2]
        assert "administrative_area" not in written.get("registrant", {})

    def test_no_foreign_key_in_record(self):
        r = self._make_redis()
        session = self._make_session(_SEARCH_RESULT, _make_details())
        with patch("time.sleep"):
            run_pipeline(session, r, REDIS_TTL, 0.1)
        written = r.json.return_value.set.call_args.args[2]
        assert "foreign_key" not in written

    def test_fire_and_forget_no_mget(self):
        r = self._make_redis()
        session = self._make_session(_SEARCH_RESULT, _make_details())
        with patch("time.sleep"):
            run_pipeline(session, r, REDIS_TTL, 0.1)
        r.json.return_value.mget.assert_not_called()

    def test_expire_correct_ttl(self):
        r = self._make_redis()
        session = self._make_session(_SEARCH_RESULT, _make_details())
        with patch("time.sleep"):
            run_pipeline(session, r, REDIS_TTL, 0.1)
        r.expire.assert_called_with("aircraft:registry:406B48", REDIS_TTL)

    def _run(self, session, interval=1.0):
        sleeps = []
        with patch("time.sleep", side_effect=sleeps.append):
            count = run_pipeline(session, self._make_redis(), REDIS_TTL, interval)
        return count, sleeps

    @pytest.mark.parametrize("status", [403, 429])
    def test_refused_search_is_retried_in_place_after_waiting(self, status):
        session = MagicMock()
        session.post.side_effect = [_http_error(status)] + [_ok([])] * 676
        _, sleeps = self._run(session)
        prefixes = [c.kwargs["json"]["Registration"] for c in session.post.call_args_list]
        assert prefixes[:3] == ["AA", "AA", "AB"]
        assert sleeps[:3] == [1.0, 60, 1.0]

    def test_refused_details_request_is_retried_and_written(self):
        session = MagicMock()
        session.post.side_effect = [_ok(_SEARCH_RESULT)] + [_ok([])] * 675
        session.get.side_effect = [_http_error(403), _ok(_make_details())]
        count, sleeps = self._run(session)
        assert count == 1
        assert 60 in sleeps

    def test_waits_increase_then_run_stops(self):
        session = MagicMock()
        session.post.return_value = _http_error(403)
        sleeps = []
        with patch("time.sleep", side_effect=sleeps.append):
            with pytest.raises(RateLimitedError):
                run_pipeline(session, self._make_redis(), REDIS_TTL, 1.0)
        assert [s for s in sleeps if s != 1.0] == [60, 120, 300, 600, 900]
        assert session.post.call_count == 6

    def test_rate_limited_run_reports_records_already_written(self):
        session = MagicMock()
        session.post.side_effect = [_ok(_SEARCH_RESULT)] + [_http_error(403)] * 6
        session.get.return_value = _ok(_make_details())
        with patch("time.sleep"):
            with pytest.raises(RateLimitedError) as raised:
                run_pipeline(session, self._make_redis(), REDIS_TTL, 1.0)
        assert raised.value.records_imported == 1

    @pytest.mark.parametrize("header, expected", [
        ("200", 200), ("5", 60), ("999999", 3600), ("soon", 60),
    ])
    def test_retry_after_header(self, header, expected):
        session = MagicMock()
        session.post.side_effect = [_http_error(429, {"Retry-After": header})] + [_ok([])] * 676
        _, sleeps = self._run(session)
        assert sleeps[1] == expected

    def test_refusal_is_logged_with_diagnostics(self, caplog):
        session = MagicMock()
        session.post.side_effect = (
            [_ok([]), _http_error(403, {"x-azure-ref": "abc123"})] + [_ok([])] * 675
        )
        with caplog.at_level(logging.INFO, logger="uk-caa-registry"):
            self._run(session)
        assert "after 1 accepted requests" in caplog.text
        assert "x-azure-ref=abc123" in caplog.text
        assert "blocked" in caplog.text
        assert "Accepted again after waiting 60s" in caplog.text

    def test_error_page_is_logged_as_plain_text(self):
        page = "<!DOCTYPE html><html><style>body { }</style>\n<h2>Request  blocked</h2></html>"
        assert _mod._page_text(page) == "Request blocked"

    def test_server_error_is_retried(self):
        session = MagicMock()
        session.post.side_effect = [_http_error(503)] + [_ok([])] * 676
        _, sleeps = self._run(session)
        assert sleeps[1] == 2

    def test_connection_error_is_retried(self):
        session = MagicMock()
        session.post.side_effect = [requests.ConnectionError()] + [_ok([])] * 676
        _, sleeps = self._run(session)
        assert sleeps[1] == 2

    def test_lost_prefix_search_marks_run_incomplete(self):
        session = MagicMock()
        session.post.side_effect = [_http_error(400)] + [_ok([])] * 675
        with pytest.raises(IncompleteRunError):
            self._run(session)
        assert session.post.call_count == 676

    def test_many_failed_aircraft_mark_run_incomplete(self):
        session = MagicMock()
        session.post.return_value = _ok(_SEARCH_RESULT)
        session.get.return_value = _http_error(404)
        with pytest.raises(IncompleteRunError):
            self._run(session)
        assert session.get.call_count == 676

    def test_few_failed_aircraft_still_succeed(self):
        session = MagicMock()
        session.post.return_value = _ok(_SEARCH_RESULT)
        session.get.side_effect = [_http_error(404)] + [_ok(_make_details())] * 675
        count, _ = self._run(session)
        assert count == 675

    def test_skips_non_registered_details_status(self):
        r = self._make_redis()
        session = self._make_session(_SEARCH_RESULT, _make_details(reg_status="De-registered"))
        with patch("time.sleep"):
            count = run_pipeline(session, r, REDIS_TTL, 0.1)
        assert count == 0
        r.json.return_value.set.assert_not_called()

    def test_every_request_is_paced(self):
        session = MagicMock()
        session.post.side_effect = [_ok(_SEARCH_RESULT)] + [_ok([])] * 675
        session.get.return_value = _ok(_make_details())
        _, sleeps = self._run(session, interval=0.25)
        assert sleeps == [0.25] * 677


# ---------------------------------------------------------------------------
# Tests: main
# ---------------------------------------------------------------------------

class TestMain:
    def _run_main(self, monkeypatch, **pipeline):
        monkeypatch.setenv("REDIS_HOST", "localhost")
        monkeypatch.setenv("REDIS_PASSWORD", "x")
        with patch.object(_mod, "build_redis_client"), \
                patch.object(_mod, "configure_logging"), \
                patch.object(_mod, "_ensure_search_index"), \
                patch.object(_mod, "run_pipeline", **pipeline) as run, \
                patch.object(_mod, "publish_completion_stats") as publish:
            _mod.main()
        return run, publish

    @pytest.mark.parametrize("value, expected", [(None, 1.0), ("2.5", 2.5)])
    def test_request_interval_from_environment(self, monkeypatch, value, expected):
        monkeypatch.delenv("REQUEST_INTERVAL_SECONDS", raising=False)
        if value:
            monkeypatch.setenv("REQUEST_INTERVAL_SECONDS", value)
        run, publish = self._run_main(monkeypatch, return_value=5)
        assert run.call_args.args[3] == expected
        assert publish.call_args.args[1:] == (5, "success")

    def test_incomplete_run_publishes_failure_with_count_and_exits_nonzero(self, monkeypatch):
        with patch.object(_mod.sys, "exit") as exit_:
            _, publish = self._run_main(
                monkeypatch, side_effect=RateLimitedError("refused", 42)
            )
        assert publish.call_args.args[1:] == (42, "failure")
        exit_.assert_called_once_with(1)


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
        with patch("uk_caa_registry_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("uk_caa_registry_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 99, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "99"

    def test_publishes_last_run_at(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("uk_caa_registry_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/last_run_at" in topics

    def test_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("uk_caa_registry_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "Failure"

    def test_no_mqtt_config_skips(self):
        cfg = {}
        mc = self._setup_mock_client()
        with patch("uk_caa_registry_main.mqtt.Client", return_value=mc):
            publish_completion_stats(cfg, 0, "success")
        mc.connect.assert_not_called()

    def test_blank_host_skips_without_crashing(self):
        """mqtt_config() returns {"host": ""} rather than None/{} when MQTT is
        unset, so a bare `if not mc` check misses it and crashes on
        None.on_connect. Must not raise."""
        cfg = {"mqtt": {"host": "", "port": 1883, "username": "", "password": ""}}
        publish_completion_stats(cfg, 0, "success")

    def test_ha_autodiscovery_three_sensors(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("uk_caa_registry_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 100, "success")
        ha_topics = [
            c.args[0] for c in mc.publish.call_args_list
            if c.args[0].startswith("homeassistant/")
        ]
        assert len(ha_topics) == 3
        assert "homeassistant/sensor/SkyFollower_runner_uk_caa_registry_records_imported/config" in ha_topics
        assert "homeassistant/sensor/SkyFollower_runner_uk_caa_registry_last_run_at/config" in ha_topics
        assert "homeassistant/sensor/SkyFollower_runner_uk_caa_registry_last_run_status/config" in ha_topics
