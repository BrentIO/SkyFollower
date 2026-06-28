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
import os
import sys
from unittest.mock import MagicMock, patch

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
_search_by_prefix = _mod._search_by_prefix
_get_aircraft_details = _mod._get_aircraft_details
enumerate_registrations = _mod.enumerate_registrations
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
        _search_by_prefix(session, "VA")
        url = session.post.call_args.args[0]
        assert "/api/aircraft/search" in url

    def test_sends_registration_prefix(self):
        session = self._make_session(_SEARCH_RESULT)
        _search_by_prefix(session, "VA")
        payload = session.post.call_args.kwargs["json"]
        assert payload == {"Registration": "VA"}

    def test_returns_results_list(self):
        session = self._make_session(_SEARCH_RESULT)
        results = _search_by_prefix(session, "VA")
        assert results == _SEARCH_RESULT

    def test_empty_response_returns_empty_list(self):
        session = self._make_session([])
        results = _search_by_prefix(session, "ZZ")
        assert results == []

    def test_none_response_returns_empty_list(self):
        session = self._make_session(None)
        results = _search_by_prefix(session, "ZZ")
        assert results == []


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
        _get_aircraft_details(session, 66819)
        url = session.get.call_args.args[0]
        assert "/api/aircraft/details/66819" in url

    def test_returns_parsed_json(self):
        details = _make_details()
        session = self._make_session(details)
        result = _get_aircraft_details(session, 66819)
        assert result == details


# ---------------------------------------------------------------------------
# Tests: enumerate_registrations
# ---------------------------------------------------------------------------

class TestEnumerateRegistrations:
    def _make_session(self, results: list) -> MagicMock:
        session = MagicMock()
        resp = MagicMock()
        resp.json.return_value = results
        resp.raise_for_status.return_value = None
        session.post.return_value = resp
        return session

    def test_calls_all_676_prefixes(self):
        session = self._make_session([])
        with patch("time.sleep"):
            enumerate_registrations(session, 0.1)
        assert session.post.call_count == 676

    def test_includes_first_prefix_aa(self):
        session = self._make_session([])
        with patch("time.sleep"):
            enumerate_registrations(session, 0.1)
        called_prefixes = [c.kwargs["json"]["Registration"] for c in session.post.call_args_list]
        assert "AA" in called_prefixes

    def test_includes_last_prefix_zz(self):
        session = self._make_session([])
        with patch("time.sleep"):
            enumerate_registrations(session, 0.1)
        called_prefixes = [c.kwargs["json"]["Registration"] for c in session.post.call_args_list]
        assert "ZZ" in called_prefixes

    def test_filters_non_registered_status(self):
        results = [
            {"AircraftID": 1, "RegistrationStatus": "R"},
            {"AircraftID": 2, "RegistrationStatus": "D"},
        ]
        session = self._make_session(results)
        with patch("time.sleep"):
            ids = enumerate_registrations(session, 0.1)
        # Only "R" entries; 676 calls × 1 "R" each
        assert 2 not in ids
        assert all(i == 1 for i in ids)

    def test_returns_aircraft_ids(self):
        session = self._make_session(_SEARCH_RESULT)
        with patch("time.sleep"):
            ids = enumerate_registrations(session, 0.1)
        assert 66819 in ids

    def test_skips_missing_aircraft_id(self):
        results = [{"RegistrationStatus": "R"}]  # no AircraftID
        session = self._make_session(results)
        with patch("time.sleep"):
            ids = enumerate_registrations(session, 0.1)
        assert ids == []

    def test_error_on_prefix_skipped_enumeration_continues(self):
        session = MagicMock()
        good_resp = MagicMock()
        good_resp.json.return_value = _SEARCH_RESULT
        good_resp.raise_for_status.return_value = None

        bad_resp = MagicMock()
        bad_resp.raise_for_status.side_effect = Exception("HTTP 503")

        # First call fails, remaining succeed
        session.post.side_effect = [bad_resp] + [good_resp] * 675

        with patch("time.sleep"):
            ids = enumerate_registrations(session, 0.1)

        assert session.post.call_count == 676
        assert len(ids) == 675


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def _make_redis(self) -> MagicMock:
        r = MagicMock()
        r_json = MagicMock()
        r.json.return_value = r_json
        return r

    def _make_session(self, details: dict) -> MagicMock:
        session = MagicMock()
        resp = MagicMock()
        resp.json.return_value = details
        resp.raise_for_status.return_value = None
        session.get.return_value = resp
        return session

    def test_returns_count(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            count = write_to_redis([66819], r, session, REDIS_TTL, 0.1)
        assert count == 1

    def test_writes_to_correct_key(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([66819], r, session, REDIS_TTL, 0.1)
        key_arg = r.json.return_value.set.call_args.args[0]
        assert key_arg == "aircraft:detail:406B48"

    def test_writes_at_root_path(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([66819], r, session, REDIS_TTL, 0.1)
        assert r.json.return_value.set.call_args.args[1] == "$"

    def test_writes_source_field(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([66819], r, session, REDIS_TTL, 0.1)
        written = r.json.return_value.set.call_args.args[2]
        assert written["source"] == "uk-caa"

    def test_no_foreign_key_in_record(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([66819], r, session, REDIS_TTL, 0.1)
        written = r.json.return_value.set.call_args.args[2]
        assert "foreign_key" not in written

    def test_fire_and_forget_no_mget(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([66819], r, session, REDIS_TTL, 0.1)
        r.json.return_value.mget.assert_not_called()

    def test_expire_correct_ttl(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([66819], r, session, REDIS_TTL, 0.1)
        r.expire.assert_called_once_with("aircraft:detail:406B48", REDIS_TTL)

    def test_calls_details_with_aircraft_id(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            write_to_redis([66819], r, session, REDIS_TTL, 0.1)
        url = session.get.call_args.args[0]
        assert "/api/aircraft/details/66819" in url

    def test_skips_non_registered_status(self):
        r = self._make_redis()
        session = self._make_session(_make_details(reg_status="De-registered"))
        with patch("time.sleep"):
            count = write_to_redis([66819], r, session, REDIS_TTL, 0.1)
        assert count == 0
        r.json.return_value.set.assert_not_called()

    def test_details_error_skips_and_continues(self):
        r = self._make_redis()
        session = MagicMock()

        error_resp = MagicMock()
        error_resp.raise_for_status.side_effect = Exception("HTTP 503")

        good_resp = MagicMock()
        good_resp.json.return_value = _make_details(mark="BCDE", icao_hex="406B49")
        good_resp.raise_for_status.return_value = None

        session.get.side_effect = [error_resp, good_resp]

        with patch("time.sleep"):
            count = write_to_redis([66819, 66820], r, session, REDIS_TTL, 0.1)
        assert count == 1

    def test_empty_aircraft_ids_returns_zero(self):
        r = self._make_redis()
        session = self._make_session(_make_details())
        with patch("time.sleep"):
            count = write_to_redis([], r, session, REDIS_TTL, 0.1)
        assert count == 0
        r.json.return_value.set.assert_not_called()

    def test_multiple_aircraft_written(self):
        r = self._make_redis()
        session = MagicMock()

        resp1 = MagicMock()
        resp1.json.return_value = _make_details(mark="VAHH", icao_hex="406B48")
        resp1.raise_for_status.return_value = None

        resp2 = MagicMock()
        resp2.json.return_value = _make_details(mark="BCDE", icao_hex="406B49")
        resp2.raise_for_status.return_value = None

        session.get.side_effect = [resp1, resp2]

        with patch("time.sleep"):
            count = write_to_redis([66819, 66820], r, session, REDIS_TTL, 0.1)
        assert count == 2
        assert r.json.return_value.set.call_count == 2


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
