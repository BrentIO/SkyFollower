"""Tests for the Iceland Samgöngustofa data runner."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Module import
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
_RUNNER_DIR = os.path.dirname(_HERE)
_REPO_ROOT = os.path.abspath(os.path.join(_RUNNER_DIR, "..", ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "is_samgongustofa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["is_samgongustofa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_decode_country = _mod._decode_country
_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_aircraft(
    identifiers="TF-ABC",
    serial_number="12345",
    production_year=2010,
    aircraft_type="Cessna 172",
    operator_name="Íslandsflug hf",
    operator_address="Flugvellir 1",
    operator_city="Reykjavik",
    operator_postcode="101",
    operator_country="Ísland",
) -> dict:
    return {
        "identifiers": identifiers,
        "serialNumber": serial_number,
        "productionYear": production_year,
        "type": aircraft_type,
        "operator": {
            "name": operator_name,
            "address": operator_address,
            "city": operator_city,
            "postcode": operator_postcode,
            "country": operator_country,
        },
    }


def _make_redis_with_search(icao_hex="4CA123", registration="TF-ABC"):
    r = MagicMock()
    doc = MagicMock()
    doc.id = f"aircraft:mictronics:{icao_hex}"
    doc.registration = registration
    results = MagicMock()
    results.docs = [doc]
    r.ft.return_value.search.return_value = results
    r.json.return_value.get.return_value = None
    return r


def _make_redis_no_match():
    r = MagicMock()
    results = MagicMock()
    results.docs = []
    r.ft.return_value.search.return_value = results
    return r


# ---------------------------------------------------------------------------
# Tests: _decode_country
# ---------------------------------------------------------------------------

class TestDecodeCountry:
    def test_island_maps_to_is(self):
        assert _decode_country("Ísland") == "IS"

    def test_iceland_english_maps_to_is(self):
        assert _decode_country("Iceland") == "IS"

    def test_ireland_maps_to_ie(self):
        assert _decode_country("Ireland") == "IE"

    def test_united_kingdom_maps_to_gb(self):
        assert _decode_country("United Kingdom") == "GB"

    def test_germany_maps_to_de(self):
        assert _decode_country("Germany") == "DE"

    def test_unknown_passes_through(self):
        assert _decode_country("Utopia") == "Utopia"

    def test_empty_returns_none(self):
        assert _decode_country("") is None

    def test_none_returns_none(self):
        assert _decode_country(None) is None

    def test_whitespace_only_returns_none(self):
        assert _decode_country("   ") is None

    def test_strips_whitespace(self):
        assert _decode_country("  Ísland  ") == "IS"


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        aircraft = _make_aircraft()
        record = _build_record(aircraft, "4CA123", "TF-ABC")
        assert record["icao_hex"] == "4CA123"
        assert record["registration"] == "TF-ABC"
        assert record["source"] == "is-samgongustofa"
        assert record["aircraft"]["model"] == "Cessna 172"
        assert record["aircraft"]["serial_number"] == "12345"
        assert record["aircraft"]["manufactured_date"] == "2010-01-01"
        assert record["registrant"]["names"] == ["Íslandsflug hf"]
        assert record["registrant"]["street"] == ["Flugvellir 1"]
        assert record["registrant"]["city"] == "Reykjavik"
        assert record["registrant"]["postal_code"] == "101"
        assert record["registrant"]["country"] == "IS"

    def test_missing_type_omitted(self):
        aircraft = _make_aircraft(aircraft_type="")
        record = _build_record(aircraft, "4CA123", "TF-ABC")
        assert "model" not in record.get("aircraft", {})

    def test_missing_serial_omitted(self):
        aircraft = _make_aircraft(serial_number="")
        record = _build_record(aircraft, "4CA123", "TF-ABC")
        assert "serial_number" not in record.get("aircraft", {})

    def test_zero_production_year_omitted(self):
        aircraft = _make_aircraft(production_year=0)
        record = _build_record(aircraft, "4CA123", "TF-ABC")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_none_production_year_omitted(self):
        aircraft = _make_aircraft(production_year=None)
        record = _build_record(aircraft, "4CA123", "TF-ABC")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_missing_operator_name_omitted(self):
        aircraft = _make_aircraft(operator_name="")
        record = _build_record(aircraft, "4CA123", "TF-ABC")
        assert "names" not in record.get("registrant", {})

    def test_missing_operator_city_omitted(self):
        aircraft = _make_aircraft(operator_city="")
        record = _build_record(aircraft, "4CA123", "TF-ABC")
        assert "city" not in record.get("registrant", {})

    def test_null_operator_produces_no_registrant(self):
        aircraft = _make_aircraft()
        aircraft["operator"] = None
        record = _build_record(aircraft, "4CA123", "TF-ABC")
        assert "registrant" not in record or not record["registrant"]

    def test_no_aircraft_fields_omits_aircraft_key(self):
        aircraft = _make_aircraft(aircraft_type="", serial_number="", production_year=0)
        record = _build_record(aircraft, "4CA123", "TF-ABC")
        assert "aircraft" not in record


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("TFABC") == "TFABC"

    def test_hyphen_escaped(self):
        result = _escape_tag("TF-ABC")
        assert "\\-" in result

    def test_dot_escaped(self):
        result = _escape_tag("TF.ABC")
        assert "\\." in result

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        aircraft = [_make_aircraft()]
        r = _make_redis_with_search(icao_hex="4CA123", registration="TF-ABC")
        count = write_to_redis(aircraft, r, REDIS_TTL)
        assert count == 1

    def test_record_not_written_when_not_in_redis(self):
        aircraft = [_make_aircraft()]
        r = _make_redis_no_match()
        count = write_to_redis(aircraft, r, REDIS_TTL)
        assert count == 0

    def test_empty_registration_skipped(self):
        aircraft = [_make_aircraft(identifiers="")]
        r = _make_redis_with_search()
        count = write_to_redis(aircraft, r, REDIS_TTL)
        assert count == 0
        r.ft.return_value.search.assert_not_called()

    def test_writes_to_detail_key(self):
        aircraft = [_make_aircraft()]
        r = _make_redis_with_search(icao_hex="4CA123", registration="TF-ABC")
        write_to_redis(aircraft, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call is not None
        key_used = set_call[0][0]
        assert key_used == "aircraft:registry:4CA123"

    def test_source_field_in_written_record(self):
        aircraft = [_make_aircraft()]
        r = _make_redis_with_search(icao_hex="4CA123", registration="TF-ABC")
        write_to_redis(aircraft, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call is not None
        written = set_call[0][2]
        assert written.get("source") == "is-samgongustofa"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_multiple_records_written(self):
        aircrafts = [
            _make_aircraft(identifiers="TF-ABC"),
            _make_aircraft(identifiers="TF-XYZ", serial_number="99999"),
        ]
        r = MagicMock()

        call_count = [0]

        def _search(query):
            call_count[0] += 1
            results = MagicMock()
            docs = []
            for reg in ["TF-ABC", "TF-XYZ"]:
                doc = MagicMock()
                doc.id = f"aircraft:mictronics:4CA12{call_count[0]}"
                doc.registration = reg
                docs.append(doc)
            results.docs = docs
            return results

        r.ft.return_value.search.side_effect = _search
        count = write_to_redis(aircrafts, r, REDIS_TTL)
        assert count == 2

    def test_ttl_applied(self):
        aircraft = [_make_aircraft()]
        r = _make_redis_with_search(icao_hex="4CA123", registration="TF-ABC")
        write_to_redis(aircraft, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:4CA123", REDIS_TTL)

    def test_null_fields_omitted_from_written_record(self):
        aircraft = [_make_aircraft(aircraft_type="", serial_number="")]
        r = _make_redis_with_search(icao_hex="4CA123", registration="TF-ABC")
        write_to_redis(aircraft, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call is not None
        assert set_call[0][1] == "$"
        written = set_call[0][2]
        assert "model" not in written["aircraft"]
        assert "serial_number" not in written["aircraft"]


# ---------------------------------------------------------------------------
# Tests: publish_completion_stats
# ---------------------------------------------------------------------------

class TestPublishCompletionStats:
    def _setup_mock_client(self):
        mock_client = MagicMock()

        def fake_connect(host, port, keepalive):
            mock_client.on_connect(mock_client, None, None, 0, None)

        mock_client.connect.side_effect = fake_connect
        return mock_client

    def test_no_mqtt_config_skips(self):
        publish_completion_stats({}, 100, "success")

    def test_mqtt_connect_timeout_does_not_raise(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        with patch("is_samgongustofa_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("is_samgongustofa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("is_samgongustofa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 77, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "77"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("is_samgongustofa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/is-samgongustofa"
