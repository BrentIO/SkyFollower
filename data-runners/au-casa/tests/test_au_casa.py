"""Tests for the Australia CASA data runner."""

from __future__ import annotations

import importlib.util
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
        "au_casa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["au_casa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_decode_aircraft_type = _mod._decode_aircraft_type
_decode_engine_type = _mod._decode_engine_type
_decode_country = _mod._decode_country
_parse_manufactured_date = _mod._parse_manufactured_date
_parse_int = _mod._parse_int
_type_tokens = _mod._type_tokens
_type_check_passes = _mod._type_check_passes
_escape_tag = _mod._escape_tag
_build_record = _mod._build_record
_apply_type_lookup = _mod._apply_type_lookup
download_registry = _mod.download_registry
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
DOWNLOAD_URL = _mod.DOWNLOAD_URL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    mark="ABC",
    suspendstatus="",
    airframe="Power Driven Aeroplane",
    manu="Cessna",
    model="172S",
    serial="172S12345",
    yearmanu="2010",
    icao_type_desig="C172",
    engnum="1",
    engmanu="Lycoming",
    engtype="Piston",
    engmodel="IO-360",
    regholdname="John Smith",
    regholdadd1="1 Main St",
    regholdadd2="",
    regholdsuburb="Sydney",
    regholdstate="NSW",
    regholdpostcode="2000",
    regholdcountry="Australia",
) -> dict:
    return {
        "Mark": mark,
        "suspendstatus": suspendstatus,
        "Airframe": airframe,
        "Manu": manu,
        "Model": model,
        "Serial": serial,
        "Yearmanu": yearmanu,
        "ICAOtypedesig": icao_type_desig,
        "engnum": engnum,
        "Engmanu": engmanu,
        "Engtype": engtype,
        "Engmodel": engmodel,
        "regholdname": regholdname,
        "regholdadd1": regholdadd1,
        "regholdadd2": regholdadd2,
        "regholdSuburb": regholdsuburb,
        "regholdState": regholdstate,
        "regholdPostcode": regholdpostcode,
        "regholdCountry": regholdcountry,
    }


def _make_redis():
    r = MagicMock()
    return r


def _make_redis_with_search(icao_hex="7C1234", registration="VH-ABC", simple_record=None, type_doc=None):
    """Mock Redis client that resolves one registration via the Mictronics search index.

    ``simple_record`` is what ``r.json().get(...)`` returns for the
    aircraft:mictronics key used by the type sanity check. ``None`` (the
    default) simulates the key not existing, which does NOT block the write.

    ``type_doc`` is what ``r.json().get(...)`` returns for the aircraft:type
    key used by ``_apply_type_lookup``. Kept distinct from ``simple_record``
    via a key-aware side_effect, since both go through the same
    ``r.json().get(...)`` call shape but query different keys.
    """
    r = _make_redis()
    doc = MagicMock()
    doc.id = f"aircraft:mictronics:{icao_hex}"
    doc.registration = registration
    results = MagicMock()
    results.docs = [doc]
    r.ft.return_value.search.return_value = results

    def _json_get(key):
        if key == f"aircraft:mictronics:{icao_hex}":
            return simple_record
        return type_doc

    r.json.return_value.get.side_effect = _json_get
    return r


def _make_redis_no_match():
    r = _make_redis()
    results = MagicMock()
    results.docs = []
    r.ft.return_value.search.return_value = results
    return r


# ---------------------------------------------------------------------------
# Tests: _decode_aircraft_type
# ---------------------------------------------------------------------------

class TestDecodeAircraftType:
    def test_power_driven_aeroplane_maps_to_airplane(self):
        assert _decode_aircraft_type("Power Driven Aeroplane") == "Airplane"

    def test_rotorcraft_maps_to_helicopter(self):
        assert _decode_aircraft_type("Rotorcraft") == "Helicopter"

    def test_motor_glider_maps_to_powered_glider(self):
        assert _decode_aircraft_type("Motor-Glider") == "Powered Glider"

    def test_unknown_passes_through(self):
        assert _decode_aircraft_type("Something Else") == "Something Else"

    def test_empty_returns_none(self):
        assert _decode_aircraft_type("") is None


# ---------------------------------------------------------------------------
# Tests: _decode_engine_type
# ---------------------------------------------------------------------------

class TestDecodeEngineType:
    def test_piston_maps_to_piston(self):
        assert _decode_engine_type("Piston") == "Piston"

    def test_turbofan_maps_to_turbo_fan(self):
        assert _decode_engine_type("Turbofan") == "Turbo-fan"

    def test_not_applicable_maps_to_none(self):
        assert _decode_engine_type("Not Applicable") is None

    def test_unknown_passes_through(self):
        assert _decode_engine_type("Something Else") == "Something Else"

    def test_empty_returns_none(self):
        assert _decode_engine_type("") is None


# ---------------------------------------------------------------------------
# Tests: _decode_country
# ---------------------------------------------------------------------------

class TestDecodeCountry:
    def test_australia_maps_to_au(self):
        assert _decode_country("Australia") == "AU"

    def test_united_states_maps_to_us(self):
        assert _decode_country("United States of America") == "US"

    def test_unknown_passes_through(self):
        assert _decode_country("Utopia") == "Utopia"

    def test_empty_returns_none(self):
        assert _decode_country("") is None


# ---------------------------------------------------------------------------
# Tests: _parse_manufactured_date
# ---------------------------------------------------------------------------

class TestParseManufacturedDate:
    def test_valid_year(self):
        assert _parse_manufactured_date("2010") == "2010-01-01T00:00:00Z"

    def test_empty_returns_none(self):
        assert _parse_manufactured_date("") is None

    def test_invalid_returns_none(self):
        assert _parse_manufactured_date("unknown") is None


# ---------------------------------------------------------------------------
# Tests: _parse_int
# ---------------------------------------------------------------------------

class TestParseInt:
    def test_valid_int(self):
        assert _parse_int("2") == 2

    def test_zero_returns_none(self):
        assert _parse_int("0") is None

    def test_empty_returns_none(self):
        assert _parse_int("") is None

    def test_invalid_returns_none(self):
        assert _parse_int("abc") is None


# ---------------------------------------------------------------------------
# Tests: _type_tokens / _type_check_passes
# ---------------------------------------------------------------------------

class TestTypeCheckPasses:
    def test_empty_detail_model_always_passes(self):
        assert _type_check_passes({"type_designator": "C172"}, "") is True

    def test_no_simple_tokens_passes(self):
        assert _type_check_passes({}, "C172") is True

    def test_matching_tokens_pass(self):
        simple = {"type_designator": "C172"}
        assert _type_check_passes(simple, "CESSNA 172S") is True

    def test_mismatched_tokens_fail(self):
        simple = {"type_designator": "AW109"}
        assert _type_check_passes(simple, "LEONARDO AW139") is False


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("VHABC") == "VHABC"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("VH-ABC")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "7C1234", "VH-ABC")
        assert record["icao_hex"] == "7C1234"
        assert record["registration"] == "VH-ABC"
        assert record["military"] is False
        assert record["aircraft"]["type"] == "Airplane"
        assert record["aircraft"]["manufacturer"] == "Cessna"
        assert record["aircraft"]["model"] == "172S"
        assert record["aircraft"]["serial_number"] == "172S12345"
        assert record["aircraft"]["manufactured_date"] == "2010-01-01T00:00:00Z"
        assert record["aircraft"]["type_designator"] == "C172"
        assert record["aircraft"]["powerplant"]["count"] == 1
        assert record["aircraft"]["powerplant"]["manufacturer"] == "Lycoming"
        assert record["aircraft"]["powerplant"]["type"] == "Piston"
        assert record["aircraft"]["powerplant"]["model"] == "IO-360"
        assert record["registrant"]["names"] == ["John Smith"]
        assert record["registrant"]["street"] == ["1 Main St"]
        assert record["registrant"]["city"] == "Sydney"
        assert record["registrant"]["administrative_area"] == "NSW"
        assert record["registrant"]["postal_code"] == "2000"
        assert record["registrant"]["country"] == "AU"

    def test_two_street_lines(self):
        row = _make_row(regholdadd1="Unit 1", regholdadd2="1 Main St")
        record = _build_record(row, "7C1234", "VH-ABC")
        assert record["registrant"]["street"] == ["Unit 1", "1 Main St"]

    def test_empty_address_line_2_omitted_from_street(self):
        row = _make_row(regholdadd1="1 Main St", regholdadd2="")
        record = _build_record(row, "7C1234", "VH-ABC")
        assert record["registrant"]["street"] == ["1 Main St"]

    def test_not_applicable_engine_type_omitted(self):
        row = _make_row(engtype="Not Applicable")
        record = _build_record(row, "7C1234", "VH-ABC")
        assert "type" not in record["aircraft"]["powerplant"]

    def test_zero_engine_count_omitted(self):
        row = _make_row(engnum="0")
        record = _build_record(row, "7C1234", "VH-ABC")
        assert "count" not in record["aircraft"].get("powerplant", {})

    def test_no_powerplant_fields_omits_powerplant_key(self):
        row = _make_row(engnum="", engmanu="", engtype="", engmodel="")
        record = _build_record(row, "7C1234", "VH-ABC")
        assert "powerplant" not in record.get("aircraft", {})

    def test_no_registrant_name_or_address_omits_registrant(self):
        row = _make_row(
            regholdname="", regholdadd1="", regholdadd2="",
            regholdsuburb="", regholdstate="", regholdpostcode="", regholdcountry="",
        )
        record = _build_record(row, "7C1234", "VH-ABC")
        assert "registrant" not in record

    def test_empty_manufacturer_omitted(self):
        row = _make_row(manu="")
        record = _build_record(row, "7C1234", "VH-ABC")
        assert "manufacturer" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial="")
        record = _build_record(row, "7C1234", "VH-ABC")
        assert "serial_number" not in record.get("aircraft", {})


# ---------------------------------------------------------------------------
# Tests: _apply_type_lookup
# ---------------------------------------------------------------------------

class TestApplyTypeLookup:
    def _make_redis(self, type_doc=None) -> MagicMock:
        r = MagicMock()
        r.json.return_value.get.return_value = type_doc
        return r

    def test_designator_resolves_sets_manufacturer_model(self):
        record = {"aircraft": {"type_designator": "EC25"}}
        r = self._make_redis({"manufacturer_model": "AIRBUS HELICOPTERS EC-225/725"})
        _apply_type_lookup(record, r)
        assert record["aircraft"]["manufacturer_model"] == "AIRBUS HELICOPTERS EC-225/725"

    def test_designator_resolves_sets_wake_turbulence_category(self):
        record = {"aircraft": {"type_designator": "R44"}}
        r = self._make_redis({"manufacturer_model": "ROBINSON R-44 Raven", "wake_turbulence_category": "Light"})
        _apply_type_lookup(record, r)
        assert record["aircraft"]["wake_turbulence_category"] == "Light"

    def test_lookup_key_uses_type_designator(self):
        record = {"aircraft": {"type_designator": "R44"}}
        r = self._make_redis({"manufacturer_model": "ROBINSON R-44 Raven"})
        _apply_type_lookup(record, r)
        r.json.return_value.get.assert_called_once_with("aircraft:type:R44")

    def test_write_happens_even_if_mictronics_manufacturer_model_already_present(self):
        record = {"aircraft": {"type_designator": "R44", "manufacturer_model": "STALE VALUE"}}
        r = self._make_redis({"manufacturer_model": "ROBINSON R-44 Raven"})
        _apply_type_lookup(record, r)
        assert record["aircraft"]["manufacturer_model"] == "ROBINSON R-44 Raven"

    def test_designator_not_found_leaves_fields_unset(self):
        record = {"aircraft": {"type_designator": "ZZZZ"}}
        r = self._make_redis(None)
        _apply_type_lookup(record, r)
        assert "manufacturer_model" not in record["aircraft"]
        assert "wake_turbulence_category" not in record["aircraft"]

    def test_no_type_designator_skips_lookup_entirely(self):
        record = {"aircraft": {"model": "Some Model"}}
        r = self._make_redis({"manufacturer_model": "SHOULD NOT BE USED"})
        _apply_type_lookup(record, r)
        assert "manufacturer_model" not in record["aircraft"]
        r.json.return_value.get.assert_not_called()

    def test_no_aircraft_object_does_not_crash(self):
        record = {"icao_hex": "7C1234"}
        r = self._make_redis({"manufacturer_model": "SHOULD NOT BE USED"})
        _apply_type_lookup(record, r)
        assert "aircraft" not in record

    def test_redis_lookup_failure_degrades_gracefully(self):
        record = {"aircraft": {"type_designator": "R44"}}
        r = MagicMock()
        r.json.return_value.get.side_effect = Exception("connection refused")
        _apply_type_lookup(record, r)
        assert "manufacturer_model" not in record["aircraft"]

    def test_manufacturer_model_and_wake_turbulence_category_independent(self):
        record = {"aircraft": {"type_designator": "XXXX"}}
        r = self._make_redis({"wake_turbulence_category": "Light"})
        _apply_type_lookup(record, r)
        assert "manufacturer_model" not in record["aircraft"]
        assert record["aircraft"]["wake_turbulence_category"] == "Light"


# ---------------------------------------------------------------------------
# Tests: download_registry
# ---------------------------------------------------------------------------

class TestDownloadRegistry:
    def test_parses_rows(self):
        with patch("au_casa_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 200
            resp.content = (
                "Mark,Airframe\r\n"
                "ABC,Power Driven Aeroplane\r\n"
            ).encode("utf-8-sig")
            mock_get.return_value = resp
            rows = download_registry(DOWNLOAD_URL)
        assert len(rows) == 1
        assert rows[0]["Mark"] == "ABC"

    def test_raises_on_http_error(self):
        with patch("au_casa_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 503
            mock_get.return_value = resp
            with pytest.raises(RuntimeError, match="HTTP 503"):
                download_registry(DOWNLOAD_URL)


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="7C1234", registration="VH-ABC")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_manufacturer_model_set_from_type_lookup_end_to_end(self):
        rows = [_make_row(icao_type_desig="C172")]
        type_doc = {"manufacturer_model": "CESSNA 172 Skyhawk", "wake_turbulence_category": "Light"}
        r = _make_redis_with_search(icao_hex="7C1234", registration="VH-ABC", type_doc=type_doc)
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        written = set_call[0][2]
        assert written["aircraft"]["manufacturer_model"] == "CESSNA 172 Skyhawk"
        assert written["aircraft"]["wake_turbulence_category"] == "Light"

    def test_no_redis_match_not_written(self):
        rows = [_make_row()]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_suspended_record_filtered(self):
        rows = [_make_row(suspendstatus="Suspended")]
        r = _make_redis_with_search(icao_hex="7C1234", registration="VH-ABC")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_suspended_status_case_insensitive(self):
        rows = [_make_row(suspendstatus="SUSPENDED")]
        r = _make_redis_with_search(icao_hex="7C1234", registration="VH-ABC")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_missing_simple_record_still_written(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="7C1234", registration="VH-ABC", simple_record=None)
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_type_check_mismatch_skips_record(self):
        rows = [_make_row(model="LEONARDO AW139")]
        simple = {"type_designator": "AW109", "manufacturer_model": "Leonardo AW109"}
        r = _make_redis_with_search(icao_hex="7C1234", registration="VH-ABC", simple_record=simple)
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_type_check_match_writes_record(self):
        rows = [_make_row(model="C172")]
        simple = {"type_designator": "C172", "manufacturer_model": "Cessna 172"}
        r = _make_redis_with_search(icao_hex="7C1234", registration="VH-ABC", simple_record=simple)
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_writes_to_registry_key(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="7C1234", registration="VH-ABC")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:7C1234"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="7C1234", registration="VH-ABC")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "au-casa"

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="7C1234", registration="VH-ABC")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:7C1234", REDIS_TTL)

    def test_empty_mark_skipped(self):
        rows = [_make_row(mark="")]
        r = _make_redis_with_search()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_empty_rows_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_registration_prefixed_with_vh(self):
        rows = [_make_row(mark="ABC")]
        r = _make_redis_with_search(icao_hex="7C1234", registration="VH-ABC")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["registration"] == "VH-ABC"


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
        with patch("au_casa_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("au_casa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 366, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("au_casa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 366, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "366"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("au_casa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/au-casa"
