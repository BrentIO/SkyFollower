"""Tests for the Switzerland BAZL data runner."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
import requests

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
        "ch_bazl_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ch_bazl_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_decode_aircraft_type = _mod._decode_aircraft_type
_decode_engine_type = _mod._decode_engine_type
_parse_year = _mod._parse_year
_parse_seats = _mod._parse_seats
_parse_engine_model = _mod._parse_engine_model
_parse_registrant = _mod._parse_registrant
_build_record = _mod._build_record
_ensure_search_index = _mod._ensure_search_index
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    registration="HB-JNA",
    icao_hex="4B1916",
    status="Registered",
    aircraft_type="Aeroplane",
    manufacturer="THE BOEING COMPANY",
    model="777-3DEER",
    type_designator="B77W",
    year="2016",
    serial="44582",
    mopsc="320",
    minimum_crew="2",
    engine_category="Jet Engine",
    engine_manufacturer="GE AVIATION",
    engine="GE90-115BL",
    owner="Swiss International Air Lines Ltd., Obstgartenstrasse 25, 8302 Kloten, Switzerland",
    operator="Swiss International Air Lines Ltd., Obstgartenstrasse 25, 8302 Kloten, Switzerland",
) -> dict:
    return {
        " Registration": registration,
        " Aircraft Address HEX": icao_hex,
        " Status": status,
        " Aircraft Type": aircraft_type,
        " Manufacturer": manufacturer,
        " Aicraft Model": model,
        " ICAO Aircraft Type": type_designator,
        " Year of Manufacture": year,
        " Serial Number": serial,
        " MOPSC": mopsc,
        " Minimum Crew": minimum_crew,
        " Engine Category": engine_category,
        " Engine manufacturer": engine_manufacturer,
        " Engine": engine,
        " Main Owner": owner,
        " Main Operator": operator,
    }


def _make_redis():
    return MagicMock()


# ---------------------------------------------------------------------------
# Tests: _decode_aircraft_type
# ---------------------------------------------------------------------------

class TestDecodeAircraftType:
    def test_aeroplane(self):
        assert _decode_aircraft_type("Aeroplane") == "Airplane"

    def test_homebuilt_airplane(self):
        assert _decode_aircraft_type("Homebuilt Airplane") == "Airplane"

    def test_helicopter(self):
        assert _decode_aircraft_type("Helicopter") == "Helicopter"

    def test_homebuilt_helicopter(self):
        assert _decode_aircraft_type("Homebuilt Helicopter") == "Helicopter"

    def test_glider(self):
        assert _decode_aircraft_type("Glider") == "Glider"

    def test_powered_glider(self):
        assert _decode_aircraft_type("Powered Glider") == "Powered Glider"

    def test_homebuild_glider_typo(self):
        assert _decode_aircraft_type("Homebuild Glider") == "Glider"

    def test_balloon_hot_air(self):
        assert _decode_aircraft_type("Balloon (Hot-air)") == "Balloon"

    def test_balloon_gas(self):
        assert _decode_aircraft_type("Balloon (Gas)") == "Balloon"

    def test_airship(self):
        assert _decode_aircraft_type("Airship (Hot-air)") == "Airship"

    def test_ultralight_gyrocopter(self):
        assert _decode_aircraft_type("Ultralight Gyrocopter") == "Gyroplane"

    def test_homebuilt_gyrocopter(self):
        assert _decode_aircraft_type("Homebuilt Gyrocopter") == "Gyroplane"

    def test_ultralight_3axis(self):
        assert _decode_aircraft_type("Ultralight (3-axis control)") == "Microlight"

    def test_ecolight(self):
        assert _decode_aircraft_type("Ecolight") == "Microlight"

    def test_trike(self):
        assert _decode_aircraft_type("Trike") == "Weight-Shift-Control"

    def test_empty_returns_none(self):
        assert _decode_aircraft_type("") is None

    def test_unknown_passes_through(self):
        assert _decode_aircraft_type("Novel Aircraft Type") == "Novel Aircraft Type"


# ---------------------------------------------------------------------------
# Tests: _decode_engine_type
# ---------------------------------------------------------------------------

class TestDecodeEngineType:
    def test_piston(self):
        assert _decode_engine_type("Piston Engine") == "Piston"

    def test_turboshaft(self):
        assert _decode_engine_type("Turboshaft Engine") == "Turbo-shaft"

    def test_turboprop(self):
        assert _decode_engine_type("Turboprop Engine") == "Turbo-prop"

    def test_jet(self):
        assert _decode_engine_type("Jet Engine") == "Turbo-jet"

    def test_electric(self):
        assert _decode_engine_type("Electrical Engine") == "Electric"

    def test_hydrogen_electric(self):
        assert _decode_engine_type("Hydrogen-Electric") == "Hydrogen-Electric"

    def test_multi_engine_takes_first(self):
        assert _decode_engine_type("Jet Engine, Jet Engine") == "Turbo-jet"

    def test_multi_engine_piston_takes_first(self):
        assert _decode_engine_type("Piston Engine, Piston Engine") == "Piston"

    def test_empty_returns_none(self):
        assert _decode_engine_type("") is None

    def test_unknown_passes_through(self):
        assert _decode_engine_type("Warp Drive") == "Warp Drive"


# ---------------------------------------------------------------------------
# Tests: _parse_year
# ---------------------------------------------------------------------------

class TestParseYear:
    def test_valid_year(self):
        assert _parse_year("2016") == "2016-01-01"

    def test_empty_returns_none(self):
        assert _parse_year("") is None

    def test_non_numeric_returns_none(self):
        assert _parse_year("N/A") is None

    def test_partial_year_returns_none(self):
        assert _parse_year("201") is None


# ---------------------------------------------------------------------------
# Tests: _parse_seats
# ---------------------------------------------------------------------------

class TestParseSeats:
    def test_valid_seats(self):
        assert _parse_seats("320") == 320

    def test_zero(self):
        assert _parse_seats("0") == 0

    def test_empty_returns_none(self):
        assert _parse_seats("") is None

    def test_non_numeric_returns_none(self):
        assert _parse_seats("N/A") is None


# ---------------------------------------------------------------------------
# Tests: _parse_engine_model
# ---------------------------------------------------------------------------

class TestParseEngineModel:
    def test_single_engine(self):
        assert _parse_engine_model("GE90-115BL") == "GE90-115BL"

    def test_multi_engine_takes_first(self):
        assert _parse_engine_model("CFM56-5B4/3, CFM56-5B4/P") == "CFM56-5B4/3"

    def test_empty_returns_none(self):
        assert _parse_engine_model("") is None

    def test_whitespace_stripped(self):
        assert _parse_engine_model("  GE90-115BL  ") == "GE90-115BL"


# ---------------------------------------------------------------------------
# Tests: download_register URL logging
# ---------------------------------------------------------------------------

class TestDownloadRegisterLogging:
    def test_logs_api_url(self):
        mock_response = MagicMock()
        mock_response.content = (
            " Registration; Aircraft Address HEX; Status\r\n"
        ).encode("utf-16")
        mock_session = MagicMock()
        mock_session.post.return_value = mock_response

        with patch("ch_bazl_main.logger") as mock_logger:
            try:
                _mod.download_register(mock_session)
            except Exception:
                pass
            logged_messages = [str(c) for c in mock_logger.info.call_args_list]
            assert any(_mod.API_URL in msg for msg in logged_messages)


# ---------------------------------------------------------------------------
# Tests: _parse_registrant
# ---------------------------------------------------------------------------

class TestParseRegistrant:
    def test_company_full_address(self):
        raw = "Swiss International Air Lines Ltd., Obstgartenstrasse 25, 8302 Kloten, Switzerland"
        result = _parse_registrant(raw)
        assert result["names"] == ["Swiss International Air Lines Ltd."]
        assert result["street"] == ["Obstgartenstrasse 25"]
        assert result["postal_code"] == "8302"
        assert result["city"] == "Kloten"
        assert result["country"] == "CH"

    def test_individual_with_canton(self):
        raw = "Wullschleger, Marcel, SO, Zelglistrasse 5, 4600 Olten, Switzerland"
        result = _parse_registrant(raw)
        assert result["names"] == ["Wullschleger, Marcel"]
        assert result["street"] == ["Zelglistrasse 5"]
        assert result["postal_code"] == "4600"
        assert result["city"] == "Olten"
        assert result["country"] == "CH"

    def test_organization_short_address(self):
        raw = "Alpine Segelflugschule Schänis AG, Flugplatz, 8718 Schänis, Switzerland"
        result = _parse_registrant(raw)
        assert result["names"] == ["Alpine Segelflugschule Schänis AG"]
        assert result["street"] == ["Flugplatz"]
        assert result["postal_code"] == "8718"
        assert result["city"] == "Schänis"
        assert result["country"] == "CH"

    def test_empty_returns_none(self):
        assert _parse_registrant("") is None

    def test_na_returns_none(self):
        assert _parse_registrant("N/A") is None

    def test_name_only_no_street(self):
        raw = "Edelweiss Air AG, 8058 Zürich-Flughafen, Switzerland"
        result = _parse_registrant(raw)
        assert result["names"] == ["Edelweiss Air AG"]
        assert "street" not in result
        assert result["postal_code"] == "8058"
        assert result["city"] == "Zürich-Flughafen"

    def test_country_always_ch(self):
        raw = "Swiss International Air Lines Ltd., Obstgartenstrasse 25, 8302 Kloten, Switzerland"
        assert _parse_registrant(raw)["country"] == "CH"


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_icao_hex_uppercased(self):
        record = _build_record(_make_row(icao_hex="4b1916"))
        assert record["icao_hex"] == "4B1916"

    def test_registration(self):
        record = _build_record(_make_row())
        assert record["registration"] == "HB-JNA"

    def test_aircraft_type(self):
        record = _build_record(_make_row(aircraft_type="Aeroplane"))
        assert record["aircraft"]["type"] == "Airplane"

    def test_manufacturer(self):
        record = _build_record(_make_row())
        assert record["aircraft"]["manufacturer"] == "THE BOEING COMPANY"

    def test_model(self):
        record = _build_record(_make_row())
        assert record["aircraft"]["model"] == "777-3DEER"

    def test_type_designator(self):
        record = _build_record(_make_row())
        assert record["aircraft"]["type_designator"] == "B77W"

    def test_manufactured_date(self):
        record = _build_record(_make_row(year="2016"))
        assert record["aircraft"]["manufactured_date"] == "2016-01-01"

    def test_serial_number(self):
        record = _build_record(_make_row())
        assert record["aircraft"]["serial_number"] == "44582"

    def test_seats_mopsc_plus_crew(self):
        record = _build_record(_make_row(mopsc="3", minimum_crew="1"))
        assert record["aircraft"]["seats"] == 4

    def test_seats_mopsc_only_when_no_crew(self):
        record = _build_record(_make_row(mopsc="320", minimum_crew=""))
        assert record["aircraft"]["seats"] == 320

    def test_seats_none_when_both_empty(self):
        record = _build_record(_make_row(mopsc="", minimum_crew=""))
        assert "seats" not in record.get("aircraft", {})

    def test_engine_type(self):
        record = _build_record(_make_row(engine_category="Jet Engine"))
        assert record["aircraft"]["powerplant"]["type"] == "Turbo-jet"

    def test_engine_manufacturer(self):
        record = _build_record(_make_row(engine_manufacturer="AVCO LYCOMING"))
        assert record["aircraft"]["powerplant"]["manufacturer"] == "AVCO LYCOMING"

    def test_engine_manufacturer_absent_when_empty(self):
        record = _build_record(_make_row(engine_manufacturer=""))
        assert "manufacturer" not in record.get("aircraft", {}).get("powerplant", {})

    def test_engine_model(self):
        record = _build_record(_make_row(engine="GE90-115BL"))
        assert record["aircraft"]["powerplant"]["model"] == "GE90-115BL"

    def test_registrant_name(self):
        record = _build_record(_make_row())
        assert record["registrant"]["names"] == ["Swiss International Air Lines Ltd."]


    def test_short_hex_returns_none(self):
        assert _build_record(_make_row(icao_hex="4B19")) is None

    def test_missing_registration_returns_none(self):
        assert _build_record(_make_row(registration="")) is None

    def test_no_engine_omits_powerplant(self):
        record = _build_record(_make_row(engine_category="", engine_manufacturer="", engine=""))
        assert "powerplant" not in record.get("aircraft", {})

    def test_glider_no_engine_omits_powerplant(self):
        record = _build_record(_make_row(aircraft_type="Glider", engine_category="", engine_manufacturer="", engine=""))
        assert "powerplant" not in record.get("aircraft", {})


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_writes_registered_record(self):
        r = _make_redis()
        count = write_to_redis([_make_row()], r, REDIS_TTL)
        assert count == 1

    def test_skips_non_registered(self):
        r = _make_redis()
        count = write_to_redis([_make_row(status="De-registered")], r, REDIS_TTL)
        assert count == 0
        r.json.return_value.set.assert_not_called()

    def test_registration_in_progress_skipped(self):
        r = _make_redis()
        count = write_to_redis([_make_row(status="Registration in Progress")], r, REDIS_TTL)
        assert count == 0

    def test_writes_to_correct_key(self):
        r = _make_redis()
        write_to_redis([_make_row(icao_hex="4B1916")], r, REDIS_TTL)
        key = r.json.return_value.set.call_args.args[0]
        assert key == "aircraft:registry:4B1916"

    def test_uses_root_json_path(self):
        r = _make_redis()
        write_to_redis([_make_row()], r, REDIS_TTL)
        assert r.json.return_value.set.call_args.args[1] == "$"

    def test_sets_ttl(self):
        r = _make_redis()
        write_to_redis([_make_row()], r, REDIS_TTL)
        r.expire.assert_called_once_with("aircraft:registry:4B1916", REDIS_TTL)

    def test_source_field_set(self):
        r = _make_redis()
        write_to_redis([_make_row()], r, REDIS_TTL)
        written = r.json.return_value.set.call_args.args[2]
        assert written["source"] == "ch-bazl"

    def test_invalid_row_counts_as_error(self):
        r = _make_redis()
        count = write_to_redis([_make_row(icao_hex="4B19")], r, REDIS_TTL)
        assert count == 0
        r.json.return_value.set.assert_not_called()

    def test_multiple_rows(self):
        r = _make_redis()
        rows = [
            _make_row(registration="HB-JNA", icao_hex="4B1916"),
            _make_row(registration="HB-JMI", icao_hex="4B1904"),
        ]
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 2

    def test_hex_uppercased_in_key(self):
        r = _make_redis()
        write_to_redis([_make_row(icao_hex="4b1916")], r, REDIS_TTL)
        key = r.json.return_value.set.call_args.args[0]
        assert key == "aircraft:registry:4B1916"


# ---------------------------------------------------------------------------
# Tests: _ensure_search_index
# ---------------------------------------------------------------------------

class TestEnsureSearchIndex:
    def test_skips_create_when_index_exists(self):
        r = MagicMock()
        r.ft.return_value.info.return_value = {"index_name": "idx:aircraft:registry"}
        _ensure_search_index(r)
        r.ft.return_value.create_index.assert_not_called()

    def test_creates_index_when_missing(self):
        r = MagicMock()
        r.ft.return_value.info.side_effect = Exception("Unknown index name")
        _ensure_search_index(r)
        r.ft.return_value.create_index.assert_called_once()

    def test_create_index_uses_correct_prefix(self):
        r = MagicMock()
        r.ft.return_value.info.side_effect = Exception("Unknown index name")
        _ensure_search_index(r)
        call_kwargs = r.ft.return_value.create_index.call_args.kwargs
        definition = call_kwargs["definition"]
        assert "aircraft:registry:" in definition.args


# ---------------------------------------------------------------------------
# Tests: MQTT completion stats
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
        with patch("ch_bazl_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ch_bazl_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 99, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "99"

    def test_publishes_last_run_at(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ch_bazl_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/last_run_at" in topics

    def test_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ch_bazl_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_no_mqtt_config_skips(self):
        cfg = {}
        publish_completion_stats(cfg, 0, "success")

    def test_ha_autodiscovery_three_sensors(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ch_bazl_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "success")
        discovery_topics = [
            c.args[0] for c in mc.publish.call_args_list
            if c.args[0].startswith("homeassistant/")
        ]
        assert len(discovery_topics) == 3
