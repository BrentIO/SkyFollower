"""
Tests for the Mictronics data runner.

Covers:
- Parsing logic (using sample input data)
- Redis key construction
- MQTT completion logic (mocked)
"""

from __future__ import annotations

import io
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import zipfile
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Module import helper
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
_RUNNER_DIR = os.path.dirname(_HERE)          # data-runners/mictronics/
_REPO_ROOT = os.path.abspath(os.path.join(_RUNNER_DIR, "..", ".."))

# Ensure shared/ is importable
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    """Load data-runners/mictronics/main.py as a top-level module."""
    spec = importlib.util.spec_from_file_location(
        "mictronics_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["mictronics_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

# Convenience aliases
_decode_wtc = _mod._decode_wtc
_split_manufacturer_model = _mod._split_manufacturer_model
build_aircraft_record = _mod.build_aircraft_record
build_operator_record = _mod.build_operator_record
stage_data = _mod.stage_data
write_to_redis = _mod.write_to_redis
write_operators_to_redis = _mod.write_operators_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT


# ---------------------------------------------------------------------------
# Sample data fixtures
# ---------------------------------------------------------------------------

SAMPLE_AIRCRAFTS = {
    "A8AE7F": ["N659DL", "B763", "00"],   # not military, not interesting
    "AA0001": ["N12345", "C172", "00"],
    "AA0002": ["MILCRAFT", "F16", "10"],   # military, not interesting
    "AA0003": ["", "", "00"],
    "AA0004": ["N99999", "B763", "01"],   # not military, interesting
    "AA0005": ["N88888", "F16", "11"],    # military and interesting
}

SAMPLE_OPERATORS = {
    "DAL": ["Delta Air Lines", "United States", "DELTA"],
    "AAL": ["American Airlines", "United States", "AMERICAN"],
}

SAMPLE_TYPES = {
    "B763": ["Boeing 767-332ER", "L2J", "H"],
    "C172": ["Cessna 172 Skyhawk", "L1P", "L"],
    "F16": ["Lockheed Martin F-16", "L1J", "M"],
}


def _make_zip_files(aircrafts=None, operators=None, types=None) -> dict[str, bytes]:
    """Build the files dict that stage_data expects."""
    files: dict[str, bytes] = {}
    if aircrafts is not None:
        files["aircrafts.json"] = json.dumps(aircrafts).encode()
    if operators is not None:
        files["operators.json"] = json.dumps(operators).encode()
    if types is not None:
        files["types.json"] = json.dumps(types).encode()
    return files


# ---------------------------------------------------------------------------
# Tests: _decode_wtc
# ---------------------------------------------------------------------------

class TestDecodeWtc:
    def test_heavy(self):
        assert _decode_wtc("H") == "Heavy"

    def test_light(self):
        assert _decode_wtc("L") == "Light"

    def test_medium(self):
        assert _decode_wtc("M") == "Medium"

    def test_super(self):
        assert _decode_wtc("J") == "Super"

    def test_medium_light(self):
        assert _decode_wtc("M/L") == "Medium/Light"

    def test_dash_is_unknown_none(self):
        assert _decode_wtc("-") == "Unknown/None"

    def test_empty_returns_none(self):
        assert _decode_wtc("") is None

    def test_unknown_code_returns_unknown(self):
        assert _decode_wtc("X") == "Unknown"


# ---------------------------------------------------------------------------
# Tests: _split_manufacturer_model
# ---------------------------------------------------------------------------

class TestSplitManufacturerModel:
    def test_normal_split(self):
        mfr, mdl = _split_manufacturer_model("Boeing 767-332ER")
        assert mfr == "Boeing"
        assert mdl == "767-332ER"

    def test_single_word(self):
        mfr, mdl = _split_manufacturer_model("Airbus")
        assert mfr == "Airbus"
        assert mdl is None

    def test_empty_string(self):
        mfr, mdl = _split_manufacturer_model("")
        assert mfr is None
        assert mdl is None

    def test_multi_word_model(self):
        mfr, mdl = _split_manufacturer_model("Cessna 172 Skyhawk")
        assert mfr == "Cessna"
        assert mdl == "172 Skyhawk"


# ---------------------------------------------------------------------------
# Tests: stage_data (parsing)
# ---------------------------------------------------------------------------

class TestStageData:
    def test_aircraft_count(self):
        files = _make_zip_files(
            aircrafts=SAMPLE_AIRCRAFTS,
            operators=SAMPLE_OPERATORS,
            types=SAMPLE_TYPES,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(files, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM aircraft")
            assert cur.fetchone()[0] == len(SAMPLE_AIRCRAFTS)
            conn.close()

    def test_aircraft_fields(self):
        files = _make_zip_files(
            aircrafts=SAMPLE_AIRCRAFTS,
            operators=SAMPLE_OPERATORS,
            types=SAMPLE_TYPES,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(files, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute(
                "SELECT icao_hex, registration, type_designator, military "
                "FROM aircraft WHERE icao_hex = 'A8AE7F'"
            )
            row = cur.fetchone()
            assert row[0] == "A8AE7F"
            assert row[1] == "N659DL"
            assert row[2] == "B763"
            assert row[3] == 0
            conn.close()

    def test_military_flag(self):
        files = _make_zip_files(
            aircrafts=SAMPLE_AIRCRAFTS,
            operators=SAMPLE_OPERATORS,
            types=SAMPLE_TYPES,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(files, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT military FROM aircraft WHERE icao_hex = 'AA0002'")
            assert bool(cur.fetchone()[0]) is True
            conn.close()

    def test_not_military_flag(self):
        files = _make_zip_files(
            aircrafts=SAMPLE_AIRCRAFTS,
            operators=SAMPLE_OPERATORS,
            types=SAMPLE_TYPES,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(files, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT military FROM aircraft WHERE icao_hex = 'A8AE7F'")
            assert bool(cur.fetchone()[0]) is False
            conn.close()

    def test_interesting_flag(self):
        files = _make_zip_files(
            aircrafts=SAMPLE_AIRCRAFTS,
            operators=SAMPLE_OPERATORS,
            types=SAMPLE_TYPES,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(files, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT interesting FROM aircraft WHERE icao_hex = 'AA0004'")
            assert bool(cur.fetchone()[0]) is True
            conn.close()

    def test_military_and_interesting_flags(self):
        files = _make_zip_files(
            aircrafts=SAMPLE_AIRCRAFTS,
            operators=SAMPLE_OPERATORS,
            types=SAMPLE_TYPES,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(files, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT military, interesting FROM aircraft WHERE icao_hex = 'AA0005'")
            row = cur.fetchone()
            assert bool(row[0]) is True
            assert bool(row[1]) is True
            conn.close()

    def test_empty_type_becomes_null(self):
        aircrafts = {"AAAAAA": ["N00001", "", [0, 0]]}
        files = _make_zip_files(aircrafts=aircrafts, operators={}, types={})
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(files, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT type_designator FROM aircraft WHERE icao_hex = 'AAAAAA'")
            assert cur.fetchone()[0] is None
            conn.close()

    def test_operators_staged(self):
        files = _make_zip_files(
            aircrafts=SAMPLE_AIRCRAFTS,
            operators=SAMPLE_OPERATORS,
            types=SAMPLE_TYPES,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(files, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM operators")
            assert cur.fetchone()[0] == len(SAMPLE_OPERATORS)
            conn.close()

    def test_types_staged_with_wtc(self):
        files = _make_zip_files(
            aircrafts=SAMPLE_AIRCRAFTS,
            operators=SAMPLE_OPERATORS,
            types=SAMPLE_TYPES,
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(files, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute(
                "SELECT manufacturer_model, wake_turbulence_category "
                "FROM types WHERE type_designator = 'B763'"
            )
            row = cur.fetchone()
            assert row[0] == "Boeing 767-332ER"
            assert row[1] == "Heavy"
            conn.close()

    def test_icao_hex_uppercased(self):
        aircrafts = {"a8ae7f": ["N659DL", "B763", [0, 0]]}
        files = _make_zip_files(aircrafts=aircrafts, operators={}, types={})
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(files, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT icao_hex FROM aircraft")
            row = cur.fetchone()
            assert row[0] == "A8AE7F"
            conn.close()


# ---------------------------------------------------------------------------
# Tests: build_aircraft_record
# ---------------------------------------------------------------------------

class TestBuildAircraftRecord:
    def _row(self, icao_hex, registration, type_designator, military, interesting=False, manufacturer_model=None):
        return {
            "icao_hex": icao_hex,
            "registration": registration,
            "type_designator": type_designator,
            "military": military,
            "interesting": interesting,
            "manufacturer_model": manufacturer_model,
            "wake_turbulence_category": None,
        }

    def test_full_record_shape(self):
        row = self._row("A8AE7F", "N659DL", "B763", False, manufacturer_model="Boeing 767-332ER")
        record = build_aircraft_record(row, row)

        assert record["icao_hex"] == "A8AE7F"
        assert record["registration"] == "N659DL"
        assert record["military"] is False
        assert "source" not in record
        ac = record["aircraft"]
        assert ac["type_designator"] == "B763"
        assert ac["manufacturer"] == "Boeing"
        assert ac["manufacturer_model"] == "Boeing 767-332ER"

    def test_no_source_field(self):
        row = self._row("A8AE7F", "N659DL", "B763", False)
        record = build_aircraft_record(row, None)
        assert "source" not in record

    def test_no_unowned_fields(self):
        """Non-data-dictionary fields must be absent from the record."""
        row = self._row("A8AE7F", "N659DL", "B763", False, manufacturer_model="Boeing 767-332ER")
        record = build_aircraft_record(row, row)
        for field in ("interesting", "is_private_operator", "operator", "airline_code", "serial_number", "year_built"):
            assert field not in record, f"Unexpected field: {field!r}"

    def test_military_true(self):
        row = self._row("AA0002", "MILCRAFT", "F16", True)
        record = build_aircraft_record(row, None)
        assert record["military"] is True

    def test_military_false(self):
        row = self._row("A8AE7F", "N659DL", "B763", False)
        record = build_aircraft_record(row, None)
        assert record["military"] is False

    def test_no_types_row_type_designator_in_aircraft(self):
        """When types_row is None, aircraft still contains type_designator if set."""
        row = self._row("A8AE7F", "N659DL", "B763", 0)
        record = build_aircraft_record(row, None)
        assert record["aircraft"]["type_designator"] == "B763"
        assert "manufacturer" not in record["aircraft"]
        assert "manufacturer_model" not in record["aircraft"]

    def test_no_types_row_no_type_designator_omits_aircraft(self):
        """When both types_row is None and type_designator is None, aircraft key is omitted."""
        row = self._row("AA0003", "", None, 0)
        record = build_aircraft_record(row, None)
        assert "aircraft" not in record

    def test_empty_registration_becomes_none(self):
        row = self._row("AA0003", "", None, 0)
        record = build_aircraft_record(row, None)
        assert record["registration"] is None


# ---------------------------------------------------------------------------
# Tests: build_operator_record
# ---------------------------------------------------------------------------

class TestBuildOperatorRecord:
    def _row(self, designator, name=None, country=None, callsign=None):
        return {
            "airline_designator": designator,
            "name": name,
            "country": country,
            "callsign": callsign,
        }

    def test_full_record(self):
        row = self._row("DAL", "Delta Air Lines", "United States", "DELTA")
        record = build_operator_record(row)
        assert record == {
            "airline_designator": "DAL",
            "name": "Delta Air Lines",
            "country": "United States",
            "callsign": "DELTA",
        }

    def test_null_fields_omitted(self):
        row = self._row("XYZ", name=None, country=None, callsign=None)
        record = build_operator_record(row)
        assert record == {"airline_designator": "XYZ"}
        assert "name" not in record
        assert "country" not in record
        assert "callsign" not in record

    def test_partial_fields(self):
        row = self._row("AAL", "American Airlines", country=None, callsign="AMERICAN")
        record = build_operator_record(row)
        assert record["name"] == "American Airlines"
        assert record["callsign"] == "AMERICAN"
        assert "country" not in record


# ---------------------------------------------------------------------------
# Tests: write_operators_to_redis
# ---------------------------------------------------------------------------

class TestWriteOperatorsToRedis:
    def _make_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(_mod._SCHEMA)
        conn.execute(
            "INSERT INTO operators (airline_designator, name, country, callsign) "
            "VALUES ('DAL', 'Delta Air Lines', 'United States', 'DELTA')"
        )
        conn.execute(
            "INSERT INTO operators (airline_designator, name, country, callsign) "
            "VALUES ('AAL', 'American Airlines', 'United States', 'AMERICAN')"
        )
        conn.commit()
        return conn

    def _mock_redis(self):
        r = MagicMock()
        pipe = MagicMock()
        pipe_json = MagicMock()
        r.pipeline.return_value = pipe
        pipe.json.return_value = pipe_json
        pipe.execute.return_value = []
        return r, pipe, pipe_json

    def test_count_matches_operators(self):
        conn = self._make_db()
        r, _, _ = self._mock_redis()
        assert write_operators_to_redis(conn, r, REDIS_TTL) == 2
        conn.close()

    def test_operator_key_written(self):
        conn = self._make_db()
        r, _, pipe_json = self._mock_redis()
        write_operators_to_redis(conn, r, REDIS_TTL)
        keys = [c.args[0] for c in pipe_json.set.call_args_list]
        assert "operator:DAL" in keys
        assert "operator:AAL" in keys
        conn.close()

    def test_json_set_uses_root_path(self):
        conn = self._make_db()
        r, _, pipe_json = self._mock_redis()
        write_operators_to_redis(conn, r, REDIS_TTL)
        for c in pipe_json.set.call_args_list:
            assert c.args[1] == "$"
        conn.close()

    def test_expire_called_with_correct_ttl(self):
        conn = self._make_db()
        r, pipe, _ = self._mock_redis()
        write_operators_to_redis(conn, r, REDIS_TTL)
        expire_ttls = [c.args[1] for c in pipe.expire.call_args_list]
        assert all(ttl == REDIS_TTL for ttl in expire_ttls)
        conn.close()

    def test_empty_designator_skipped(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(_mod._SCHEMA)
        conn.execute(
            "INSERT INTO operators (airline_designator, name, country, callsign) "
            "VALUES ('', 'No Designator', 'US', 'NONE')"
        )
        conn.commit()
        r, _, pipe_json = self._mock_redis()
        count = write_operators_to_redis(conn, r, REDIS_TTL)
        assert count == 0
        pipe_json.set.assert_not_called()
        conn.close()


# ---------------------------------------------------------------------------
# Tests: Redis key construction
# ---------------------------------------------------------------------------

class TestRedisKeys:
    def test_aircraft_simple_key_format(self):
        from shared.redis_keys import aircraft_simple_key
        assert aircraft_simple_key("a8ae7f") == "aircraft:simple:A8AE7F"

    def test_aircraft_simple_key_already_upper(self):
        from shared.redis_keys import aircraft_simple_key
        assert aircraft_simple_key("A8AE7F") == "aircraft:simple:A8AE7F"

    def test_aircraft_simple_search_index_name(self):
        from shared.redis_keys import AIRCRAFT_SIMPLE_SEARCH_INDEX
        assert AIRCRAFT_SIMPLE_SEARCH_INDEX == "idx:aircraft:simple"


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def _make_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(_mod._SCHEMA)
        conn.execute(
            "INSERT INTO aircraft (icao_hex, registration, type_designator, military, interesting) "
            "VALUES ('A8AE7F', 'N659DL', 'B763', 0, 0)"
        )
        conn.execute(
            "INSERT INTO aircraft (icao_hex, registration, type_designator, military, interesting) "
            "VALUES ('AA0001', 'N12345', 'C172', 0, 0)"
        )
        conn.execute(
            "INSERT INTO types (type_designator, manufacturer_model, wake_turbulence_category) "
            "VALUES ('B763', 'Boeing 767-332ER', 'Heavy')"
        )
        conn.commit()
        return conn

    def _mock_redis(self, existing: Optional[dict] = None):
        r = MagicMock()
        r_json = MagicMock()
        r.json.return_value = r_json
        r_json.mget.side_effect = lambda keys, path: [
            [existing] if existing and k == f"aircraft:simple:{existing['icao_hex']}" else None
            for k in keys
        ]

        pipe = MagicMock()
        pipe_json = MagicMock()
        r.pipeline.return_value = pipe
        pipe.json.return_value = pipe_json
        pipe.execute.return_value = []
        return r, pipe, pipe_json

    def test_count_matches_aircraft(self):
        conn = self._make_db()
        r, _, _ = self._mock_redis()
        assert write_to_redis(conn, r, REDIS_TTL) == 2
        conn.close()

    def test_icao_hex_json_set_written(self):
        conn = self._make_db()
        r, _, pipe_json = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        keys = [c.args[0] for c in pipe_json.set.call_args_list]
        assert "aircraft:simple:A8AE7F" in keys
        conn.close()

    def test_json_set_uses_root_path(self):
        conn = self._make_db()
        r, _, pipe_json = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        for c in pipe_json.set.call_args_list:
            assert c.args[1] == "$"
        conn.close()

    def test_expire_called_with_correct_ttl(self):
        conn = self._make_db()
        r, pipe, _ = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        expire_ttls = [c.args[1] for c in pipe.expire.call_args_list]
        assert all(ttl == REDIS_TTL for ttl in expire_ttls)
        conn.close()

    def test_no_registration_key_written(self):
        conn = self._make_db()
        r, pipe, pipe_json = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        all_keys = (
            [c.args[0] for c in pipe.set.call_args_list]
            + [c.args[0] for c in pipe_json.set.call_args_list]
        )
        assert not any(k.startswith("registration:") for k in all_keys)
        conn.close()

    def test_merges_with_existing_record(self):
        """Mictronics fields overwrite existing; unowned fields from other runners are preserved."""
        conn = self._make_db()
        existing = {
            "icao_hex": "A8AE7F",
            "military": True,
            "registrant": {"names": ["DELTA AIR LINES INC"]},
            "aircraft": {"type": "Airplane", "model": "737-800"},
        }
        r, _, pipe_json = self._mock_redis(existing=existing)
        write_to_redis(conn, r, REDIS_TTL)
        calls = {c.args[0]: c.args[2] for c in pipe_json.set.call_args_list}
        record = calls["aircraft:simple:A8AE7F"]
        # mictronics owns military — its value (False) overwrites the existing True
        assert record["military"] is False
        # registrant is not owned by mictronics — must be preserved
        assert record["registrant"] == {"names": ["DELTA AIR LINES INC"]}
        # aircraft sub-fields from existing (type, model) are preserved alongside mictronics fields
        assert record["aircraft"]["type"] == "Airplane"
        assert record["aircraft"]["model"] == "737-800"
        assert record["aircraft"]["type_designator"] == "B763"
        conn.close()


# ---------------------------------------------------------------------------
# Tests: MQTT completion stats (mocked)
# ---------------------------------------------------------------------------

class TestMqttCompletionStats:
    def _setup_mock_client(self):
        mock_client = MagicMock()

        def fake_connect(host, port, keepalive):
            # Trigger on_connect callback synchronously
            mock_client.on_connect(mock_client, None, None, 0, None)

        mock_client.connect.side_effect = fake_connect
        return mock_client

    def test_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("mictronics_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, 10, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("mictronics_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 99, 5, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "99"

    def test_publishes_operators_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("mictronics_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, 77, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/operators_imported"] == "77"

    def test_publishes_last_run_at(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("mictronics_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, 0, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/last_run_at" in topics

    def test_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("mictronics_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_no_mqtt_config_skips(self):
        cfg = {}
        mc = self._setup_mock_client()
        with patch("mictronics_main.mqtt.Client", return_value=mc):
            publish_completion_stats(cfg, 0, 0, "success")
        mc.connect.assert_not_called()

    def test_ha_autodiscovery_four_sensors(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("mictronics_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 100, 10, "success")
        ha_topics = [
            c.args[0] for c in mc.publish.call_args_list
            if c.args[0].startswith("homeassistant/")
        ]
        assert len(ha_topics) == 4
        assert "homeassistant/sensor/SkyFollower_runner_mictronics_records_imported/config" in ha_topics
        assert "homeassistant/sensor/SkyFollower_runner_mictronics_operators_imported/config" in ha_topics
        assert "homeassistant/sensor/SkyFollower_runner_mictronics_last_run_at/config" in ha_topics
        assert "homeassistant/sensor/SkyFollower_runner_mictronics_last_run_status/config" in ha_topics
