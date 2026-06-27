"""Tests for the Cayman Islands CAA data runner."""

from __future__ import annotations

import importlib.util
import io
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
        "ky_caa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ky_caa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_normalise_header = _mod._normalise_header
_normalise_cell = _mod._normalise_cell
_build_record = _mod._build_record
_deep_merge = _mod._deep_merge
_escape_tag = _mod._escape_tag
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
COL_REGISTRATION = _mod.COL_REGISTRATION
COL_OWNER = _mod.COL_OWNER
COL_SERIES_TYPE = _mod.COL_SERIES_TYPE
COL_SERIAL = _mod.COL_SERIAL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    registration="VP-CAD",
    owner="Castle Air Limited",
    series_type="Leonardo S.p.A. AW139",
    serial="31475",
) -> dict:
    return {
        COL_REGISTRATION: registration,
        COL_OWNER: owner,
        COL_SERIES_TYPE: series_type,
        COL_SERIAL: serial,
    }


def _make_redis(existing=None):
    r = MagicMock()
    mget_result = [[existing]] if existing is not None else [None]
    r.json.return_value.mget.return_value = mget_result
    return r


def _make_redis_with_search(icao_hex="C00001", registration="VP-CAD"):
    r = _make_redis()
    doc = MagicMock()
    doc.id = f"icao_hex:{icao_hex}"
    doc.registration = registration
    results = MagicMock()
    results.docs = [doc]
    r.ft.return_value.search.return_value = results
    return r


# ---------------------------------------------------------------------------
# Tests: _normalise_header
# ---------------------------------------------------------------------------

class TestNormaliseHeader:
    def test_plain_header_unchanged(self):
        assert _normalise_header("Series Type") == "Series Type"

    def test_newline_in_header_becomes_space(self):
        assert _normalise_header("Aircraft\nRegistration") == "Aircraft Registration"

    def test_serial_number_header(self):
        assert _normalise_header("Serial\nNumber") == "Serial Number"

    def test_date_registered_header(self):
        assert _normalise_header("Date\nRegistered") == "Date Registered"

    def test_strips_leading_trailing_whitespace(self):
        assert _normalise_header("  Series Type  ") == "Series Type"

    def test_multiple_newlines_collapsed(self):
        assert _normalise_header("Aircraft\n\nRegistration") == "Aircraft Registration"


# ---------------------------------------------------------------------------
# Tests: _normalise_cell
# ---------------------------------------------------------------------------

class TestNormaliseCell:
    def test_plain_value_unchanged(self):
        assert _normalise_cell("VP-CAD") == "VP-CAD"

    def test_newline_in_cell_becomes_space(self):
        assert _normalise_cell("Gulfstream Aerospace\nCorporation G-IV") == "Gulfstream Aerospace Corporation G-IV"

    def test_none_returns_empty_string(self):
        assert _normalise_cell(None) == ""

    def test_empty_string_returns_empty_string(self):
        assert _normalise_cell("") == ""

    def test_strips_whitespace(self):
        assert _normalise_cell("  VP-CAD  ") == "VP-CAD"

    def test_multiline_manufacturer_model(self):
        assert _normalise_cell("Airbus Helicopters\nDeutschland MBB-BK117 D-2") == "Airbus Helicopters Deutschland MBB-BK117 D-2"


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record("C00001", "VP-CAD", row)
        assert record["icao_hex"] == "C00001"
        assert record["registration"] == "VP-CAD"
        assert record["aircraft"]["model"] == "Leonardo S.p.A. AW139"
        assert record["aircraft"]["serial_number"] == "31475"
        assert record["registrant"]["names"] == ["Castle Air Limited"]

    def test_combined_manufacturer_model_stored_as_model(self):
        row = _make_row(series_type="Gulfstream Aerospace Corporation G-IV")
        record = _build_record("C00002", "VP-CAI", row)
        assert record["aircraft"]["model"] == "Gulfstream Aerospace Corporation G-IV"

    def test_empty_owner_omits_registrant(self):
        row = _make_row(owner="")
        record = _build_record("C00001", "VP-CAD", row)
        assert "registrant" not in record

    def test_empty_series_type_omits_model(self):
        row = _make_row(series_type="")
        record = _build_record("C00001", "VP-CAD", row)
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omits_serial_number(self):
        row = _make_row(serial="")
        record = _build_record("C00001", "VP-CAD", row)
        assert "serial_number" not in record.get("aircraft", {})

    def test_empty_series_and_serial_omits_aircraft_key(self):
        row = _make_row(series_type="", serial="")
        record = _build_record("C00001", "VP-CAD", row)
        assert "aircraft" not in record

    def test_registration_preserved(self):
        row = _make_row(registration="VP-CBB")
        record = _build_record("C00003", "VP-CBB", row)
        assert record["registration"] == "VP-CBB"

    def test_multiline_owner_normalised(self):
        row = _make_row(owner="Cayman Airways Express Limited")
        record = _build_record("C00004", "VP-CAW", row)
        assert record["registrant"]["names"] == ["Cayman Airways Express Limited"]


# ---------------------------------------------------------------------------
# Tests: _deep_merge
# ---------------------------------------------------------------------------

class TestDeepMerge:
    def test_flat_update_wins(self):
        result = _deep_merge({"a": 1, "b": 2}, {"b": 99})
        assert result == {"a": 1, "b": 99}

    def test_nested_merge(self):
        base = {"aircraft": {"type": "Airplane", "manufacturer": "Boeing"}}
        update = {"aircraft": {"model": "737"}}
        result = _deep_merge(base, update)
        assert result["aircraft"]["type"] == "Airplane"
        assert result["aircraft"]["manufacturer"] == "Boeing"
        assert result["aircraft"]["model"] == "737"

    def test_update_overwrites_nested_value(self):
        base = {"aircraft": {"model": "Old Model"}}
        update = {"aircraft": {"model": "New Model"}}
        result = _deep_merge(base, update)
        assert result["aircraft"]["model"] == "New Model"

    def test_new_key_added(self):
        result = _deep_merge({"a": 1}, {"b": 2})
        assert result == {"a": 1, "b": 2}

    def test_base_unchanged(self):
        base = {"a": {"x": 1}}
        _deep_merge(base, {"a": {"y": 2}})
        assert "y" not in base["a"]


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("VPCAD") == "VPCAD"

    def test_hyphen_escaped(self):
        result = _escape_tag("VP-CAD")
        assert "\\-" in result

    def test_dot_escaped(self):
        result = _escape_tag("VP.CAD")
        assert "\\." in result


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_active_record_written(self):
        row = _make_row()
        r = _make_redis_with_search(icao_hex="C00001", registration="VP-CAD")
        count = write_to_redis([row], r, REDIS_TTL)
        assert count == 1
        r.json.return_value.mget.assert_called_once()

    def test_no_redis_match_not_written(self):
        row = _make_row()
        r = _make_redis()
        results = MagicMock()
        results.docs = []
        r.ft.return_value.search.return_value = results
        count = write_to_redis([row], r, REDIS_TTL)
        assert count == 0
        r.json.return_value.mget.assert_not_called()

    def test_merges_with_existing_record(self):
        row = _make_row()
        existing = {"icao_hex": "C00001", "registration": "VP-CAD", "military": False}
        r = _make_redis_with_search(icao_hex="C00001", registration="VP-CAD")
        r.json.return_value.mget.return_value = [[existing]]

        captured = []

        def _capture_set(key, path, value):
            captured.append(value)
            return MagicMock()

        r.pipeline.return_value.__enter__ = MagicMock(return_value=r.pipeline.return_value)
        r.pipeline.return_value.__exit__ = MagicMock(return_value=False)
        r.pipeline.return_value.json.return_value.set.side_effect = _capture_set

        write_to_redis([row], r, REDIS_TTL)
        assert len(captured) == 1
        merged = captured[0]
        assert merged["military"] is False
        assert merged["aircraft"]["model"] == "Leonardo S.p.A. AW139"

    def test_empty_registration_skipped(self):
        row = _make_row(registration="")
        r = _make_redis_with_search()
        count = write_to_redis([row], r, REDIS_TTL)
        assert count == 0

    def test_empty_list_returns_zero(self):
        r = _make_redis()
        results = MagicMock()
        results.docs = []
        r.ft.return_value.search.return_value = results
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_multiple_records_written(self):
        rows = [
            _make_row(registration="VP-CAD"),
            _make_row(registration="VP-CAF", series_type="Bombardier Inc. BD-700-1A10", serial="9686"),
        ]
        r = _make_redis()

        call_count = [0]

        def _search(query):
            call_count[0] += 1
            results = MagicMock()
            docs = []
            for reg in ["VP-CAD", "VP-CAF"]:
                doc = MagicMock()
                doc.id = f"icao_hex:C0000{call_count[0]}"
                doc.registration = reg
                docs.append(doc)
            results.docs = docs
            return results

        r.ft.return_value.search.side_effect = _search
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 2


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
        with patch("ky_caa_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ky_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ky_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 77, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "77"

    def test_mqtt_publishes_last_run_at(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ky_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/last_run_at" in topics

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ky_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/ky-caa"
