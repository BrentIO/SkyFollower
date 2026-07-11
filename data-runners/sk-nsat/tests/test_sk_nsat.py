"""Tests for the Slovakia NSAT data runner."""

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
        "sk_nsat_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["sk_nsat_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_normalize_registration = _mod._normalize_registration
_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
_COL_REGISTRATION = _mod._COL_REGISTRATION
_COL_TYPE = _mod._COL_TYPE
_COL_SERIAL = _mod._COL_SERIAL
_COL_OWNER = _mod._COL_OWNER
_COL_OPERATOR = _mod._COL_OPERATOR


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    reg="OM - 0101",
    type_="Let L-410UVP-E20",
    serial="912526",
    owner="Slovak Air Lines s.r.o.",
    operator="Slovak Air Lines s.r.o.",
    lien="",
) -> dict:
    return {
        _COL_REGISTRATION: reg,
        _COL_TYPE: type_,
        _COL_SERIAL: serial,
        _COL_OWNER: owner,
        _COL_OPERATOR: operator,
        "Záložné právo": lien,
    }


def _make_redis_with_search(icao_hex="710ABC", registration="OM-0101"):
    r = MagicMock()
    doc = MagicMock()
    doc.id = f"aircraft:mictronics:{icao_hex}"
    doc.registration = registration
    results = MagicMock()
    results.docs = [doc]
    r.ft.return_value.search.return_value = results
    return r


def _make_redis_no_match():
    r = MagicMock()
    results = MagicMock()
    results.docs = []
    r.ft.return_value.search.return_value = results
    return r


# ---------------------------------------------------------------------------
# Tests: _normalize_registration
# ---------------------------------------------------------------------------

class TestNormalizeRegistration:
    def test_spaced_format_normalized(self):
        assert _normalize_registration("OM - 0101") == "OM-0101"

    def test_already_normalized(self):
        assert _normalize_registration("OM-0101") == "OM-0101"

    def test_leading_trailing_whitespace_stripped(self):
        assert _normalize_registration("  OM - 0101  ") == "OM-0101"

    def test_internal_spaces_removed(self):
        assert _normalize_registration("OM -  0101") == "OM-0101"

    def test_empty_string(self):
        assert _normalize_registration("") == ""

    def test_no_hyphen(self):
        assert _normalize_registration("OM0101") == "OM0101"


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "710ABC", "OM-0101")
        assert record["icao_hex"] == "710ABC"
        assert record["registration"] == "OM-0101"
        assert record["source"] == "sk-nsat"
        assert record["military"] is False
        assert record["aircraft"]["model"] == "Let L-410UVP-E20"
        assert record["aircraft"]["serial_number"] == "912526"
        assert record["registrant"]["names"] == ["Slovak Air Lines s.r.o."]

    def test_model_newlines_collapsed(self):
        row = _make_row(type_="Let\nL-410UVP-E20")
        record = _build_record(row, "710ABC", "OM-0101")
        assert record["aircraft"]["model"] == "Let L-410UVP-E20"

    def test_serial_newlines_collapsed(self):
        row = _make_row(serial="9125\n26")
        record = _build_record(row, "710ABC", "OM-0101")
        assert record["aircraft"]["serial_number"] == "9125 26"

    def test_owner_newlines_collapsed(self):
        row = _make_row(owner="Slovak Air\nLines s.r.o.", operator="")
        record = _build_record(row, "710ABC", "OM-0101")
        assert record["registrant"]["names"] == ["Slovak Air Lines s.r.o."]

    def test_operator_newlines_collapsed(self):
        row = _make_row(owner="Owner Corp", operator="Operator\nLtd")
        record = _build_record(row, "710ABC", "OM-0101")
        assert record["registrant"]["names"] == ["Owner Corp", "Operator Ltd"]

    def test_owner_and_operator_deduplicated(self):
        row = _make_row(owner="Owner Corp", operator="Owner Corp")
        record = _build_record(row, "710ABC", "OM-0101")
        assert record["registrant"]["names"] == ["Owner Corp"]

    def test_different_operator_included(self):
        row = _make_row(owner="Owner Corp", operator="Operator Ltd")
        record = _build_record(row, "710ABC", "OM-0101")
        assert record["registrant"]["names"] == ["Owner Corp", "Operator Ltd"]

    def test_empty_owner_with_operator(self):
        row = _make_row(owner="", operator="Operator Ltd")
        record = _build_record(row, "710ABC", "OM-0101")
        assert record["registrant"]["names"] == ["Operator Ltd"]

    def test_empty_owner_and_operator_omits_registrant(self):
        row = _make_row(owner="", operator="")
        record = _build_record(row, "710ABC", "OM-0101")
        assert "registrant" not in record

    def test_empty_model_omits_field(self):
        row = _make_row(type_="")
        record = _build_record(row, "710ABC", "OM-0101")
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omits_field(self):
        row = _make_row(serial="")
        record = _build_record(row, "710ABC", "OM-0101")
        assert "serial_number" not in record.get("aircraft", {})

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(type_="", serial="")
        record = _build_record(row, "710ABC", "OM-0101")
        assert "aircraft" not in record

    def test_lien_not_stored(self):
        row = _make_row(lien="Yes")
        record = _build_record(row, "710ABC", "OM-0101")
        assert "Záložné právo" not in record
        assert "lien" not in record


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("OM0101") == "OM0101"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("OM-0101")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="710ABC", registration="OM-0101")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_record_not_written_when_not_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_registration_normalized_before_lookup(self):
        rows = [_make_row(reg="OM - 0101")]
        r = _make_redis_with_search(icao_hex="710ABC", registration="OM-0101")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_redis_registration_with_spaces_normalized(self):
        # If Mictronics stores "OM - 0101" (with spaces), we still match it.
        rows = [_make_row(reg="OM - 0101")]
        r = _make_redis_with_search(icao_hex="710ABC", registration="OM - 0101")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_empty_registration_skipped(self):
        rows = [_make_row(reg="")]
        r = _make_redis_with_search()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0
        r.ft.return_value.search.assert_not_called()

    def test_writes_to_detail_key(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="710ABC", registration="OM-0101")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:710ABC"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="710ABC", registration="OM-0101")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "sk-nsat"

    def test_null_fields_omitted_from_written_record(self):
        rows = [_make_row(type_="")]
        r = _make_redis_with_search(icao_hex="710ABC", registration="OM-0101")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        record = set_call[0][2]
        assert "model" not in record.get("aircraft", {})

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="710ABC", registration="OM-0101")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:710ABC", REDIS_TTL)


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
        with patch("sk_nsat_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("sk_nsat_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 818, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("sk_nsat_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 818, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "818"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("sk_nsat_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/sk-nsat"
