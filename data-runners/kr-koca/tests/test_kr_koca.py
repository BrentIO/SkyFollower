"""Tests for the South Korea KOCA data runner."""

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
        "kr_koca_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["kr_koca_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

from shared.url_reachability import assert_url_reachable

_parse_date = _mod._parse_date
_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    reg="HL9301",
    owner="(주)더스카이",
    air_type="BELL206B",
    build_no="8550",
    build_date="78.07.19",
) -> dict:
    return {
        "REG_SNO": reg,
        "REG_CUSER": owner,
        "AIR_TYPE": air_type,
        "AIR_BUILD_NO": build_no,
        "AIR_BUILD_DATE": build_date,
        "AIR_LIMIT_MAN": "5",
        "AIR_FLY_WEIGHT": "1450kg",
    }


def _make_redis_with_search(icao_hex="710ABC", registration="HL9301"):
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
# Tests: _parse_date
# ---------------------------------------------------------------------------

class TestParseDate:
    def test_year_ge_50_maps_to_1900s(self):
        assert _parse_date("78.07.19") == "1978-07-19"

    def test_year_lt_50_maps_to_2000s(self):
        assert _parse_date("12.10.18") == "2012-10-18"

    def test_pivot_exactly_50(self):
        assert _parse_date("50.01.01") == "1950-01-01"

    def test_pivot_49(self):
        assert _parse_date("49.12.31") == "2049-12-31"

    def test_empty_returns_none(self):
        assert _parse_date("") is None

    def test_whitespace_returns_none(self):
        assert _parse_date("   ") is None

    def test_wrong_separator_returns_none(self):
        assert _parse_date("78/07/19") is None

    def test_wrong_part_count_returns_none(self):
        assert _parse_date("78.07") is None

    def test_invalid_date_returns_none(self):
        assert _parse_date("99.13.01") is None

    def test_non_numeric_returns_none(self):
        assert _parse_date("aa.bb.cc") is None

    def test_leading_trailing_whitespace_stripped(self):
        assert _parse_date("  78.07.19  ") == "1978-07-19"


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "710abc", "HL9301")
        assert record["icao_hex"] == "710abc"
        assert record["registration"] == "HL9301"
        assert record["source"] == "kr-koca"
        assert record["military"] is False
        assert record["aircraft"]["model"] == "BELL206B"
        assert record["aircraft"]["serial_number"] == "8550"
        assert record["aircraft"]["manufactured_date"] == "1978-07-19"
        assert record["registrant"]["names"] == ["(주)더스카이"]

    def test_empty_model_omits_field(self):
        row = _make_row(air_type="")
        record = _build_record(row, "710abc", "HL9301")
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omits_field(self):
        row = _make_row(build_no="")
        record = _build_record(row, "710abc", "HL9301")
        assert "serial_number" not in record.get("aircraft", {})

    def test_invalid_date_omits_field(self):
        row = _make_row(build_date="")
        record = _build_record(row, "710abc", "HL9301")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_empty_owner_omits_registrant(self):
        row = _make_row(owner="")
        record = _build_record(row, "710abc", "HL9301")
        assert "registrant" not in record

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(air_type="", build_no="", build_date="")
        record = _build_record(row, "710abc", "HL9301")
        assert "aircraft" not in record

    def test_unused_fields_not_stored(self):
        row = _make_row()
        record = _build_record(row, "710abc", "HL9301")
        assert "AIR_LIMIT_MAN" not in record
        assert "AIR_FLY_WEIGHT" not in record

    def test_korean_owner_stored_as_utf8(self):
        row = _make_row(owner="대한항공")
        record = _build_record(row, "710abc", "HL9301")
        assert record["registrant"]["names"] == ["대한항공"]


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("HL9301") == "HL9301"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("HL-9301")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="710ABC", registration="HL9301")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_record_not_written_when_not_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_empty_reg_sno_skipped(self):
        rows = [_make_row(reg="")]
        r = _make_redis_with_search()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0
        r.ft.return_value.search.assert_not_called()

    def test_writes_to_detail_key(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="710ABC", registration="HL9301")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:710ABC"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="710ABC", registration="HL9301")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "kr-koca"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="710ABC", registration="HL9301")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:710ABC", REDIS_TTL)

    def test_null_fields_omitted_from_written_record(self):
        rows = [_make_row(air_type="", build_no="")]
        r = _make_redis_with_search(icao_hex="710ABC", registration="HL9301")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][1] == "$"
        record = set_call[0][2]
        assert "model" not in record["aircraft"]
        assert "serial_number" not in record["aircraft"]


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
        with patch("kr_koca_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("kr_koca_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 907, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("kr_koca_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 907, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "907"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("kr_koca_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/kr-koca"


# ---------------------------------------------------------------------------
# Tests: network (real outbound HTTP call — see #405)
# ---------------------------------------------------------------------------

class TestNetwork:
    @pytest.mark.network
    def test_url_reachable(self):
        assert_url_reachable(_mod.REGISTER_URL, "kr-koca", headers={"User-Agent": _mod._BROWSER_UA})
