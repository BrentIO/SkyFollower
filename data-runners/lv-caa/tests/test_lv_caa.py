"""Tests for the Latvia CAA data runner."""

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
        "lv_caa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["lv_caa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
download_and_parse = _mod.download_and_parse
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
_API_URL = _mod._API_URL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_api_response(records: list[dict], success: bool = True) -> MagicMock:
    resp = MagicMock()
    resp.ok = True
    resp.json.return_value = {
        "success": success,
        "result": {"records": records},
    }
    return resp


def _make_api_record(
    registration="YL-AAA",
    model="80TA",
    serial="0231291",
    year=1991,
) -> dict:
    return {
        "Registration_Mark": registration,
        "Model": model,
        "Serial_No": serial,
        "Construction_Year": year,
        "Registered_on": "1995-07-31T00:00:00",
        "Aircraft_Model_Category": "Balloon (hot-air)",
    }


def _make_row(
    registration="YL-AAA",
    model="80TA",
    serial="0231291",
    year_built=1991,
    category="Balloon (hot-air)",
) -> dict:
    return {
        "registration": registration,
        "model": model,
        "serial": serial,
        "year_built": year_built,
        "category": category,
    }


def _make_redis_with_search(icao_hex="502100", registration="YL-AAA"):
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
# Tests: download_and_parse
# ---------------------------------------------------------------------------

class TestDownloadAndParse:
    def test_returns_yl_records(self):
        session = MagicMock()
        session.get.return_value = _make_api_response([_make_api_record()])
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "YL-AAA"

    def test_filters_non_yl_registrations(self):
        session = MagicMock()
        session.get.return_value = _make_api_response([
            _make_api_record(registration="YL-AAA"),
            _make_api_record(registration="N12345"),
        ])
        records = download_and_parse(session)
        assert len(records) == 1

    def test_raises_on_http_error(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 500
        session.get.return_value = resp
        with pytest.raises(RuntimeError, match="HTTP 500"):
            download_and_parse(session)

    def test_raises_on_api_success_false(self):
        session = MagicMock()
        session.get.return_value = _make_api_response([], success=False)
        with pytest.raises(RuntimeError, match="success=false"):
            download_and_parse(session)

    def test_logs_api_url(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 503
        session.get.return_value = resp
        import logging
        with patch.object(logging.getLogger("lv-caa"), "info") as mock_log:
            with pytest.raises(RuntimeError):
                download_and_parse(session)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert _API_URL in logged

    def test_maps_all_fields(self):
        session = MagicMock()
        session.get.return_value = _make_api_response([_make_api_record()])
        records = download_and_parse(session)
        r = records[0]
        assert r["registration"] == "YL-AAA"
        assert r["model"] == "80TA"
        assert r["serial"] == "0231291"
        assert r["year_built"] == 1991
        assert r["category"] == "Balloon (hot-air)"

    def test_registered_on_not_stored(self):
        session = MagicMock()
        session.get.return_value = _make_api_response([_make_api_record()])
        records = download_and_parse(session)
        assert "Registered_on" not in records[0]
        assert "registered_on" not in records[0]

    def test_null_fields_handled(self):
        session = MagicMock()
        rec = _make_api_record()
        rec["Model"] = None
        rec["Serial_No"] = None
        rec["Construction_Year"] = None
        session.get.return_value = _make_api_response([rec])
        records = download_and_parse(session)
        assert records[0]["model"] == ""
        assert records[0]["serial"] == ""
        assert records[0]["year_built"] is None

    def test_uses_api_url(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 404
        session.get.return_value = resp
        with pytest.raises(RuntimeError):
            download_and_parse(session)
        session.get.assert_called_once_with(_API_URL, timeout=60)


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "502100", "YL-AAA")
        assert record["icao_hex"] == "502100"
        assert record["registration"] == "YL-AAA"
        assert record["source"] == "lv-caa"
        assert record["military"] is False
        assert record["aircraft"]["model"] == "80TA"
        assert record["aircraft"]["type"] == "Balloon (hot-air)"
        assert record["aircraft"]["serial_number"] == "0231291"
        assert record["aircraft"]["manufactured_date"] == "1991-01-01"

    def test_category_stored_as_type(self):
        row = _make_row(category="Aeroplane")
        record = _build_record(row, "502100", "YL-AAA")
        assert record["aircraft"]["type"] == "Aeroplane"

    def test_empty_category_omits_type(self):
        row = _make_row(category="")
        record = _build_record(row, "502100", "YL-AAA")
        assert "type" not in record.get("aircraft", {})

    def test_no_registrant_key(self):
        row = _make_row()
        record = _build_record(row, "502100", "YL-AAA")
        assert "registrant" not in record

    def test_integer_year_stored_as_date(self):
        row = _make_row(year_built=2005)
        record = _build_record(row, "502100", "YL-AAA")
        assert record["aircraft"]["manufactured_date"] == "2005-01-01"

    def test_string_year_stored_as_date(self):
        row = _make_row(year_built="2005")
        record = _build_record(row, "502100", "YL-AAA")
        assert record["aircraft"]["manufactured_date"] == "2005-01-01"

    def test_none_year_omitted(self):
        row = _make_row(year_built=None)
        record = _build_record(row, "502100", "YL-AAA")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_invalid_year_omitted(self):
        row = _make_row(year_built="unknown")
        record = _build_record(row, "502100", "YL-AAA")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_out_of_range_year_omitted(self):
        row = _make_row(year_built=1800)
        record = _build_record(row, "502100", "YL-AAA")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_empty_model_omitted(self):
        row = _make_row(model="")
        record = _build_record(row, "502100", "YL-AAA")
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial="")
        record = _build_record(row, "502100", "YL-AAA")
        assert "serial_number" not in record.get("aircraft", {})

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(model="", serial="", year_built=None, category="")
        record = _build_record(row, "502100", "YL-AAA")
        assert "aircraft" not in record

    def test_model_whitespace_collapsed(self):
        row = _make_row(model="Cessna  172  S")
        record = _build_record(row, "502100", "YL-AAA")
        assert record["aircraft"]["model"] == "Cessna 172 S"


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("YLAAA") == "YLAAA"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("YL-AAA")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="502100", registration="YL-AAA")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_record_not_written_when_not_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_empty_registration_skipped(self):
        rows = [_make_row(registration="")]
        r = _make_redis_with_search()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0
        r.ft.return_value.search.assert_not_called()

    def test_writes_to_detail_key(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="502100", registration="YL-AAA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:502100"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="502100", registration="YL-AAA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "lv-caa"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="502100", registration="YL-AAA")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:502100", REDIS_TTL)

    def test_null_fields_omitted_from_written_record(self):
        rows = [_make_row(model="", category="")]
        r = _make_redis_with_search(icao_hex="502100", registration="YL-AAA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][1] == "$"
        record = set_call[0][2]
        assert "model" not in record["aircraft"]
        assert "type" not in record["aircraft"]

    def test_multiple_records(self):
        rows = [_make_row(registration="YL-AAA"), _make_row(registration="YL-BBB")]
        r = MagicMock()
        doc_a = MagicMock()
        doc_a.id = "aircraft:mictronics:502100"
        doc_a.registration = "YL-AAA"
        doc_b = MagicMock()
        doc_b.id = "aircraft:mictronics:502101"
        doc_b.registration = "YL-BBB"
        results = MagicMock()
        results.docs = [doc_a, doc_b]
        r.ft.return_value.search.return_value = results
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
        with patch("lv_caa_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("lv_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 366, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("lv_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 366, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "366"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("lv_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/lv-caa"
