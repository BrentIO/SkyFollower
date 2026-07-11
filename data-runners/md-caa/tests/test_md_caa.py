"""Tests for the Moldova CAA data runner."""

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
        "md_caa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["md_caa_main"] = mod
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
_PDF_URL = _mod._PDF_URL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    registration="ER-AXA",
    model="Boeing 737-800",
    serial="35014",
) -> dict:
    return {
        "registration": registration,
        "model": model,
        "serial": serial,
    }


def _make_pdf_page(rows: list[list[str]]):
    page = MagicMock()
    page.extract_table.return_value = rows
    return page


def _make_pdf(pages):
    pdf = MagicMock()
    pdf.__enter__ = MagicMock(return_value=pdf)
    pdf.__exit__ = MagicMock(return_value=False)
    pdf.pages = pages
    return pdf


def _make_redis_with_search(icao_hex="4B4100", registration="ER-AXA"):
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
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "4B4100", "ER-AXA")
        assert record["icao_hex"] == "4B4100"
        assert record["registration"] == "ER-AXA"
        assert record["source"] == "md-caa"
        assert record["military"] is False
        assert record["aircraft"]["model"] == "Boeing 737-800"
        assert record["aircraft"]["serial_number"] == "35014"

    def test_model_whitespace_collapsed(self):
        row = _make_row(model="Boeing  737\n800")
        record = _build_record(row, "4B4100", "ER-AXA")
        assert record["aircraft"]["model"] == "Boeing 737 800"

    def test_empty_model_omitted(self):
        row = _make_row(model="")
        record = _build_record(row, "4B4100", "ER-AXA")
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial="")
        record = _build_record(row, "4B4100", "ER-AXA")
        assert "serial_number" not in record.get("aircraft", {})

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(model="", serial="")
        record = _build_record(row, "4B4100", "ER-AXA")
        assert "aircraft" not in record

    def test_no_registrant_key(self):
        row = _make_row()
        record = _build_record(row, "4B4100", "ER-AXA")
        assert "registrant" not in record

    def test_operator_code_not_stored(self):
        row = _make_row()
        row["operator"] = "MDV"
        record = _build_record(row, "4B4100", "ER-AXA")
        assert "operator" not in record
        assert "registrant" not in record


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("ERAXA") == "ERAXA"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("ER-AXA")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: download_and_parse
# ---------------------------------------------------------------------------

class TestDownloadAndParse:
    def test_raises_on_http_error(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 404
        session.get.return_value = resp
        with pytest.raises(RuntimeError, match="HTTP 404"):
            download_and_parse(session)

    def test_logs_pdf_url(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 503
        session.get.return_value = resp
        import logging
        with patch.object(logging.getLogger("md-caa"), "info") as mock_log:
            with pytest.raises(RuntimeError):
                download_and_parse(session)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert _PDF_URL in logged

    def test_filters_non_er_rows(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.content = b"%PDF"
        session.get.return_value = resp
        page = _make_pdf_page([
            ["1", "Boeing 737", "ER-AXA", "35014", "MDV"],
            ["2", "Cessna 172", "N12345", "17281000", ""],
        ])
        with patch("md_caa_main.pdfplumber.open", return_value=_make_pdf([page])):
            records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "ER-AXA"

    def test_parses_model_and_serial(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.content = b"%PDF"
        session.get.return_value = resp
        page = _make_pdf_page([
            ["1", "Boeing 737-800", "ER-AXA", "35014", "MDV"],
        ])
        with patch("md_caa_main.pdfplumber.open", return_value=_make_pdf([page])):
            records = download_and_parse(session)
        assert records[0]["model"] == "Boeing 737-800"
        assert records[0]["serial"] == "35014"

    def test_skips_empty_rows(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.content = b"%PDF"
        session.get.return_value = resp
        page = _make_pdf_page([None, [], ["1", "Boeing 737", "ER-AXA", "35014", "MDV"]])
        with patch("md_caa_main.pdfplumber.open", return_value=_make_pdf([page])):
            records = download_and_parse(session)
        assert len(records) == 1

    def test_skips_page_with_no_table(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.content = b"%PDF"
        session.get.return_value = resp
        page = MagicMock()
        page.extract_table.return_value = None
        page2 = _make_pdf_page([["1", "Boeing 737", "ER-BBB", "99999", ""]])
        with patch("md_caa_main.pdfplumber.open", return_value=_make_pdf([page, page2])):
            records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "ER-BBB"

    def test_multiple_pages_combined(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.content = b"%PDF"
        session.get.return_value = resp
        page1 = _make_pdf_page([["1", "Boeing 737", "ER-AXA", "35014", "MDV"]])
        page2 = _make_pdf_page([["2", "Airbus A320", "ER-BBB", "1234", "AIR"]])
        with patch("md_caa_main.pdfplumber.open", return_value=_make_pdf([page1, page2])):
            records = download_and_parse(session)
        assert len(records) == 2

    def test_uses_static_pdf_url(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 404
        session.get.return_value = resp
        with pytest.raises(RuntimeError):
            download_and_parse(session)
        session.get.assert_called_once_with(_PDF_URL, timeout=180)


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4B4100", registration="ER-AXA")
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
        r = _make_redis_with_search(icao_hex="4B4100", registration="ER-AXA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:4B4100"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4B4100", registration="ER-AXA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "md-caa"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4B4100", registration="ER-AXA")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:4B4100", REDIS_TTL)

    def test_null_fields_omitted_from_written_record(self):
        rows = [_make_row(model="")]
        r = _make_redis_with_search(icao_hex="4B4100", registration="ER-AXA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        record = set_call[0][2]
        assert "model" not in record.get("aircraft", {})

    def test_multiple_records(self):
        rows = [_make_row(registration="ER-AXA"), _make_row(registration="ER-BBB")]
        r = MagicMock()
        doc_a = MagicMock()
        doc_a.id = "aircraft:mictronics:4B4100"
        doc_a.registration = "ER-AXA"
        doc_b = MagicMock()
        doc_b.id = "aircraft:mictronics:4B4101"
        doc_b.registration = "ER-BBB"
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
        with patch("md_caa_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("md_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 77, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("md_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 77, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "77"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("md_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/md-caa"
