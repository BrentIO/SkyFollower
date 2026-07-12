"""Tests for the Singapore CAAS data runner."""

from __future__ import annotations

import importlib.util
import io
import json
import os
import sys
from unittest.mock import MagicMock, patch

import openpyxl
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
        "sg_caas_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["sg_caas_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

from shared.url_reachability import assert_url_reachable

_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
_COL_REGISTRATION = _mod._COL_REGISTRATION
_COL_SERIAL = _mod._COL_SERIAL
_COL_MODEL = _mod._COL_MODEL
_COL_OPERATOR = _mod._COL_OPERATOR
_COL_MANUFACTURER = _mod._COL_MANUFACTURER
_COL_ENGINE_MODEL = _mod._COL_ENGINE_MODEL
_COL_ENGINE_MFR = _mod._COL_ENGINE_MFR


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    reg="9V-SKA",
    serial="27671",
    model="B737-832",
    operator="Singapore Airlines",
    manufacturer="BOEING",
    engine_model="CFM56-7B27",
    engine_mfr="CFM International",
) -> dict:
    return {
        _COL_REGISTRATION: reg,
        _COL_SERIAL: serial,
        _COL_MODEL: model,
        _COL_OPERATOR: operator,
        _COL_MANUFACTURER: manufacturer,
        _COL_ENGINE_MODEL: engine_model,
        _COL_ENGINE_MFR: engine_mfr,
    }


def _make_redis_with_search(icao_hex="76CE26", registration="9V-SKA"):
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


def _make_xlsx_bytes(rows: list[list]) -> bytes:
    """Build an in-memory xlsx with a header row followed by data rows."""
    wb = openpyxl.Workbook()
    ws = wb.active
    headers = [
        _COL_REGISTRATION, _COL_SERIAL, _COL_MODEL, _COL_OPERATOR,
        _COL_MANUFACTURER, _COL_ENGINE_MODEL, _COL_ENGINE_MFR,
    ]
    ws.append(headers)
    for row in rows:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "76CE26", "9V-SKA")
        assert record["icao_hex"] == "76CE26"
        assert record["registration"] == "9V-SKA"
        assert record["source"] == "sg-caas"
        assert record["military"] is False
        assert record["aircraft"]["manufacturer"] == "BOEING"
        assert record["aircraft"]["model"] == "B737-832"
        assert record["aircraft"]["serial_number"] == "27671"
        assert record["aircraft"]["powerplant"]["manufacturer"] == "CFM International"
        assert record["aircraft"]["powerplant"]["model"] == "CFM56-7B27"
        assert record["registrant"]["names"] == ["Singapore Airlines"]

    def test_empty_manufacturer_omitted(self):
        row = _make_row(manufacturer="")
        record = _build_record(row, "76CE26", "9V-SKA")
        assert "manufacturer" not in record.get("aircraft", {})

    def test_empty_model_omitted(self):
        row = _make_row(model="")
        record = _build_record(row, "76CE26", "9V-SKA")
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial="")
        record = _build_record(row, "76CE26", "9V-SKA")
        assert "serial_number" not in record.get("aircraft", {})

    def test_empty_engine_mfr_omitted(self):
        row = _make_row(engine_mfr="")
        record = _build_record(row, "76CE26", "9V-SKA")
        assert "manufacturer" not in record.get("aircraft", {}).get("powerplant", {})

    def test_empty_engine_model_omitted(self):
        row = _make_row(engine_model="")
        record = _build_record(row, "76CE26", "9V-SKA")
        assert "model" not in record.get("aircraft", {}).get("powerplant", {})

    def test_empty_operator_omits_registrant(self):
        row = _make_row(operator="")
        record = _build_record(row, "76CE26", "9V-SKA")
        assert "registrant" not in record

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(manufacturer="", model="", serial="", engine_mfr="", engine_model="")
        record = _build_record(row, "76CE26", "9V-SKA")
        assert "aircraft" not in record
        assert "powerplant" not in record.get("aircraft", {})


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("9VSKA") == "9VSKA"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("9V-SKA")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="76CE26", registration="9V-SKA")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_record_not_written_when_not_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_empty_registration_skipped(self):
        rows = [_make_row(reg="")]
        r = _make_redis_with_search()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0
        r.ft.return_value.search.assert_not_called()

    def test_writes_to_detail_key(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="76CE26", registration="9V-SKA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:76CE26"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="76CE26", registration="9V-SKA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "sg-caas"

    def test_null_fields_omitted_from_written_record(self):
        rows = [_make_row(engine_mfr="", engine_model="")]
        r = _make_redis_with_search(icao_hex="76CE26", registration="9V-SKA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        record = set_call[0][2]
        assert "powerplant" not in record.get("aircraft", {})

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="76CE26", registration="9V-SKA")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:76CE26", REDIS_TTL)

    def test_multiple_records(self):
        rows = [_make_row(reg="9V-SKA"), _make_row(reg="9V-SKB")]
        r = MagicMock()
        doc_a = MagicMock()
        doc_a.id = "aircraft:mictronics:76CE26"
        doc_a.registration = "9V-SKA"
        doc_b = MagicMock()
        doc_b.id = "aircraft:mictronics:76CE27"
        doc_b.registration = "9V-SKB"
        results = MagicMock()
        results.docs = [doc_a, doc_b]
        r.ft.return_value.search.return_value = results
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 2


# ---------------------------------------------------------------------------
# Tests: download_and_parse
# ---------------------------------------------------------------------------

class TestDownloadAndParse:
    def _make_response(self, content: bytes, status_code: int = 200):
        resp = MagicMock()
        resp.ok = status_code < 400
        resp.status_code = status_code
        resp.content = content
        return resp

    def _make_index_response(self, xlsx_url: str):
        resp = MagicMock()
        resp.ok = True
        resp.status_code = 200
        resp.text = (
            f'<html><body><a href="{xlsx_url}">Aircraft Register</a></body></html>'
        )
        return resp

    def test_parses_9v_rows(self):
        xlsx_bytes = _make_xlsx_bytes([
            ["9V-SKA", "27671", "B737-832", "Singapore Airlines", "BOEING", "CFM56-7B27", "CFM International"],
        ])
        xlsx_url = f"https://isomer-user-content.by.gov.sg/175/abc/Aircraft-Register.xlsx"
        session = MagicMock()
        session.get.side_effect = [
            self._make_index_response(xlsx_url),
            self._make_response(xlsx_bytes),
        ]
        from sg_caas_main import download_and_parse
        rows = download_and_parse(session)
        assert len(rows) == 1
        assert rows[0][_COL_REGISTRATION] == "9V-SKA"
        assert rows[0][_COL_MODEL] == "B737-832"

    def test_skips_non_9v_rows(self):
        xlsx_bytes = _make_xlsx_bytes([
            ["9V-SKA", "27671", "B737-832", "Singapore Airlines", "BOEING", "CFM56-7B27", "CFM International"],
            ["N12345", "99999", "B737-800", "Other Airline", "BOEING", "CFM56-7B27", "CFM International"],
        ])
        xlsx_url = "https://isomer-user-content.by.gov.sg/175/abc/Aircraft-Register.xlsx"
        session = MagicMock()
        session.get.side_effect = [
            self._make_index_response(xlsx_url),
            self._make_response(xlsx_bytes),
        ]
        from sg_caas_main import download_and_parse
        rows = download_and_parse(session)
        assert len(rows) == 1

    def test_skips_blank_rows(self):
        xlsx_bytes = _make_xlsx_bytes([
            ["9V-SKA", "27671", "B737-832", "Singapore Airlines", "BOEING", "CFM56-7B27", "CFM International"],
            [None, None, None, None, None, None, None],
        ])
        xlsx_url = "https://isomer-user-content.by.gov.sg/175/abc/Aircraft-Register.xlsx"
        session = MagicMock()
        session.get.side_effect = [
            self._make_index_response(xlsx_url),
            self._make_response(xlsx_bytes),
        ]
        from sg_caas_main import download_and_parse
        rows = download_and_parse(session)
        assert len(rows) == 1

    def test_raises_on_index_error(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 503
        session.get.return_value = resp
        from sg_caas_main import download_and_parse
        with pytest.raises(RuntimeError, match="HTTP 503"):
            download_and_parse(session)

    def test_raises_when_no_xlsx_link(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.text = "<html><body><p>No links here</p></body></html>"
        session.get.return_value = resp
        from sg_caas_main import download_and_parse
        with pytest.raises(RuntimeError, match="No xlsx link"):
            download_and_parse(session)

    def test_raises_on_xlsx_download_error(self):
        xlsx_url = "https://isomer-user-content.by.gov.sg/175/abc/Aircraft-Register.xlsx"
        session = MagicMock()
        index_resp = MagicMock()
        index_resp.ok = True
        index_resp.text = f'<html><body><a href="{xlsx_url}">Register</a></body></html>'
        xlsx_resp = MagicMock()
        xlsx_resp.ok = False
        xlsx_resp.status_code = 403
        session.get.side_effect = [index_resp, xlsx_resp]
        from sg_caas_main import download_and_parse
        with pytest.raises(RuntimeError, match="HTTP 403"):
            download_and_parse(session)

    def test_referer_sent_on_xlsx_download(self):
        xlsx_bytes = _make_xlsx_bytes([])
        xlsx_url = "https://isomer-user-content.by.gov.sg/175/abc/Aircraft-Register.xlsx"
        session = MagicMock()
        session.get.side_effect = [
            self._make_index_response(xlsx_url),
            self._make_response(xlsx_bytes),
        ]
        from sg_caas_main import download_and_parse, _INDEX_URL
        download_and_parse(session)
        _, kwargs = session.get.call_args_list[1]
        assert kwargs.get("headers", {}).get("Referer") == _INDEX_URL


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
        with patch("sg_caas_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("sg_caas_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 242, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("sg_caas_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 242, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "242"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("sg_caas_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/sg-caas"


# ---------------------------------------------------------------------------
# Tests: network (real outbound HTTP call — see #405)
# ---------------------------------------------------------------------------

class TestNetwork:
    @pytest.mark.network
    def test_url_reachable(self):
        assert_url_reachable(_mod._INDEX_URL, "sg-caas", headers={"User-Agent": _mod._BROWSER_UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"})
