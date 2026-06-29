"""Tests for the Papua New Guinea CASA PNG data runner."""

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
        "pg_casapng_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["pg_casapng_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

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
    reg="P2-ANG",
    manufacturer="Cessna",
    model="172S",
    operator="Air Niugini",
    address="PO Box 7186, Boroko NCD, PNG",
) -> dict:
    return {
        "registration": reg,
        "manufacturer": manufacturer,
        "model": model,
        "operator": operator,
        "address": address,
    }


def _make_redis_with_search(icao_hex="898A00", registration="P2-ANG"):
    r = MagicMock()
    doc = MagicMock()
    doc.id = f"aircraft:simple:{icao_hex}"
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


def _make_index_response(pdf_url: str):
    resp = MagicMock()
    resp.ok = True
    resp.status_code = 200
    resp.text = f'<html><body><a href="{pdf_url}">Aircraft Register</a></body></html>'
    return resp


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "898A00", "P2-ANG")
        assert record["icao_hex"] == "898A00"
        assert record["registration"] == "P2-ANG"
        assert record["source"] == "pg-casapng"
        assert record["aircraft"]["manufacturer"] == "Cessna"
        assert record["aircraft"]["model"] == "172S"
        assert record["registrant"]["names"] == ["Air Niugini"]
        assert record["registrant"]["street"] == "PO Box 7186, Boroko NCD"
        assert record["registrant"]["country"] == "Papua New Guinea"

    def test_png_country_normalized(self):
        row = _make_row(address="PO Box 1, Port Moresby, PNG")
        record = _build_record(row, "898A00", "P2-ANG")
        assert record["registrant"]["country"] == "Papua New Guinea"

    def test_foreign_country_stored_as_is(self):
        row = _make_row(address="123 Smith St, Brisbane, AUSTRALIA")
        record = _build_record(row, "898A00", "P2-ANG")
        assert record["registrant"]["country"] == "AUSTRALIA"
        assert record["registrant"]["street"] == "123 Smith St, Brisbane"

    def test_address_newlines_collapsed_before_split(self):
        row = _make_row(address="WEST NEW\nBRITAIN PROVINCE, PNG")
        record = _build_record(row, "898A00", "P2-ANG")
        assert record["registrant"]["street"] == "WEST NEW BRITAIN PROVINCE"
        assert record["registrant"]["country"] == "Papua New Guinea"

    def test_address_no_comma_treated_as_street_only(self):
        row = _make_row(address="Port Moresby")
        record = _build_record(row, "898A00", "P2-ANG")
        assert record["registrant"]["street"] == "Port Moresby"
        assert "country" not in record.get("registrant", {})

    def test_empty_manufacturer_omitted(self):
        row = _make_row(manufacturer="")
        record = _build_record(row, "898A00", "P2-ANG")
        assert "manufacturer" not in record.get("aircraft", {})

    def test_empty_model_omitted(self):
        row = _make_row(model="")
        record = _build_record(row, "898A00", "P2-ANG")
        assert "model" not in record.get("aircraft", {})

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(manufacturer="", model="")
        record = _build_record(row, "898A00", "P2-ANG")
        assert "aircraft" not in record

    def test_empty_operator_omits_names(self):
        row = _make_row(operator="")
        record = _build_record(row, "898A00", "P2-ANG")
        assert "names" not in record.get("registrant", {})

    def test_empty_address_omits_street_and_country(self):
        row = _make_row(address="")
        record = _build_record(row, "898A00", "P2-ANG")
        assert "street" not in record.get("registrant", {})
        assert "country" not in record.get("registrant", {})

    def test_empty_operator_and_address_omits_registrant(self):
        row = _make_row(operator="", address="")
        record = _build_record(row, "898A00", "P2-ANG")
        assert "registrant" not in record


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("P2ANG") == "P2ANG"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("P2-ANG")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: download_and_parse
# ---------------------------------------------------------------------------

class TestDownloadAndParse:
    def _make_pdf_response(self, content: bytes, status_code: int = 200):
        resp = MagicMock()
        resp.ok = status_code < 400
        resp.status_code = status_code
        resp.content = content
        return resp

    def test_raises_on_index_error(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 404
        session.get.return_value = resp
        from pg_casapng_main import download_and_parse
        with pytest.raises(RuntimeError, match="HTTP 404"):
            download_and_parse(session)

    def test_raises_when_no_pdf_link(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.text = "<html><body><p>No links here</p></body></html>"
        session.get.return_value = resp
        from pg_casapng_main import download_and_parse
        with pytest.raises(RuntimeError, match="No PDF link"):
            download_and_parse(session)

    def test_raises_on_pdf_download_error(self):
        pdf_url = "https://casapng.gov.pg/register.pdf"
        session = MagicMock()
        index_resp = _make_index_response(pdf_url)
        pdf_resp = MagicMock()
        pdf_resp.ok = False
        pdf_resp.status_code = 403
        session.get.side_effect = [index_resp, pdf_resp]
        from pg_casapng_main import download_and_parse
        with pytest.raises(RuntimeError, match="HTTP 403"):
            download_and_parse(session)

    def test_relative_pdf_url_resolved(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.text = '<html><body><a href="/files/register.pdf">PDF</a></body></html>'
        pdf_resp = MagicMock()
        pdf_resp.ok = False
        pdf_resp.status_code = 404
        session.get.side_effect = [resp, pdf_resp]
        from pg_casapng_main import download_and_parse
        with pytest.raises(RuntimeError):
            download_and_parse(session)
        called_url = session.get.call_args_list[1][0][0]
        assert called_url.startswith("https://casapng.gov.pg")

    def test_logs_index_url(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 503
        session.get.return_value = resp
        import logging
        from pg_casapng_main import download_and_parse, _INDEX_URL
        with patch.object(logging.getLogger("pg-casapng"), "info") as mock_log:
            with pytest.raises(RuntimeError):
                download_and_parse(session)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert _INDEX_URL in logged


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="898A00", registration="P2-ANG")
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
        r = _make_redis_with_search(icao_hex="898A00", registration="P2-ANG")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:detail:898A00"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="898A00", registration="P2-ANG")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "pg-casapng"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="898A00", registration="P2-ANG")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:detail:898A00", REDIS_TTL)

    def test_multiple_records(self):
        rows = [_make_row(reg="P2-ANG"), _make_row(reg="P2-ANH")]
        r = MagicMock()
        doc_a = MagicMock()
        doc_a.id = "aircraft:simple:898A00"
        doc_a.registration = "P2-ANG"
        doc_b = MagicMock()
        doc_b.id = "aircraft:simple:898A01"
        doc_b.registration = "P2-ANH"
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
        with patch("pg_casapng_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("pg_casapng_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 217, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("pg_casapng_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 217, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "217"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("pg_casapng_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/pg-casapng"
