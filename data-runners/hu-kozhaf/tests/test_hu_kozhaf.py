"""Tests for the Hungary KoZHAF data runner."""

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
        "hu_kozhaf_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["hu_kozhaf_main"] = mod
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    registration="HA-GZQ",
    model="Cessna 172S",
    serial="17281234",
    year="2005",
    owner_name="Magyar Airlines Kft.",
    owner_address="1234 Budapest, Fő utca 1.",
    operator_name="",
) -> dict:
    return {
        "registration": registration,
        "model": model,
        "serial": serial,
        "year": year,
        "owner_name": owner_name,
        "owner_address": owner_address,
        "operator_name": operator_name,
    }


def _make_redis_with_search(icao_hex="4D1234", registration="HA-GZQ"):
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
    def test_strips_space_after_ha_hyphen(self):
        assert _normalize_registration("HA- GZQ") == "HA-GZQ"

    def test_strips_multiple_spaces(self):
        assert _normalize_registration("HA-  GZQ") == "HA-GZQ"

    def test_already_normalized(self):
        assert _normalize_registration("HA-GZQ") == "HA-GZQ"

    def test_strips_surrounding_whitespace(self):
        assert _normalize_registration("  HA- GZQ  ") == "HA-GZQ"

    def test_lowercase_preserved(self):
        assert _normalize_registration("HA- abc") == "HA-abc"


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "4D1234", "HA-GZQ")
        assert record["icao_hex"] == "4D1234"
        assert record["registration"] == "HA-GZQ"
        assert record["source"] == "hu-kozhaf"
        assert record["aircraft"]["model"] == "Cessna 172S"
        assert record["aircraft"]["serial_number"] == "17281234"
        assert record["aircraft"]["manufactured_date"] == "2005-01-01"
        assert record["registrant"]["names"] == ["Magyar Airlines Kft."]
        assert record["registrant"]["street"] == "1234 Budapest, Fő utca 1."

    def test_operator_different_from_owner_included(self):
        row = _make_row(owner_name="Owner Kft.", operator_name="Operator Zrt.")
        record = _build_record(row, "4D1234", "HA-GZQ")
        assert record["registrant"]["names"] == ["Owner Kft.", "Operator Zrt."]

    def test_operator_same_as_owner_not_duplicated(self):
        row = _make_row(owner_name="Owner Kft.", operator_name="Owner Kft.")
        record = _build_record(row, "4D1234", "HA-GZQ")
        assert record["registrant"]["names"] == ["Owner Kft."]

    def test_empty_operator_omitted(self):
        row = _make_row(operator_name="")
        record = _build_record(row, "4D1234", "HA-GZQ")
        assert record["registrant"]["names"] == ["Magyar Airlines Kft."]

    def test_model_newlines_collapsed(self):
        row = _make_row(model="Cessna\n172S")
        record = _build_record(row, "4D1234", "HA-GZQ")
        assert record["aircraft"]["model"] == "Cessna 172S"

    def test_year_only_digit_stored_as_date(self):
        row = _make_row(year="1998")
        record = _build_record(row, "4D1234", "HA-GZQ")
        assert record["aircraft"]["manufactured_date"] == "1998-01-01"

    def test_non_digit_year_omitted(self):
        row = _make_row(year="N/A")
        record = _build_record(row, "4D1234", "HA-GZQ")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_partial_year_omitted(self):
        row = _make_row(year="200")
        record = _build_record(row, "4D1234", "HA-GZQ")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_empty_model_omitted(self):
        row = _make_row(model="")
        record = _build_record(row, "4D1234", "HA-GZQ")
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial="")
        record = _build_record(row, "4D1234", "HA-GZQ")
        assert "serial_number" not in record.get("aircraft", {})

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(model="", serial="", year="")
        record = _build_record(row, "4D1234", "HA-GZQ")
        assert "aircraft" not in record

    def test_empty_owner_name_omits_names(self):
        row = _make_row(owner_name="")
        record = _build_record(row, "4D1234", "HA-GZQ")
        assert "names" not in record.get("registrant", {})

    def test_empty_address_omits_street(self):
        row = _make_row(owner_address="")
        record = _build_record(row, "4D1234", "HA-GZQ")
        assert "street" not in record.get("registrant", {})

    def test_empty_owner_and_address_omits_registrant(self):
        row = _make_row(owner_name="", owner_address="", operator_name="")
        record = _build_record(row, "4D1234", "HA-GZQ")
        assert "registrant" not in record


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("HAGZQ") == "HAGZQ"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("HA-GZQ")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: download_and_parse
# ---------------------------------------------------------------------------

class TestDownloadAndParse:
    def test_raises_on_index_error(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 404
        session.get.return_value = resp
        from hu_kozhaf_main import download_and_parse
        with pytest.raises(RuntimeError, match="HTTP 404"):
            download_and_parse(session)

    def test_raises_when_no_pdf_link(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.text = "<html><body><p>No links here</p></body></html>"
        session.get.return_value = resp
        from hu_kozhaf_main import download_and_parse
        with pytest.raises(RuntimeError, match="No PDF download link"):
            download_and_parse(session)

    def test_raises_on_pdf_download_error(self):
        pdf_url = "https://kozlekedesihatosag.kormany.hu/documents/66238/342548/register.pdf?download=true"
        session = MagicMock()
        index_resp = MagicMock()
        index_resp.ok = True
        index_resp.text = f'<html><body><a href="{pdf_url}">PDF</a></body></html>'
        pdf_resp = MagicMock()
        pdf_resp.ok = False
        pdf_resp.status_code = 403
        session.get.side_effect = [index_resp, pdf_resp]
        from hu_kozhaf_main import download_and_parse
        with pytest.raises(RuntimeError, match="HTTP 403"):
            download_and_parse(session)

    def test_relative_pdf_url_resolved(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.text = '<html><body><a href="/documents/66238/342548/register.pdf?download=true">PDF</a></body></html>'
        pdf_resp = MagicMock()
        pdf_resp.ok = False
        pdf_resp.status_code = 404
        session.get.side_effect = [resp, pdf_resp]
        from hu_kozhaf_main import download_and_parse
        with pytest.raises(RuntimeError):
            download_and_parse(session)
        called_url = session.get.call_args_list[1][0][0]
        assert called_url.startswith("https://kozlekedesihatosag.kormany.hu")

    def test_logs_index_url(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 503
        session.get.return_value = resp
        import logging
        from hu_kozhaf_main import download_and_parse, _INDEX_URL
        with patch.object(logging.getLogger("hu-kozhaf"), "info") as mock_log:
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
        r = _make_redis_with_search(icao_hex="4D1234", registration="HA-GZQ")
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
        r = _make_redis_with_search(icao_hex="4D1234", registration="HA-GZQ")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:4D1234"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4D1234", registration="HA-GZQ")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "hu-kozhaf"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4D1234", registration="HA-GZQ")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:4D1234", REDIS_TTL)

    def test_multiple_records(self):
        rows = [_make_row(registration="HA-GZQ"), _make_row(registration="HA-ABC")]
        r = MagicMock()
        doc_a = MagicMock()
        doc_a.id = "aircraft:mictronics:4D1234"
        doc_a.registration = "HA-GZQ"
        doc_b = MagicMock()
        doc_b.id = "aircraft:mictronics:4D1235"
        doc_b.registration = "HA-ABC"
        results = MagicMock()
        results.docs = [doc_a, doc_b]
        r.ft.return_value.search.return_value = results
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 2

    def test_null_fields_omitted_from_written_record(self):
        rows = [_make_row(model="")]
        r = _make_redis_with_search(icao_hex="4D1234", registration="HA-GZQ")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert "model" not in set_call[0][2]["aircraft"]


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
        with patch("hu_kozhaf_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("hu_kozhaf_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 150, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("hu_kozhaf_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 150, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "150"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("hu_kozhaf_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/hu-kozhaf"
