"""Tests for the Estonia Transpordiamet data runner."""

from __future__ import annotations

import importlib.util
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
        "ee_transpordiamet_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ee_transpordiamet_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

download_and_parse = _mod.download_and_parse
_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
_PAGE_URL = _mod._PAGE_URL


# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

_HEADER_1 = "<tr><th>-</th><th>Registreerimisnumber</th><th>-</th><th>-</th><th>Tüüp</th><th>Seerianumber</th><th>Omanik</th><th>Käitaja</th><th>-</th></tr>"
_HEADER_2 = "<tr><th>-</th><th>Registration mark</th><th>-</th><th>-</th><th>Type of Aircraft</th><th>Serial number</th><th>Owner</th><th>Operator</th><th>-</th></tr>"


def _data_row(
    reg="ES-AAA",
    col2="",
    col3="",
    model="Cessna 172",
    serial="17260001",
    owner="Owner Co",
    operator="Operator Co",
    col8="",
) -> str:
    return (
        f"<tr><td></td><td>{reg}</td><td>{col2}</td><td>{col3}</td>"
        f"<td>{model}</td><td>{serial}</td><td>{owner}</td>"
        f"<td>{operator}</td><td>{col8}</td></tr>"
    )


def _page_html(data_rows: list[str]) -> str:
    rows = _HEADER_1 + _HEADER_2 + "".join(data_rows)
    return f"<html><body><table>{rows}</table></body></html>"


def _make_response(html: str = "", status_code: int = 200):
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.text = html
    return resp


def _make_row(
    registration="ES-AAA",
    model="Cessna 172",
    serial="17260001",
    owner="Owner Co",
) -> dict:
    return {
        "registration": registration,
        "model": model,
        "serial": serial,
        "owner": owner,
    }


def _make_redis_with_search(icao_hex="4B1234", registration="ES-AAA"):
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
    def test_raises_on_http_error(self):
        session = MagicMock()
        session.get.return_value = _make_response(status_code=503)
        with pytest.raises(RuntimeError, match="HTTP 503"):
            download_and_parse(session)

    def test_raises_when_no_table(self):
        session = MagicMock()
        session.get.return_value = _make_response("<html><body>No table here</body></html>")
        with pytest.raises(RuntimeError, match="No table found"):
            download_and_parse(session)

    def test_logs_url(self):
        session = MagicMock()
        session.get.return_value = _make_response(status_code=503)
        import logging
        with patch.object(logging.getLogger("ee-transpordiamet"), "info") as mock_log:
            with pytest.raises(RuntimeError):
                download_and_parse(session)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert _PAGE_URL in logged

    def test_parses_basic_row(self):
        html = _page_html([_data_row()])
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 1
        r = records[0]
        assert r["registration"] == "ES-AAA"
        assert r["model"] == "Cessna 172"
        assert r["serial"] == "17260001"
        assert r["owner"] == "Owner Co"
        assert "operator" not in r

    def test_skips_header_rows(self):
        html = _page_html([_data_row()])
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 1

    def test_skips_non_es_rows(self):
        html = _page_html([_data_row(reg="G-ABCD")])
        session = MagicMock()
        session.get.return_value = _make_response(html)
        assert download_and_parse(session) == []

    def test_normalizes_spaced_registration(self):
        """ES - MBA (with spaces around hyphen) should normalize to ES-MBA."""
        html = _page_html([_data_row(reg="ES - MBA")])
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "ES-MBA"

    def test_normalizes_spaced_suffix(self):
        """ES-1005 (no extra spaces) should remain ES-1005."""
        html = _page_html([_data_row(reg="ES-1005")])
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert records[0]["registration"] == "ES-1005"

    def test_normalizes_internal_spaces(self):
        """ES - A BC (multiple internal spaces) should become ES-ABC."""
        html = _page_html([_data_row(reg="ES - A BC")])
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert records[0]["registration"] == "ES-ABC"

    def test_multiple_rows(self):
        html = _page_html([
            _data_row(reg="ES-AAA"),
            _data_row(reg="ES-BBB"),
        ])
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 2

    def test_short_row_skipped(self):
        html = f"<html><body><table>{_HEADER_1}{_HEADER_2}<tr><td>ES-AAA</td></tr></table></body></html>"
        session = MagicMock()
        session.get.return_value = _make_response(html)
        assert download_and_parse(session) == []


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "4B1234", "ES-AAA")
        assert record["icao_hex"] == "4B1234"
        assert record["registration"] == "ES-AAA"
        assert record["source"] == "ee-transpordiamet"
        assert record["military"] is False
        assert record["aircraft"]["model"] == "Cessna 172"
        assert record["aircraft"]["serial_number"] == "17260001"
        assert record["registrant"]["names"] == ["Owner Co"]

    def test_no_registrant_when_owner_empty(self):
        row = _make_row(owner="")
        record = _build_record(row, "4B1234", "ES-AAA")
        assert "registrant" not in record

    def test_empty_model_omitted(self):
        row = _make_row(model="")
        record = _build_record(row, "4B1234", "ES-AAA")
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial="")
        record = _build_record(row, "4B1234", "ES-AAA")
        assert "serial_number" not in record.get("aircraft", {})

    def test_no_aircraft_when_both_empty(self):
        row = _make_row(model="", serial="")
        record = _build_record(row, "4B1234", "ES-AAA")
        assert "aircraft" not in record

    def test_source_is_ee_transpordiamet(self):
        row = _make_row()
        record = _build_record(row, "4B1234", "ES-AAA")
        assert record["source"] == "ee-transpordiamet"


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("ES-AAA")

    def test_plain_value_unchanged(self):
        assert _escape_tag("ABCD") == "ABCD"


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4B1234", registration="ES-AAA")
        assert write_to_redis(rows, r, REDIS_TTL) == 1

    def test_record_not_written_when_not_found(self):
        rows = [_make_row()]
        r = _make_redis_no_match()
        assert write_to_redis(rows, r, REDIS_TTL) == 0

    def test_empty_registration_skipped(self):
        rows = [_make_row(registration="")]
        r = _make_redis_with_search()
        assert write_to_redis(rows, r, REDIS_TTL) == 0
        r.ft.return_value.search.assert_not_called()

    def test_writes_to_detail_key(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4B1234", registration="ES-AAA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:4B1234"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4B1234", registration="ES-AAA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "ee-transpordiamet"

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4B1234", registration="ES-AAA")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:4B1234", REDIS_TTL)

    def test_empty_list_returns_zero(self):
        assert write_to_redis([], _make_redis_no_match(), REDIS_TTL) == 0

    def test_null_fields_omitted_from_written_record(self):
        rows = [_make_row(model="")]
        r = _make_redis_with_search(icao_hex="4B1234", registration="ES-AAA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert "model" not in set_call[0][2]["aircraft"]


# ---------------------------------------------------------------------------
# Tests: publish_completion_stats
# ---------------------------------------------------------------------------

class TestPublishCompletionStats:
    def _setup_mock_client(self):
        mc = MagicMock()

        def fake_connect(host, port, keepalive):
            mc.on_connect(mc, None, None, 0, None)

        mc.connect.side_effect = fake_connect
        return mc

    def test_no_mqtt_config_skips(self):
        publish_completion_stats({}, 100, "success")

    def test_mqtt_connect_timeout_does_not_raise(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        with patch("ee_transpordiamet_main.mqtt.Client") as mock_cls:
            mock_cls.return_value = MagicMock()
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ee_transpordiamet_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 166, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ee_transpordiamet_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 166, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "166"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ee_transpordiamet_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/ee-transpordiamet"
