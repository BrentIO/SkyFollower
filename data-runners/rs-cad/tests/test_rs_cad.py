"""Tests for the Serbia CAD data runner."""

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
        "rs_cad_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["rs_cad_main"] = mod
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
_FIELD_REGISTRATION = _mod._FIELD_REGISTRATION
_FIELD_TYPE = _mod._FIELD_TYPE
_FIELD_MANUFACTURER = _mod._FIELD_MANUFACTURER
_FIELD_MODEL = _mod._FIELD_MODEL
_FIELD_SERIAL = _mod._FIELD_SERIAL
_FIELD_OPERATOR = _mod._FIELD_OPERATOR


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    reg="YU-HRD",
    manufacturer="Agusta Westland S.p.A.",
    model="AW109SP",
    serial="22280",
    operator="Kompanija Takovo d.o.o. Gornji Milanovac",
    vrsta="Helikopter",
) -> dict:
    return {
        _FIELD_REGISTRATION: reg,
        _FIELD_TYPE: vrsta,
        _FIELD_MANUFACTURER: manufacturer,
        _FIELD_MODEL: model,
        _FIELD_SERIAL: serial,
        _FIELD_OPERATOR: operator,
    }


def _make_api_response(items: list[dict], status_code: int = 200):
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.json.return_value = {"items": items}
    return resp


def _make_redis_with_search(icao_hex="4C0A3E", registration="YU-HRD"):
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
    def test_parses_yu_rows(self):
        session = MagicMock()
        session.get.return_value = _make_api_response([_make_row()])
        rows = download_and_parse(session)
        assert len(rows) == 1
        assert rows[0][_FIELD_REGISTRATION] == "YU-HRD"

    def test_skips_non_yu_rows(self):
        session = MagicMock()
        session.get.return_value = _make_api_response([
            _make_row(reg="YU-HRD"),
            _make_row(reg="N12345"),
        ])
        rows = download_and_parse(session)
        assert len(rows) == 1

    def test_empty_items_returns_empty(self):
        session = MagicMock()
        session.get.return_value = _make_api_response([])
        rows = download_and_parse(session)
        assert rows == []

    def test_raises_on_http_error(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 500
        session.get.return_value = resp
        with pytest.raises(RuntimeError, match="HTTP 500"):
            download_and_parse(session)

    def test_missing_items_key_returns_empty(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.json.return_value = {}
        session.get.return_value = resp
        rows = download_and_parse(session)
        assert rows == []

    def test_logs_api_url(self):
        session = MagicMock()
        session.get.return_value = _make_api_response([])
        import logging
        with patch.object(logging.getLogger("rs-cad"), "info") as mock_log:
            download_and_parse(session)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert "apps.cad.gov.rs" in logged


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "4C0A3E", "YU-HRD")
        assert record["icao_hex"] == "4C0A3E"
        assert record["registration"] == "YU-HRD"
        assert record["source"] == "rs-cad"
        assert record["aircraft"]["type"] == "Helicopter"
        assert record["aircraft"]["manufacturer"] == "Agusta Westland S.p.A."
        assert record["aircraft"]["model"] == "AW109SP"
        assert record["aircraft"]["serial_number"] == "22280"
        assert record["registrant"]["names"] == ["Kompanija Takovo d.o.o. Gornji Milanovac"]

    def test_type_decoded_to_english(self):
        mappings = [
            ("Avion", "Airplane"),
            ("Balon", "Balloon"),
            ("Helikopter", "Helicopter"),
            ("Jedrilica", "Glider"),
            ("Motorna jedrilica", "Glider"),
            ("Motorni zmaj", "Weight-Shift-Control"),
            ("Žirokopter", "Gyroplane"),
        ]
        for serbian, english in mappings:
            row = _make_row(vrsta=serbian)
            record = _build_record(row, "4C0A3E", "YU-HRD")
            assert record["aircraft"]["type"] == english, f"{serbian!r} should decode to {english!r}"

    def test_unknown_type_stored_as_is(self):
        row = _make_row(vrsta="Nepoznato")
        record = _build_record(row, "4C0A3E", "YU-HRD")
        assert record["aircraft"]["type"] == "Nepoznato"

    def test_empty_vrsta_omits_type(self):
        row = _make_row(vrsta="")
        record = _build_record(row, "4C0A3E", "YU-HRD")
        assert "type" not in record.get("aircraft", {})

    def test_empty_manufacturer_omitted(self):
        row = _make_row(manufacturer="")
        record = _build_record(row, "4C0A3E", "YU-HRD")
        assert "manufacturer" not in record.get("aircraft", {})

    def test_empty_model_omitted(self):
        row = _make_row(model="")
        record = _build_record(row, "4C0A3E", "YU-HRD")
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial="")
        record = _build_record(row, "4C0A3E", "YU-HRD")
        assert "serial_number" not in record.get("aircraft", {})

    def test_empty_operator_omits_registrant(self):
        row = _make_row(operator="")
        record = _build_record(row, "4C0A3E", "YU-HRD")
        assert "registrant" not in record

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(vrsta="", manufacturer="", model="", serial="")
        record = _build_record(row, "4C0A3E", "YU-HRD")
        assert "aircraft" not in record

    def test_none_field_values_handled(self):
        row = {
            _FIELD_REGISTRATION: "YU-HRD",
            _FIELD_MANUFACTURER: None,
            _FIELD_MODEL: None,
            _FIELD_SERIAL: None,
            _FIELD_OPERATOR: None,
        }
        record = _build_record(row, "4C0A3E", "YU-HRD")
        assert "aircraft" not in record
        assert "registrant" not in record


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("YUHRD") == "YUHRD"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("YU-HRD")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4C0A3E", registration="YU-HRD")
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
        r = _make_redis_with_search(icao_hex="4C0A3E", registration="YU-HRD")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:4C0A3E"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4C0A3E", registration="YU-HRD")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "rs-cad"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4C0A3E", registration="YU-HRD")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:4C0A3E", REDIS_TTL)

    def test_multiple_records(self):
        rows = [_make_row(reg="YU-HRD"), _make_row(reg="YU-ANA")]
        r = MagicMock()
        doc_a = MagicMock()
        doc_a.id = "aircraft:mictronics:4C0A3E"
        doc_a.registration = "YU-HRD"
        doc_b = MagicMock()
        doc_b.id = "aircraft:mictronics:4C0A3F"
        doc_b.registration = "YU-ANA"
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
        with patch("rs_cad_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("rs_cad_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 550, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("rs_cad_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 550, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "550"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("rs_cad_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/rs-cad"
