"""Tests for the Spain AESA data runner."""

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
        "es_aesa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["es_aesa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_cell = _mod._cell
_decode_clase = _mod._decode_clase
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
    matricula="EC-ABC",
    fecha="01/01/2020",
    fabricante="Airbus",
    modelo="A320",
    serial="1234",
    ano="2019",
    marca_motor="CFM",
    modelo_motor="CFM56-5B",
    num_mot="2",
    clase="AVION",
) -> dict:
    # Column names use space (not newline) — headers are normalized during parsing
    return {
        "Matrícula": matricula,
        "Fecha matric.": fecha,
        "Fabricante": fabricante,
        "Modelo": modelo,
        "Nº serie": serial,
        "Año cons.": ano,
        "Marca Motor": marca_motor,
        "Modelo Motor": modelo_motor,
        "Nº mot.": num_mot,
        "Clase": clase,
    }


def _make_redis_with_search(icao_hex="340123", registration="EC-ABC"):
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
# Tests: header normalization (via _cell + replace)
# ---------------------------------------------------------------------------

class TestHeaderNormalization:
    def test_newline_in_header_replaced_with_space(self):
        # Simulates what download_and_parse does: _cell(v).replace("\n", " ")
        raw = "Año\ncons."
        normalized = _cell(raw).replace("\n", " ")
        assert normalized == "Año cons."

    def test_multiword_header_with_newline(self):
        raw = "Nº\nmot."
        normalized = _cell(raw).replace("\n", " ")
        assert normalized == "Nº mot."

    def test_plain_header_unchanged(self):
        raw = "Fabricante"
        normalized = _cell(raw).replace("\n", " ")
        assert normalized == "Fabricante"


# ---------------------------------------------------------------------------
# Tests: _cell
# ---------------------------------------------------------------------------

class TestCell:
    def test_none_returns_empty(self):
        assert _cell(None) == ""

    def test_strips_whitespace(self):
        assert _cell("  Airbus  ") == "Airbus"

    def test_plain_string(self):
        assert _cell("EC-ABC") == "EC-ABC"

    def test_non_breaking_hyphen_normalized(self):
        # U+2011 non-breaking hyphen → ASCII hyphen
        assert _cell("EC‑FTR") == "EC-FTR"

    def test_en_dash_normalized(self):
        assert _cell("EC–FTR") == "EC-FTR"

    def test_soft_hyphen_normalized(self):
        # U+00AD soft hyphen
        assert _cell("EC­FTR") == "EC-FTR"


# ---------------------------------------------------------------------------
# Tests: _decode_clase
# ---------------------------------------------------------------------------

class TestDecodeClase:
    def test_avion(self):
        assert _decode_clase("AVION") == "Airplane"

    def test_helicoptero(self):
        assert _decode_clase("HELICOPTERO (VTOL)") == "Helicopter"

    def test_autogiro(self):
        assert _decode_clase("AUTOGIRO") == "Gyroplane"

    def test_globo(self):
        assert _decode_clase("GLOBO") == "Balloon"

    def test_planeador_with_newline(self):
        assert _decode_clase("PLANEADOR/MOTOPL\nANEADOR") == "Glider"

    def test_ulm_avion(self):
        assert _decode_clase("ULM-AVION") == "Airplane"

    def test_ulm_autogiro(self):
        assert _decode_clase("ULM-AUTOGIRO") == "Gyroplane"

    def test_afi_avion(self):
        assert _decode_clase("AFI-AVION") == "Airplane"

    def test_unknown_returns_none(self):
        assert _decode_clase("DESCONOCIDO") is None

    def test_empty_returns_none(self):
        assert _decode_clase("") is None


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "340123", "EC-ABC")
        assert record["icao_hex"] == "340123"
        assert record["registration"] == "EC-ABC"
        assert record["source"] == "es-aesa"
        assert record["aircraft"]["manufacturer"] == "Airbus"
        assert record["aircraft"]["model"] == "A320"
        assert record["aircraft"]["serial_number"] == "1234"
        assert record["aircraft"]["manufactured_date"] == "2019-01-01"
        assert record["aircraft"]["powerplant"]["manufacturer"] == "CFM"
        assert record["aircraft"]["powerplant"]["model"] == "CFM56-5B"
        assert record["aircraft"]["powerplant"]["count"] == 2
        assert record["aircraft"]["type"] == "Airplane"

    def test_no_registrant_key(self):
        row = _make_row()
        record = _build_record(row, "340123", "EC-ABC")
        assert "registrant" not in record

    def test_fecha_not_stored(self):
        row = _make_row(fecha="15/03/2021")
        record = _build_record(row, "340123", "EC-ABC")
        assert "Fecha matric." not in record
        assert "date_of_registration" not in record

    def test_serial_no_disponible_omitted(self):
        row = _make_row(serial="NO\nDISPONIBLE")
        record = _build_record(row, "340123", "EC-ABC")
        assert "serial_number" not in record.get("aircraft", {})

    def test_year_1900_omits_manufactured_date(self):
        row = _make_row(ano="1900")
        record = _build_record(row, "340123", "EC-ABC")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_year_zero_omits_manufactured_date(self):
        row = _make_row(ano="")
        record = _build_record(row, "340123", "EC-ABC")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_powerplant_count_is_integer(self):
        row = _make_row(num_mot="4")
        record = _build_record(row, "340123", "EC-ABC")
        assert record["aircraft"]["powerplant"]["count"] == 4
        assert isinstance(record["aircraft"]["powerplant"]["count"], int)

    def test_invalid_powerplant_count_omitted(self):
        row = _make_row(num_mot="N/A")
        record = _build_record(row, "340123", "EC-ABC")
        assert "count" not in record.get("aircraft", {}).get("powerplant", {})

    def test_unknown_clase_omits_type(self):
        row = _make_row(clase="DESCONOCIDO")
        record = _build_record(row, "340123", "EC-ABC")
        assert "type" not in record.get("aircraft", {})

    def test_empty_manufacturer_omits_field(self):
        row = _make_row(fabricante="")
        record = _build_record(row, "340123", "EC-ABC")
        assert "manufacturer" not in record.get("aircraft", {})

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(fabricante="", modelo="", serial="", ano="", marca_motor="", modelo_motor="", num_mot="", clase="")
        record = _build_record(row, "340123", "EC-ABC")
        assert "aircraft" not in record

    def test_planeador_clase(self):
        row = _make_row(clase="PLANEADOR/MOTOPL\nANEADOR")
        record = _build_record(row, "340123", "EC-ABC")
        assert record["aircraft"]["type"] == "Glider"


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("ECABC") == "ECABC"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("EC-ABC")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="340123", registration="EC-ABC")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_record_not_written_when_not_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_empty_matricula_skipped(self):
        rows = [_make_row(matricula="")]
        r = _make_redis_with_search()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0
        r.ft.return_value.search.assert_not_called()

    def test_writes_to_detail_key(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="340123", registration="EC-ABC")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:340123"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="340123", registration="EC-ABC")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "es-aesa"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="340123", registration="EC-ABC")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:340123", REDIS_TTL)


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
        with patch("es_aesa_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("es_aesa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 241, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("es_aesa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 241, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "241"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("es_aesa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/es-aesa"
