"""
Tests for the Brazil ANAC data runner.

Covers:
- Active record filter (_is_active)
- Registration formatter (_format_registration)
- CDCLS decoder (_decode_cdcls)
- Year parser (_parse_year)
- Seats parser (_parse_seats)
- PROPRIETARIOSJSON parser (_parse_proprietarios)
- Record builder (_build_record)
- Redis write logic (write_to_redis) — mocked
- MQTT completion stats — mocked
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Module import helper
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
_RUNNER_DIR = os.path.dirname(_HERE)
_REPO_ROOT = os.path.abspath(os.path.join(_RUNNER_DIR, "..", ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "br_anac_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["br_anac_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_is_active = _mod._is_active
_format_registration = _mod._format_registration
_decode_cdcls = _mod._decode_cdcls
_parse_year = _mod._parse_year
_parse_seats = _mod._parse_seats
_parse_proprietarios = _mod._parse_proprietarios
_build_record = _mod._build_record
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    marca="PPAJH",
    dtcanc=None,
    cdinterdicao="N",
    nmfabricante="CESSNA",
    dsmodelo="C172",
    cdtipoicao="C172",
    nrserie="12345",
    nrassentos="4",
    nranofabricacao="1980",
    cdcls="L1P",
    proprietariosjson=None,
) -> dict:
    return {
        "MARCA": marca,
        "DTCANC": dtcanc,
        "CDINTERDICAO": cdinterdicao,
        "NMFABRICANTE": nmfabricante,
        "DSMODELO": dsmodelo,
        "CDTIPOICAO": cdtipoicao,
        "NRSERIE": nrserie,
        "NRASSENTOS": nrassentos,
        "NRANOFABRICACAO": nranofabricacao,
        "CDCLS": cdcls,
        "PROPRIETARIOSJSON": proprietariosjson,
    }


# ---------------------------------------------------------------------------
# Tests: _is_active
# ---------------------------------------------------------------------------

class TestIsActive:
    def test_normal_record_is_active(self):
        assert _is_active(_make_row(cdinterdicao="N")) is True

    def test_dtcanc_set_is_inactive(self):
        assert _is_active(_make_row(dtcanc="01/01/2020")) is False

    def test_cdinterdicao_r_is_inactive(self):
        assert _is_active(_make_row(cdinterdicao="R")) is False

    def test_cdinterdicao_r_with_digits_is_inactive(self):
        assert _is_active(_make_row(cdinterdicao="R1")) is False

    def test_cdinterdicao_m_is_inactive(self):
        assert _is_active(_make_row(cdinterdicao="M")) is False

    def test_cdinterdicao_m_with_digits_is_inactive(self):
        assert _is_active(_make_row(cdinterdicao="M1")) is False

    def test_ultralight_u_is_active(self):
        assert _is_active(_make_row(cdinterdicao="U")) is True

    def test_experimental_z_is_active(self):
        assert _is_active(_make_row(cdinterdicao="Z")) is True

    def test_certificate_c_is_active(self):
        assert _is_active(_make_row(cdinterdicao="C1")) is True

    def test_suspended_s_is_active(self):
        assert _is_active(_make_row(cdinterdicao="S1")) is True

    def test_grounded_x_is_active(self):
        assert _is_active(_make_row(cdinterdicao="X")) is True

    def test_null_cdinterdicao_is_active(self):
        assert _is_active(_make_row(cdinterdicao=None)) is True

    def test_null_dtcanc_is_active(self):
        assert _is_active(_make_row(dtcanc=None)) is True


# ---------------------------------------------------------------------------
# Tests: _format_registration
# ---------------------------------------------------------------------------

class TestFormatRegistration:
    def test_inserts_hyphen_after_position_2(self):
        assert _format_registration("PPAJH") == "PP-AJH"

    def test_pr_prefix(self):
        assert _format_registration("PRXYZ") == "PR-XYZ"

    def test_pt_prefix(self):
        assert _format_registration("PTABC") == "PT-ABC"

    def test_ps_prefix(self):
        assert _format_registration("PSDEF") == "PS-DEF"

    def test_pu_prefix(self):
        assert _format_registration("PUGHI") == "PU-GHI"

    def test_strips_whitespace(self):
        assert _format_registration("  PPAJH  ") == "PP-AJH"

    def test_lowercased_input(self):
        assert _format_registration("ppajh") == "PP-AJH"

    def test_too_short_returns_none(self):
        assert _format_registration("PP") is None

    def test_empty_returns_none(self):
        assert _format_registration("") is None


# ---------------------------------------------------------------------------
# Tests: _decode_cdcls
# ---------------------------------------------------------------------------

class TestDecodeCdcls:
    def test_l1p_airplane_piston(self):
        atype, count, etype = _decode_cdcls("L1P")
        assert atype == "Airplane"
        assert count == 1
        assert etype == "Piston"

    def test_l2j_airplane_turbojet(self):
        atype, count, etype = _decode_cdcls("L2J")
        assert atype == "Airplane"
        assert count == 2
        assert etype == "Turbo-jet"

    def test_l2t_airplane_turboprop(self):
        atype, count, etype = _decode_cdcls("L2T")
        assert atype == "Airplane"
        assert count == 2
        assert etype == "Turbo-prop"

    def test_h1t_helicopter_turboshaft(self):
        atype, count, etype = _decode_cdcls("H1T")
        assert atype == "Helicopter"
        assert count == 1
        assert etype == "Turbo-shaft"

    def test_h2t_helicopter_turboshaft(self):
        atype, count, etype = _decode_cdcls("H2T")
        assert atype == "Helicopter"
        assert count == 2
        assert etype == "Turbo-shaft"

    def test_a1t_amphibian_turboprop_not_turboshaft(self):
        atype, count, etype = _decode_cdcls("A1T")
        assert atype == "Amphibian"
        assert etype == "Turbo-prop"

    def test_g1p_gyroplane_piston(self):
        atype, count, etype = _decode_cdcls("G1P")
        assert atype == "Gyroplane"
        assert count == 1
        assert etype == "Piston"

    def test_s1p_seaplane_piston(self):
        atype, count, etype = _decode_cdcls("S1P")
        assert atype == "Seaplane"
        assert count == 1
        assert etype == "Piston"

    def test_l00_glider_no_count_no_type(self):
        atype, count, etype = _decode_cdcls("L00")
        assert atype == "Airplane"
        assert count is None
        assert etype is None

    def test_rpa_drone(self):
        atype, count, etype = _decode_cdcls("RPA")
        assert atype == "Drone"
        assert count is None
        assert etype is None

    def test_l1e_electric(self):
        atype, count, etype = _decode_cdcls("L1E")
        assert etype == "Electric"

    def test_l4j_four_engines(self):
        atype, count, etype = _decode_cdcls("L4J")
        assert count == 4

    def test_null_returns_all_none(self):
        assert _decode_cdcls(None) == (None, None, None)

    def test_empty_returns_all_none(self):
        assert _decode_cdcls("") == (None, None, None)

    def test_case_insensitive(self):
        atype, count, etype = _decode_cdcls("l1p")
        assert atype == "Airplane"
        assert etype == "Piston"


# ---------------------------------------------------------------------------
# Tests: _parse_year
# ---------------------------------------------------------------------------

class TestParseYear:
    def test_valid_year(self):
        assert _parse_year("1980") == "1980-01-01T00:00:00Z"

    def test_none_returns_none(self):
        assert _parse_year(None) is None

    def test_empty_returns_none(self):
        assert _parse_year("") is None

    def test_non_numeric_returns_none(self):
        assert _parse_year("UNKN") is None

    def test_partial_year_returns_none(self):
        assert _parse_year("198") is None


# ---------------------------------------------------------------------------
# Tests: _parse_seats
# ---------------------------------------------------------------------------

class TestParseSeats:
    def test_valid_integer(self):
        assert _parse_seats("4") == 4

    def test_none_returns_none(self):
        assert _parse_seats(None) is None

    def test_empty_returns_none(self):
        assert _parse_seats("") is None

    def test_non_numeric_returns_none(self):
        assert _parse_seats("N/A") is None


# ---------------------------------------------------------------------------
# Tests: _parse_proprietarios
# ---------------------------------------------------------------------------

class TestParseProprietarios:
    def _encode(self, owners: list) -> str:
        raw = json.dumps(owners)
        return raw.replace('"', '/""')

    def test_returns_first_nome(self):
        raw = self._encode([{"NOME": "João Silva", "DOCUMENTO": "123"}])
        assert _parse_proprietarios(raw) == "João Silva"

    def test_indisponivel_returns_none(self):
        raw = self._encode([{"NOME": "Indisponível"}])
        assert _parse_proprietarios(raw) is None

    def test_empty_nome_returns_none(self):
        raw = self._encode([{"NOME": ""}])
        assert _parse_proprietarios(raw) is None

    def test_empty_list_returns_none(self):
        raw = self._encode([])
        assert _parse_proprietarios(raw) is None

    def test_none_input_returns_none(self):
        assert _parse_proprietarios(None) is None

    def test_invalid_json_returns_none(self):
        assert _parse_proprietarios("not json") is None

    def test_uses_first_entry_only(self):
        raw = self._encode([
            {"NOME": "First Owner"},
            {"NOME": "Second Owner"},
        ])
        assert _parse_proprietarios(raw) == "First Owner"

    def test_strips_whitespace(self):
        raw = self._encode([{"NOME": "  João Silva  "}])
        assert _parse_proprietarios(raw) == "João Silva"


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_top_level_fields(self):
        record = _build_record("E491A0", "PP-AJH", _make_row())
        assert record["icao_hex"] == "E491A0"
        assert record["registration"] == "PP-AJH"

    def test_aircraft_manufacturer(self):
        record = _build_record("E491A0", "PP-AJH", _make_row(nmfabricante="CESSNA"))
        assert record["aircraft"]["manufacturer"] == "CESSNA"

    def test_aircraft_model(self):
        record = _build_record("E491A0", "PP-AJH", _make_row(dsmodelo="C172"))
        assert record["aircraft"]["model"] == "C172"

    def test_aircraft_type_designator(self):
        record = _build_record("E491A0", "PP-AJH", _make_row(cdtipoicao="C172"))
        assert record["aircraft"]["type_designator"] == "C172"

    def test_aircraft_serial_number(self):
        record = _build_record("E491A0", "PP-AJH", _make_row(nrserie="12345"))
        assert record["aircraft"]["serial_number"] == "12345"

    def test_aircraft_seats(self):
        record = _build_record("E491A0", "PP-AJH", _make_row(nrassentos="4"))
        assert record["aircraft"]["seats"] == 4

    def test_aircraft_manufactured_date(self):
        record = _build_record("E491A0", "PP-AJH", _make_row(nranofabricacao="1980"))
        assert record["aircraft"]["manufactured_date"] == "1980-01-01T00:00:00Z"

    def test_aircraft_type_from_cdcls(self):
        record = _build_record("E491A0", "PP-AJH", _make_row(cdcls="L1P"))
        assert record["aircraft"]["type"] == "Airplane"

    def test_powerplant_count_from_cdcls(self):
        record = _build_record("E491A0", "PP-AJH", _make_row(cdcls="L2P"))
        assert record["powerplant"]["count"] == 2

    def test_powerplant_type_from_cdcls(self):
        record = _build_record("E491A0", "PP-AJH", _make_row(cdcls="H1T"))
        assert record["powerplant"]["type"] == "Turbo-shaft"

    def test_no_powerplant_for_glider(self):
        record = _build_record("E491A0", "PP-AJH", _make_row(cdcls="L00"))
        assert "powerplant" not in record

    def test_registrant_from_proprietariosjson(self):
        raw = json.dumps([{"NOME": "João Silva"}]).replace('"', '/""')
        record = _build_record("E491A0", "PP-AJH", _make_row(proprietariosjson=raw))
        assert record["registrant"]["names"] == ["João Silva"]

    def test_no_registrant_when_nome_unavailable(self):
        record = _build_record("E491A0", "PP-AJH", _make_row(proprietariosjson=None))
        assert "registrant" not in record

    def test_null_cdcls_omits_type_and_powerplant(self):
        record = _build_record("E491A0", "PP-AJH", _make_row(cdcls=None))
        assert "type" not in record.get("aircraft", {})
        assert "powerplant" not in record


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def _make_redis(self, simple_record: dict = None) -> MagicMock:
        r = MagicMock()
        r_json = MagicMock()
        r.json.return_value = r_json
        r_json.get.return_value = simple_record
        return r

    def _patch_reg_map(self, reg_map: dict):
        return patch("br_anac_main._build_registration_map", return_value=reg_map)

    def test_writes_active_record(self):
        r = self._make_redis()
        rows = [_make_row()]
        with self._patch_reg_map({"PP-AJH": "E491A0"}):
            count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_writes_to_correct_key(self):
        r = self._make_redis()
        rows = [_make_row()]
        with self._patch_reg_map({"PP-AJH": "E491A0"}):
            write_to_redis(rows, r, REDIS_TTL)
        key_arg = r.json.return_value.set.call_args.args[0]
        assert key_arg == "aircraft:detail:E491A0"

    def test_writes_source_field(self):
        r = self._make_redis()
        rows = [_make_row()]
        with self._patch_reg_map({"PP-AJH": "E491A0"}):
            write_to_redis(rows, r, REDIS_TTL)
        written = r.json.return_value.set.call_args.args[2]
        assert written["source"] == "br-anac"

    def test_filters_inactive_records(self):
        r = self._make_redis()
        rows = [_make_row(dtcanc="01/01/2020")]
        with self._patch_reg_map({}):
            count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0
        r.json.return_value.set.assert_not_called()

    def test_skips_type_mismatch(self):
        simple = {"type_designator": "B737", "manufacturer_model": "BOEING 737"}
        r = self._make_redis(simple_record=simple)
        rows = [_make_row(dsmodelo="CESSNA C172", cdtipoicao="C172")]
        with self._patch_reg_map({"PP-AJH": "E491A0"}):
            count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_accepts_type_match(self):
        simple = {"type_designator": "C172", "manufacturer_model": "CESSNA 172"}
        r = self._make_redis(simple_record=simple)
        rows = [_make_row(dsmodelo="C172 SKYHAWK")]
        with self._patch_reg_map({"PP-AJH": "E491A0"}):
            count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_no_simple_record_still_writes(self):
        r = self._make_redis(simple_record=None)
        rows = [_make_row()]
        with self._patch_reg_map({"PP-AJH": "E491A0"}):
            count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_not_in_redis_skipped(self):
        r = self._make_redis()
        rows = [_make_row()]
        with self._patch_reg_map({}):
            count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0
        r.json.return_value.set.assert_not_called()

    def test_fire_and_forget_no_mget(self):
        r = self._make_redis()
        rows = [_make_row()]
        with self._patch_reg_map({"PP-AJH": "E491A0"}):
            write_to_redis(rows, r, REDIS_TTL)
        r.json.return_value.mget.assert_not_called()

    def test_expire_correct_ttl(self):
        r = self._make_redis()
        rows = [_make_row()]
        with self._patch_reg_map({"PP-AJH": "E491A0"}):
            write_to_redis(rows, r, REDIS_TTL)
        r.expire.assert_called_once_with("aircraft:detail:E491A0", REDIS_TTL)


# ---------------------------------------------------------------------------
# Tests: MQTT completion stats (mocked)
# ---------------------------------------------------------------------------

class TestMqttCompletionStats:
    def _setup_mock_client(self):
        mock_client = MagicMock()

        def fake_connect(host, port, keepalive):
            mock_client.on_connect(mock_client, None, None, 0, None)

        mock_client.connect.side_effect = fake_connect
        return mock_client

    def test_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("br_anac_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("br_anac_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 99, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "99"

    def test_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("br_anac_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_no_mqtt_config_skips(self):
        cfg = {}
        mc = self._setup_mock_client()
        with patch("br_anac_main.mqtt.Client", return_value=mc):
            publish_completion_stats(cfg, 0, "success")
        mc.connect.assert_not_called()

    def test_ha_autodiscovery_three_sensors(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("br_anac_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 100, "success")
        ha_topics = [
            c.args[0] for c in mc.publish.call_args_list
            if c.args[0].startswith("homeassistant/")
        ]
        assert len(ha_topics) == 3
        assert "homeassistant/sensor/SkyFollower_runner_br_anac_records_imported/config" in ha_topics
