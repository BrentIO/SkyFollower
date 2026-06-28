"""
Tests for the US FAA data runner.

Covers:
- Decode helpers (engine type, registrant type)
- CSV parsing and SQLite staging
- Redis record construction
- Redis key construction
- Redis write logic (mocked)
- MQTT completion stats (mocked)
"""

from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Module import helper
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
_RUNNER_DIR = os.path.dirname(_HERE)          # data-runners/us-faa/
_REPO_ROOT = os.path.abspath(os.path.join(_RUNNER_DIR, "..", ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "us_faa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["us_faa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_decode_engine_type = _mod._decode_engine_type
_decode_registrant_type = _mod._decode_registrant_type
_decode_aircraft_type = _mod._decode_aircraft_type
_decode_aircraft_class = _mod._decode_aircraft_class
build_aircraft_record = _mod.build_aircraft_record
stage_data = _mod.stage_data
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT


# ---------------------------------------------------------------------------
# Sample CSV fixtures
# ---------------------------------------------------------------------------

# acftref.txt: CODE, MFG, MODEL, TYPE-ACFT, TYPE-ENG, CLASS, RULES, NO-ENG, NO-SEATS, AC-WEIGHT, SPEED
_ACFTREF_CSV = (
    "CODE,MFG,MODEL,TYPE-ACFT,TYPE-ENG,CLASS,RULES,NO-ENG,NO-SEATS,AC-WEIGHT,SPEED\n"
    "0060V,CESSNA,172,4,1,1,0,1,4,CLASS 1,105\n"
    "2071M,BOEING,737-800,5,5,1,0,2,189,CLASS 3,465\n"
).encode()

# engine.txt: CODE, MFR, MODEL, TYPE, HORSEPOWER, THRUST
_ENGINE_CSV = (
    "CODE,MFR,MODEL,TYPE,HORSEPOWER,THRUST,\n"
    "00001,LYCOMING,O-320-D2J,1,00160,000000,\n"       # piston, 160 hp
    "00002,PRATT & WHITNEY,JT8D-217,5,00000,015500,\n"  # turbo-fan, 15500 lbf thrust
).encode()

# master.txt: 34 columns (0-33); key columns are 0, 1, 2, 4, 5, 6, 24-28, 33
# Row layout: N-NUMBER(0), SERIAL(1), MFR-CODE(2), ENG-CODE(3), YEAR(4), REG-TYPE(5),
#   NAME(6), STREET(7), STREET2(8), CITY(9), STATE(10), ZIP(11), REGION(12), COUNTY(13),
#   COUNTRY(14), LAST-ACTION(15), CERT-ISSUE(16), CERT(17), AC-TYPE(18), ENG-TYPE(19),
#   STATUS(20), MODE-S-OCT(21), FRACT(22), AIRWTH(23), OTHER1(24), OTHER2(25),
#   OTHER3(26), OTHER4(27), OTHER5(28), EXPIRE(29), UNIQUE-ID(30), KIT-MFR(31),
#   KIT-MDL(32), MODE-S-HEX(33)

def _master_row(*fields):
    """Return a 34-element CSV row as bytes (header not included)."""
    row = list(fields) + [""] * (34 - len(fields))
    return ",".join(str(f) for f in row[:34]) + "\n"


_MASTER_HEADER = (
    "N-NUMBER,SERIAL NUMBER,MFR MDL CODE,ENG MFR MDL,YEAR MFR,TYPE REGISTRANT,"
    "NAME,STREET,STREET2,CITY,STATE,ZIP CODE,REGION,COUNTY,COUNTRY,"
    "LAST ACTION DATE,CERT ISSUE DATE,CERTIFICATION,TYPE AIRCRAFT,TYPE ENGINE,"
    "STATUS CODE,MODE S CODE,FRACT OWNER,AIR WORTH DATE,"
    "OTHER NAMES(1),OTHER NAMES(2),OTHER NAMES(3),OTHER NAMES(4),OTHER NAMES(5),"
    "EXPIRATION DATE,UNIQUE ID,KIT MFR,KIT MODEL,MODE S CODE HEX\n"
)

# N659DL  — Delta 737, Corporation
_ROW_N659DL = _master_row(
    "659DL", "28014", "2071M", "00002", "1999", "3",
    "DELTA AIR LINES INC", "123 MAIN ST", "", "ATLANTA", "GA", "30320",
    "7", "", "US", "20230101", "19990101", "1TN", "5", "5",
    "V", "52345670", "N", "19990101",
    "", "", "", "", "",
    "20261231", "12345678", "", "", "A8AE7F",
)

# N12345  — Individual Cessna
_ROW_N12345 = _master_row(
    "12345", "1234", "0060V", "00001", "2005", "1",
    "JOHN DOE", "456 OAK AVE", "", "DALLAS", "TX", "75201",
    "3", "", "US", "20230101", "20050101", "1N", "4", "1",
    "V", "45678901", "N", "20050101",
    "", "", "", "", "",
    "20261231", "87654321", "", "", "AA0001",
)

# No matching aircraft ref
_ROW_NO_ACFT = _master_row(
    "99999", "9999", "NOTYPE", "", "2010", "2",
    "SOME PARTNERSHIP", "", "", "", "", "",
    "1", "", "US", "", "", "", "", "",
    "V", "", "", "",
    "", "", "", "", "",
    "", "", "", "", "AB1234",
)

# Invalid ICAO hex (too short) — should be skipped
_ROW_BAD_HEX = _master_row(
    "11111", "", "", "", "", "1",
    "SKIP ME", "", "", "", "", "",
    "", "", "", "", "", "", "", "",
    "", "", "", "",
    "", "", "", "", "",
    "", "", "", "", "BAD",
)

_MASTER_CSV = (
    _MASTER_HEADER
    + _ROW_N659DL
    + _ROW_N12345
    + _ROW_NO_ACFT
    + _ROW_BAD_HEX
).encode()


def _make_files(**overrides) -> dict[str, bytes]:
    base = {
        "acftref.txt": _ACFTREF_CSV,
        "engine.txt": _ENGINE_CSV,
        "master.txt": _MASTER_CSV,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Tests: _decode_aircraft_type
# ---------------------------------------------------------------------------

class TestDecodeAircraftType:
    def test_fixed_wing_single(self):
        assert _decode_aircraft_type("4") == "Airplane"

    def test_fixed_wing_multi(self):
        assert _decode_aircraft_type("5") == "Airplane"

    def test_rotorcraft(self):
        assert _decode_aircraft_type("6") == "Rotorcraft"

    def test_glider(self):
        assert _decode_aircraft_type("1") == "Glider"

    def test_unknown_returns_none(self):
        assert _decode_aircraft_type("99") is None


# ---------------------------------------------------------------------------
# Tests: _decode_aircraft_class
# ---------------------------------------------------------------------------

class TestDecodeAircraftClass:
    def test_land(self):
        assert _decode_aircraft_class("1") == "Land"

    def test_sea(self):
        assert _decode_aircraft_class("2") == "Sea"

    def test_amphibian(self):
        assert _decode_aircraft_class("3") == "Amphibian"

    def test_unknown_returns_none(self):
        assert _decode_aircraft_class("9") is None


# ---------------------------------------------------------------------------
# Tests: _decode_engine_type
# ---------------------------------------------------------------------------

class TestDecodeEngineType:
    def test_piston(self):
        assert _decode_engine_type("1") == "Piston"

    def test_turbo_fan(self):
        assert _decode_engine_type("5") == "Turbo-fan"

    def test_electric(self):
        assert _decode_engine_type("10") == "Electric"

    def test_none_code(self):
        assert _decode_engine_type("0") == "None"

    def test_unknown_code_returns_none(self):
        assert _decode_engine_type("99") is None

    def test_strips_whitespace(self):
        assert _decode_engine_type(" 1 ") == "Piston"


# ---------------------------------------------------------------------------
# Tests: _decode_registrant_type
# ---------------------------------------------------------------------------

class TestDecodeRegistrantType:
    def test_individual(self):
        assert _decode_registrant_type("1") == "Individual"

    def test_corporation(self):
        assert _decode_registrant_type("3") == "Corporation"

    def test_government(self):
        assert _decode_registrant_type("5") == "Government"

    def test_llc(self):
        assert _decode_registrant_type("7") == "LLC"

    def test_empty_is_none(self):
        assert _decode_registrant_type("") == "None"

    def test_unknown_code(self):
        assert _decode_registrant_type("X") == "Unknown"


# ---------------------------------------------------------------------------
# Tests: stage_data
# ---------------------------------------------------------------------------

class TestStageData:
    def test_aircraft_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM aircraft")
            assert cur.fetchone()[0] == 2
            conn.close()

    def test_aircraft_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute(
                "SELECT aircraft_type, manufacturer, model, seats, category, engine_type, engine_count "
                "FROM aircraft WHERE code = '2071M'"
            )
            row = cur.fetchone()
            assert row[0] == "Airplane"
            assert row[1] == "BOEING"
            assert row[2] == "737-800"
            assert row[3] == 189
            assert row[4] == "Land"
            assert row[5] == "Turbo-fan"
            assert row[6] == 2
            conn.close()

    def test_registration_count(self):
        # Bad hex row should be filtered out
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM registrations")
            assert cur.fetchone()[0] == 3
            conn.close()

    def test_engine_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM engines")
            assert cur.fetchone()[0] == 2
            conn.close()

    def test_engine_fields_piston(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT manufacturer, model, engine_type, horsepower, thrust FROM engines WHERE code = '00001'")
            row = cur.fetchone()
            assert row[0] == "LYCOMING"
            assert row[1] == "O-320-D2J"
            assert row[2] == "Piston"
            assert row[3] == 160
            assert row[4] is None
            conn.close()

    def test_engine_fields_turbofan(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT manufacturer, model, engine_type, horsepower, thrust FROM engines WHERE code = '00002'")
            row = cur.fetchone()
            assert row[0] == "PRATT & WHITNEY"
            assert row[1] == "JT8D-217"
            assert row[2] == "Turbo-fan"
            assert row[3] is None
            assert row[4] == 15500
            conn.close()

    def test_registration_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute(
                "SELECT icao_hex, registration, serial_number, code_engine, manufactured_year, "
                "name_1, street_1, city, administrative_area, postal_code, country, registrant_type "
                "FROM registrations WHERE icao_hex = 'A8AE7F'"
            )
            row = cur.fetchone()
            assert row["icao_hex"] == "A8AE7F"
            assert row["registration"] == "N659DL"
            assert row["serial_number"] == "28014"
            assert row["code_engine"] == "00002"
            assert row["manufactured_year"] == "1999"
            assert row["name_1"] == "DELTA AIR LINES INC"
            assert row["street_1"] == "123 MAIN ST"
            assert row["city"] == "ATLANTA"
            assert row["administrative_area"] == "GA"
            assert row["postal_code"] == "30320"
            assert row["country"] == "US"
            assert row["registrant_type"] == "Corporation"
            conn.close()

    def test_missing_engine_txt_skipped_gracefully(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data({"acftref.txt": _ACFTREF_CSV, "master.txt": _MASTER_CSV},
                              os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM engines")
            assert cur.fetchone()[0] == 0
            conn.close()

    def test_icao_hex_uppercased(self):
        row = _master_row(
            "LOWER1", "", "", "", "", "1",
            "TEST", "", "", "", "", "",
            "", "", "", "", "", "", "", "",
            "V", "", "", "",
            "", "", "", "", "",
            "", "", "", "", "a8ae7f",
        )
        master = (_MASTER_HEADER + row).encode()
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data({"acftref.txt": _ACFTREF_CSV, "master.txt": master},
                              os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT icao_hex FROM registrations")
            assert cur.fetchone()[0] == "A8AE7F"
            conn.close()

    def test_n_prefix_prepended(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT registration FROM registrations WHERE icao_hex = 'AA0001'")
            assert cur.fetchone()[0] == "N12345"
            conn.close()

    def test_missing_acftref_skipped_gracefully(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data({"master.txt": _MASTER_CSV},
                              os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM aircraft")
            assert cur.fetchone()[0] == 0
            conn.close()

    def test_missing_master_skipped_gracefully(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data({"acftref.txt": _ACFTREF_CSV},
                              os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM registrations")
            assert cur.fetchone()[0] == 0
            conn.close()

    def test_bad_hex_row_skipped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT icao_hex FROM registrations")
            hexes = {r[0] for r in cur.fetchall()}
            assert "BAD" not in hexes
            conn.close()


# ---------------------------------------------------------------------------
# Tests: build_aircraft_record
# ---------------------------------------------------------------------------

class TestBuildAircraftRecord:
    def _fetch_rows(self, icao_hex: str):
        """Return (reg_row, acft_row, eng_row) for the given ICAO hex."""
        conn = stage_data(_make_files(), tempfile.mktemp(suffix=".db"))
        cur = conn.cursor()
        cur.execute(
            """
            SELECT r.icao_hex, r.registration, r.serial_number, r.manufactured_year,
                   r.registrant_type, r.code_engine,
                   r.name_1, r.name_2, r.name_3, r.name_4, r.name_5, r.name_6,
                   r.street_1, r.street_2, r.city, r.administrative_area,
                   r.postal_code, r.country,
                   a.aircraft_type, a.manufacturer, a.model, a.seats,
                   a.category, a.engine_type, a.engine_count
            FROM registrations r
            LEFT JOIN aircraft a ON r.code_aircraft = a.code
            WHERE r.icao_hex = ?
            """,
            (icao_hex,),
        )
        row = cur.fetchone()
        acft_row = row if row and row["manufacturer"] is not None else None

        eng_cur = conn.cursor()
        eng_cur.execute(
            "SELECT code, manufacturer, model, engine_type, horsepower, thrust FROM engines WHERE code = ?",
            (row["code_engine"],) if row and row["code_engine"] else ("",),
        )
        eng_row = eng_cur.fetchone()
        conn.close()
        return row, acft_row, eng_row

    def test_top_level_fields(self):
        row, acft_row, eng_row = self._fetch_rows("A8AE7F")
        record = build_aircraft_record(row, acft_row, eng_row)
        assert record["icao_hex"] == "A8AE7F"
        assert record["registration"] == "N659DL"
        assert "source" not in record
        assert "military" not in record

    def test_registrant_names(self):
        row, acft_row, eng_row = self._fetch_rows("A8AE7F")
        record = build_aircraft_record(row, acft_row, eng_row)
        assert record["registrant"]["names"] == ["DELTA AIR LINES INC"]

    def test_registrant_address(self):
        row, acft_row, eng_row = self._fetch_rows("A8AE7F")
        record = build_aircraft_record(row, acft_row, eng_row)
        reg = record["registrant"]
        assert reg["street"] == ["123 MAIN ST"]
        assert reg["city"] == "ATLANTA"
        assert reg["administrative_area"] == "GA"
        assert reg["postal_code"] == "30320"
        assert reg["country"] == "US"

    def test_registrant_type_corporation(self):
        row, acft_row, eng_row = self._fetch_rows("A8AE7F")
        record = build_aircraft_record(row, acft_row, eng_row)
        assert record["registrant"]["type"] == "Corporation"

    def test_registrant_type_individual(self):
        row, acft_row, eng_row = self._fetch_rows("AA0001")
        record = build_aircraft_record(row, acft_row, eng_row)
        assert record["registrant"]["type"] == "Individual"

    def test_aircraft_fields(self):
        row, acft_row, eng_row = self._fetch_rows("A8AE7F")
        record = build_aircraft_record(row, acft_row, eng_row)
        ac = record["aircraft"]
        assert ac["type"] == "Airplane"
        assert ac["manufacturer"] == "BOEING"
        assert ac["model"] == "737-800"
        assert ac["seats"] == 189
        assert ac["category"] == "Land"
        assert ac["serial_number"] == "28014"
        assert ac["manufactured_date"] == "1999-01-01T00:00:00Z"

    def test_aircraft_mictronics_fields_absent(self):
        """Fields owned by mictronics must be omitted entirely from the FAA record."""
        row, acft_row, eng_row = self._fetch_rows("A8AE7F")
        record = build_aircraft_record(row, acft_row, eng_row)
        ac = record["aircraft"]
        assert "type_designator" not in ac
        assert "manufacturer_model" not in ac
        assert "wake_turbulence_category" not in ac

    def test_powerplant_turbofan(self):
        row, acft_row, eng_row = self._fetch_rows("A8AE7F")
        record = build_aircraft_record(row, acft_row, eng_row)
        pp = record["powerplant"]
        assert pp["count"] == 2
        assert pp["type"] == "Turbo-fan"
        assert pp["manufacturer"] == "PRATT & WHITNEY"
        assert pp["model"] == "JT8D-217"
        assert pp["power_type"] == "Thrust"
        assert pp["power_value"] == 15500

    def test_powerplant_piston(self):
        row, acft_row, eng_row = self._fetch_rows("AA0001")
        record = build_aircraft_record(row, acft_row, eng_row)
        pp = record["powerplant"]
        assert pp["type"] == "Piston"
        assert pp["manufacturer"] == "LYCOMING"
        assert pp["model"] == "O-320-D2J"
        assert pp["power_type"] == "Horsepower"
        assert pp["power_value"] == 160

    def test_no_aircraft_ref_gives_null_aircraft(self):
        row, _, _ = self._fetch_rows("AB1234")
        record = build_aircraft_record(row, None, None)
        assert record["aircraft"] is None
        assert record["powerplant"] is None

    def test_manufactured_year_none_gives_null_date(self):
        row, acft_row, eng_row = self._fetch_rows("AB1234")
        record = build_aircraft_record(row, acft_row, eng_row)
        if record["aircraft"]:
            assert record["aircraft"]["manufactured_date"] is None


# ---------------------------------------------------------------------------
# Tests: Redis key construction
# ---------------------------------------------------------------------------

class TestRedisKeys:
    def test_aircraft_detail_key_format(self):
        from shared.redis_keys import aircraft_detail_key
        assert aircraft_detail_key("a8ae7f") == "aircraft:detail:A8AE7F"

    def test_aircraft_detail_search_index_name(self):
        from shared.redis_keys import AIRCRAFT_DETAIL_SEARCH_INDEX
        assert AIRCRAFT_DETAIL_SEARCH_INDEX == "idx:aircraft:detail"


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def _make_db(self) -> sqlite3.Connection:
        with tempfile.TemporaryDirectory() as tmpdir:
            return stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))

    def _mock_redis(self):
        r = MagicMock()
        r_json = MagicMock()
        r.json.return_value = r_json

        pipe = MagicMock()
        pipe_json = MagicMock()
        r.pipeline.return_value = pipe
        pipe.json.return_value = pipe_json
        pipe.execute.return_value = []
        return r, pipe, pipe_json

    def test_count_matches_staged_registrations(self):
        conn = self._make_db()
        r, _, _ = self._mock_redis()
        assert write_to_redis(conn, r, REDIS_TTL) == 3
        conn.close()

    def test_aircraft_detail_key_written(self):
        conn = self._make_db()
        r, _, pipe_json = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        keys = [c.args[0] for c in pipe_json.set.call_args_list]
        assert "aircraft:detail:A8AE7F" in keys
        conn.close()

    def test_json_set_uses_root_path(self):
        conn = self._make_db()
        r, _, pipe_json = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        for c in pipe_json.set.call_args_list:
            assert c.args[1] == "$"
        conn.close()

    def test_expire_called_with_correct_ttl(self):
        conn = self._make_db()
        r, pipe, _ = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        expire_ttls = [c.args[1] for c in pipe.expire.call_args_list]
        assert all(ttl == REDIS_TTL for ttl in expire_ttls)
        conn.close()

    def test_no_registration_key_written(self):
        conn = self._make_db()
        r, pipe, pipe_json = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        all_keys = (
            [c.args[0] for c in pipe.set.call_args_list]
            + [c.args[0] for c in pipe_json.set.call_args_list]
        )
        assert not any(k.startswith("registration:") for k in all_keys)
        conn.close()

    def test_record_written_as_dict(self):
        conn = self._make_db()
        r, _, pipe_json = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        calls = {c.args[0]: c.args[2] for c in pipe_json.set.call_args_list}
        record = calls["aircraft:detail:A8AE7F"]
        assert isinstance(record, dict)
        assert record["source"] == "us-faa"
        assert record["registration"] == "N659DL"
        conn.close()

    def test_does_not_read_before_write(self):
        """write_to_redis must fire-and-forget; no mget read before writing."""
        conn = self._make_db()
        r, _, pipe_json = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        r.json().mget.assert_not_called()
        conn.close()

    def test_source_field_set_on_all_records(self):
        """Every record written to Redis must carry source='us-faa'."""
        conn = self._make_db()
        r, _, pipe_json = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        records = [c.args[2] for c in pipe_json.set.call_args_list]
        assert all(rec.get("source") == "us-faa" for rec in records)
        conn.close()


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
        with patch("us_faa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("us_faa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 99, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "99"

    def test_publishes_last_run_at(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("us_faa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/last_run_at" in topics

    def test_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("us_faa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_no_mqtt_config_skips(self):
        cfg = {}
        mc = self._setup_mock_client()
        with patch("us_faa_main.mqtt.Client", return_value=mc):
            publish_completion_stats(cfg, 0, "success")
        mc.connect.assert_not_called()

    def test_ha_autodiscovery_three_sensors(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("us_faa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 100, "success")
        ha_topics = [
            c.args[0] for c in mc.publish.call_args_list
            if c.args[0].startswith("homeassistant/")
        ]
        assert len(ha_topics) == 3
        assert "homeassistant/sensor/SkyFollower_runner_us_faa_records_imported/config" in ha_topics
        assert "homeassistant/sensor/SkyFollower_runner_us_faa_last_run_at/config" in ha_topics
        assert "homeassistant/sensor/SkyFollower_runner_us_faa_last_run_status/config" in ha_topics
