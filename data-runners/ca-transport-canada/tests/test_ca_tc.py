"""
Tests for the Transport Canada data runner.

Covers:
- Parse/decode helpers (registration, ICAO hex, date, aircraft/engine category, country)
- CSV parsing and SQLite staging
- Redis record construction
- Active-record filtering (ineffective_date)
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
_RUNNER_DIR = os.path.dirname(_HERE)        # data-runners/ca-transport-canada/
_REPO_ROOT = os.path.abspath(os.path.join(_RUNNER_DIR, "..", ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "ca_tc_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ca_tc_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

from shared.url_reachability import assert_url_reachable

_parse_registration = _mod._parse_registration
_parse_icao_hex = _mod._parse_icao_hex
_parse_date_yyyymmdd = _mod._parse_date_yyyymmdd
_parse_int = _mod._parse_int
_parse_float = _mod._parse_float
_decode_aircraft_type = _mod._decode_aircraft_type
_decode_engine_category = _mod._decode_engine_category
_decode_country = _mod._decode_country
build_aircraft_record = _mod.build_aircraft_record
stage_data = _mod.stage_data
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT


# ---------------------------------------------------------------------------
# ICAO hex binary strings for test records
# format(int(binary, 2), '06X') must produce a 6-char hex
# ---------------------------------------------------------------------------

_BIN_C00001 = "110000000000000000000001"   # → C00001
_BIN_C00002 = "110000000000000000000010"   # → C00002
_BIN_C00003 = "110000000000000000000011"   # → C00003 (inactive)


# ---------------------------------------------------------------------------
# Sample CSV fixtures
# ---------------------------------------------------------------------------

def _acft_row(**fields) -> str:
    """Build a 47-column carscurr.txt data row from keyword {index: value} overrides."""
    row = [""] * 47
    for idx, val in fields.items():
        row[int(idx)] = str(val)
    return ",".join(row)


_ACFT_HEADER = (
    "COL0,COL1,COL2,COL3,COL4,COL5,COL6,COL7,COL8,COL9,"   # 0-9
    "COL10,COL11,COL12,COL13,COL14,COL15,COL16,COL17,COL18,COL19,"  # 10-19
    "COL20,COL21,COL22,COL23,COL24,COL25,COL26,COL27,COL28,COL29,"  # 20-29
    "COL30,COL31,COL32,COL33,COL34,COL35,COL36,COL37,COL38,COL39,"  # 30-39
    "COL40,COL41,COL42,COL43,COL44,COL45,COL46\n"  # 40-46
)

# Active — C-GABC: full data including col 46 TRIMMED_MARK
_ROW_C_GABC = _acft_row(**{
    "0":  "GABC",
    "4":  "172P",
    "5":  "SN001",
    "7":  "CESSNA AIRCRAFT CO",
    "10": "Aeroplane",
    "13": "LYCOMING",
    "15": "Piston",
    "17": "1",
    "18": "4",
    "19": "1100.0",
    "23": "",                        # active — no ineffective date
    "31": "2000/01/15",
    "42": _BIN_C00001,
    "46": "GABC",                    # TRIMMED_MARK (col 46)
})

# Active — CF-XYZ style (old 3-char mark); data dict says 'C-' prefix always
_ROW_C_XYZ = _acft_row(**{
    "0":  "XYZ",
    "4":  "737-200",
    "5":  "SN002",
    "7":  "THE BOEING COMPANY",
    "10": "Aeroplane",
    "13": "CFM INTERNATIONAL",
    "15": "Turbo Fan",               # raw TC string — decoded to "Turbo-fan"
    "17": "2",
    "18": "150",
    "31": "1995/06/01",
    "42": _BIN_C00002,
    "46": "XYZ",                     # TRIMMED_MARK
})

# Inactive — C-GINV (ineffective_date set → filtered from Redis)
_ROW_C_GINV = _acft_row(**{
    "0":  "GINV",
    "7":  "PIPER",
    "4":  "PA-28",
    "5":  "SN003",
    "23": "2020/03/01",
    "42": _BIN_C00003,
    "46": "GINV",
})

# Row with invalid binary ICAO — must be skipped
_ROW_BAD_ICAO = _acft_row(**{
    "0":  "BADC",
    "7":  "TEST",
    "42": "NOTBINARY",
    "46": "BADC",
})

_ACFT_CSV = (
    _ACFT_HEADER
    + _ROW_C_GABC + "\n"
    + _ROW_C_XYZ + "\n"
    + _ROW_C_GINV + "\n"
    + _ROW_BAD_ICAO + "\n"
).encode("iso-8859-1")


def _owner_row(**fields) -> str:
    """Build a 20-column carsownr.txt data row."""
    row = [""] * 20
    for idx, val in fields.items():
        row[int(idx)] = str(val)
    return ",".join(row)


_OWNER_HEADER = (
    "COL0,COL1,COL2,COL3,COL4,COL5,COL6,COL7,COL8,COL9,"
    "COL10,COL11,COL12,COL13,COL14,COL15,COL16,COL17,COL18,COL19\n"
)

# Active owner for C-GABC
_OWNER_C_GABC = _owner_row(**{
    "0":  "GABC",
    "1":  "JOHN DOE",
    "2":  "DOE AVIATION",
    "3":  "123 MAPLE ST",
    "5":  "VANCOUVER",
    "6":  "BC",
    "8":  "V6Z 1A1",
    "9":  "CANADA",
    "11": "Individual",
    "13": "A",
})

# Inactive owner for C-XYZ — should not appear as registrant
_OWNER_C_XYZ = _owner_row(**{
    "0":  "XYZ",
    "1":  "OLD CORP",
    "5":  "TORONTO",
    "9":  "CANADA",
    "11": "Entity",
    "13": "I",
})

_OWNER_CSV = (
    _OWNER_HEADER
    + _OWNER_C_GABC + "\n"
    + _OWNER_C_XYZ + "\n"
).encode("iso-8859-1")


def _make_files(**overrides) -> dict[str, bytes]:
    base = {
        "carscurr.txt": _ACFT_CSV,
        "carsownr.txt": _OWNER_CSV,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Tests: _parse_registration
# ---------------------------------------------------------------------------

class TestParseRegistration:
    def test_four_char_gives_c_prefix(self):
        assert _parse_registration("GABC") == "C-GABC"

    def test_three_char_gives_c_prefix(self):
        # Data dictionary: always 'C-' regardless of mark length
        assert _parse_registration("XYZ") == "C-XYZ"

    def test_strips_whitespace(self):
        assert _parse_registration("  GABC  ") == "C-GABC"

    def test_empty_returns_none(self):
        assert _parse_registration("") is None

    def test_whitespace_only_returns_none(self):
        assert _parse_registration("   ") is None


# ---------------------------------------------------------------------------
# Tests: _parse_icao_hex
# ---------------------------------------------------------------------------

class TestParseIcaoHex:
    def test_binary_converts_correctly(self):
        assert _parse_icao_hex(_BIN_C00001) == "C00001"

    def test_result_is_uppercase(self):
        result = _parse_icao_hex(_BIN_C00001)
        assert result == result.upper()

    def test_empty_returns_none(self):
        assert _parse_icao_hex("") is None

    def test_non_binary_returns_none(self):
        assert _parse_icao_hex("NOTBINARY") is None

    def test_wrong_length_returns_none(self):
        # 23-bit number produces a 5-char hex, not 6
        assert _parse_icao_hex("11000000000000000000001") is None

    def test_strips_whitespace(self):
        assert _parse_icao_hex("  " + _BIN_C00001 + "  ") == "C00001"


# ---------------------------------------------------------------------------
# Tests: _parse_date_yyyymmdd
# ---------------------------------------------------------------------------

class TestParseDateYyyymmdd:
    def test_valid_date(self):
        assert _parse_date_yyyymmdd("2000/01/15") == "2000-01-15T00:00:00Z"

    def test_empty_returns_none(self):
        assert _parse_date_yyyymmdd("") is None

    def test_strips_whitespace(self):
        assert _parse_date_yyyymmdd("  2000/01/15  ") == "2000-01-15T00:00:00Z"

    def test_invalid_format_returns_none(self):
        assert _parse_date_yyyymmdd("2000-01-15") is None


# ---------------------------------------------------------------------------
# Tests: _decode_aircraft_type
# ---------------------------------------------------------------------------

class TestDecodeAircraftType:
    def test_aeroplane_maps_to_airplane(self):
        assert _decode_aircraft_type("Aeroplane") == "Airplane"

    def test_helicopter(self):
        assert _decode_aircraft_type("Helicopter") == "Helicopter"

    def test_glider(self):
        assert _decode_aircraft_type("Glider") == "Glider"

    def test_balloon(self):
        assert _decode_aircraft_type("Balloon") == "Balloon"

    def test_gyroplane(self):
        assert _decode_aircraft_type("Gyroplane") == "Gyroplane"

    def test_unknown_returns_none(self):
        assert _decode_aircraft_type("Unknown") is None

    def test_strips_whitespace(self):
        assert _decode_aircraft_type("  Aeroplane  ") == "Airplane"


# ---------------------------------------------------------------------------
# Tests: _decode_engine_category
# ---------------------------------------------------------------------------

class TestDecodeEngineCategory:
    def test_piston(self):
        assert _decode_engine_category("Piston") == "Piston"

    def test_turbo_fan_with_space(self):
        assert _decode_engine_category("Turbo Fan") == "Turbo-fan"

    def test_turbo_prop(self):
        assert _decode_engine_category("Turbo Prop") == "Turbo-prop"

    def test_turbo_shaft(self):
        assert _decode_engine_category("Turbo Shaft") == "Turbo-shaft"

    def test_turbo_jet(self):
        assert _decode_engine_category("Turbo Jet") == "Turbo-jet"

    def test_electric(self):
        assert _decode_engine_category("Electric") == "Electric"

    def test_other_maps_to_unknown(self):
        assert _decode_engine_category("Other") == "Unknown"

    def test_unrecognised_returns_none(self):
        assert _decode_engine_category("Rocket") is None

    def test_strips_whitespace(self):
        assert _decode_engine_category("  Piston  ") == "Piston"


# ---------------------------------------------------------------------------
# Tests: _decode_country
# ---------------------------------------------------------------------------

class TestDecodeCountry:
    def test_canada(self):
        assert _decode_country("CANADA") == "CA"

    def test_case_insensitive(self):
        assert _decode_country("canada") == "CA"

    def test_united_states(self):
        assert _decode_country("UNITED STATES") == "US"

    def test_unknown_defaults_to_ca(self):
        assert _decode_country("RURITANIA") == "CA"

    def test_strips_whitespace(self):
        assert _decode_country("  CANADA  ") == "CA"


# ---------------------------------------------------------------------------
# Tests: _parse_int and _parse_float
# ---------------------------------------------------------------------------

class TestParseHelpers:
    def test_parse_int_valid(self):
        assert _parse_int("4") == 4

    def test_parse_int_zero_returns_none(self):
        assert _parse_int("0") is None

    def test_parse_int_empty_returns_none(self):
        assert _parse_int("") is None

    def test_parse_float_valid(self):
        assert _parse_float("1100.0") == 1100.0

    def test_parse_float_zero_returns_none(self):
        assert _parse_float("0.0") is None

    def test_parse_float_empty_returns_none(self):
        assert _parse_float("") is None


# ---------------------------------------------------------------------------
# Tests: stage_data
# ---------------------------------------------------------------------------

class TestStageData:
    def test_aircraft_count_includes_inactive(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM aircraft")
            # GABC + XYZ + GINV staged; bad ICAO skipped
            assert cur.fetchone()[0] == 3
            conn.close()

    def test_active_aircraft_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM aircraft WHERE ineffective_date = ''")
            assert cur.fetchone()[0] == 2
            conn.close()

    def test_aircraft_fields_c_gabc(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute(
                "SELECT registration, aircraft_type, manufacturer_name, model, serial_number, "
                "engine_manufacturer, engine_category, engine_count, seat_count, manufactured_date "
                "FROM aircraft WHERE icao_hex = 'C00001'"
            )
            row = cur.fetchone()
            assert row["registration"] == "C-GABC"
            assert row["aircraft_type"] == "Airplane"
            assert row["manufacturer_name"] == "CESSNA AIRCRAFT CO"
            assert row["model"] == "172P"
            assert row["serial_number"] == "SN001"
            assert row["engine_manufacturer"] == "LYCOMING"
            assert row["engine_category"] == "Piston"
            assert row["engine_count"] == 1
            assert row["seat_count"] == 4
            assert row["manufactured_date"] == "2000-01-15T00:00:00Z"
            conn.close()

    def test_aircraft_trimmed_mark_used_for_registration(self):
        # col 46 (TRIMMED_MARK) preferred over col 0 when present
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT registration FROM aircraft WHERE icao_hex = 'C00002'")
            # TRIMMED_MARK = "XYZ", always prepend 'C-'
            assert cur.fetchone()["registration"] == "C-XYZ"
            conn.close()

    def test_three_char_mark_gets_c_prefix(self):
        # Data dictionary: always 'C-' regardless of mark length
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT registration FROM aircraft WHERE icao_hex = 'C00002'")
            assert cur.fetchone()["registration"] == "C-XYZ"
            conn.close()

    def test_engine_category_decoded(self):
        # "Turbo Fan" (raw TC) → "Turbo-fan" (canonical)
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT engine_category FROM aircraft WHERE icao_hex = 'C00002'")
            assert cur.fetchone()["engine_category"] == "Turbo-fan"
            conn.close()

    def test_inactive_aircraft_has_ineffective_date(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT ineffective_date FROM aircraft WHERE icao_hex = 'C00003'")
            assert cur.fetchone()["ineffective_date"] != ""
            conn.close()

    def test_bad_icao_skipped(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT icao_hex FROM aircraft")
            hexes = {r[0] for r in cur.fetchall()}
            assert "NOTBINARY" not in hexes
            conn.close()

    def test_owner_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM owners")
            assert cur.fetchone()[0] == 2
            conn.close()

    def test_active_owner_fields(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute(
                "SELECT name, trade_name, street_1, city, province, postal_code, country, owner_type, status "
                "FROM owners WHERE registration = 'C-GABC'"
            )
            row = cur.fetchone()
            assert row["name"] == "JOHN DOE"
            assert row["trade_name"] == "DOE AVIATION"
            assert row["street_1"] == "123 MAPLE ST"
            assert row["city"] == "VANCOUVER"
            assert row["province"] == "BC"
            assert row["postal_code"] == "V6Z 1A1"
            assert row["country"] == "CA"           # decoded from "CANADA"
            assert row["owner_type"] == "Individual"
            assert row["status"] == "Active"
            conn.close()

    def test_country_decoded_from_english_name(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT country FROM owners WHERE registration = 'C-GABC'")
            assert cur.fetchone()["country"] == "CA"
            conn.close()

    def test_inactive_owner_status(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT status FROM owners WHERE registration = 'C-XYZ'")
            assert cur.fetchone()["status"] == "Inactive"
            conn.close()

    def test_missing_aircraft_file_skips_gracefully(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data({"carsownr.txt": _OWNER_CSV}, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM aircraft")
            assert cur.fetchone()[0] == 0
            conn.close()

    def test_missing_owner_file_skips_gracefully(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data({"carscurr.txt": _ACFT_CSV}, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM owners")
            assert cur.fetchone()[0] == 0
            conn.close()


# ---------------------------------------------------------------------------
# Tests: build_aircraft_record
# ---------------------------------------------------------------------------

class TestBuildAircraftRecord:
    def _make_conn(self) -> sqlite3.Connection:
        with tempfile.TemporaryDirectory() as tmpdir:
            return stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))

    def _fetch_acft_row(self, icao_hex: str, conn: sqlite3.Connection) -> sqlite3.Row:
        cur = conn.cursor()
        cur.execute(
            "SELECT icao_hex, registration, aircraft_type, manufacturer_name, model, serial_number, "
            "engine_manufacturer, engine_category, engine_count, seat_count, manufactured_date "
            "FROM aircraft WHERE icao_hex = ?",
            (icao_hex,),
        )
        return cur.fetchone()

    def _fetch_owner_rows(self, registration: str, conn: sqlite3.Connection) -> list[sqlite3.Row]:
        cur = conn.cursor()
        cur.execute(
            "SELECT name, trade_name, street_1, street_2, city, province, "
            "postal_code, country, owner_type FROM owners "
            "WHERE registration = ? AND status = 'Active' LIMIT 1",
            (registration,),
        )
        return cur.fetchall()

    def test_top_level_fields(self):
        conn = self._make_conn()
        acft = self._fetch_acft_row("C00001", conn)
        owners = self._fetch_owner_rows("C-GABC", conn)
        record = build_aircraft_record(acft, owners)
        assert record["icao_hex"] == "C00001"
        assert record["registration"] == "C-GABC"
        assert "source" not in record
        assert record["military"] is False
        conn.close()

    def test_registrant_with_active_owner(self):
        conn = self._make_conn()
        acft = self._fetch_acft_row("C00001", conn)
        owners = self._fetch_owner_rows("C-GABC", conn)
        record = build_aircraft_record(acft, owners)
        reg = record["registrant"]
        assert "JOHN DOE" in reg["names"]
        assert "DOE AVIATION" in reg["names"]
        assert "123 MAPLE ST" in reg["street"]
        assert reg["city"] == "VANCOUVER"
        assert reg["administrative_area"] == "BC"
        assert reg["postal_code"] == "V6Z 1A1"
        assert reg["country"] == "CA"
        assert reg["type"] == "Individual"
        conn.close()

    def test_registrant_none_when_no_active_owners(self):
        conn = self._make_conn()
        acft = self._fetch_acft_row("C00002", conn)
        owners = self._fetch_owner_rows("C-XYZ", conn)
        record = build_aircraft_record(acft, owners)
        assert record["registrant"] is None
        conn.close()

    def test_aircraft_type_decoded(self):
        conn = self._make_conn()
        acft = self._fetch_acft_row("C00001", conn)
        owners = self._fetch_owner_rows("C-GABC", conn)
        record = build_aircraft_record(acft, owners)
        # AIRCRAFT_CATEGORY_E "Aeroplane" → aircraft.type "Airplane"
        assert record["aircraft"]["type"] == "Airplane"
        conn.close()

    def test_aircraft_category_absent(self):
        """aircraft.category (Land/Sea/Amphibian) is FAA-only — must be omitted from TC record."""
        conn = self._make_conn()
        acft = self._fetch_acft_row("C00001", conn)
        owners = self._fetch_owner_rows("C-GABC", conn)
        record = build_aircraft_record(acft, owners)
        assert "category" not in record["aircraft"]
        conn.close()

    def test_aircraft_manufacturer_from_col7(self):
        # ID_PLATE_MANUFACTURERS_NAME (col 7) — not COMMON_NAME (col 3)
        conn = self._make_conn()
        acft = self._fetch_acft_row("C00001", conn)
        owners = self._fetch_owner_rows("C-GABC", conn)
        record = build_aircraft_record(acft, owners)
        assert record["aircraft"]["manufacturer"] == "CESSNA AIRCRAFT CO"
        conn.close()

    def test_aircraft_mictronics_fields_absent(self):
        """Fields owned by mictronics must be omitted entirely from the TC record."""
        conn = self._make_conn()
        acft = self._fetch_acft_row("C00001", conn)
        owners = self._fetch_owner_rows("C-GABC", conn)
        record = build_aircraft_record(acft, owners)
        ac = record["aircraft"]
        assert "type_designator" not in ac
        assert "manufacturer_model" not in ac
        assert "wake_turbulence_category" not in ac
        conn.close()

    def test_powerplant_piston(self):
        conn = self._make_conn()
        acft = self._fetch_acft_row("C00001", conn)
        owners = self._fetch_owner_rows("C-GABC", conn)
        record = build_aircraft_record(acft, owners)
        pp = record["aircraft"]["powerplant"]
        assert pp["count"] == 1
        assert pp["type"] == "Piston"
        assert pp["manufacturer"] == "LYCOMING"
        assert "model" not in pp
        assert "power_type" not in pp
        assert "power_value" not in pp
        conn.close()

    def test_powerplant_turbofan(self):
        conn = self._make_conn()
        acft = self._fetch_acft_row("C00002", conn)
        owners = self._fetch_owner_rows("C-XYZ", conn)
        record = build_aircraft_record(acft, owners)
        pp = record["aircraft"]["powerplant"]
        assert pp["count"] == 2
        assert pp["type"] == "Turbo-fan"
        assert pp["manufacturer"] == "CFM INTERNATIONAL"
        conn.close()


# ---------------------------------------------------------------------------
# Tests: Redis key construction
# ---------------------------------------------------------------------------

class TestRedisKeys:
    def test_aircraft_registry_key_format(self):
        from shared.redis_keys import aircraft_registry_key
        assert aircraft_registry_key("c00001") == "aircraft:registry:C00001"

    def test_aircraft_detail_search_index_name(self):
        from shared.redis_keys import AIRCRAFT_REGISTRY_SEARCH_INDEX
        assert AIRCRAFT_REGISTRY_SEARCH_INDEX == "idx:aircraft:registry"


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def _make_db(self) -> sqlite3.Connection:
        with tempfile.TemporaryDirectory() as tmpdir:
            return stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))

    def _mock_redis(self):
        r = MagicMock()
        pipe = MagicMock()
        pipe_json = MagicMock()
        r.pipeline.return_value = pipe
        pipe.json.return_value = pipe_json
        pipe.execute.return_value = []
        return r, pipe, pipe_json

    def test_only_active_records_written(self):
        conn = self._make_db()
        r, _, _ = self._mock_redis()
        count = write_to_redis(conn, r, REDIS_TTL)
        assert count == 2
        conn.close()

    def test_active_icao_hex_written(self):
        conn = self._make_db()
        r, _, pipe_json = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        keys = [c.args[0] for c in pipe_json.set.call_args_list]
        assert "aircraft:registry:C00001" in keys
        assert "aircraft:registry:C00002" in keys
        conn.close()

    def test_inactive_icao_hex_not_written(self):
        conn = self._make_db()
        r, _, pipe_json = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        keys = [c.args[0] for c in pipe_json.set.call_args_list]
        assert "aircraft:registry:C00003" not in keys
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
        record = calls["aircraft:registry:C00001"]
        assert isinstance(record, dict)
        assert record["source"] == "ca-transport-canada"
        assert record["registration"] == "C-GABC"
        conn.close()

    def test_source_set_on_all_records(self):
        """Every record written to Redis carries source='ca-transport-canada'."""
        conn = self._make_db()
        r, _, pipe_json = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        for call in pipe_json.set.call_args_list:
            record = call.args[2]
            assert record["source"] == "ca-transport-canada"
        conn.close()

    def test_null_fields_omitted_from_written_record(self):
        """C-XYZ has no active owner, so build_aircraft_record sets registrant=None;
        the written record must omit the key entirely, not write it as None."""
        conn = self._make_db()
        r, _, pipe_json = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        calls = {c.args[0]: c.args[2] for c in pipe_json.set.call_args_list}
        record = calls["aircraft:registry:C00002"]
        assert "registrant" not in record
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
        with patch("ca_tc_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ca_tc_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 99, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "99"

    def test_publishes_last_run_at(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ca_tc_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/last_run_at" in topics

    def test_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ca_tc_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_no_mqtt_config_skips(self):
        cfg = {}
        mc = self._setup_mock_client()
        with patch("ca_tc_main.mqtt.Client", return_value=mc):
            publish_completion_stats(cfg, 0, "success")
        mc.connect.assert_not_called()

    def test_ha_autodiscovery_three_sensors(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ca_tc_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 100, "success")
        ha_topics = [
            c.args[0] for c in mc.publish.call_args_list
            if c.args[0].startswith("homeassistant/")
        ]
        assert len(ha_topics) == 3
        assert "homeassistant/sensor/SkyFollower_runner_ca_transport_canada_records_imported/config" in ha_topics
        assert "homeassistant/sensor/SkyFollower_runner_ca_transport_canada_last_run_at/config" in ha_topics
        assert "homeassistant/sensor/SkyFollower_runner_ca_transport_canada_last_run_status/config" in ha_topics


# ---------------------------------------------------------------------------
# Tests: network (real outbound HTTP call — see #405)
# ---------------------------------------------------------------------------

class TestNetwork:
    @pytest.mark.network
    def test_url_reachable(self):
        assert_url_reachable(_mod.DOWNLOAD_URL, "ca-transport-canada", headers={"User-Agent": "P5Software SkyFollower"})
