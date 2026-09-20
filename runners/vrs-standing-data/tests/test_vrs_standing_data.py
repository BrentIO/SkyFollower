"""
Tests for the vrs-standing-data (Virtual Radar Server Standing Data) runner.

Covers:
- Tarball download/extraction (routes/code-blocks/countries CSVs, top-level dir stripped)
- CSV parsing and SQLite staging (ident/route rename, BOM handling)
- Redis write logic (routes as a plain string; code-blocks/countries as RedisJSON) (mocked)
- MQTT completion stats (mocked)
"""

from __future__ import annotations

import importlib.util
import io
import os
import sqlite3
import sys
import tarfile
import tempfile
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Module import helper
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
_RUNNER_DIR = os.path.dirname(_HERE)          # runners/vrs-standing-data/
_REPO_ROOT = os.path.abspath(os.path.join(_RUNNER_DIR, "..", ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "vrs_standing_data_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["vrs_standing_data_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()


download_tarball = _mod.download_tarball
extract_files = _mod.extract_files
open_staging_db = _mod.open_staging_db
_SCHEMA = _mod._SCHEMA
stage_routes = _mod.stage_routes
stage_code_blocks = _mod.stage_code_blocks
stage_countries = _mod.stage_countries
write_routes_to_redis = _mod.write_routes_to_redis
build_code_blocks_array = _mod.build_code_blocks_array
write_code_blocks_to_redis = _mod.write_code_blocks_to_redis
build_countries_object = _mod.build_countries_object
write_countries_to_redis = _mod.write_countries_to_redis
publish_completion_stats = _mod.publish_completion_stats
route_key = _mod.route_key
icao_code_blocks_key = _mod.icao_code_blocks_key
icao_countries_key = _mod.icao_countries_key
REDIS_TTL = _mod.ROUTE_TTL_SECONDS
ENRICHMENT_TTL = _mod.ENRICHMENT_TTL_SECONDS
MQTT_ROOT = _mod.MQTT_ROOT
_ROUTES_PATH_PREFIX = _mod._ROUTES_PATH_PREFIX
_CODE_BLOCKS_PATH_PREFIX = _mod._CODE_BLOCKS_PATH_PREFIX
_COUNTRIES_PATH_PREFIX = _mod._COUNTRIES_PATH_PREFIX


# ---------------------------------------------------------------------------
# Sample CSV fixtures (real column layout: Callsign,Code,Number,AirlineCode,AirportCodes)
# ---------------------------------------------------------------------------

_AAL_CSV = (
    "﻿Callsign,Code,Number,AirlineCode,AirportCodes\n"
    "AAL1,AAL,1,AAL,KJFK-KLAX\n"
    "AAL1005,AAL,1005,AAL,KDFW-MYNN-KDFW\n"
).encode("utf-8")

_DAL_CSV = (
    "Callsign,Code,Number,AirlineCode,AirportCodes\n"
    "DAL659,DAL,659,DAL,KATL-KLAX\n"
).encode("utf-8")


def _make_tarball(files: dict[str, bytes], top_level: str = "standing-data-main") -> bytes:
    """Build an in-memory GitHub-style tarball: files keyed by path relative
    to the repo root, wrapped in a single top-level directory."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for path, data in files.items():
            info = tarfile.TarInfo(name=f"{top_level}/{path}")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _make_files() -> dict[str, bytes]:
    return {
        "routes/schema-01/A/AAL-all.csv": _AAL_CSV,
        "routes/schema-01/D/DAL-all.csv": _DAL_CSV,
    }


_CODE_BLOCKS_CSV = (
    "﻿Start,Finish,Count,Bitmask,SignificantBitmask,IsMilitary,CountryISO2\n"
    "000000,7FFFFF,8388608,000000,800000,0,ZZ\n"  # unassigned catch-all -- must be dropped
    "800000,FFFFFF,8388608,800000,800000,0,ZZ\n"  # unassigned catch-all -- must be dropped
    "A00000,AFFFFF,1048576,A00000,F00000,0,US\n"  # broad US block
    "A00001,A00001,1,A00001,FFFFFF,1,US\n"  # narrow, more-specific US block (military)
    "400000,7BFFFF,4194304,400000,FC0000,0,GB\n"  # broad UK block
).encode("utf-8")

_COUNTRIES_CSV = (
    "﻿ISO,Name\n"
    "US,United States\n"
    "GB,United Kingdom\n"
    "ZZ,Unknown or unassigned country\n"
).encode("utf-8")


def _make_code_block_files() -> dict[str, bytes]:
    return {"code-blocks/schema-01/code-blocks.csv": _CODE_BLOCKS_CSV}


def _make_country_files() -> dict[str, bytes]:
    return {"countries/schema-01/countries.csv": _COUNTRIES_CSV}


def _staged_conn(tmpdir: str) -> sqlite3.Connection:
    return open_staging_db(os.path.join(tmpdir, "staging.db"), _SCHEMA)


# ---------------------------------------------------------------------------
# Tests: download_tarball / extract_files
# ---------------------------------------------------------------------------

class TestDownloadTarballAndExtractFiles:
    def _mock_response(self, content: bytes, status_code: int = 200):
        resp = MagicMock()
        resp.status_code = status_code
        resp.content = content
        return resp

    def test_extracts_route_csvs(self):
        tarball = _make_tarball(_make_files())
        files = extract_files(tarball, _ROUTES_PATH_PREFIX)
        assert set(files.keys()) == {
            "routes/schema-01/A/AAL-all.csv",
            "routes/schema-01/D/DAL-all.csv",
        }

    def test_strips_top_level_directory(self):
        tarball = _make_tarball(_make_files())
        files = extract_files(tarball, _ROUTES_PATH_PREFIX)
        assert all(not p.startswith("standing-data-main/") for p in files)

    def test_ignores_files_outside_prefix(self):
        tarball = _make_tarball({
            **_make_files(),
            "aircraft/schema-01/A/foo.csv": b"not a route file",
            "routes/schema-01/README.md": b"not a csv",
        })
        files = extract_files(tarball, _ROUTES_PATH_PREFIX)
        assert "aircraft/schema-01/A/foo.csv" not in files
        assert "routes/schema-01/README.md" not in files

    def test_extracts_only_the_requested_prefix_from_a_shared_tarball(self):
        """One download serves all three imports -- extract_files() is
        called once per prefix against the same tarball bytes, and each call
        must only return files under its own prefix."""
        tarball = _make_tarball({
            **_make_files(),
            **_make_code_block_files(),
            **_make_country_files(),
        })
        assert set(extract_files(tarball, _ROUTES_PATH_PREFIX)) == {
            "routes/schema-01/A/AAL-all.csv",
            "routes/schema-01/D/DAL-all.csv",
        }
        assert set(extract_files(tarball, _CODE_BLOCKS_PATH_PREFIX)) == {
            "code-blocks/schema-01/code-blocks.csv",
        }
        assert set(extract_files(tarball, _COUNTRIES_PATH_PREFIX)) == {
            "countries/schema-01/countries.csv",
        }

    def test_download_tarball_raises_on_non_200(self):
        with patch("vrs_standing_data_main.requests.get", return_value=self._mock_response(b"", status_code=404)):
            with pytest.raises(RuntimeError):
                download_tarball("https://example.test/archive.tar.gz")

    def test_download_tarball_returns_raw_bytes(self):
        tarball = _make_tarball(_make_files())
        with patch("vrs_standing_data_main.requests.get", return_value=self._mock_response(tarball)):
            result = download_tarball("https://example.test/archive.tar.gz")
        assert result == tarball


# ---------------------------------------------------------------------------
# Tests: stage_routes
# ---------------------------------------------------------------------------

class TestStageRoutes:
    def test_route_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_routes(conn, _make_files())
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM routes")
            assert cur.fetchone()[0] == 3
            conn.close()

    def test_simple_route_passed_through_unchanged(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_routes(conn, _make_files())
            cur = conn.cursor()
            cur.execute("SELECT route FROM routes WHERE ident = 'AAL1'")
            assert cur.fetchone()["route"] == "KJFK-KLAX"
            conn.close()

    def test_multi_leg_route_not_filtered_or_split(self):
        """The out-and-back case: a 3-airport route must be stored whole,
        not skipped or truncated to 2 airports."""
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_routes(conn, _make_files())
            cur = conn.cursor()
            cur.execute("SELECT route FROM routes WHERE ident = 'AAL1005'")
            assert cur.fetchone()["route"] == "KDFW-MYNN-KDFW"
            conn.close()

    def test_bom_stripped_from_first_ident(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_routes(conn, _make_files())
            cur = conn.cursor()
            cur.execute("SELECT ident FROM routes WHERE ident = 'AAL1'")
            row = cur.fetchone()
            assert row is not None
            assert row["ident"] == "AAL1"
            conn.close()

    def test_ident_uppercased(self):
        files = {"routes/schema-01/A/AAL-all.csv": (
            "Callsign,Code,Number,AirlineCode,AirportCodes\n"
            "aal1,AAL,1,AAL,KJFK-KLAX\n"
        ).encode()}
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_routes(conn, files)
            cur = conn.cursor()
            cur.execute("SELECT ident FROM routes")
            assert cur.fetchone()["ident"] == "AAL1"
            conn.close()

    def test_short_row_skipped(self):
        files = {"routes/schema-01/A/AAL-all.csv": (
            "Callsign,Code,Number,AirlineCode,AirportCodes\n"
            "AAL1,AAL,1,AAL\n"  # missing AirportCodes column entirely
        ).encode()}
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_routes(conn, files)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM routes")
            assert cur.fetchone()[0] == 0
            conn.close()

    def test_blank_airport_codes_skipped(self):
        files = {"routes/schema-01/A/AAL-all.csv": (
            "Callsign,Code,Number,AirlineCode,AirportCodes\n"
            "AAL1,AAL,1,AAL,\n"
        ).encode()}
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_routes(conn, files)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM routes")
            assert cur.fetchone()[0] == 0
            conn.close()

    def test_duplicate_ident_last_write_wins(self):
        files = {
            "routes/schema-01/A/AAL-all.csv": (
                "Callsign,Code,Number,AirlineCode,AirportCodes\n"
                "AAL1,AAL,1,AAL,KJFK-KLAX\n"
            ).encode(),
            "routes/schema-01/A/AAL2-all.csv": (
                "Callsign,Code,Number,AirlineCode,AirportCodes\n"
                "AAL1,AAL,1,AAL,KJFK-EGLL\n"
            ).encode(),
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_routes(conn, files)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM routes")
            assert cur.fetchone()[0] == 1
            conn.close()

    def test_zero_padded_ident_normalized_on_stage(self):
        """The source CSV's own zero-padding convention isn't trusted -- an
        ident is normalized to its canonical (unpadded numeric) form before
        it's staged, so the eventual route:{ident} key matches a normalized
        query regardless of which side the padding was on."""
        files = {"routes/schema-01/A/AFR-all.csv": (
            "Callsign,Code,Number,AirlineCode,AirportCodes\n"
            "AFR0096,AFR,0096,AFR,LFPG-KJFK\n"
            "VIR096K,VIR,096,VIR,EGLL-KJFK\n"
        ).encode()}
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_routes(conn, files)
            cur = conn.cursor()
            cur.execute("SELECT ident FROM routes ORDER BY ident")
            assert [r["ident"] for r in cur.fetchall()] == ["AFR96", "VIR96K"]
            conn.close()

    def test_padding_only_difference_collapses_to_one_route(self):
        """Two CSV rows for what's really the same route, differing only by
        the source's zero-padding, must collapse into a single staged key
        (last write wins) rather than two divergent keys."""
        files = {
            "routes/schema-01/A/AFR-all.csv": (
                "Callsign,Code,Number,AirlineCode,AirportCodes\n"
                "AFR0096,AFR,0096,AFR,LFPG-KJFK\n"
            ).encode(),
            "routes/schema-01/A/AFR2-all.csv": (
                "Callsign,Code,Number,AirlineCode,AirportCodes\n"
                "AFR96,AFR,96,AFR,LFPG-KBOS\n"
            ).encode(),
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_routes(conn, files)
            cur = conn.cursor()
            cur.execute("SELECT ident, route FROM routes")
            rows = cur.fetchall()
            assert len(rows) == 1
            assert rows[0]["ident"] == "AFR96"
            conn.close()


# ---------------------------------------------------------------------------
# Tests: stage_code_blocks
# ---------------------------------------------------------------------------

class TestStageCodeBlocks:
    def test_row_count_excludes_zz_catchall(self):
        """The 2 synthetic ZZ catch-all rows must never be staged -- only
        the 3 real country rows in the fixture."""
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            count = stage_code_blocks(conn, _make_code_block_files())
            assert count == 3
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM code_blocks")
            assert cur.fetchone()[0] == 3
            conn.close()

    def test_zz_rows_not_present_by_country_code(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_code_blocks(conn, _make_code_block_files())
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM code_blocks WHERE country_code = 'ZZ'")
            assert cur.fetchone()[0] == 0
            conn.close()

    def test_bitmask_and_significant_bitmask_converted_from_hex_to_int(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_code_blocks(conn, _make_code_block_files())
            cur = conn.cursor()
            cur.execute(
                "SELECT bitmask, significant_bitmask FROM code_blocks WHERE country_code = 'GB'"
            )
            row = cur.fetchone()
            assert row["bitmask"] == 0x400000
            assert row["significant_bitmask"] == 0xFC0000
            conn.close()

    def test_country_code_uppercased(self):
        files = {"code-blocks/schema-01/code-blocks.csv": (
            "Start,Finish,Count,Bitmask,SignificantBitmask,IsMilitary,CountryISO2\n"
            "A00000,AFFFFF,1048576,A00000,F00000,0,us\n"
        ).encode()}
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_code_blocks(conn, files)
            cur = conn.cursor()
            cur.execute("SELECT country_code FROM code_blocks")
            assert cur.fetchone()["country_code"] == "US"
            conn.close()

    def test_short_row_skipped(self):
        files = {"code-blocks/schema-01/code-blocks.csv": (
            "Start,Finish,Count,Bitmask,SignificantBitmask,IsMilitary,CountryISO2\n"
            "A00000,AFFFFF,1048576,A00000,F00000,0\n"  # missing CountryISO2
        ).encode()}
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            count = stage_code_blocks(conn, files)
            assert count == 0
            conn.close()

    def test_unparseable_bitmask_skipped(self):
        files = {"code-blocks/schema-01/code-blocks.csv": (
            "Start,Finish,Count,Bitmask,SignificantBitmask,IsMilitary,CountryISO2\n"
            "A00000,AFFFFF,1048576,NOTHEX,F00000,0,US\n"
        ).encode()}
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            count = stage_code_blocks(conn, files)
            assert count == 0
            conn.close()


# ---------------------------------------------------------------------------
# Tests: stage_countries
# ---------------------------------------------------------------------------

class TestStageCountries:
    def test_row_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            count = stage_countries(conn, _make_country_files())
            assert count == 3
            conn.close()

    def test_name_resolved_by_iso2(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_countries(conn, _make_country_files())
            cur = conn.cursor()
            cur.execute("SELECT name FROM countries WHERE iso2 = 'US'")
            assert cur.fetchone()["name"] == "United States"
            conn.close()

    def test_iso2_uppercased(self):
        files = {"countries/schema-01/countries.csv": (
            "ISO,Name\nus,United States\n"
        ).encode()}
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_countries(conn, files)
            cur = conn.cursor()
            cur.execute("SELECT iso2 FROM countries")
            assert cur.fetchone()["iso2"] == "US"
            conn.close()

    def test_blank_name_skipped(self):
        files = {"countries/schema-01/countries.csv": (
            "ISO,Name\nZZ,\n"
        ).encode()}
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            count = stage_countries(conn, files)
            assert count == 0
            conn.close()

    def test_zz_is_staged_here_unlike_code_blocks(self):
        """countries.csv's ZZ row ('Unknown or unassigned country') is a
        legitimate name mapping and is staged normally -- only
        stage_code_blocks() drops ZZ, since only there does its presence
        create a fake always-matches range."""
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_countries(conn, _make_country_files())
            cur = conn.cursor()
            cur.execute("SELECT name FROM countries WHERE iso2 = 'ZZ'")
            assert cur.fetchone()["name"] == "Unknown or unassigned country"
            conn.close()


# ---------------------------------------------------------------------------
# Tests: write_routes_to_redis (mocked)
# ---------------------------------------------------------------------------

class TestWriteRoutesToRedis:
    def _make_db(self) -> sqlite3.Connection:
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_routes(conn, _make_files())
            return conn

    def _mock_redis(self):
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        pipe.execute.return_value = []
        return r, pipe

    def test_count_matches_staged_routes(self):
        conn = self._make_db()
        r, _ = self._mock_redis()
        assert write_routes_to_redis(conn, r, REDIS_TTL) == 3
        conn.close()

    def test_route_key_written(self):
        conn = self._make_db()
        r, pipe = self._mock_redis()
        write_routes_to_redis(conn, r, REDIS_TTL)
        keys = [c.args[0] for c in pipe.set.call_args_list]
        assert "route:AAL1" in keys
        assert "route:DAL659" in keys
        conn.close()

    def test_value_written_as_plain_string_not_json(self):
        conn = self._make_db()
        r, pipe = self._mock_redis()
        write_routes_to_redis(conn, r, REDIS_TTL)
        calls = {c.args[0]: c.args[1] for c in pipe.set.call_args_list}
        assert calls["route:AAL1005"] == "KDFW-MYNN-KDFW"
        assert isinstance(calls["route:AAL1005"], str)
        conn.close()

    def test_expire_applied_via_ex_kwarg(self):
        conn = self._make_db()
        r, pipe = self._mock_redis()
        write_routes_to_redis(conn, r, REDIS_TTL)
        for c in pipe.set.call_args_list:
            assert c.kwargs["ex"] == REDIS_TTL
        conn.close()

    def test_never_uses_json_client(self):
        """route:{ident} is a plain Redis string -- must never go through the
        RedisJSON client used by every other runner."""
        conn = self._make_db()
        r, _ = self._mock_redis()
        write_routes_to_redis(conn, r, REDIS_TTL)
        r.json.assert_not_called()
        conn.close()


# ---------------------------------------------------------------------------
# Tests: build_code_blocks_array / write_code_blocks_to_redis (mocked)
# ---------------------------------------------------------------------------

class TestCodeBlocksArray:
    def _make_db(self) -> sqlite3.Connection:
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_code_blocks(conn, _make_code_block_files())
            return conn

    def test_sorted_descending_by_significant_bitmask(self):
        """merge_aircraft.lua's linear scan depends on the most specific
        (highest significant_bitmask) rows appearing first."""
        conn = self._make_db()
        blocks = build_code_blocks_array(conn)
        significances = [b["significant_bitmask"] for b in blocks]
        assert significances == sorted(significances, reverse=True)
        conn.close()

    def test_narrow_military_block_sorts_before_broad_block(self):
        conn = self._make_db()
        blocks = build_code_blocks_array(conn)
        us_blocks = [b for b in blocks if b["country_code"] == "US"]
        assert us_blocks[0]["bitmask"] == 0xA00001  # the narrow, more-specific block
        assert us_blocks[1]["bitmask"] == 0xA00000  # the broad block
        conn.close()

    def test_no_start_finish_count_or_is_military_fields(self):
        """Only bitmask/significant_bitmask/country_code are carried into
        the Redis array -- Start/Finish/Count/IsMilitary are dropped."""
        conn = self._make_db()
        blocks = build_code_blocks_array(conn)
        assert all(set(b.keys()) == {"bitmask", "significant_bitmask", "country_code"} for b in blocks)
        conn.close()

    def test_write_code_blocks_to_redis_uses_redisjson(self):
        conn = self._make_db()
        r = MagicMock()
        write_code_blocks_to_redis(conn, r, ENRICHMENT_TTL)
        r.json.assert_called_once()
        conn.close()

    def test_write_code_blocks_to_redis_key_and_ttl(self):
        conn = self._make_db()
        r = MagicMock()
        count = write_code_blocks_to_redis(conn, r, ENRICHMENT_TTL)
        assert count == 3
        r.expire.assert_called_once_with(icao_code_blocks_key(), ENRICHMENT_TTL)
        conn.close()


# ---------------------------------------------------------------------------
# Tests: build_countries_object / write_countries_to_redis (mocked)
# ---------------------------------------------------------------------------

class TestCountriesObject:
    def _make_db(self) -> sqlite3.Connection:
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = _staged_conn(tmpdir)
            stage_countries(conn, _make_country_files())
            return conn

    def test_object_shape(self):
        conn = self._make_db()
        countries = build_countries_object(conn)
        assert countries["US"] == "United States"
        assert countries["GB"] == "United Kingdom"
        conn.close()

    def test_write_countries_to_redis_uses_redisjson(self):
        conn = self._make_db()
        r = MagicMock()
        write_countries_to_redis(conn, r, ENRICHMENT_TTL)
        r.json.assert_called_once()
        conn.close()

    def test_write_countries_to_redis_key_and_ttl(self):
        conn = self._make_db()
        r = MagicMock()
        count = write_countries_to_redis(conn, r, ENRICHMENT_TTL)
        assert count == 3
        r.expire.assert_called_once_with(icao_countries_key(), ENRICHMENT_TTL)
        conn.close()


# ---------------------------------------------------------------------------
# Tests: route_key / icao_code_blocks_key / icao_countries_key
# ---------------------------------------------------------------------------

class TestRouteKey:
    def test_format(self):
        assert route_key("aal15") == "route:AAL15"


class TestIcaoLookupKeys:
    def test_code_blocks_key_is_fixed(self):
        assert icao_code_blocks_key() == "lookup:icao-code-blocks"

    def test_countries_key_is_fixed(self):
        assert icao_countries_key() == "lookup:icao-countries"


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
        with patch("vrs_standing_data_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("vrs_standing_data_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 99, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "99"

    def test_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("vrs_standing_data_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "Failure"

    def test_no_mqtt_config_skips(self):
        cfg = {}
        mc = self._setup_mock_client()
        with patch("vrs_standing_data_main.mqtt.Client", return_value=mc):
            publish_completion_stats(cfg, 0, "success")
        mc.connect.assert_not_called()

    def test_blank_host_skips_without_crashing(self):
        """Regression test: shared/config.py's mqtt_config() always returns a
        populated dict with host="" (never None/{}) when MQTT_HOST is unset
        -- the documented way to disable MQTT entirely. A guard that only
        checks `if not mc` doesn't catch this, since the dict itself is
        truthy; it then calls build_mqtt_client() (which correctly returns
        None for a blank host) and crashes assigning .on_connect on None.
        That crash gets silently swallowed by main()'s outer try/except, so
        the runner "succeeds" but MQTT stats never publish and a bogus
        warning gets logged every run. Must not raise."""
        cfg = {"mqtt": {"host": "", "port": 1883, "username": "", "password": ""}}
        publish_completion_stats(cfg, 0, "success")

    def test_ha_autodiscovery_three_sensors(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("vrs_standing_data_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 100, "success")
        ha_topics = [
            c.args[0] for c in mc.publish.call_args_list
            if c.args[0].startswith("homeassistant/")
        ]
        assert len(ha_topics) == 3
        assert "homeassistant/sensor/SkyFollower_runner_vrs_standing_data_records_imported/config" in ha_topics
        assert "homeassistant/sensor/SkyFollower_runner_vrs_standing_data_last_run_at/config" in ha_topics
        assert "homeassistant/sensor/SkyFollower_runner_vrs_standing_data_last_run_status/config" in ha_topics
