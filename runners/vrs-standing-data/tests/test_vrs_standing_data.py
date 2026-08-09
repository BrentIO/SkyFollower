"""
Tests for the vrs-standing-data (Virtual Radar Server Standing Data) runner.

Covers:
- Tarball download/extraction (route CSVs only, top-level dir stripped)
- CSV parsing and SQLite staging (ident/route rename, BOM handling)
- Redis write logic (plain string, not JSON) (mocked)
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


download_and_extract_routes = _mod.download_and_extract_routes
stage_data = _mod.stage_data
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
route_key = _mod.route_key
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT


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


# ---------------------------------------------------------------------------
# Tests: download_and_extract_routes
# ---------------------------------------------------------------------------

class TestDownloadAndExtractRoutes:
    def _mock_response(self, content: bytes, status_code: int = 200):
        resp = MagicMock()
        resp.status_code = status_code
        resp.content = content
        return resp

    def test_extracts_route_csvs(self):
        tarball = _make_tarball(_make_files())
        with patch("vrs_standing_data_main.requests.get", return_value=self._mock_response(tarball)):
            files = download_and_extract_routes("https://example.test/archive.tar.gz")
        assert set(files.keys()) == {
            "routes/schema-01/A/AAL-all.csv",
            "routes/schema-01/D/DAL-all.csv",
        }

    def test_strips_top_level_directory(self):
        tarball = _make_tarball(_make_files())
        with patch("vrs_standing_data_main.requests.get", return_value=self._mock_response(tarball)):
            files = download_and_extract_routes("https://example.test/archive.tar.gz")
        assert all(not p.startswith("standing-data-main/") for p in files)

    def test_ignores_non_route_files(self):
        tarball = _make_tarball({
            **_make_files(),
            "aircraft/schema-01/A/foo.csv": b"not a route file",
            "routes/schema-01/README.md": b"not a csv",
        })
        with patch("vrs_standing_data_main.requests.get", return_value=self._mock_response(tarball)):
            files = download_and_extract_routes("https://example.test/archive.tar.gz")
        assert "aircraft/schema-01/A/foo.csv" not in files
        assert "routes/schema-01/README.md" not in files

    def test_raises_on_non_200(self):
        with patch("vrs_standing_data_main.requests.get", return_value=self._mock_response(b"", status_code=404)):
            with pytest.raises(RuntimeError):
                download_and_extract_routes("https://example.test/archive.tar.gz")


# ---------------------------------------------------------------------------
# Tests: stage_data
# ---------------------------------------------------------------------------

class TestStageData:
    def test_route_count(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM routes")
            assert cur.fetchone()[0] == 3
            conn.close()

    def test_simple_route_passed_through_unchanged(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT route FROM routes WHERE ident = 'AAL1'")
            assert cur.fetchone()["route"] == "KJFK-KLAX"
            conn.close()

    def test_multi_leg_route_not_filtered_or_split(self):
        """The out-and-back case: a 3-airport route must be stored whole,
        not skipped or truncated to 2 airports."""
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT route FROM routes WHERE ident = 'AAL1005'")
            assert cur.fetchone()["route"] == "KDFW-MYNN-KDFW"
            conn.close()

    def test_bom_stripped_from_first_ident(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))
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
            conn = stage_data(files, os.path.join(tmpdir, "staging.db"))
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
            conn = stage_data(files, os.path.join(tmpdir, "staging.db"))
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
            conn = stage_data(files, os.path.join(tmpdir, "staging.db"))
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
            conn = stage_data(files, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM routes")
            assert cur.fetchone()[0] == 1
            conn.close()


# ---------------------------------------------------------------------------
# Tests: write_to_redis (mocked)
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def _make_db(self) -> sqlite3.Connection:
        with tempfile.TemporaryDirectory() as tmpdir:
            return stage_data(_make_files(), os.path.join(tmpdir, "staging.db"))

    def _mock_redis(self):
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        pipe.execute.return_value = []
        return r, pipe

    def test_count_matches_staged_routes(self):
        conn = self._make_db()
        r, _ = self._mock_redis()
        assert write_to_redis(conn, r, REDIS_TTL) == 3
        conn.close()

    def test_route_key_written(self):
        conn = self._make_db()
        r, pipe = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        keys = [c.args[0] for c in pipe.set.call_args_list]
        assert "route:AAL1" in keys
        assert "route:DAL659" in keys
        conn.close()

    def test_value_written_as_plain_string_not_json(self):
        conn = self._make_db()
        r, pipe = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        calls = {c.args[0]: c.args[1] for c in pipe.set.call_args_list}
        assert calls["route:AAL1005"] == "KDFW-MYNN-KDFW"
        assert isinstance(calls["route:AAL1005"], str)
        conn.close()

    def test_expire_applied_via_ex_kwarg(self):
        conn = self._make_db()
        r, pipe = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        for c in pipe.set.call_args_list:
            assert c.kwargs["ex"] == REDIS_TTL
        conn.close()

    def test_never_uses_json_client(self):
        """route:{ident} is a plain Redis string -- must never go through the
        RedisJSON client used by every other runner."""
        conn = self._make_db()
        r, _ = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        r.json.assert_not_called()
        conn.close()


# ---------------------------------------------------------------------------
# Tests: route_key
# ---------------------------------------------------------------------------

class TestRouteKey:
    def test_format(self):
        assert route_key("aal15") == "route:AAL15"


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
