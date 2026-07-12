"""Tests for the Luxembourg DAC data runner."""

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
        "lu_dac_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["lu_dac_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

from shared.url_reachability import assert_url_reachable

_assign_column = _mod._assign_column
_cluster_rows = _mod._cluster_rows
_build_names = _mod._build_names
_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
_PRIVATE_PLACEHOLDERS = _mod._PRIVATE_PLACEHOLDERS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_parsed(
    registration="LX-ABC",
    manufacturer="Piper",
    model="PA-28",
    serial_number="28-1234",
    exploitant="Luxair SA",
    proprietaire="Luxair SA",
) -> dict:
    return {
        "registration": registration,
        "manufacturer": manufacturer,
        "model": model,
        "serial_number": serial_number,
        "exploitant": exploitant,
        "proprietaire": proprietaire,
    }


def _make_word(text: str, x0: float, top: float) -> dict:
    return {"text": text, "x0": x0, "top": top, "x1": x0 + 20, "bottom": top + 10}


def _make_redis_with_search(icao_hex="4A0123", registration="LX-ABC"):
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
# Tests: _assign_column
# ---------------------------------------------------------------------------

class TestAssignColumn:
    def test_immat_range(self):
        assert _assign_column(44) == "immat"
        assert _assign_column(90) == "immat"

    def test_constructeur_range(self):
        assert _assign_column(98) == "constructeur"
        assert _assign_column(200) == "constructeur"

    def test_type_range(self):
        assert _assign_column(256) == "type"
        assert _assign_column(400) == "type"

    def test_sn_range(self):
        assert _assign_column(421) == "sn"
        assert _assign_column(450) == "sn"

    def test_proprietaire_range(self):
        assert _assign_column(489) == "proprietaire"
        assert _assign_column(600) == "proprietaire"

    def test_exploitant_range(self):
        assert _assign_column(639) == "exploitant"
        assert _assign_column(800) == "exploitant"

    def test_below_first_boundary_returns_none(self):
        assert _assign_column(10) is None

    def test_at_or_beyond_last_boundary_returns_none(self):
        assert _assign_column(842) is None
        assert _assign_column(900) is None


# ---------------------------------------------------------------------------
# Tests: _cluster_rows
# ---------------------------------------------------------------------------

class TestClusterRows:
    def test_single_row(self):
        words = [
            _make_word("LX-ABC", 50, 100),
            _make_word("Piper", 110, 100),
            _make_word("PA-28", 270, 100),
        ]
        rows = _cluster_rows(words)
        assert len(rows) == 1
        assert rows[0]["immat"] == ["LX-ABC"]
        assert rows[0]["constructeur"] == ["Piper"]
        assert rows[0]["type"] == ["PA-28"]

    def test_two_separate_rows(self):
        words = [
            _make_word("LX-ABC", 50, 100),
            _make_word("LX-DEF", 50, 115),
        ]
        rows = _cluster_rows(words)
        assert len(rows) == 2

    def test_continuation_within_tolerance(self):
        # top values within 5 points → same row
        words = [
            _make_word("LX-ABC", 50, 100),
            _make_word("Piper", 110, 103),
        ]
        rows = _cluster_rows(words)
        assert len(rows) == 1

    def test_words_outside_column_bounds_skipped(self):
        words = [
            _make_word("ignored", 10, 100),  # x0=10, below immat start (44)
            _make_word("LX-ABC", 50, 100),
        ]
        rows = _cluster_rows(words)
        assert len(rows) == 1
        assert "immat" in rows[0]
        assert rows[0].get("constructeur") is None

    def test_empty_words_returns_empty(self):
        assert _cluster_rows([]) == []

    def test_multi_word_in_same_column_and_row(self):
        words = [
            _make_word("Piper", 110, 100),
            _make_word("Aircraft", 130, 100),
        ]
        rows = _cluster_rows(words)
        assert rows[0]["constructeur"] == ["Piper", "Aircraft"]


# ---------------------------------------------------------------------------
# Tests: _build_names
# ---------------------------------------------------------------------------

class TestBuildNames:
    def test_proprietaire_provided(self):
        names = _build_names("Holding SARL")
        assert names == ["Holding SARL"]

    def test_proprietaire_placeholder_excluded(self):
        names = _build_names("PROPRIÉTAIRE PRIVÉ")
        assert names == []

    def test_copropriete_excluded(self):
        names = _build_names("COPROPRIÉTÉ")
        assert names == []

    def test_empty_string_returns_empty(self):
        names = _build_names("")
        assert names == []


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        parsed = _make_parsed()
        record = _build_record(parsed, "4A0123")
        assert record["icao_hex"] == "4A0123"
        assert record["registration"] == "LX-ABC"
        assert record["source"] == "lu-dac"
        assert record["military"] is False
        assert record["aircraft"]["manufacturer"] == "Piper"
        assert record["aircraft"]["model"] == "PA-28"
        assert record["aircraft"]["serial_number"] == "28-1234"
        assert record["registrant"]["names"] == ["Luxair SA"]

    def test_different_exploitant_not_imported(self):
        parsed = _make_parsed(exploitant="Luxair SA", proprietaire="Holding SARL")
        record = _build_record(parsed, "4A0123")
        assert record["registrant"]["names"] == ["Holding SARL"]

    def test_exploitant_alone_does_not_populate_names(self):
        parsed = _make_parsed(exploitant="Luxair SA", proprietaire="")
        record = _build_record(parsed, "4A0123")
        assert "registrant" not in record

    def test_private_proprietaire_excluded(self):
        parsed = _make_parsed(exploitant="", proprietaire="PROPRIÉTAIRE PRIVÉ")
        record = _build_record(parsed, "4A0123")
        assert "registrant" not in record

    def test_empty_manufacturer_omitted(self):
        parsed = _make_parsed(manufacturer="")
        record = _build_record(parsed, "4A0123")
        assert "manufacturer" not in record.get("aircraft", {})

    def test_empty_model_omitted(self):
        parsed = _make_parsed(model="")
        record = _build_record(parsed, "4A0123")
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        parsed = _make_parsed(serial_number="")
        record = _build_record(parsed, "4A0123")
        assert "serial_number" not in record.get("aircraft", {})

    def test_all_empty_aircraft_omits_aircraft_key(self):
        parsed = _make_parsed(manufacturer="", model="", serial_number="")
        record = _build_record(parsed, "4A0123")
        assert "aircraft" not in record


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("LXABC") == "LXABC"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("LX-ABC")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        records = [_make_parsed()]
        r = _make_redis_with_search(icao_hex="4A0123", registration="LX-ABC")
        count = write_to_redis(records, r, REDIS_TTL)
        assert count == 1

    def test_record_not_written_when_not_in_redis(self):
        records = [_make_parsed()]
        r = _make_redis_no_match()
        count = write_to_redis(records, r, REDIS_TTL)
        assert count == 0

    def test_empty_registration_skipped(self):
        records = [_make_parsed(registration="")]
        r = _make_redis_with_search()
        count = write_to_redis(records, r, REDIS_TTL)
        assert count == 0
        r.ft.return_value.search.assert_not_called()

    def test_writes_to_detail_key(self):
        records = [_make_parsed()]
        r = _make_redis_with_search(icao_hex="4A0123", registration="LX-ABC")
        write_to_redis(records, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call is not None
        key_used = set_call[0][0]
        assert key_used == "aircraft:registry:4A0123"

    def test_source_field_in_written_record(self):
        records = [_make_parsed()]
        r = _make_redis_with_search(icao_hex="4A0123", registration="LX-ABC")
        write_to_redis(records, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call is not None
        written = set_call[0][2]
        assert written.get("source") == "lu-dac"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        records = [_make_parsed()]
        r = _make_redis_with_search(icao_hex="4A0123", registration="LX-ABC")
        write_to_redis(records, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:4A0123", REDIS_TTL)

    def test_null_fields_omitted_from_written_record(self):
        records = [_make_parsed(manufacturer="", model="")]
        r = _make_redis_with_search(icao_hex="4A0123", registration="LX-ABC")
        write_to_redis(records, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call is not None
        assert set_call[0][1] == "$"
        written = set_call[0][2]
        assert "manufacturer" not in written["aircraft"]
        assert "model" not in written["aircraft"]


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
        with patch("lu_dac_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("lu_dac_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("lu_dac_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 77, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "77"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("lu_dac_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/lu-dac"


# ---------------------------------------------------------------------------
# Tests: network (real outbound HTTP call — see #405)
# ---------------------------------------------------------------------------

class TestNetwork:
    @pytest.mark.network
    def test_url_reachable(self):
        assert_url_reachable(_mod.INDEX_URL, "lu-dac", headers={"User-Agent": "P5Software SkyFollower"})
