"""Tests for the Seychelles SCAA data runner."""

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
        "sc_scaa_registry_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["sc_scaa_registry_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()


download_and_parse = _mod.download_and_parse
_build_record = _mod._build_record
_clean = _mod._clean
_escape_tag = _mod._escape_tag
_build_registration_map = _mod._build_registration_map
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
_INDEX_URL = _mod._INDEX_URL
_REG_RE = _mod._REG_RE
MQTT_ROOT = _mod.MQTT_ROOT

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HEADER_ROW = "<tr><th>Aircraft Registration</th><th>Aircraft Type</th><th>Registered Owner</th></tr>"


def _data_row(reg, model, owner):
    return f"<tr><td>{reg}</td><td>{model}</td><td>{owner}</td></tr>"


def _make_table(rows: list[str]) -> str:
    return "<table>" + _HEADER_ROW + "".join(rows) + "</table>"


def _make_html(tables: list[str]) -> bytes:
    return ("<html><body>" + "".join(tables) + "</body></html>").encode("utf-8")


def _make_session(content=b"", status_code=200):
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.content = content
    resp.text = content.decode("utf-8", errors="replace") if isinstance(content, bytes) else content
    session = MagicMock()
    session.get.return_value = resp
    return session


# ---------------------------------------------------------------------------
# _clean
# ---------------------------------------------------------------------------


class TestClean:
    def test_strips_whitespace(self):
        assert _clean("  hello  ") == "hello"

    def test_collapses_internal_whitespace(self):
        assert _clean("hello   world") == "hello world"

    def test_none_returns_empty(self):
        assert _clean(None) == ""

    def test_empty_returns_empty(self):
        assert _clean("") == ""


# ---------------------------------------------------------------------------
# _REG_RE
# ---------------------------------------------------------------------------


class TestRegRe:
    def test_valid_mark(self):
        assert _REG_RE.match("S7-VEV")

    def test_header_text_rejected(self):
        assert not _REG_RE.match("Aircraft Registration")

    def test_wrong_prefix_rejected(self):
        assert not _REG_RE.match("9Y-ABC")

    def test_empty_rejected(self):
        assert not _REG_RE.match("")


# ---------------------------------------------------------------------------
# _escape_tag
# ---------------------------------------------------------------------------


class TestEscapeTag:
    def test_plain_unchanged(self):
        assert _escape_tag("S7VEV") == "S7VEV"

    def test_hyphen_escaped(self):
        assert _escape_tag("S7-VEV") == r"S7\-VEV"


# ---------------------------------------------------------------------------
# download_and_parse
# ---------------------------------------------------------------------------


class TestDownloadAndParse:
    def test_logs_url(self):
        session = _make_session(_make_html([_make_table([])]))
        with patch.object(_mod, "logger") as mock_logger:
            download_and_parse(session)
        mock_logger.info.assert_any_call(
            "Downloading Seychelles SCAA aircraft register from %s", _INDEX_URL
        )

    def test_http_error_raises(self):
        session = _make_session(status_code=404)
        with pytest.raises(RuntimeError, match="Register page request failed with HTTP 404"):
            download_and_parse(session)

    def test_no_table_raises(self):
        session = _make_session(b"<html><body>no table here</body></html>")
        with pytest.raises(RuntimeError, match="No table found"):
            download_and_parse(session)

    def test_single_table_parsed(self):
        rows = [_data_row("S7-VEV", "A320 Neo", "GY Aviation Lease 1740 CO.Limited")]
        session = _make_session(_make_html([_make_table(rows)]))
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "S7-VEV"

    def test_multiple_tables_all_parsed(self):
        """The real page has 3 separate <table> elements, one per operator
        grouping -- every table must be parsed, not just the first."""
        table1 = _make_table([_data_row("S7-VEV", "A320 Neo", "GY Aviation Lease 1740 CO.Limited")])
        table2 = _make_table([_data_row("S7-IDC", "B1900D", "Seychelles Government")])
        table3 = _make_table([_data_row("S7-AIR", "EC120B", "Zil Air Pty Ltd.")])
        session = _make_session(_make_html([table1, table2, table3]))
        records = download_and_parse(session)
        assert len(records) == 3
        assert {r["registration"] for r in records} == {"S7-VEV", "S7-IDC", "S7-AIR"}

    def test_header_row_not_treated_as_data(self):
        rows = [_data_row("S7-VEV", "A320 Neo", "GY Aviation Lease 1740 CO.Limited")]
        session = _make_session(_make_html([_make_table(rows)]))
        records = download_and_parse(session)
        assert all(r["registration"] != "Aircraft Registration" for r in records)

    def test_government_owner_still_civil(self):
        """S7-IDC's owner is 'Seychelles Government' -- a state aircraft,
        not military; military stays False regardless."""
        rows = [_data_row("S7-IDC", "B1900D", "Seychelles Government")]
        session = _make_session(_make_html([_make_table(rows)]))
        records = download_and_parse(session)
        assert records[0]["owner"] == "Seychelles Government"

    def test_logs_parsed_count(self):
        rows = [_data_row("S7-VEV", "A320 Neo", "GY Aviation Lease 1740 CO.Limited")]
        session = _make_session(_make_html([_make_table(rows)]))
        with patch.object(_mod, "logger") as mock_logger:
            download_and_parse(session)
        mock_logger.info.assert_any_call("Parsed %d S7- records across %d table(s).", 1, 1)


# ---------------------------------------------------------------------------
# _build_record
# ---------------------------------------------------------------------------


class TestBuildRecord:
    def _row(self, **overrides):
        row = {"model": "A320 Neo", "owner": "GY Aviation Lease 1740 CO.Limited"}
        row.update(overrides)
        return row

    def test_basic_fields(self):
        record = _build_record(self._row(), "230ABC", "S7-VEV")
        assert record["icao_hex"] == "230ABC"
        assert record["registration"] == "S7-VEV"
        assert record["source"] == "sc-scaa-registry"
        assert record["military"] is False

    def test_model_stored_as_combined_string(self):
        record = _build_record(self._row(model="DHC6-400"), "230ABC", "S7-LDI")
        assert record["aircraft"]["model"] == "DHC6-400"
        assert "manufacturer" not in record["aircraft"]

    def test_registrant_names_from_owner(self):
        record = _build_record(self._row(owner="Air Seychelles Ltd."), "230ABC", "S7-LDI")
        assert record["registrant"]["names"] == ["Air Seychelles Ltd."]

    def test_government_owner_stays_civil(self):
        record = _build_record(self._row(owner="Seychelles Government"), "230ABC", "S7-IDC")
        assert record["military"] is False
        assert record["registrant"]["names"] == ["Seychelles Government"]

    def test_empty_owner_omits_registrant(self):
        record = _build_record(self._row(owner=""), "230ABC", "S7-VEV")
        assert "registrant" not in record

    def test_empty_model_omits_aircraft(self):
        record = _build_record(self._row(model=""), "230ABC", "S7-VEV")
        assert "aircraft" not in record

    def test_source_is_sc_scaa(self):
        record = _build_record(self._row(), "230ABC", "S7-VEV")
        assert record["source"] == "sc-scaa-registry"


# ---------------------------------------------------------------------------
# _build_registration_map
# ---------------------------------------------------------------------------


class TestBuildRegistrationMap:
    def _make_redis(self, docs):
        doc_mocks = []
        for icao_hex, registration in docs:
            doc = MagicMock()
            doc.id = f"aircraft:mictronics:{icao_hex}"
            doc.registration = registration
            doc_mocks.append(doc)
        result = MagicMock()
        result.docs = doc_mocks
        r = MagicMock()
        r.ft.return_value.search.return_value = result
        return r

    def test_returns_registration_to_hex_map(self):
        r = self._make_redis([("230ABC", "S7-VEV"), ("230ABD", "S7-LDI")])
        reg_map = _build_registration_map(["S7-VEV", "S7-LDI"], r)
        assert reg_map["S7-VEV"] == "230ABC"
        assert reg_map["S7-LDI"] == "230ABD"

    def test_redis_failure_logs_warning(self):
        r = MagicMock()
        r.ft.return_value.search.side_effect = Exception("connection refused")
        with patch.object(_mod, "logger") as mock_logger:
            result = _build_registration_map(["S7-VEV"], r)
        assert result == {}
        mock_logger.warning.assert_called_once()

    def test_empty_registrations_returns_empty(self):
        r = MagicMock()
        result = _build_registration_map([], r)
        assert result == {}
        r.ft.return_value.search.assert_not_called()

    def test_batches_large_lists(self):
        docs = [(f"23{i:04d}", f"S7-{i:03d}") for i in range(150)]
        r = self._make_redis(docs)
        regs = [f"S7-{i:03d}" for i in range(150)]
        _build_registration_map(regs, r)
        assert r.ft.return_value.search.call_count == 2


# ---------------------------------------------------------------------------
# write_to_redis
# ---------------------------------------------------------------------------


class TestWriteToRedis:
    def _row(self, registration="S7-VEV"):
        return {"registration": registration, "model": "A320 Neo", "owner": "GY Aviation Lease 1740 CO.Limited"}

    def test_writes_found_registrations(self):
        rows = [self._row()]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"S7-VEV": "230ABC"}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 1
        pipe.json.return_value.set.assert_called_once()
        pipe.expire.assert_called_once()

    def test_skips_no_redis_match(self):
        rows = [self._row()]
        r = MagicMock()
        r.pipeline.return_value = MagicMock()
        with patch.object(_mod, "_build_registration_map", return_value={}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 0

    def test_uses_aircraft_registry_key(self):
        rows = [self._row()]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"S7-VEV": "230ABC"}):
            write_to_redis(rows, r, 1209600)
        set_call = pipe.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:230ABC"

    def test_pipeline_error_logs_warning(self):
        rows = [self._row()]
        r = MagicMock()
        pipe = MagicMock()
        pipe.execute.side_effect = Exception("Redis timeout")
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"S7-VEV": "230ABC"}):
            with patch.object(_mod, "logger") as mock_logger:
                write_to_redis(rows, r, 1209600)
        mock_logger.warning.assert_called()


# ---------------------------------------------------------------------------
# publish_completion_stats
# ---------------------------------------------------------------------------


class TestPublishCompletionStats:
    def _setup_mock_client(self):
        mock_client = MagicMock()

        def fake_connect(host, port, keepalive):
            mock_client.on_connect(mock_client, None, None, 0, None)

        mock_client.connect.side_effect = fake_connect
        return mock_client

    def test_missing_mqtt_block_skips(self):
        publish_completion_stats({}, 100, "success")

    def test_blank_host_skips_without_crashing(self):
        cfg = {"mqtt": {"host": "", "port": 1883, "username": "", "password": ""}}
        publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch.object(_mod.mqtt, "Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 19, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch.object(_mod.mqtt, "Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "Failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/sc-scaa-registry"
