"""Tests for the Macau AACM data runner."""

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
        "mo_aacm_registry_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["mo_aacm_registry_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()


_expand_table = _mod._expand_table
download_and_parse = _mod.download_and_parse
_build_record = _mod._build_record
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

_OPERATOR_AIR_MACAU = "澳門航空股份有限公司"
_OPERATOR_ASIA_PACIFIC = "亞太航空有限公司"
_HEADER_ROW = "<tr><th>經營人</th><th>註冊編號</th><th>型號</th></tr>"


def _first_row(operator, reg, model, rowspan):
    return f'<tr><td rowspan="{rowspan}">{operator}</td><td>{reg}</td><td>{model}</td></tr>'


def _cont_row(reg, model):
    return f"<tr><td>{reg}</td><td>{model}</td></tr>"


def _make_html(rows: list[str]) -> bytes:
    return ("<html><body><table>" + "".join(rows) + "</table></body></html>").encode("utf-8")


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
# _expand_table
# ---------------------------------------------------------------------------


class TestExpandTable:
    def test_rowspan_carried_forward(self):
        html = _make_html([
            _HEADER_ROW,
            _first_row(_OPERATOR_AIR_MACAU, "B-MBA", "空中巴士 A321-231", 3),
            _cont_row("B-MBB", "空中巴士 A321-231"),
            _cont_row("B-MBC", "空中巴士 A320-232"),
        ])
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        grid = _expand_table(table)
        assert len(grid) == 4
        assert grid[1] == [_OPERATOR_AIR_MACAU, "B-MBA", "空中巴士 A321-231"]
        assert grid[2] == [_OPERATOR_AIR_MACAU, "B-MBB", "空中巴士 A321-231"]
        assert grid[3] == [_OPERATOR_AIR_MACAU, "B-MBC", "空中巴士 A320-232"]

    def test_second_operator_after_rowspan_exhausted(self):
        html = _make_html([
            _HEADER_ROW,
            _first_row(_OPERATOR_AIR_MACAU, "B-MBA", "空中巴士 A321-231", 1),
            _first_row(_OPERATOR_ASIA_PACIFIC, "B-MHI", "阿古斯塔AW139", 2),
            _cont_row("B-MHN", "阿古斯塔AW139"),
        ])
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        table = soup.find("table")
        grid = _expand_table(table)
        assert grid[1][0] == _OPERATOR_AIR_MACAU
        assert grid[2][0] == _OPERATOR_ASIA_PACIFIC
        assert grid[3][0] == _OPERATOR_ASIA_PACIFIC


# ---------------------------------------------------------------------------
# _REG_RE
# ---------------------------------------------------------------------------


class TestRegRe:
    def test_valid_mark(self):
        assert _REG_RE.match("B-MBA")

    def test_header_text_rejected(self):
        assert not _REG_RE.match("註冊編號")

    def test_wrong_prefix_rejected(self):
        assert not _REG_RE.match("9Y-ABC")

    def test_empty_rejected(self):
        assert not _REG_RE.match("")


# ---------------------------------------------------------------------------
# _escape_tag
# ---------------------------------------------------------------------------


class TestEscapeTag:
    def test_plain_unchanged(self):
        assert _escape_tag("BMBA") == "BMBA"

    def test_hyphen_escaped(self):
        assert _escape_tag("B-MBA") == r"B\-MBA"


# ---------------------------------------------------------------------------
# download_and_parse
# ---------------------------------------------------------------------------


class TestDownloadAndParse:
    def test_logs_url(self):
        session = _make_session(_make_html([_HEADER_ROW]))
        with patch.object(_mod, "logger") as mock_logger:
            download_and_parse(session)
        mock_logger.info.assert_any_call(
            "Downloading Macau AACM aircraft register from %s", _INDEX_URL
        )

    def test_http_error_raises(self):
        session = _make_session(status_code=404)
        with pytest.raises(RuntimeError, match="Register page request failed with HTTP 404"):
            download_and_parse(session)

    def test_no_table_raises(self):
        session = _make_session(b"<html><body>no table here</body></html>")
        with pytest.raises(RuntimeError, match="No table found"):
            download_and_parse(session)

    def test_parses_rowspan_grouped_records(self):
        rows = [
            _HEADER_ROW,
            _first_row(_OPERATOR_AIR_MACAU, "B-MBA", "空中巴士 A321-231", 2),
            _cont_row("B-MBB", "空中巴士 A321-231"),
            _first_row(_OPERATOR_ASIA_PACIFIC, "B-MHI", "阿古斯塔AW139", 1),
        ]
        session = _make_session(_make_html(rows))
        records = download_and_parse(session)
        assert len(records) == 3
        assert records[0]["registration"] == "B-MBA"
        assert records[0]["operator"] == _OPERATOR_AIR_MACAU
        assert records[2]["operator"] == _OPERATOR_ASIA_PACIFIC

    def test_split_registration_text_node_normalized(self):
        """Real source has at least one registration cell split across
        text nodes (renders as 'B-MB U' instead of 'B-MBU') -- must
        recover the mark by stripping internal whitespace."""
        rows = [
            _HEADER_ROW,
            '<tr><td rowspan="1">' + _OPERATOR_AIR_MACAU + '</td><td>B-MB<span> </span>U</td><td>空中巴士 A321-271NX</td></tr>',
        ]
        session = _make_session(_make_html(rows))
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "B-MBU"

    def test_non_matching_registration_rejected(self):
        rows = [_HEADER_ROW, '<tr><td rowspan="1">Note</td><td>not-a-mark</td><td></td></tr>']
        session = _make_session(_make_html(rows))
        records = download_and_parse(session)
        assert len(records) == 0

    def test_logs_parsed_count(self):
        rows = [_HEADER_ROW, _first_row(_OPERATOR_AIR_MACAU, "B-MBA", "空中巴士 A321-231", 1)]
        session = _make_session(_make_html(rows))
        with patch.object(_mod, "logger") as mock_logger:
            download_and_parse(session)
        mock_logger.info.assert_any_call("Parsed %d B-M records.", 1)


# ---------------------------------------------------------------------------
# _build_record
# ---------------------------------------------------------------------------


class TestBuildRecord:
    def _row(self, **overrides):
        row = {"operator": _OPERATOR_AIR_MACAU, "model": "空中巴士 A321-231"}
        row.update(overrides)
        return row

    def test_basic_fields(self):
        record = _build_record(self._row(), "7C1234", "B-MBA")
        assert record["icao_hex"] == "7C1234"
        assert record["registration"] == "B-MBA"
        assert record["source"] == "mo-aacm-registry"
        assert record["military"] is False

    def test_model_stored_as_combined_string(self):
        record = _build_record(self._row(model="空中巴士 A321-231"), "7C1234", "B-MBA")
        assert record["aircraft"]["model"] == "空中巴士 A321-231"
        assert "manufacturer" not in record["aircraft"]

    def test_cjk_operator_round_trips_into_registrant_names(self):
        """No owner column exists in this source -- the operator name
        (genuine Chinese text) fills the registrant-identity role
        directly and must round-trip through the record unmangled."""
        record = _build_record(self._row(operator=_OPERATOR_ASIA_PACIFIC), "7C1234", "B-MHI")
        assert record["registrant"]["names"] == [_OPERATOR_ASIA_PACIFIC]
        assert record["registrant"]["names"][0] == "亞太航空有限公司"

    def test_empty_operator_omits_registrant(self):
        record = _build_record(self._row(operator=""), "7C1234", "B-MBA")
        assert "registrant" not in record

    def test_empty_model_omits_aircraft(self):
        record = _build_record(self._row(model=""), "7C1234", "B-MBA")
        assert "aircraft" not in record

    def test_source_is_mo_aacm(self):
        record = _build_record(self._row(), "7C1234", "B-MBA")
        assert record["source"] == "mo-aacm-registry"


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
        r = self._make_redis([("7C1234", "B-MBA"), ("7C1235", "B-MHI")])
        reg_map = _build_registration_map(["B-MBA", "B-MHI"], r)
        assert reg_map["B-MBA"] == "7C1234"
        assert reg_map["B-MHI"] == "7C1235"

    def test_redis_failure_logs_warning(self):
        r = MagicMock()
        r.ft.return_value.search.side_effect = Exception("connection refused")
        with patch.object(_mod, "logger") as mock_logger:
            result = _build_registration_map(["B-MBA"], r)
        assert result == {}
        mock_logger.warning.assert_called_once()

    def test_empty_registrations_returns_empty(self):
        r = MagicMock()
        result = _build_registration_map([], r)
        assert result == {}
        r.ft.return_value.search.assert_not_called()

    def test_batches_large_lists(self):
        docs = [(f"7C{i:04d}", f"B-M{i:02d}") for i in range(150)]
        r = self._make_redis(docs)
        regs = [f"B-M{i:02d}" for i in range(150)]
        _build_registration_map(regs, r)
        assert r.ft.return_value.search.call_count == 2


# ---------------------------------------------------------------------------
# write_to_redis
# ---------------------------------------------------------------------------


class TestWriteToRedis:
    def _row(self, registration="B-MBA"):
        return {"registration": registration, "operator": _OPERATOR_AIR_MACAU, "model": "空中巴士 A321-231"}

    def test_writes_found_registrations(self):
        rows = [self._row()]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"B-MBA": "7C1234"}):
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
        with patch.object(_mod, "_build_registration_map", return_value={"B-MBA": "7C1234"}):
            write_to_redis(rows, r, 1209600)
        set_call = pipe.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:7C1234"

    def test_pipeline_error_logs_warning(self):
        rows = [self._row()]
        r = MagicMock()
        pipe = MagicMock()
        pipe.execute.side_effect = Exception("Redis timeout")
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"B-MBA": "7C1234"}):
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
                publish_completion_stats(cfg, 25, "success")
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
        assert MQTT_ROOT == "SkyFollower/runner/mo-aacm-registry"
