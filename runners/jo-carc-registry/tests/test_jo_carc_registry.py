"""Tests for the Jordan CARC data runner."""

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
        "jo_carc_registry_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["jo_carc_registry_main"] = mod
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
_PAGE_TITLE_ROW = _mod._PAGE_TITLE_ROW
MQTT_ROOT = _mod.MQTT_ROOT

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HEADER_ROW = "<tr><td>Manufacturer</td><td>Model</td><td>Category</td><td>MSN</td><td>Reg Mark</td><td>Reg. No.</td><td>Reg. Date</td></tr>"


def _operator_row(name):
    return f"<tr><td>{name}</td></tr>"


def _data_row(manufacturer, model, msn, reg, regno="000", date="01/01/00", category="Transport"):
    return f"<tr><td>{manufacturer}</td><td>{model}</td><td>{category}</td><td>{msn}</td><td>{reg}</td><td>{regno}</td><td>{date}</td></tr>"


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
# _clean
# ---------------------------------------------------------------------------


class TestClean:
    def test_strips_whitespace(self):
        assert _clean("  hello  ") == "hello"

    def test_collapses_internal_whitespace(self):
        assert _clean("hello   world") == "hello world"

    def test_non_breaking_space_normalized(self):
        """Real source has a non-breaking space in one operator name
        ('Airlines\\xa0Solitaire Air Ltd. Co.')."""
        assert _clean("Airlines\xa0Solitaire") == "Airlines Solitaire"

    def test_none_returns_empty(self):
        assert _clean(None) == ""

    def test_empty_returns_empty(self):
        assert _clean("") == ""


# ---------------------------------------------------------------------------
# _REG_RE
# ---------------------------------------------------------------------------


class TestRegRe:
    def test_three_char_suffix(self):
        assert _REG_RE.match("JY-AGQ")

    def test_header_text_rejected(self):
        assert not _REG_RE.match("Reg Mark")

    def test_empty_rejected(self):
        assert not _REG_RE.match("")

    def test_wrong_prefix_rejected(self):
        assert not _REG_RE.match("9Y-ABC")


# ---------------------------------------------------------------------------
# _escape_tag
# ---------------------------------------------------------------------------


class TestEscapeTag:
    def test_plain_unchanged(self):
        assert _escape_tag("JYAGQ") == "JYAGQ"

    def test_hyphen_escaped(self):
        assert _escape_tag("JY-AGQ") == r"JY\-AGQ"


# ---------------------------------------------------------------------------
# download_and_parse
# ---------------------------------------------------------------------------


class TestDownloadAndParse:
    def test_logs_url(self):
        session = _make_session(_make_html([]))
        with patch.object(_mod, "logger") as mock_logger:
            download_and_parse(session)
        mock_logger.info.assert_any_call(
            "Downloading Jordan CARC aircraft register from %s", _INDEX_URL
        )

    def test_http_error_raises(self):
        session = _make_session(status_code=404)
        with pytest.raises(RuntimeError, match="Register page request failed with HTTP 404"):
            download_and_parse(session)

    def test_no_table_raises(self):
        session = _make_session(b"<html><body>no table here</body></html>")
        with pytest.raises(RuntimeError, match="No table found"):
            download_and_parse(session)

    def test_single_operator_section(self):
        rows = [
            _operator_row("Royal Jordanian Airlines"),
            _HEADER_ROW,
            _data_row("Airbus", "A310-304", "445", "JY-AGQ"),
        ]
        session = _make_session(_make_html(rows))
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "JY-AGQ"
        assert records[0]["operator"] == "Royal Jordanian Airlines"

    def test_page_title_row_does_not_become_operator(self):
        """The real page has a stray 'Jordanian Registered Aircraft' row
        right after the first operator name -- must not overwrite the real
        current operator."""
        rows = [
            _operator_row("Royal Jordanian Airlines"),
            _operator_row(_PAGE_TITLE_ROW),
            _HEADER_ROW,
            _data_row("Airbus", "A310-304", "445", "JY-AGQ"),
        ]
        session = _make_session(_make_html(rows))
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["operator"] == "Royal Jordanian Airlines"

    def test_multiple_operator_sections(self):
        rows = [
            _operator_row("Royal Jordanian Airlines"),
            _HEADER_ROW,
            _data_row("Airbus", "A310-304", "445", "JY-AGQ"),
            _operator_row("Jordan Aviation"),
            _HEADER_ROW,
            _data_row("Boeing", "B737-322", "24662", "JY-JAD"),
            _operator_row("Arab Wings"),
            _HEADER_ROW,
            _data_row("Beechcraft", "Hawker 800XP", "258520", "JY-AWD"),
        ]
        session = _make_session(_make_html(rows))
        records = download_and_parse(session)
        assert len(records) == 3
        operators = {r["registration"]: r["operator"] for r in records}
        assert operators["JY-AGQ"] == "Royal Jordanian Airlines"
        assert operators["JY-JAD"] == "Jordan Aviation"
        assert operators["JY-AWD"] == "Arab Wings"

    def test_category_not_captured_in_row(self):
        """Category is always 'Transport' and never mapped to aircraft.type
        -- confirm it's simply absent from the parsed row dict."""
        rows = [
            _operator_row("Royal Jordanian Airlines"),
            _HEADER_ROW,
            _data_row("Airbus", "A310-304", "445", "JY-AGQ"),
        ]
        session = _make_session(_make_html(rows))
        records = download_and_parse(session)
        assert "category" not in records[0]

    def test_non_matching_registration_rejected(self):
        rows = [
            _operator_row("Royal Jordanian Airlines"),
            _HEADER_ROW,
        ]
        session = _make_session(_make_html(rows))
        records = download_and_parse(session)
        assert len(records) == 0

    def test_logs_parsed_count(self):
        rows = [
            _operator_row("Royal Jordanian Airlines"),
            _HEADER_ROW,
            _data_row("Airbus", "A310-304", "445", "JY-AGQ"),
        ]
        session = _make_session(_make_html(rows))
        with patch.object(_mod, "logger") as mock_logger:
            download_and_parse(session)
        mock_logger.info.assert_any_call("Parsed %d JY- records.", 1)


# ---------------------------------------------------------------------------
# _build_record
# ---------------------------------------------------------------------------


class TestBuildRecord:
    def _row(self, **overrides):
        row = {
            "manufacturer": "Airbus",
            "model": "A310-304",
            "serial": "445",
            "operator": "Royal Jordanian Airlines",
        }
        row.update(overrides)
        return row

    def test_basic_fields(self):
        record = _build_record(self._row(), "740ABC", "JY-AGQ")
        assert record["icao_hex"] == "740ABC"
        assert record["registration"] == "JY-AGQ"
        assert record["source"] == "jo-carc-registry"
        assert record["military"] is False

    def test_manufacturer(self):
        record = _build_record(self._row(manufacturer="Boeing"), "740ABC", "JY-JAD")
        assert record["aircraft"]["manufacturer"] == "Boeing"

    def test_model(self):
        record = _build_record(self._row(model="B737-322"), "740ABC", "JY-JAD")
        assert record["aircraft"]["model"] == "B737-322"

    def test_serial_number(self):
        record = _build_record(self._row(serial="24662"), "740ABC", "JY-JAD")
        assert record["aircraft"]["serial_number"] == "24662"

    def test_registrant_names_from_operator_section(self):
        """No owner column exists in this source -- the operator-section
        heading fills the registrant-identity role directly."""
        record = _build_record(self._row(operator="Arab Wings"), "740ABC", "JY-AWD")
        assert record["registrant"]["names"] == ["Arab Wings"]

    def test_empty_operator_omits_registrant(self):
        record = _build_record(self._row(operator=""), "740ABC", "JY-AGQ")
        assert "registrant" not in record

    def test_empty_fields_omit_aircraft(self):
        record = _build_record(self._row(manufacturer="", model="", serial=""), "740ABC", "JY-AGQ")
        assert "aircraft" not in record

    def test_source_is_jo_carc(self):
        record = _build_record(self._row(), "740ABC", "JY-AGQ")
        assert record["source"] == "jo-carc-registry"


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
        r = self._make_redis([("740ABC", "JY-AGQ"), ("740ABD", "JY-JAD")])
        reg_map = _build_registration_map(["JY-AGQ", "JY-JAD"], r)
        assert reg_map["JY-AGQ"] == "740ABC"
        assert reg_map["JY-JAD"] == "740ABD"

    def test_redis_failure_logs_warning(self):
        r = MagicMock()
        r.ft.return_value.search.side_effect = Exception("connection refused")
        with patch.object(_mod, "logger") as mock_logger:
            result = _build_registration_map(["JY-AGQ"], r)
        assert result == {}
        mock_logger.warning.assert_called_once()

    def test_empty_registrations_returns_empty(self):
        r = MagicMock()
        result = _build_registration_map([], r)
        assert result == {}
        r.ft.return_value.search.assert_not_called()

    def test_batches_large_lists(self):
        docs = [(f"74{i:04d}", f"JY-{i:03d}") for i in range(150)]
        r = self._make_redis(docs)
        regs = [f"JY-{i:03d}" for i in range(150)]
        _build_registration_map(regs, r)
        assert r.ft.return_value.search.call_count == 2


# ---------------------------------------------------------------------------
# write_to_redis
# ---------------------------------------------------------------------------


class TestWriteToRedis:
    def _row(self, registration="JY-AGQ"):
        return {
            "registration": registration,
            "manufacturer": "Airbus",
            "model": "A310-304",
            "serial": "445",
            "operator": "Royal Jordanian Airlines",
        }

    def test_writes_found_registrations(self):
        rows = [self._row()]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"JY-AGQ": "740ABC"}):
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
        with patch.object(_mod, "_build_registration_map", return_value={"JY-AGQ": "740ABC"}):
            write_to_redis(rows, r, 1209600)
        set_call = pipe.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:740ABC"

    def test_pipeline_error_logs_warning(self):
        rows = [self._row()]
        r = MagicMock()
        pipe = MagicMock()
        pipe.execute.side_effect = Exception("Redis timeout")
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"JY-AGQ": "740ABC"}):
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
                publish_completion_stats(cfg, 48, "success")
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
        assert MQTT_ROOT == "SkyFollower/runner/jo-carc-registry"
