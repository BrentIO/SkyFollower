"""Tests for the Bosnia and Herzegovina BHDCA data runner."""

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
        "ba_bhdca_registry_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ba_bhdca_registry_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()


_discover_pdf_url = _mod._discover_pdf_url
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


def _make_row(
    registration="E7-ABC",
    model="P2006T",
    manufacturer="Construzioni Aeronautiche TECNAM S.r.l.",
    serial="175",
    owner="Auto Gyro Adriatic d.o.o.",
):
    return {
        "registration": registration,
        "model": model,
        "manufacturer": manufacturer,
        "serial": serial,
        "owner": owner,
    }


def _make_session(content=b"", status_code=200):
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.content = content
    resp.text = content.decode("utf-8", errors="replace") if isinstance(content, bytes) else content
    session = MagicMock()
    session.get.return_value = resp
    return session


def _make_pdf_mock(tables_by_page):
    pages = []
    for table in tables_by_page:
        page = MagicMock()
        page.extract_table.return_value = table
        pages.append(page)
    pdf_mock = MagicMock()
    pdf_mock.__enter__ = MagicMock(return_value=pdf_mock)
    pdf_mock.__exit__ = MagicMock(return_value=False)
    pdf_mock.pages = pages
    return pdf_mock


_INDEX_HTML = b"""
<html><body>
<a href="/english/dokumenti/airworthiness/BiH%20Aircraft%20Register_eng.pdf">Aircraft Register</a>
<a href="/english/dokumenti/airworthiness/List%20of%20approved%20maintenance%20organization.pdf">Approved MOs</a>
</body></html>
"""

_INDEX_HTML_FULL_URL = b"""
<html><body>
<a href="http://www.bhdca.gov.ba/english/dokumenti/airworthiness/BiH%20Aircraft%20Register_eng.pdf">Aircraft Register</a>
</body></html>
"""

_HEADER_ROW = ["", "Registrаtion\nmark", "Designation", "Manifacturer", "Serial number", "Owner", "Registration\ndate"]


# ---------------------------------------------------------------------------
# _clean
# ---------------------------------------------------------------------------


class TestClean:
    def test_strips_whitespace(self):
        assert _clean("  hello  ") == "hello"

    def test_collapses_internal_whitespace(self):
        assert _clean("hello   world") == "hello world"

    def test_newlines_become_space(self):
        assert _clean("hello\nworld") == "hello world"

    def test_none_returns_empty(self):
        assert _clean(None) == ""

    def test_empty_returns_empty(self):
        assert _clean("") == ""


# ---------------------------------------------------------------------------
# _REG_RE
# ---------------------------------------------------------------------------


class TestRegRe:
    def test_valid_three_char_suffix(self):
        assert _REG_RE.match("E7-ABC")

    def test_valid_alphanumeric_suffix(self):
        assert _REG_RE.match("E7-5341")

    def test_valid_longer_suffix(self):
        assert _REG_RE.match("E7-M006")

    def test_header_text_rejected(self):
        assert not _REG_RE.match("Registrаtion mark")

    def test_empty_rejected(self):
        assert not _REG_RE.match("")

    def test_wrong_prefix_rejected(self):
        assert not _REG_RE.match("9A-ABC")

    def test_lowercase_rejected(self):
        assert not _REG_RE.match("e7-abc")

    def test_row_number_rejected(self):
        assert not _REG_RE.match("1.")


# ---------------------------------------------------------------------------
# _escape_tag
# ---------------------------------------------------------------------------


class TestEscapeTag:
    def test_plain_unchanged(self):
        assert _escape_tag("E7ABC") == "E7ABC"

    def test_hyphen_escaped(self):
        assert _escape_tag("E7-ABC") == r"E7\-ABC"


# ---------------------------------------------------------------------------
# _discover_pdf_url
# ---------------------------------------------------------------------------


class TestDiscoverPdfUrl:
    def test_returns_absolute_url_from_relative_href(self):
        session = _make_session(_INDEX_HTML)
        url = _discover_pdf_url(session)
        assert url == (
            "http://www.bhdca.gov.ba/english/dokumenti/airworthiness/"
            "BiH%20Aircraft%20Register_eng.pdf"
        )

    def test_returns_absolute_url_unchanged(self):
        session = _make_session(_INDEX_HTML_FULL_URL)
        url = _discover_pdf_url(session)
        assert url == (
            "http://www.bhdca.gov.ba/english/dokumenti/airworthiness/"
            "BiH%20Aircraft%20Register_eng.pdf"
        )

    def test_ignores_maintenance_organization_links(self):
        session = _make_session(_INDEX_HTML)
        url = _discover_pdf_url(session)
        assert "Aircraft%20Register" in url

    def test_logs_index_url(self):
        session = _make_session(_INDEX_HTML)
        with patch.object(_mod, "logger") as mock_logger:
            _discover_pdf_url(session)
        mock_logger.info.assert_any_call(
            "Downloading BHDCA airworthiness index page from %s", _INDEX_URL
        )

    def test_http_error_raises(self):
        session = _make_session(status_code=404)
        with pytest.raises(RuntimeError, match="Index page request failed with HTTP 404"):
            _discover_pdf_url(session)

    def test_no_pdf_link_raises(self):
        session = _make_session(b"<html><body><a href='/other/page'>no pdf</a></body></html>")
        with pytest.raises(RuntimeError, match="No Aircraft Register PDF link found"):
            _discover_pdf_url(session)


# ---------------------------------------------------------------------------
# download_and_parse
# ---------------------------------------------------------------------------


class TestDownloadAndParse:
    def _make_two_call_session(self, index_html, pdf_bytes=b"%PDF-1.4", pdf_status=200):
        index_resp = MagicMock()
        index_resp.ok = True
        index_resp.status_code = 200
        index_resp.content = index_html
        index_resp.text = index_html.decode()

        pdf_resp = MagicMock()
        pdf_resp.ok = pdf_status < 400
        pdf_resp.status_code = pdf_status
        pdf_resp.content = pdf_bytes

        session = MagicMock()
        session.get.side_effect = [index_resp, pdf_resp]
        return session

    def test_logs_pdf_url(self):
        session = self._make_two_call_session(_INDEX_HTML)
        pdf_mock = _make_pdf_mock([[]])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            with patch.object(_mod, "logger") as mock_logger:
                download_and_parse(session)
        mock_logger.info.assert_any_call(
            "Downloading BHDCA aircraft register from %s",
            (
                "http://www.bhdca.gov.ba/english/dokumenti/airworthiness/"
                "BiH%20Aircraft%20Register_eng.pdf"
            ),
        )

    def test_pdf_http_error_raises(self):
        session = self._make_two_call_session(_INDEX_HTML, pdf_status=503)
        with pytest.raises(RuntimeError, match="PDF request failed with HTTP 503"):
            download_and_parse(session)

    def test_parses_valid_rows(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [
            _HEADER_ROW,
            ["1.", "E7-ABC", "P2006T", "Construzioni Aeronautiche TECNAM S.r.l.", "175", "Owner A", "16.05.16"],
            ["2.", "E7-XYZ", "MTO-Sport", "AutoGyro GmbH", "M01333", "Owner B", "08.7.16"],
        ]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 2
        assert rows[0]["registration"] == "E7-ABC"
        assert rows[1]["registration"] == "E7-XYZ"

    def test_skips_header_row(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [
            _HEADER_ROW,
            ["1.", "E7-ABC", "P2006T", "TECNAM", "175", "Owner", "16.05.16"],
        ]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 1

    def test_skips_rows_too_short(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [
            ["1.", "E7-ABC", "P2006T"],  # only 3 cols
            ["2.", "E7-XYZ", "MTO-Sport", "AutoGyro GmbH", "M01333", "Owner", "08.7.16"],
        ]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 1

    def test_skips_empty_pages(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [["1.", "E7-ABC", "P2006T", "TECNAM", "175", "Owner", "16.05.16"]]
        pdf_mock = _make_pdf_mock([None, table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 1

    def test_multi_page_aggregated(self):
        session = self._make_two_call_session(_INDEX_HTML)
        page1 = [["1.", "E7-AAA", "P2006T", "TECNAM", "175", "Owner A", "16.05.16"]]
        page2 = [["2.", "E7-BBB", "MTO-Sport", "AutoGyro GmbH", "M01333", "Owner B", "08.7.16"]]
        pdf_mock = _make_pdf_mock([page1, page2])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 2

    def test_newlines_in_cells_collapsed(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [["1.", "E7-ABC", "P2006T", "TEC\nNAM", "175", "Own\ner", "16.05.16"]]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert rows[0]["manufacturer"] == "TEC NAM"
        assert rows[0]["owner"] == "Own er"

    def test_stray_space_after_hyphen_normalized(self):
        """The real source PDF has a few rows like 'E7- NEL' (space after
        the hyphen) instead of 'E7-NEL' — must still be picked up."""
        session = self._make_two_call_session(_INDEX_HTML)
        table = [["91.", "E7- NEL", "SAAB SF 340A", "SAAB AB", "340 A-078", "ICAR AIR", "10.10.23."]]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 1
        assert rows[0]["registration"] == "E7-NEL"

    def test_non_matching_registration_rejected(self):
        """Rows whose registration column doesn't match E7- (e.g. a footer/note row) are dropped."""
        session = self._make_two_call_session(_INDEX_HTML)
        table = [["", "Note: register current as of...", "", "", "", "", ""]]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 0

    def test_logs_parsed_count(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [["1.", "E7-ABC", "P2006T", "TECNAM", "175", "Owner", "16.05.16"]]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            with patch.object(_mod, "logger") as mock_logger:
                download_and_parse(session)
        mock_logger.info.assert_any_call("Parsed %d E7- records.", 1)


# ---------------------------------------------------------------------------
# _build_record
# ---------------------------------------------------------------------------


class TestBuildRecord:
    def test_basic_fields(self):
        record = _build_record(_make_row(), "501234", "E7-ABC")
        assert record["icao_hex"] == "501234"
        assert record["registration"] == "E7-ABC"
        assert record["source"] == "ba-bhdca-registry"
        assert record["military"] is False

    def test_aircraft_model(self):
        record = _build_record(_make_row(model="P2006T"), "501234", "E7-ABC")
        assert record["aircraft"]["model"] == "P2006T"

    def test_aircraft_manufacturer(self):
        record = _build_record(_make_row(manufacturer="AutoGyro GmbH"), "501234", "E7-ABC")
        assert record["aircraft"]["manufacturer"] == "AutoGyro GmbH"

    def test_aircraft_serial_number(self):
        record = _build_record(_make_row(serial="M01333"), "501234", "E7-ABC")
        assert record["aircraft"]["serial_number"] == "M01333"

    def test_registrant_names(self):
        record = _build_record(_make_row(owner="Auto Gyro Adriatic d.o.o."), "501234", "E7-ABC")
        assert record["registrant"]["names"] == ["Auto Gyro Adriatic d.o.o."]

    def test_empty_owner_omits_registrant(self):
        record = _build_record(_make_row(owner=""), "501234", "E7-ABC")
        assert "registrant" not in record

    def test_empty_fields_omit_aircraft(self):
        record = _build_record(_make_row(model="", manufacturer="", serial=""), "501234", "E7-ABC")
        assert "aircraft" not in record

    def test_source_is_ba_bhdca(self):
        record = _build_record(_make_row(), "501234", "E7-ABC")
        assert record["source"] == "ba-bhdca-registry"


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
        r = self._make_redis([("501234", "E7-ABC"), ("501235", "E7-XYZ")])
        reg_map = _build_registration_map(["E7-ABC", "E7-XYZ"], r)
        assert reg_map["E7-ABC"] == "501234"
        assert reg_map["E7-XYZ"] == "501235"

    def test_redis_failure_logs_warning(self):
        r = MagicMock()
        r.ft.return_value.search.side_effect = Exception("connection refused")
        with patch.object(_mod, "logger") as mock_logger:
            result = _build_registration_map(["E7-ABC"], r)
        assert result == {}
        mock_logger.warning.assert_called_once()

    def test_empty_registrations_returns_empty(self):
        r = MagicMock()
        result = _build_registration_map([], r)
        assert result == {}
        r.ft.return_value.search.assert_not_called()

    def test_batches_large_lists(self):
        docs = [(f"50{i:04d}", f"E7-{i:03d}") for i in range(150)]
        r = self._make_redis(docs)
        regs = [f"E7-{i:03d}" for i in range(150)]
        _build_registration_map(regs, r)
        assert r.ft.return_value.search.call_count == 2


# ---------------------------------------------------------------------------
# write_to_redis
# ---------------------------------------------------------------------------


class TestWriteToRedis:
    def test_writes_found_registrations(self):
        rows = [_make_row(registration="E7-ABC")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"E7-ABC": "501234"}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 1
        pipe.json.return_value.set.assert_called_once()
        pipe.expire.assert_called_once()

    def test_skips_no_redis_match(self):
        rows = [_make_row(registration="E7-ABC")]
        r = MagicMock()
        r.pipeline.return_value = MagicMock()
        with patch.object(_mod, "_build_registration_map", return_value={}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 0

    def test_skips_empty_registration(self):
        rows = [_make_row(registration="")]
        r = MagicMock()
        r.pipeline.return_value = MagicMock()
        with patch.object(_mod, "_build_registration_map", return_value={}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 0

    def test_uses_aircraft_registry_key(self):
        rows = [_make_row(registration="E7-ABC")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"E7-ABC": "501234"}):
            write_to_redis(rows, r, 1209600)
        set_call = pipe.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:501234"

    def test_pipeline_error_logs_warning(self):
        rows = [_make_row(registration="E7-ABC")]
        r = MagicMock()
        pipe = MagicMock()
        pipe.execute.side_effect = Exception("Redis timeout")
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"E7-ABC": "501234"}):
            with patch.object(_mod, "logger") as mock_logger:
                write_to_redis(rows, r, 1209600)
        mock_logger.warning.assert_called()

    def test_null_fields_omitted_from_written_record(self):
        rows = [_make_row(registration="E7-ABC", manufacturer="")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"E7-ABC": "501234"}):
            write_to_redis(rows, r, 1209600)
        set_call = pipe.json.return_value.set.call_args
        assert "manufacturer" not in set_call[0][2]["aircraft"]


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
        """cfg with no "mqtt" key at all (e.g. an incomplete test double)."""
        publish_completion_stats({}, 100, "success")

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
        publish_completion_stats(cfg, 100, "success")

    def test_mqtt_connect_timeout_does_not_raise(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        with patch.object(_mod, "mqtt") as mock_mqtt_module:
            mock_client = MagicMock()
            mock_mqtt_module.Client.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch.object(_mod.mqtt, "Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 103, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch.object(_mod.mqtt, "Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 103, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "103"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch.object(_mod.mqtt, "Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "Failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/ba-bhdca-registry"
