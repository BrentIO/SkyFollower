"""Tests for the Bahamas CAA data runner."""

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
        "bs_caa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["bs_caa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

from shared.url_reachability import assert_url_reachable

_find_download_url = _mod._find_download_url
download_registry = _mod.download_registry
parse_pdf = _mod.parse_pdf
_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
_type_tokens = _mod._type_tokens
_type_check_passes = _mod._type_check_passes
_build_registration_map = _mod._build_registration_map
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
INDEX_URL = _mod.INDEX_URL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_HEADERS = [
    "AIRCRAFT REGISTRATION NUMBER",
    "AIRCRAFT TYPE - MAKE/MODEL",
    "SERIAL #",
    "REGISTERED OWNER OF AIRCRAFT",
]


def _make_row(
    registration="C6-ABC",
    make_model="Cessna 172S",
    serial="172S12345",
    owner="John Smith",
) -> dict:
    return {
        "AIRCRAFT REGISTRATION NUMBER": registration,
        "AIRCRAFT TYPE - MAKE/MODEL": make_model,
        "SERIAL #": serial,
        "REGISTERED OWNER OF AIRCRAFT": owner,
    }


def _make_pdf_mock(pages_tables: list):
    """Build a mock object mimicking pdfplumber.open()'s context manager.

    ``pages_tables`` is a list of table results (one per page), where each
    table result is either a list of rows (list of cell values) or None to
    simulate a page with no detected table.
    """
    pages = []
    for table in pages_tables:
        page = MagicMock()
        page.extract_table.return_value = table
        pages.append(page)
    pdf_mock = MagicMock()
    pdf_mock.pages = pages
    pdf_mock.__enter__ = MagicMock(return_value=pdf_mock)
    pdf_mock.__exit__ = MagicMock(return_value=False)
    return pdf_mock


def _make_redis():
    return MagicMock()


def _make_redis_with_search(hex_val="0A80AD", registration="C6-ABC", simple_record=None):
    """Mock Redis client that resolves one registration via the Mictronics search index.

    ``simple_record`` is what ``r.json().get(...)`` returns for the
    aircraft:mictronics key used by the type sanity check. Unlike some other
    runners, a missing/falsy simple record here DOES block the write.
    """
    r = _make_redis()
    doc = MagicMock()
    doc.id = f"aircraft:mictronics:{hex_val}"
    doc.registration = registration
    results = MagicMock()
    results.docs = [doc]
    r.ft.return_value.search.return_value = results
    r.json.return_value.get.return_value = simple_record if simple_record is not None else {}
    return r


def _make_redis_no_match():
    r = _make_redis()
    results = MagicMock()
    results.docs = []
    r.ft.return_value.search.return_value = results
    return r


# ---------------------------------------------------------------------------
# Tests: _find_download_url
# ---------------------------------------------------------------------------

class TestFindDownloadUrl:
    def test_finds_absolute_url(self):
        html = (
            'https://caabahamas.com/wp-content/uploads/'
            'Updated-THE-BAHAMAS-CIVIL-AIRCRAFT-REGISTER-2026.pdf'
        )
        with patch("bs_caa_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 200
            resp.text = html
            mock_get.return_value = resp
            url = _find_download_url()
        assert url.endswith("Updated-THE-BAHAMAS-CIVIL-AIRCRAFT-REGISTER-2026.pdf")

    def test_finds_relative_url(self):
        html = '<a href="/uploads/Updated-THE-BAHAMAS-CIVIL-AIRCRAFT-REGISTER-2026.pdf">reg</a>'
        with patch("bs_caa_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 200
            resp.text = html
            mock_get.return_value = resp
            url = _find_download_url()
        assert url == "https://caabahamas.com/uploads/Updated-THE-BAHAMAS-CIVIL-AIRCRAFT-REGISTER-2026.pdf"

    def test_raises_on_http_error(self):
        with patch("bs_caa_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 503
            mock_get.return_value = resp
            with pytest.raises(RuntimeError, match="HTTP 503"):
                _find_download_url()

    def test_raises_when_no_link_found(self):
        with patch("bs_caa_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 200
            resp.text = "<html><body>nothing here</body></html>"
            mock_get.return_value = resp
            with pytest.raises(RuntimeError, match="Could not find"):
                _find_download_url()


# ---------------------------------------------------------------------------
# Tests: download_registry
# ---------------------------------------------------------------------------

class TestDownloadRegistry:
    def test_writes_temp_file(self):
        with patch("bs_caa_main._find_download_url", return_value="https://caabahamas.com/x.pdf"):
            with patch("bs_caa_main.requests.get") as mock_get:
                resp = MagicMock()
                resp.status_code = 200
                resp.content = b"fake pdf bytes"
                mock_get.return_value = resp
                path = download_registry()
        try:
            assert os.path.exists(path)
            with open(path, "rb") as f:
                assert f.read() == b"fake pdf bytes"
        finally:
            os.unlink(path)

    def test_raises_on_http_error(self):
        with patch("bs_caa_main._find_download_url", return_value="https://caabahamas.com/x.pdf"):
            with patch("bs_caa_main.requests.get") as mock_get:
                resp = MagicMock()
                resp.status_code = 503
                mock_get.return_value = resp
                with pytest.raises(RuntimeError, match="HTTP 503"):
                    download_registry()


# ---------------------------------------------------------------------------
# Tests: parse_pdf
# ---------------------------------------------------------------------------

class TestParsePdf:
    def test_parses_data_rows(self):
        table = [
            _HEADERS,
            ["C6-ABC", "Cessna 172S", "172S12345", "John Smith"],
        ]
        pdf_mock = _make_pdf_mock([table])
        with patch("bs_caa_main.pdfplumber.open", return_value=pdf_mock):
            rows = parse_pdf("fake.pdf")
        assert len(rows) == 1
        assert rows[0]["AIRCRAFT REGISTRATION NUMBER"] == "C6-ABC"
        assert rows[0]["REGISTERED OWNER OF AIRCRAFT"] == "John Smith"

    def test_repeated_header_row_skipped(self):
        """Excel-generated PDFs repeat the header row on every page."""
        table = [
            _HEADERS,
            ["C6-ABC", "Cessna 172S", "172S12345", "John Smith"],
            _HEADERS,
            ["C6-XYZ", "Piper PA-28", "28-98765", "Jane Doe"],
        ]
        pdf_mock = _make_pdf_mock([table])
        with patch("bs_caa_main.pdfplumber.open", return_value=pdf_mock):
            rows = parse_pdf("fake.pdf")
        assert len(rows) == 2
        assert rows[0]["AIRCRAFT REGISTRATION NUMBER"] == "C6-ABC"
        assert rows[1]["AIRCRAFT REGISTRATION NUMBER"] == "C6-XYZ"

    def test_blank_row_skipped(self):
        table = [
            _HEADERS,
            ["", "", "", ""],
            ["C6-ABC", "Cessna 172S", "172S12345", "John Smith"],
        ]
        pdf_mock = _make_pdf_mock([table])
        with patch("bs_caa_main.pdfplumber.open", return_value=pdf_mock):
            rows = parse_pdf("fake.pdf")
        assert len(rows) == 1

    def test_none_cell_handled(self):
        table = [
            _HEADERS,
            ["C6-ABC", None, "172S12345", "John Smith"],
        ]
        pdf_mock = _make_pdf_mock([table])
        with patch("bs_caa_main.pdfplumber.open", return_value=pdf_mock):
            rows = parse_pdf("fake.pdf")
        assert rows[0]["AIRCRAFT TYPE - MAKE/MODEL"] == ""

    def test_page_with_no_table_skipped(self):
        table = [
            _HEADERS,
            ["C6-ABC", "Cessna 172S", "172S12345", "John Smith"],
        ]
        pdf_mock = _make_pdf_mock([None, table])
        with patch("bs_caa_main.pdfplumber.open", return_value=pdf_mock):
            rows = parse_pdf("fake.pdf")
        assert len(rows) == 1

    def test_multi_page_rows_accumulated(self):
        page1 = [_HEADERS, ["C6-ABC", "Cessna 172S", "172S12345", "John Smith"]]
        page2 = [_HEADERS, ["C6-XYZ", "Piper PA-28", "28-98765", "Jane Doe"]]
        pdf_mock = _make_pdf_mock([page1, page2])
        with patch("bs_caa_main.pdfplumber.open", return_value=pdf_mock):
            rows = parse_pdf("fake.pdf")
        assert len(rows) == 2

    def test_raises_when_no_table_data_found(self):
        pdf_mock = _make_pdf_mock([None, None])
        with patch("bs_caa_main.pdfplumber.open", return_value=pdf_mock):
            with pytest.raises(RuntimeError, match="No table data"):
                parse_pdf("fake.pdf")


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record("0A80AD", "C6-ABC", row)
        assert record["icao_hex"] == "0A80AD"
        assert record["registration"] == "C6-ABC"
        assert record["source"] == "bs-caa"
        assert record["military"] is False
        assert record["aircraft"]["model"] == "Cessna 172S"
        assert record["aircraft"]["serial_number"] == "172S12345"
        assert record["registrant"]["names"] == ["John Smith"]

    def test_no_manufacturer_or_type_designator_fields(self):
        """This register has no manufacturer/type-designator columns at all."""
        row = _make_row()
        record = _build_record("0A80AD", "C6-ABC", row)
        assert "manufacturer" not in record["aircraft"]
        assert "type_designator" not in record["aircraft"]
        assert "powerplant" not in record["aircraft"]

    def test_empty_make_model_omitted(self):
        row = _make_row(make_model="")
        record = _build_record("0A80AD", "C6-ABC", row)
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial="")
        record = _build_record("0A80AD", "C6-ABC", row)
        assert "serial_number" not in record.get("aircraft", {})

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(make_model="", serial="")
        record = _build_record("0A80AD", "C6-ABC", row)
        assert "aircraft" not in record

    def test_empty_owner_omits_registrant(self):
        row = _make_row(owner="")
        record = _build_record("0A80AD", "C6-ABC", row)
        assert "registrant" not in record

    def test_owner_newlines_collapsed(self):
        row = _make_row(owner="John\nSmith")
        record = _build_record("0A80AD", "C6-ABC", row)
        assert record["registrant"]["names"] == ["John Smith"]

    def test_make_model_newlines_collapsed(self):
        row = _make_row(make_model="Cessna\n172S")
        record = _build_record("0A80AD", "C6-ABC", row)
        assert record["aircraft"]["model"] == "Cessna 172S"

    def test_serial_newlines_collapsed(self):
        row = _make_row(serial="172S\n12345")
        record = _build_record("0A80AD", "C6-ABC", row)
        assert record["aircraft"]["serial_number"] == "172S 12345"


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("C6ABC") == "C6ABC"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("C6-ABC")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: _type_tokens / _type_check_passes
# ---------------------------------------------------------------------------

class TestTypeCheckPasses:
    def test_empty_detail_model_always_passes(self):
        assert _type_check_passes({"type_designator": "C172"}, "") is True

    def test_no_simple_tokens_passes(self):
        assert _type_check_passes({}, "Cessna 172S") is True

    def test_matching_tokens_pass(self):
        simple = {"type_designator": "C172"}
        assert _type_check_passes(simple, "Cessna 172S") is True

    def test_mismatched_tokens_fail(self):
        simple = {"type_designator": "AW109"}
        assert _type_check_passes(simple, "Leonardo AW139") is False


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        simple = {"type_designator": "C172"}
        r = _make_redis_with_search(hex_val="0A80AD", registration="C6-ABC", simple_record=simple)
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_no_redis_match_not_written(self):
        rows = [_make_row()]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_missing_simple_record_skips_write(self):
        """Unlike some other runners, a missing/empty Mictronics simple
        record DOES block the write here."""
        rows = [_make_row()]
        r = _make_redis_with_search(hex_val="0A80AD", registration="C6-ABC", simple_record={})
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_type_check_mismatch_skips_record(self):
        rows = [_make_row(make_model="Leonardo AW139")]
        simple = {"type_designator": "AW109"}
        r = _make_redis_with_search(hex_val="0A80AD", registration="C6-ABC", simple_record=simple)
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_type_check_match_writes_record(self):
        rows = [_make_row(make_model="Cessna 172S")]
        simple = {"type_designator": "C172"}
        r = _make_redis_with_search(hex_val="0A80AD", registration="C6-ABC", simple_record=simple)
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_writes_to_registry_key(self):
        rows = [_make_row()]
        simple = {"type_designator": "C172"}
        r = _make_redis_with_search(hex_val="0A80AD", registration="C6-ABC", simple_record=simple)
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:0A80AD"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        simple = {"type_designator": "C172"}
        r = _make_redis_with_search(hex_val="0A80AD", registration="C6-ABC", simple_record=simple)
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "bs-caa"

    def test_ttl_applied(self):
        rows = [_make_row()]
        simple = {"type_designator": "C172"}
        r = _make_redis_with_search(hex_val="0A80AD", registration="C6-ABC", simple_record=simple)
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:0A80AD", REDIS_TTL)

    def test_empty_registration_skipped(self):
        rows = [_make_row(registration="")]
        r = _make_redis_with_search()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_empty_rows_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0


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
        with patch("bs_caa_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("bs_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 366, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("bs_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 366, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "366"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("bs_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/bs-caa"


# ---------------------------------------------------------------------------
# Tests: network (real outbound HTTP call — see #405)
# ---------------------------------------------------------------------------

class TestNetwork:
    @pytest.mark.network
    def test_url_reachable(self):
        assert_url_reachable(_mod.INDEX_URL, "bs-caa", headers={"User-Agent": "P5Software SkyFollower"})
