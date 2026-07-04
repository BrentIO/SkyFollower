"""Tests for the Croatia CCAA data runner."""

from __future__ import annotations

import importlib.util
import io
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
        "hr_ccaa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["hr_ccaa_main"] = mod
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
_INDEX_URL = _mod._INDEX_URL
_SUFFIX_RE = _mod._SUFFIX_RE

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_row(
    registration="9A-ABC",
    manufacturer="Cessna",
    model="172 Skyhawk",
    serial="17280001",
    owner="ACME Aviation d.o.o.",
    address="Zagreb, Croatia",
):
    return {
        "registration": registration,
        "manufacturer": manufacturer,
        "model": model,
        "serial": serial,
        "owner": owner,
        "address": address,
    }


def _make_session(content=b"", status_code=200, content_type="text/html"):
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
<a href="/file/abc123def456">Download register (PDF)</a>
</body></html>
"""

_INDEX_HTML_FULL_URL = b"""
<html><body>
<a href="https://www.ccaa.hr/file/abc123def456">Download register (PDF)</a>
</body></html>
"""


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
# _suffix_re
# ---------------------------------------------------------------------------


class TestSuffixRe:
    def test_two_letter_suffix(self):
        assert _SUFFIX_RE.match("AB")

    def test_three_letter_suffix(self):
        assert _SUFFIX_RE.match("ABC")

    def test_four_char_suffix(self):
        assert _SUFFIX_RE.match("AB12")

    def test_header_text_rejected(self):
        assert not _SUFFIX_RE.match("REG. OZNAKA")

    def test_empty_rejected(self):
        assert not _SUFFIX_RE.match("")

    def test_single_letter_rejected(self):
        assert not _SUFFIX_RE.match("A")

    def test_lowercase_rejected(self):
        assert not _SUFFIX_RE.match("abc")


# ---------------------------------------------------------------------------
# _escape_tag
# ---------------------------------------------------------------------------


class TestEscapeTag:
    def test_plain_unchanged(self):
        assert _escape_tag("9AABC") == "9AABC"

    def test_hyphen_escaped(self):
        assert _escape_tag("9A-ABC") == r"9A\-ABC"


# ---------------------------------------------------------------------------
# _discover_pdf_url
# ---------------------------------------------------------------------------


class TestDiscoverPdfUrl:
    def test_returns_absolute_url_from_relative_href(self):
        session = _make_session(_INDEX_HTML)
        url = _discover_pdf_url(session)
        assert url == "https://www.ccaa.hr/file/abc123def456"

    def test_returns_absolute_url_unchanged(self):
        session = _make_session(_INDEX_HTML_FULL_URL)
        url = _discover_pdf_url(session)
        assert url == "https://www.ccaa.hr/file/abc123def456"

    def test_logs_index_url(self):
        session = _make_session(_INDEX_HTML)
        with patch.object(_mod, "logger") as mock_logger:
            _discover_pdf_url(session)
        mock_logger.info.assert_any_call(
            "Downloading Croatia CCAA index page from %s", _INDEX_URL
        )

    def test_http_error_raises(self):
        session = _make_session(status_code=404)
        with pytest.raises(RuntimeError, match="Index page request failed with HTTP 404"):
            _discover_pdf_url(session)

    def test_no_file_link_raises(self):
        session = _make_session(b"<html><body><a href='/other/page'>no pdf</a></body></html>")
        with pytest.raises(RuntimeError, match="No /file/ link found"):
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
            "Downloading Croatia CCAA aircraft register from %s",
            "https://www.ccaa.hr/file/abc123def456",
        )

    def test_pdf_http_error_raises(self):
        session = self._make_two_call_session(_INDEX_HTML, pdf_status=503)
        with pytest.raises(RuntimeError, match="PDF request failed with HTTP 503"):
            download_and_parse(session)

    def test_parses_valid_rows(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [
            ["REG. OZNAKA", "REDNI BROJ", "PROIZVOĐAČ", "OZNAKA", "SERIJSKI BROJ", "VLASNIK", "ADRESA"],
            ["ABC", "001", "Cessna", "172", "SN001", "Owner A", "Zagreb"],
            ["XYZ", "002", "Piper", "PA-28", "SN002", "Owner B", "Split"],
        ]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 2
        assert rows[0]["registration"] == "9A-ABC"
        assert rows[1]["registration"] == "9A-XYZ"

    def test_prepends_9a_prefix(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [["ABC", "001", "Cessna", "172", "SN001", "Owner", "Addr"]]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert rows[0]["registration"] == "9A-ABC"

    def test_skips_header_rows(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [
            ["REG. OZNAKA", "REDNI BROJ", "PROIZVOĐAČ", "OZNAKA", "SERIJSKI BROJ", "VLASNIK", "ADRESA"],
            ["ABC", "001", "Cessna", "172", "SN001", "Owner", "Addr"],
        ]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 1

    def test_skips_rows_too_short(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [
            ["ABC", "001", "Cessna", "172"],  # only 4 cols
            ["XYZ", "002", "Piper", "PA-28", "SN002", "Owner", "Addr"],
        ]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 1

    def test_skips_empty_pages(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [["ABC", "001", "Cessna", "172", "SN001", "Owner", "Addr"]]
        pdf_mock = _make_pdf_mock([None, table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 1

    def test_multi_page_aggregated(self):
        session = self._make_two_call_session(_INDEX_HTML)
        page1 = [["AAA", "001", "Cessna", "172", "SN1", "Owner A", "Addr A"]]
        page2 = [["BBB", "002", "Piper", "PA-28", "SN2", "Owner B", "Addr B"]]
        pdf_mock = _make_pdf_mock([page1, page2])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 2

    def test_newlines_in_cells_collapsed(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [["ABC", "001", "Ces\nna", "172", "SN001", "Own\ner", "Za\ngreb"]]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert rows[0]["manufacturer"] == "Ces na"
        assert rows[0]["owner"] == "Own er"

    def test_logs_parsed_count(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [["ABC", "001", "Cessna", "172", "SN001", "Owner", "Addr"]]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            with patch.object(_mod, "logger") as mock_logger:
                download_and_parse(session)
        mock_logger.info.assert_any_call("Parsed %d 9A- records.", 1)


# ---------------------------------------------------------------------------
# _build_record
# ---------------------------------------------------------------------------


class TestBuildRecord:
    def test_basic_fields(self):
        record = _build_record(_make_row(), "501234", "9A-ABC")
        assert record["icao_hex"] == "501234"
        assert record["registration"] == "9A-ABC"
        assert record["source"] == "hr-ccaa"

    def test_aircraft_manufacturer(self):
        record = _build_record(_make_row(manufacturer="Cessna"), "501234", "9A-ABC")
        assert record["aircraft"]["manufacturer"] == "Cessna"

    def test_aircraft_model(self):
        record = _build_record(_make_row(model="172 Skyhawk"), "501234", "9A-ABC")
        assert record["aircraft"]["model"] == "172 Skyhawk"

    def test_aircraft_serial_number(self):
        record = _build_record(_make_row(serial="17280001"), "501234", "9A-ABC")
        assert record["aircraft"]["serial_number"] == "17280001"

    def test_registrant_names(self):
        record = _build_record(_make_row(owner="ACME Aviation d.o.o."), "501234", "9A-ABC")
        assert record["registrant"]["names"] == ["ACME Aviation d.o.o."]

    def test_registrant_street_split_by_comma(self):
        record = _build_record(_make_row(address="Zagreb, Croatia"), "501234", "9A-ABC")
        assert record["registrant"]["street"] == ["Zagreb", "Croatia"]

    def test_registrant_street_single_part(self):
        record = _build_record(_make_row(address="Zagreb"), "501234", "9A-ABC")
        assert record["registrant"]["street"] == ["Zagreb"]

    def test_registrant_street_multiple_commas(self):
        record = _build_record(_make_row(address="Ilica 1, 10000 Zagreb, Croatia"), "501234", "9A-ABC")
        assert record["registrant"]["street"] == ["Ilica 1", "10000 Zagreb", "Croatia"]

    def test_empty_owner_omits_names(self):
        record = _build_record(_make_row(owner=""), "501234", "9A-ABC")
        assert "names" not in record.get("registrant", {})

    def test_empty_address_omits_street(self):
        record = _build_record(_make_row(address=""), "501234", "9A-ABC")
        assert "street" not in record.get("registrant", {})

    def test_empty_manufacturer_omits_aircraft(self):
        record = _build_record(_make_row(manufacturer="", model="", serial=""), "501234", "9A-ABC")
        assert "aircraft" not in record

    def test_source_is_hr_ccaa(self):
        record = _build_record(_make_row(), "501234", "9A-ABC")
        assert record["source"] == "hr-ccaa"


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
        r = self._make_redis([("501234", "9A-ABC"), ("501235", "9A-XYZ")])
        reg_map = _build_registration_map(["9A-ABC", "9A-XYZ"], r)
        assert reg_map["9A-ABC"] == "501234"
        assert reg_map["9A-XYZ"] == "501235"

    def test_redis_failure_logs_warning(self):
        r = MagicMock()
        r.ft.return_value.search.side_effect = Exception("connection refused")
        with patch.object(_mod, "logger") as mock_logger:
            result = _build_registration_map(["9A-ABC"], r)
        assert result == {}
        mock_logger.warning.assert_called_once()

    def test_empty_registrations_returns_empty(self):
        r = MagicMock()
        result = _build_registration_map([], r)
        assert result == {}
        r.ft.return_value.search.assert_not_called()

    def test_batches_large_lists(self):
        docs = [(f"50{i:04d}", f"9A-{i:03d}") for i in range(150)]
        r = self._make_redis(docs)
        regs = [f"9A-{i:03d}" for i in range(150)]
        _build_registration_map(regs, r)
        assert r.ft.return_value.search.call_count == 2


# ---------------------------------------------------------------------------
# write_to_redis
# ---------------------------------------------------------------------------


class TestWriteToRedis:
    def test_writes_found_registrations(self):
        rows = [_make_row(registration="9A-ABC")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"9A-ABC": "501234"}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 1
        pipe.json.return_value.set.assert_called_once()
        pipe.expire.assert_called_once()

    def test_skips_no_redis_match(self):
        rows = [_make_row(registration="9A-ABC")]
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
        rows = [_make_row(registration="9A-ABC")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"9A-ABC": "501234"}):
            write_to_redis(rows, r, 1209600)
        set_call = pipe.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:501234"

    def test_pipeline_error_logs_warning(self):
        rows = [_make_row(registration="9A-ABC")]
        r = MagicMock()
        pipe = MagicMock()
        pipe.execute.side_effect = Exception("Redis timeout")
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"9A-ABC": "501234"}):
            with patch.object(_mod, "logger") as mock_logger:
                write_to_redis(rows, r, 1209600)
        mock_logger.warning.assert_called()
