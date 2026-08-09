"""Tests for the Sri Lanka CAA data runner."""

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
        "lk_caa_registry_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["lk_caa_registry_main"] = mod
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
_REG_RE = _mod._REG_RE

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_row(
    registration="4R-ABL",
    manufacturer="AIRBUS",
    model="A320-232",
    operator="SriLankan Airlines Ltd.",
):
    return {
        "registration": registration,
        "manufacturer": manufacturer,
        "model": model,
        "operator": operator,
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
<ul class="pdf">
<li><a href="/images/pdf/2026_March/1_Civil_aircraft_registered_in_sri_lanka_as_at_11032026.pdf">Civil Aircraft Registered in Sri Lanka as at&nbsp;11.03.2026</a></li>
</ul>
</body></html>
"""

_INDEX_HTML_FULL_URL = b"""
<html><body>
<a href="https://www.caa.lk/images/pdf/2026_March/register.pdf">Civil Aircraft Registered in Sri Lanka as at 11.03.2026</a>
</body></html>
"""

_INDEX_HTML_UNRELATED_PDF = b"""
<html><body>
<a href="/images/pdf/2025_OCTOBER/Air_Service_Agreement_Update.pdf">Air Service Agreement Update</a>
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
# _reg_re
# ---------------------------------------------------------------------------


class TestRegRe:
    def test_three_char_suffix(self):
        assert _REG_RE.match("4R-ABL")

    def test_two_char_suffix(self):
        assert _REG_RE.match("4R-AB")

    def test_four_char_suffix(self):
        assert _REG_RE.match("4R-AB12")

    def test_empty_rejected(self):
        assert not _REG_RE.match("")

    def test_wrong_prefix_rejected(self):
        assert not _REG_RE.match("9A-ABC")

    def test_lowercase_rejected(self):
        assert not _REG_RE.match("4r-abl")


# ---------------------------------------------------------------------------
# _escape_tag
# ---------------------------------------------------------------------------


class TestEscapeTag:
    def test_plain_unchanged(self):
        assert _escape_tag("4RABL") == "4RABL"

    def test_hyphen_escaped(self):
        assert _escape_tag("4R-ABL") == r"4R\-ABL"


# ---------------------------------------------------------------------------
# _discover_pdf_url
# ---------------------------------------------------------------------------


class TestDiscoverPdfUrl:
    def test_returns_absolute_url_from_relative_href(self):
        session = _make_session(_INDEX_HTML)
        url = _discover_pdf_url(session)
        assert url == (
            "https://www.caa.lk/images/pdf/2026_March/"
            "1_Civil_aircraft_registered_in_sri_lanka_as_at_11032026.pdf"
        )

    def test_returns_absolute_url_unchanged(self):
        session = _make_session(_INDEX_HTML_FULL_URL)
        url = _discover_pdf_url(session)
        assert url == "https://www.caa.lk/images/pdf/2026_March/register.pdf"

    def test_ignores_unrelated_pdf_links(self):
        session = _make_session(_INDEX_HTML_UNRELATED_PDF)
        with pytest.raises(RuntimeError, match="No Sri Lanka aircraft register PDF link found"):
            _discover_pdf_url(session)

    def test_logs_index_url(self):
        session = _make_session(_INDEX_HTML)
        with patch.object(_mod, "logger") as mock_logger:
            _discover_pdf_url(session)
        mock_logger.info.assert_any_call(
            "Downloading Sri Lanka CAA index page from %s", _INDEX_URL
        )

    def test_http_error_raises(self):
        session = _make_session(status_code=404)
        with pytest.raises(RuntimeError, match="Index page request failed with HTTP 404"):
            _discover_pdf_url(session)

    def test_no_pdf_link_raises(self):
        session = _make_session(b"<html><body><a href='/other/page'>no pdf</a></body></html>")
        with pytest.raises(RuntimeError, match="No Sri Lanka aircraft register PDF link found"):
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
            "Downloading Sri Lanka CAA aircraft register from %s",
            (
                "https://www.caa.lk/images/pdf/2026_March/"
                "1_Civil_aircraft_registered_in_sri_lanka_as_at_11032026.pdf"
            ),
        )

    def test_pdf_http_error_raises(self):
        session = self._make_two_call_session(_INDEX_HTML, pdf_status=503)
        with pytest.raises(RuntimeError, match="PDF request failed with HTTP 503"):
            download_and_parse(session)

    def test_parses_valid_rows(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [
            ["Ref.No", "Make", "Model No.", "Registration", "Operator"],
            ["1", "AIRBUS", "A320-232", "4R-ABL", "SriLankan Airlines Ltd."],
            ["2", "AIRBUS", "A320-214", "4R-ABM", "SriLankan Airlines Ltd."],
        ]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 2
        assert rows[0]["registration"] == "4R-ABL"
        assert rows[1]["registration"] == "4R-ABM"

    def test_skips_header_rows(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [
            ["Ref.No", "Make", "Model No.", "Registration", "Operator"],
            ["1", "AIRBUS", "A320-232", "4R-ABL", "SriLankan Airlines Ltd."],
        ]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 1

    def test_skips_wrapped_make_ghost_rows(self):
        """Real source: 'Hot Air Balloon' entries have a two-line Make cell
        that pdfplumber splits into a second, otherwise-empty row."""
        session = self._make_two_call_session(_INDEX_HTML)
        table = [
            ["72", "CAMERON - Hot Air", "Z-210", "4R-ISN", "Sun Rise Ballooning (Pvt) Ltd."],
            ["", "Balloon", "", "", ""],
        ]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 1
        assert rows[0]["registration"] == "4R-ISN"

    def test_strips_internal_whitespace_in_registration(self):
        """Real source has a stray space after the hyphen in some marks
        (e.g. '4R- MDA')."""
        session = self._make_two_call_session(_INDEX_HTML)
        table = [["51", "PIPER CHEROKEE", "PA28-140", "4R- MDE", "Openskies Flight Training (Pvt) Ltd."]]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert rows[0]["registration"] == "4R-MDE"

    def test_skips_rows_too_short(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [
            ["1", "AIRBUS", "A320-232", "4R-ABL"],  # only 4 cols
            ["2", "AIRBUS", "A320-214", "4R-ABM", "SriLankan Airlines Ltd."],
        ]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 1

    def test_skips_empty_pages(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [["1", "AIRBUS", "A320-232", "4R-ABL", "SriLankan Airlines Ltd."]]
        pdf_mock = _make_pdf_mock([None, table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 1

    def test_multi_page_aggregated(self):
        session = self._make_two_call_session(_INDEX_HTML)
        page1 = [["1", "AIRBUS", "A320-232", "4R-ABL", "SriLankan Airlines Ltd."]]
        page2 = [["47", "CESSNA", "152", "4R-MDA", "Openskies Flight Training (Pvt) Ltd."]]
        pdf_mock = _make_pdf_mock([page1, page2])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 2

    def test_newlines_in_cells_collapsed(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [["1", "Ces\nna", "A320-232", "4R-ABL", "SriLankan\nAirlines Ltd."]]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert rows[0]["manufacturer"] == "Ces na"
        assert rows[0]["operator"] == "SriLankan Airlines Ltd."

    def test_logs_parsed_count(self):
        session = self._make_two_call_session(_INDEX_HTML)
        table = [["1", "AIRBUS", "A320-232", "4R-ABL", "SriLankan Airlines Ltd."]]
        pdf_mock = _make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            with patch.object(_mod, "logger") as mock_logger:
                download_and_parse(session)
        mock_logger.info.assert_any_call("Parsed %d 4R- records.", 1)


# ---------------------------------------------------------------------------
# _build_record
# ---------------------------------------------------------------------------


class TestBuildRecord:
    def test_basic_fields(self):
        record = _build_record(_make_row(), "501234", "4R-ABL")
        assert record["icao_hex"] == "501234"
        assert record["registration"] == "4R-ABL"
        assert record["source"] == "lk-caa-registry"
        assert record["military"] is False

    def test_aircraft_manufacturer(self):
        record = _build_record(_make_row(manufacturer="AIRBUS"), "501234", "4R-ABL")
        assert record["aircraft"]["manufacturer"] == "AIRBUS"

    def test_aircraft_model(self):
        record = _build_record(_make_row(model="A320-232"), "501234", "4R-ABL")
        assert record["aircraft"]["model"] == "A320-232"

    def test_registrant_names_from_operator(self):
        record = _build_record(_make_row(operator="SriLankan Airlines Ltd."), "501234", "4R-ABL")
        assert record["registrant"]["names"] == ["SriLankan Airlines Ltd."]

    def test_no_aircraft_operator_field(self):
        """This codebase never writes AircraftRecord's top-level operator
        field -- an operator-only source fills registrant.names instead."""
        record = _build_record(_make_row(), "501234", "4R-ABL")
        assert "operator" not in record

    def test_empty_operator_omits_names(self):
        record = _build_record(_make_row(operator=""), "501234", "4R-ABL")
        assert "names" not in record.get("registrant", {})

    def test_empty_manufacturer_and_model_omits_aircraft(self):
        record = _build_record(_make_row(manufacturer="", model=""), "501234", "4R-ABL")
        assert "aircraft" not in record

    def test_source_is_lk_caa(self):
        record = _build_record(_make_row(), "501234", "4R-ABL")
        assert record["source"] == "lk-caa-registry"


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
        r = self._make_redis([("501234", "4R-ABL"), ("501235", "4R-ABM")])
        reg_map = _build_registration_map(["4R-ABL", "4R-ABM"], r)
        assert reg_map["4R-ABL"] == "501234"
        assert reg_map["4R-ABM"] == "501235"

    def test_redis_failure_logs_warning(self):
        r = MagicMock()
        r.ft.return_value.search.side_effect = Exception("connection refused")
        with patch.object(_mod, "logger") as mock_logger:
            result = _build_registration_map(["4R-ABL"], r)
        assert result == {}
        mock_logger.warning.assert_called_once()

    def test_empty_registrations_returns_empty(self):
        r = MagicMock()
        result = _build_registration_map([], r)
        assert result == {}
        r.ft.return_value.search.assert_not_called()

    def test_batches_large_lists(self):
        docs = [(f"50{i:04d}", f"4R-{i:03d}") for i in range(150)]
        r = self._make_redis(docs)
        regs = [f"4R-{i:03d}" for i in range(150)]
        _build_registration_map(regs, r)
        assert r.ft.return_value.search.call_count == 2


# ---------------------------------------------------------------------------
# write_to_redis
# ---------------------------------------------------------------------------


class TestWriteToRedis:
    def test_writes_found_registrations(self):
        rows = [_make_row(registration="4R-ABL")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"4R-ABL": "501234"}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 1
        pipe.json.return_value.set.assert_called_once()
        pipe.expire.assert_called_once()

    def test_skips_no_redis_match(self):
        rows = [_make_row(registration="4R-ABL")]
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
        rows = [_make_row(registration="4R-ABL")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"4R-ABL": "501234"}):
            write_to_redis(rows, r, 1209600)
        set_call = pipe.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:501234"

    def test_pipeline_error_logs_warning(self):
        rows = [_make_row(registration="4R-ABL")]
        r = MagicMock()
        pipe = MagicMock()
        pipe.execute.side_effect = Exception("Redis timeout")
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"4R-ABL": "501234"}):
            with patch.object(_mod, "logger") as mock_logger:
                write_to_redis(rows, r, 1209600)
        mock_logger.warning.assert_called()

    def test_null_fields_omitted_from_written_record(self):
        rows = [_make_row(registration="4R-ABL", manufacturer="")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"4R-ABL": "501234"}):
            write_to_redis(rows, r, 1209600)
        set_call = pipe.json.return_value.set.call_args
        assert "manufacturer" not in set_call[0][2].get("aircraft", {})
