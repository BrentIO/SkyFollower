"""Tests for the Cyprus DCA data runner."""

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
        "cy_dca_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["cy_dca_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

download_and_parse = _mod.download_and_parse
_build_record = _mod._build_record
_clean = _mod._clean
_escape_tag = _mod._escape_tag
_build_registration_map = _mod._build_registration_map
_clean_owner_part = _mod._clean_owner_part
write_to_redis = _mod.write_to_redis
_PDF_URL = _mod._PDF_URL

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_row(
    registration="5B-ABC",
    manufacturer="Cessna",
    model="172 Skyhawk",
    serial="17280001",
    owner="ACME Aviation Ltd",
):
    return {
        "registration": registration,
        "manufacturer": manufacturer,
        "model": model,
        "serial": serial,
        "owner": owner,
    }


def _make_pdf_table(rows=None):
    """Build a fake pdfplumber table (list of lists)."""
    if rows is None:
        rows = [
            ["REGISTRATION MARK", "MANUFACTURER", "AIRCRAFT TYPE", "AIRCRAFT SERIAL NO", "MTOW KGS", "AIRCRAFT OWNER/OPERATOR"],
            ["5B-ABC", "Cessna", "172 Skyhawk", "17280001", "1111", "ACME Aviation Ltd"],
            ["5B-XYZ", "Piper", "PA-28", "28-7690001", "998", "Owner One / Owner Two"],
        ]
    return rows


# ---------------------------------------------------------------------------
# _clean
# ---------------------------------------------------------------------------


class TestClean:
    def test_strips_leading_trailing_whitespace(self):
        assert _clean("  hello  ") == "hello"

    def test_collapses_internal_whitespace(self):
        assert _clean("hello   world") == "hello world"

    def test_none_returns_empty(self):
        assert _clean(None) == ""

    def test_empty_returns_empty(self):
        assert _clean("") == ""

    def test_newlines_collapsed(self):
        assert _clean("hello\nworld") == "hello world"


# ---------------------------------------------------------------------------
# _clean_owner_part
# ---------------------------------------------------------------------------


class TestCleanOwnerPart:
    def test_removes_colon(self):
        assert _clean_owner_part("ACME Corp:") == "ACME Corp"

    def test_removes_inline_colon(self):
        assert _clean_owner_part("OWNER: ACME Corp") == "OWNER ACME Corp"

    def test_removes_lessor(self):
        assert _clean_owner_part("LESSOR ACME Corp") == "ACME Corp"

    def test_removes_lessee(self):
        assert _clean_owner_part("LESSEE ACME Corp") == "ACME Corp"

    def test_case_insensitive(self):
        assert _clean_owner_part("lessor ACME Corp") == "ACME Corp"

    def test_lessor_alone_returns_empty(self):
        assert _clean_owner_part("LESSOR") == ""

    def test_collapses_whitespace_after_removal(self):
        assert _clean_owner_part("ACME  Corp") == "ACME Corp"

    def test_none_returns_empty(self):
        assert _clean_owner_part(None) == ""


# ---------------------------------------------------------------------------
# _escape_tag
# ---------------------------------------------------------------------------


class TestEscapeTag:
    def test_plain_registration_unchanged(self):
        assert _escape_tag("5BABC") == "5BABC"

    def test_hyphen_escaped(self):
        result = _escape_tag("5B-ABC")
        assert result == r"5B\-ABC"

    def test_multiple_special_chars(self):
        result = _escape_tag("A.B-C")
        assert result == r"A\.B\-C"


# ---------------------------------------------------------------------------
# download_and_parse
# ---------------------------------------------------------------------------


class TestDownloadAndParse:
    def _make_session(self, pdf_bytes=b"%PDF-1.4", status_code=200):
        resp = MagicMock()
        resp.ok = status_code < 400
        resp.status_code = status_code
        resp.content = pdf_bytes
        session = MagicMock()
        session.get.return_value = resp
        return session

    def _make_pdf_mock(self, tables_by_page):
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

    def test_logs_download_url(self):
        session = self._make_session()
        pdf_mock = self._make_pdf_mock([[]])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            with patch.object(_mod, "logger") as mock_logger:
                download_and_parse(session)
        mock_logger.info.assert_any_call(
            "Downloading Cyprus DCA aircraft register from %s", _PDF_URL
        )

    def test_http_error_raises(self):
        session = self._make_session(status_code=404)
        with pytest.raises(RuntimeError, match="PDF request failed with HTTP 404"):
            download_and_parse(session)

    def test_parses_5b_rows(self):
        session = self._make_session()
        pdf_mock = self._make_pdf_mock([_make_pdf_table()])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 2
        assert rows[0]["registration"] == "5B-ABC"
        assert rows[1]["registration"] == "5B-XYZ"

    def test_skips_non_5b_rows(self):
        session = self._make_session()
        table = [
            ["5B-ABC", "Cessna", "172", "SN1", "1000", "Owner"],
            ["G-ABCD", "Cessna", "172", "SN2", "1000", "Other"],
            ["REGISTRATION MARK", "MANUFACTURER", "AIRCRAFT TYPE", "AIRCRAFT SERIAL NO", "MTOW KGS", "AIRCRAFT OWNER/OPERATOR"],
        ]
        pdf_mock = self._make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 1
        assert rows[0]["registration"] == "5B-ABC"

    def test_skips_rows_too_short(self):
        session = self._make_session()
        table = [
            ["5B-ABC", "Cessna", "172", "SN1"],  # only 4 cols
            ["5B-XYZ", "Piper", "PA-28", "SN2", "998", "Owner"],
        ]
        pdf_mock = self._make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 1
        assert rows[0]["registration"] == "5B-XYZ"

    def test_skips_empty_table_pages(self):
        session = self._make_session()
        pdf_mock = self._make_pdf_mock([None, _make_pdf_table()])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 2

    def test_registration_whitespace_stripped(self):
        session = self._make_session()
        table = [["5B - ABC", "Cessna", "172", "SN1", "1000", "Owner"]]
        pdf_mock = self._make_pdf_mock([table])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert rows[0]["registration"] == "5B-ABC"

    def test_multi_page_aggregated(self):
        session = self._make_session()
        page1 = [["5B-AAA", "Cessna", "172", "SN1", "1000", "Owner A"]]
        page2 = [["5B-BBB", "Piper", "PA-28", "SN2", "900", "Owner B"]]
        pdf_mock = self._make_pdf_mock([page1, page2])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            rows = download_and_parse(session)
        assert len(rows) == 2
        assert {r["registration"] for r in rows} == {"5B-AAA", "5B-BBB"}

    def test_logs_parsed_count(self):
        session = self._make_session()
        pdf_mock = self._make_pdf_mock([_make_pdf_table()])
        with patch.object(_mod.pdfplumber, "open", return_value=pdf_mock):
            with patch.object(_mod, "logger") as mock_logger:
                download_and_parse(session)
        mock_logger.info.assert_any_call("Parsed %d 5B- records.", 2)


# ---------------------------------------------------------------------------
# _build_record
# ---------------------------------------------------------------------------


class TestBuildRecord:
    def test_basic_fields(self):
        record = _build_record(_make_row(), "4B0001", "5B-ABC")
        assert record["icao_hex"] == "4B0001"
        assert record["registration"] == "5B-ABC"
        assert record["source"] == "cy-dca"

    def test_aircraft_manufacturer(self):
        record = _build_record(_make_row(manufacturer="Cessna"), "4B0001", "5B-ABC")
        assert record["aircraft"]["manufacturer"] == "Cessna"

    def test_aircraft_model(self):
        record = _build_record(_make_row(model="172 Skyhawk"), "4B0001", "5B-ABC")
        assert record["aircraft"]["model"] == "172 Skyhawk"

    def test_aircraft_serial_number(self):
        record = _build_record(_make_row(serial="17280001"), "4B0001", "5B-ABC")
        assert record["aircraft"]["serial_number"] == "17280001"

    def test_single_owner_stored_as_list(self):
        record = _build_record(_make_row(owner="ACME Aviation Ltd"), "4B0001", "5B-ABC")
        assert record["registrant"]["names"] == ["ACME Aviation Ltd"]

    def test_owner_split_by_slash(self):
        record = _build_record(_make_row(owner="Owner One / Owner Two"), "4B0001", "5B-ABC")
        assert record["registrant"]["names"] == ["Owner One", "Owner Two"]

    def test_owner_multiple_slashes(self):
        record = _build_record(_make_row(owner="A / B / C"), "4B0001", "5B-ABC")
        assert record["registrant"]["names"] == ["A", "B", "C"]

    def test_empty_owner_omits_registrant(self):
        record = _build_record(_make_row(owner=""), "4B0001", "5B-ABC")
        assert "registrant" not in record

    def test_empty_manufacturer_omits_aircraft(self):
        record = _build_record(_make_row(manufacturer="", model="", serial=""), "4B0001", "5B-ABC")
        assert "aircraft" not in record

    def test_whitespace_only_manufacturer_omitted(self):
        record = _build_record(_make_row(manufacturer="   "), "4B0001", "5B-ABC")
        assert "manufacturer" not in record.get("aircraft", {})

    def test_whitespace_only_serial_omitted(self):
        record = _build_record(_make_row(serial="   "), "4B0001", "5B-ABC")
        assert "serial_number" not in record.get("aircraft", {})

    def test_slash_only_owner_omits_registrant(self):
        record = _build_record(_make_row(owner=" / "), "4B0001", "5B-ABC")
        assert "registrant" not in record

    def test_owner_label_skipped(self):
        record = _build_record(_make_row(owner="OWNER"), "4B0001", "5B-ABC")
        assert "registrant" not in record

    def test_lessor_removed_as_substring(self):
        record = _build_record(_make_row(owner="LESSOR ACME Aviation Ltd"), "4B0001", "5B-ABC")
        assert record["registrant"]["names"] == ["ACME Aviation Ltd"]

    def test_lessor_alone_omits_registrant(self):
        record = _build_record(_make_row(owner="LESSOR"), "4B0001", "5B-ABC")
        assert "registrant" not in record

    def test_lessee_removed_as_substring(self):
        record = _build_record(_make_row(owner="LESSEE Corp"), "4B0001", "5B-ABC")
        assert record["registrant"]["names"] == ["Corp"]

    def test_trustee_label_skipped(self):
        record = _build_record(_make_row(owner="TRUSTEE"), "4B0001", "5B-ABC")
        assert "registrant" not in record

    def test_owner_label_mixed_with_real_name_filters_label(self):
        record = _build_record(_make_row(owner="OWNER / ACME Aviation Ltd"), "4B0001", "5B-ABC")
        assert record["registrant"]["names"] == ["ACME Aviation Ltd"]

    def test_colon_removed_from_name(self):
        record = _build_record(_make_row(owner="ACME Aviation Ltd:"), "4B0001", "5B-ABC")
        assert record["registrant"]["names"] == ["ACME Aviation Ltd"]

    def test_colon_removed_before_label_check(self):
        record = _build_record(_make_row(owner="OWNER:"), "4B0001", "5B-ABC")
        assert "registrant" not in record

    def test_source_is_cy_dca(self):
        record = _build_record(_make_row(), "4B0001", "5B-ABC")
        assert record["source"] == "cy-dca"


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
        r = self._make_redis([("4B0001", "5B-ABC"), ("4B0002", "5B-XYZ")])
        reg_map = _build_registration_map(["5B-ABC", "5B-XYZ"], r)
        assert reg_map["5B-ABC"] == "4B0001"
        assert reg_map["5B-XYZ"] == "4B0002"

    def test_redis_failure_logs_warning(self):
        r = MagicMock()
        r.ft.return_value.search.side_effect = Exception("connection refused")
        with patch.object(_mod, "logger") as mock_logger:
            result = _build_registration_map(["5B-ABC"], r)
        assert result == {}
        mock_logger.warning.assert_called_once()

    def test_empty_registrations_returns_empty(self):
        r = MagicMock()
        result = _build_registration_map([], r)
        assert result == {}
        r.ft.return_value.search.assert_not_called()

    def test_batches_large_lists(self):
        docs = [(f"4B{i:04d}", f"5B-{i:03d}") for i in range(150)]
        r = self._make_redis(docs)
        regs = [f"5B-{i:03d}" for i in range(150)]
        _build_registration_map(regs, r)
        assert r.ft.return_value.search.call_count == 2  # ceil(150/100) = 2


# ---------------------------------------------------------------------------
# write_to_redis
# ---------------------------------------------------------------------------


class TestWriteToRedis:
    def test_writes_found_registrations(self):
        rows = [_make_row(registration="5B-ABC")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"5B-ABC": "4B0001"}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 1
        pipe.json.return_value.set.assert_called_once()
        pipe.expire.assert_called_once()

    def test_skips_rows_with_no_redis_match(self):
        rows = [_make_row(registration="5B-ABC")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 0

    def test_skips_rows_with_empty_registration(self):
        rows = [_make_row(registration="")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 0

    def test_uses_aircraft_registry_key(self):
        rows = [_make_row(registration="5B-ABC")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"5B-ABC": "4B0001"}):
            write_to_redis(rows, r, 1209600)
        set_call = pipe.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:4B0001"

    def test_pipeline_error_logs_warning(self):
        rows = [_make_row(registration="5B-ABC")]
        r = MagicMock()
        pipe = MagicMock()
        pipe.execute.side_effect = Exception("Redis timeout")
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"5B-ABC": "4B0001"}):
            with patch.object(_mod, "logger") as mock_logger:
                write_to_redis(rows, r, 1209600)
        mock_logger.warning.assert_called()

    def test_logs_found_count(self):
        rows = [_make_row(registration="5B-ABC")]
        r = MagicMock()
        r.pipeline.return_value = MagicMock()
        with patch.object(_mod, "_build_registration_map", return_value={"5B-ABC": "4B0001"}):
            with patch.object(_mod, "logger") as mock_logger:
                write_to_redis(rows, r, 1209600)
        calls = mock_logger.info.call_args_list
        assert any(
            args and "%d / %d" in str(args[0]) and args[1] == 1 and args[2] == 1
            for args in (c.args for c in calls)
        )
