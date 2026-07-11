"""Tests for the Maldives CAA data runner."""

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
        "mv_caa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["mv_caa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_build_record = _mod._build_record
_split_owner = _mod._split_owner
_escape_tag = _mod._escape_tag
_discover_pdf_url = _mod._discover_pdf_url
download_and_parse = _mod.download_and_parse
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
_INDEX_URL = _mod._INDEX_URL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_PDF_URL = "https://www.caa.gov.mv/files/abc123def456.pdf"

# 16-column row matching the PDF layout
def _make_pdf_row(
    registration="8Q-IAD",
    model="Viking Air (De Havilland)\nDHC-6-100",
    serial="501",
    year="1967",
    owner="Island Aviation Services Ltd\nMale, Maldives\nMaldives",
    status="Valid",
) -> list:
    return [
        "1",           # 0: N/S
        "MV-001",      # 1: Certificate#
        registration,  # 2: Registration
        model,         # 3: Manufacturer+Designation
        "5670",        # 4: MTOW
        serial,        # 5: Serial Number
        year,          # 6: Year Built
        owner,         # 7: Owner Name+Address
        "Same",        # 8: Legal Owner
        "Normal",      # 9: Registration Basis
        "",            # 10: Mortgage
        "",            # 11: AREDI
        "",            # 12: AREDI2
        "01/01/2000",  # 13: Original Issue Date
        "01/01/2024",  # 14: Last Revision Date
        status,        # 15: Status
    ]


def _make_row(
    registration="8Q-IAD",
    model="Viking Air (De Havilland)\nDHC-6-100",
    serial="501",
    year_built="1967",
    owner="Island Aviation Services Ltd\nMale, Maldives\nMaldives",
) -> dict:
    return {
        "registration": registration,
        "model": model,
        "serial": serial,
        "year_built": year_built,
        "owner": owner,
    }


def _make_pdf_page(rows: list[list]):
    page = MagicMock()
    page.extract_table.return_value = rows
    return page


def _make_pdf(pages):
    pdf = MagicMock()
    pdf.__enter__ = MagicMock(return_value=pdf)
    pdf.__exit__ = MagicMock(return_value=False)
    pdf.pages = pages
    return pdf


def _make_index_response(pdf_url: str = _PDF_URL):
    resp = MagicMock()
    resp.ok = True
    resp.status_code = 200
    resp.text = f'<html><body><a href="{pdf_url}">Aircraft Register</a></body></html>'
    return resp


def _make_pdf_response():
    resp = MagicMock()
    resp.ok = True
    resp.content = b"%PDF"
    return resp


def _make_redis_with_search(icao_hex="900100", registration="8Q-IAD"):
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
# Tests: _split_owner
# ---------------------------------------------------------------------------

class TestSplitOwner:
    def test_name_and_country_only(self):
        name, street, country = _split_owner("Island Aviation Services Ltd\nMaldives")
        assert name == "Island Aviation Services Ltd"
        assert street == []
        assert country == "Maldives"

    def test_name_street_country(self):
        name, street, country = _split_owner("Island Aviation Services Ltd\nMale, Maldives\nMaldives")
        assert name == "Island Aviation Services Ltd"
        assert street == ["Male, Maldives"]
        assert country == "Maldives"

    def test_name_only(self):
        name, street, country = _split_owner("Island Aviation Services Ltd")
        assert name == "Island Aviation Services Ltd"
        assert street == []
        assert country == ""

    def test_empty_string(self):
        name, street, country = _split_owner("")
        assert name == ""
        assert street == []
        assert country == ""

    def test_multiple_street_lines_preserved(self):
        name, street, country = _split_owner("Owner Name\nStreet 1\nCity\nCountry")
        assert name == "Owner Name"
        assert street == ["Street 1", "City"]
        assert country == "Country"

    def test_blank_lines_ignored(self):
        name, street, country = _split_owner("Owner Name\n\nMaldives")
        assert name == "Owner Name"
        assert street == []
        assert country == "Maldives"


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "900100", "8Q-IAD")
        assert record["icao_hex"] == "900100"
        assert record["registration"] == "8Q-IAD"
        assert record["source"] == "mv-caa"
        assert record["military"] is False
        assert record["aircraft"]["model"] == "Viking Air (De Havilland) DHC-6-100"
        assert record["aircraft"]["serial_number"] == "501"
        assert record["aircraft"]["manufactured_date"] == "1967-01-01"
        assert record["registrant"]["names"] == ["Island Aviation Services Ltd"]
        assert record["registrant"]["street"] == ["Male, Maldives"]
        assert record["registrant"]["country"] == "Maldives"

    def test_model_newlines_collapsed(self):
        row = _make_row(model="Viking Air\nDHC-6-100")
        record = _build_record(row, "900100", "8Q-IAD")
        assert record["aircraft"]["model"] == "Viking Air DHC-6-100"

    def test_empty_model_omitted(self):
        row = _make_row(model="")
        record = _build_record(row, "900100", "8Q-IAD")
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial="")
        record = _build_record(row, "900100", "8Q-IAD")
        assert "serial_number" not in record.get("aircraft", {})

    def test_year_stored_as_date(self):
        row = _make_row(year_built="2005")
        record = _build_record(row, "900100", "8Q-IAD")
        assert record["aircraft"]["manufactured_date"] == "2005-01-01"

    def test_non_digit_year_omitted(self):
        row = _make_row(year_built="unknown")
        record = _build_record(row, "900100", "8Q-IAD")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_empty_year_omitted(self):
        row = _make_row(year_built="")
        record = _build_record(row, "900100", "8Q-IAD")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_owner_name_stored_as_names(self):
        row = _make_row(owner="MANTA AIR PVT LTD\nMale\nMaldives")
        record = _build_record(row, "900100", "8Q-IAD")
        assert record["registrant"]["names"] == ["MANTA AIR PVT LTD"]

    def test_owner_address_stored_as_street(self):
        row = _make_row(owner="MANTA AIR PVT LTD\nMale, Maldives\nMaldives")
        record = _build_record(row, "900100", "8Q-IAD")
        assert record["registrant"]["street"] == ["Male, Maldives"]

    def test_owner_country_stored(self):
        row = _make_row(owner="MANTA AIR PVT LTD\nMale\nMaldives")
        record = _build_record(row, "900100", "8Q-IAD")
        assert record["registrant"]["country"] == "Maldives"

    def test_owner_multiple_address_lines_preserved(self):
        row = _make_row(owner="MANTA AIR PVT LTD\nStreet 1\nMale 20026\nMaldives")
        record = _build_record(row, "900100", "8Q-IAD")
        assert record["registrant"]["street"] == ["Street 1", "Male 20026"]
        assert record["registrant"]["country"] == "Maldives"

    def test_empty_owner_omits_registrant(self):
        row = _make_row(owner="")
        record = _build_record(row, "900100", "8Q-IAD")
        assert "registrant" not in record

    def test_owner_name_only_no_street_no_country(self):
        row = _make_row(owner="Solo Owner")
        record = _build_record(row, "900100", "8Q-IAD")
        assert record["registrant"]["names"] == ["Solo Owner"]
        assert "street" not in record["registrant"]
        assert "country" not in record["registrant"]

    def test_owner_name_and_country_no_street(self):
        row = _make_row(owner="Solo Owner\nMaldives")
        record = _build_record(row, "900100", "8Q-IAD")
        assert record["registrant"]["names"] == ["Solo Owner"]
        assert "street" not in record["registrant"]
        assert record["registrant"]["country"] == "Maldives"

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(model="", serial="", year_built="")
        record = _build_record(row, "900100", "8Q-IAD")
        assert "aircraft" not in record

    def test_legal_owner_not_stored(self):
        row = _make_row()
        row["legal_owner"] = "Some Bank"
        record = _build_record(row, "900100", "8Q-IAD")
        assert "legal_owner" not in record


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("8QIAD") == "8QIAD"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("8Q-IAD")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: _discover_pdf_url
# ---------------------------------------------------------------------------

class TestDiscoverPdfUrl:
    def test_finds_pdf_link(self):
        session = MagicMock()
        session.get.return_value = _make_index_response(_PDF_URL)
        url = _discover_pdf_url(session)
        assert url == _PDF_URL

    def test_raises_on_http_error(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 404
        session.get.return_value = resp
        with pytest.raises(RuntimeError, match="HTTP 404"):
            _discover_pdf_url(session)

    def test_raises_when_no_pdf_link(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.text = "<html><body><p>No links here</p></body></html>"
        session.get.return_value = resp
        with pytest.raises(RuntimeError, match="No PDF link found"):
            _discover_pdf_url(session)

    def test_logs_index_url(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 503
        session.get.return_value = resp
        import logging
        with patch.object(logging.getLogger("mv-caa"), "info") as mock_log:
            with pytest.raises(RuntimeError):
                _discover_pdf_url(session)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert _INDEX_URL in logged

    def test_relative_url_resolved(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.text = '<html><body><a href="/files/register.pdf">PDF</a></body></html>'
        session.get.return_value = resp
        url = _discover_pdf_url(session)
        assert url.startswith("https://")
        assert url.endswith("/files/register.pdf")


# ---------------------------------------------------------------------------
# Tests: download_and_parse
# ---------------------------------------------------------------------------

class TestDownloadAndParse:
    def _make_session(self, pdf_rows):
        session = MagicMock()
        session.get.side_effect = [
            _make_index_response(),
            _make_pdf_response(),
        ]
        return session

    def test_filters_non_8q_rows(self):
        session = self._make_session([])
        page = _make_pdf_page([
            _make_pdf_row(registration="8Q-IAD"),
            _make_pdf_row(registration="N12345"),
        ])
        with patch("mv_caa_main.pdfplumber.open", return_value=_make_pdf([page])):
            records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "8Q-IAD"

    def test_filters_invalid_status(self):
        session = self._make_session([])
        page = _make_pdf_page([
            _make_pdf_row(registration="8Q-IAD", status="Valid"),
            _make_pdf_row(registration="8Q-ZZZ", status="Invalid"),
        ])
        with patch("mv_caa_main.pdfplumber.open", return_value=_make_pdf([page])):
            records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "8Q-IAD"

    def test_raises_on_pdf_download_error(self):
        session = MagicMock()
        pdf_resp = MagicMock()
        pdf_resp.ok = False
        pdf_resp.status_code = 403
        session.get.side_effect = [_make_index_response(), pdf_resp]
        with pytest.raises(RuntimeError, match="HTTP 403"):
            download_and_parse(session)

    def test_logs_pdf_url(self):
        session = MagicMock()
        pdf_resp = MagicMock()
        pdf_resp.ok = False
        pdf_resp.status_code = 500
        session.get.side_effect = [_make_index_response(), pdf_resp]
        import logging
        with patch.object(logging.getLogger("mv-caa"), "info") as mock_log:
            with pytest.raises(RuntimeError):
                download_and_parse(session)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert _PDF_URL in logged

    def test_skips_short_rows(self):
        session = self._make_session([])
        page = _make_pdf_page([
            ["1", "MV-001", "8Q-IAD"],  # too short — missing status column
        ])
        with patch("mv_caa_main.pdfplumber.open", return_value=_make_pdf([page])):
            records = download_and_parse(session)
        assert records == []

    def test_skips_empty_rows(self):
        session = self._make_session([])
        page = _make_pdf_page([None, [], _make_pdf_row()])
        with patch("mv_caa_main.pdfplumber.open", return_value=_make_pdf([page])):
            records = download_and_parse(session)
        assert len(records) == 1

    def test_skips_page_with_no_table(self):
        session = self._make_session([])
        page1 = MagicMock()
        page1.extract_table.return_value = None
        page2 = _make_pdf_page([_make_pdf_row(registration="8Q-BBB")])
        with patch("mv_caa_main.pdfplumber.open", return_value=_make_pdf([page1, page2])):
            records = download_and_parse(session)
        assert len(records) == 1

    def test_multiple_pages_combined(self):
        session = self._make_session([])
        page1 = _make_pdf_page([_make_pdf_row(registration="8Q-AAA")])
        page2 = _make_pdf_page([_make_pdf_row(registration="8Q-BBB")])
        with patch("mv_caa_main.pdfplumber.open", return_value=_make_pdf([page1, page2])):
            records = download_and_parse(session)
        assert len(records) == 2

    def test_parses_all_fields(self):
        session = self._make_session([])
        page = _make_pdf_page([_make_pdf_row()])
        with patch("mv_caa_main.pdfplumber.open", return_value=_make_pdf([page])):
            records = download_and_parse(session)
        r = records[0]
        assert r["registration"] == "8Q-IAD"
        assert r["model"] == "Viking Air (De Havilland)\nDHC-6-100"
        assert r["serial"] == "501"
        assert r["year_built"] == "1967"
        assert r["owner"] == "Island Aviation Services Ltd\nMale, Maldives\nMaldives"


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="900100", registration="8Q-IAD")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_record_not_written_when_not_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_empty_registration_skipped(self):
        rows = [_make_row(registration="")]
        r = _make_redis_with_search()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0
        r.ft.return_value.search.assert_not_called()

    def test_writes_to_detail_key(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="900100", registration="8Q-IAD")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:900100"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="900100", registration="8Q-IAD")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "mv-caa"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="900100", registration="8Q-IAD")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:900100", REDIS_TTL)

    def test_null_fields_omitted_from_written_record(self):
        rows = [_make_row(model="", year_built="")]
        r = _make_redis_with_search(icao_hex="900100", registration="8Q-IAD")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        record = set_call[0][2]
        assert "model" not in record.get("aircraft", {})
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_multiple_records(self):
        rows = [_make_row(registration="8Q-IAD"), _make_row(registration="8Q-MBD")]
        r = MagicMock()
        doc_a = MagicMock()
        doc_a.id = "aircraft:mictronics:900100"
        doc_a.registration = "8Q-IAD"
        doc_b = MagicMock()
        doc_b.id = "aircraft:mictronics:900101"
        doc_b.registration = "8Q-MBD"
        results = MagicMock()
        results.docs = [doc_a, doc_b]
        r.ft.return_value.search.return_value = results
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 2


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
        with patch("mv_caa_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("mv_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 138, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("mv_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 138, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "138"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("mv_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/mv-caa"
