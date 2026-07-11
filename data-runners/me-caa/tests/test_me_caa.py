"""Tests for the Montenegro CAA data runner."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest
import requests

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
        "me_caa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["me_caa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_decode_category = _mod._decode_category
_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
_fetch_list_page = _mod._fetch_list_page
_fetch_detail_page = _mod._fetch_detail_page
download_and_parse = _mod.download_and_parse
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
_LIST_URL = _mod._LIST_URL


# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

def _list_html(rows: list[dict]) -> str:
    header = "<tr><th>Registarska oznaka</th><th>Redni broj u registru</th><th>Ime</th><th>Tip</th></tr>"
    body = ""
    for r in rows:
        body += (
            f"<tr>"
            f"<td>{r.get('Registarska oznaka','')}</td>"
            f"<td>{r.get('Redni broj u registru','1')}</td>"
            f"<td>{r.get('Ime','')}</td>"
            f"<td>{r.get('Tip','')}</td>"
            f"</tr>"
        )
    return f"<html><body><table>{header}{body}</table></body></html>"


def _detail_html(
    manufacturer="Embraer",
    year="2008",
    category="Transport – Airplane",
    sn="19000180",
    model="ERJ 190-200 LR",
    dereg="No",
    operator_name="ToMontenegro DOO",
    operator_address="Bulevar Džordža Vašingtona broj 98",
    operator_zip="81000 Podgorica",
    operator_country="Montenegro",
) -> str:
    return (
        f"<html><body><table>"
        f"<tr><th>Manufacturer</th><td>{manufacturer}</td></tr>"
        f"<tr><th>Year Built</th><td>{year}</td></tr>"
        f"<tr><th>MTOM</th><td>50790</td></tr>"
        f"<tr><th>Category</th><td>{category}</td></tr>"
        f"<tr><th>S/N</th><td>{sn}</td></tr>"
        f"<tr><th>Aircraft model/type</th><td>{model}</td></tr>"
        f"<tr><th>ARC expiry date</th><td>28.04.2023</td></tr>"
        f"<tr><th>Dereg</th><td>{dereg}</td></tr>"
        f"<tr><th>Name</th><td>{operator_name}</td></tr>"
        f"<tr><th>Address</th><td>{operator_address}</td></tr>"
        f"<tr><th>Zip code, town</th><td>{operator_zip}</td></tr>"
        f"<tr><th>Country</th><td>{operator_country}</td></tr>"
        f"</table></body></html>"
    )


def _make_response(html: str, status_code: int = 200):
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.text = html
    return resp


def _make_row(
    registration="4O-AOA",
    model="ERJ 190-200 LR",
    category="Transport – Airplane",
    serial_number="19000180",
    manufacturer="Embraer",
    year_built="2008",
    operator_name="ToMontenegro DOO",
    operator_address="Bulevar Džordža Vašingtona broj 98",
    operator_zip="81000 Podgorica",
    operator_country="Montenegro",
) -> dict:
    return {
        "registration": registration,
        "model": model,
        "category": category,
        "serial_number": serial_number,
        "manufacturer": manufacturer,
        "year_built": year_built,
        "operator_name": operator_name,
        "operator_address": operator_address,
        "operator_zip": operator_zip,
        "operator_country": operator_country,
    }


def _make_redis_with_search(icao_hex="4C0100", registration="4O-AOA"):
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
# Tests: _decode_category
# ---------------------------------------------------------------------------

class TestDecodeCategory:
    def test_em_dash_split(self):
        assert _decode_category("Transport – Airplane") == "Airplane"

    def test_hyphen_split(self):
        assert _decode_category("General Aviation - Helicopter") == "Helicopter"

    def test_no_separator_returns_raw(self):
        assert _decode_category("Airplane") == "Airplane"

    def test_em_dash_preferred_over_hyphen(self):
        assert _decode_category("Transport – Glider-powered") == "Glider-powered"

    def test_empty_string(self):
        assert _decode_category("") == ""

    def test_strips_whitespace(self):
        assert _decode_category("  Transport – Airplane  ") == "Airplane"


# ---------------------------------------------------------------------------
# Tests: _fetch_list_page
# ---------------------------------------------------------------------------

class TestFetchListPage:
    def test_returns_rows(self):
        session = MagicMock()
        session.get.return_value = _make_response(_list_html([
            {"Registarska oznaka": "4O-AOA", "Ime": "ToMontenegro DOO", "Tip": "ERJ190"},
        ]))
        rows = _fetch_list_page(session, 0)
        assert len(rows) == 1
        assert rows[0]["Registarska oznaka"] == "4O-AOA"

    def test_returns_empty_when_no_table(self):
        session = MagicMock()
        session.get.return_value = _make_response("<html><body><p>Nothing</p></body></html>")
        rows = _fetch_list_page(session, 0)
        assert rows == []

    def test_raises_on_http_error(self):
        session = MagicMock()
        session.get.return_value = _make_response("", 500)
        with pytest.raises(RuntimeError, match="HTTP 500"):
            _fetch_list_page(session, 0)

    def test_logs_url(self):
        session = MagicMock()
        session.get.return_value = _make_response(_list_html([]))
        import logging
        with patch.object(logging.getLogger("me-caa"), "info") as mock_log:
            _fetch_list_page(session, 2)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert "page=2" in logged

    def test_page_number_appended_to_filtered_url(self):
        session = MagicMock()
        session.get.return_value = _make_response(_list_html([]))
        _fetch_list_page(session, 3)
        called_url = session.get.call_args[0][0]
        assert "field_ispisan_iz_registra_tid=157" in called_url
        assert "page=3" in called_url


# ---------------------------------------------------------------------------
# Tests: _fetch_detail_page
# ---------------------------------------------------------------------------

class TestFetchDetailPage:
    def test_extracts_manufacturer(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html(manufacturer="Piper"))
        detail = _fetch_detail_page(session, "4O-AOA")
        assert detail["Manufacturer"] == "Piper"

    def test_extracts_year_built(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html(year="1998"))
        detail = _fetch_detail_page(session, "4O-AOA")
        assert detail["Year Built"] == "1998"

    def test_extracts_category(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html(category="Transport – Airplane"))
        detail = _fetch_detail_page(session, "4O-AOA")
        assert detail["Category"] == "Transport – Airplane"

    def test_extracts_serial_number(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html(sn="12345"))
        detail = _fetch_detail_page(session, "4O-AOA")
        assert detail["S/N"] == "12345"

    def test_extracts_model(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html(model="ERJ 190-200 LR"))
        detail = _fetch_detail_page(session, "4O-AOA")
        assert detail["Aircraft model/type"] == "ERJ 190-200 LR"

    def test_extracts_dereg(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html(dereg="No"))
        detail = _fetch_detail_page(session, "4O-AOA")
        assert detail["Dereg"] == "No"

    def test_extracts_operator_name(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html(operator_name="Air Montenegro"))
        detail = _fetch_detail_page(session, "4O-AOA")
        assert detail["Name"] == "Air Montenegro"

    def test_extracts_operator_address(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html(operator_address="Main Street 1"))
        detail = _fetch_detail_page(session, "4O-AOA")
        assert detail["Address"] == "Main Street 1"

    def test_extracts_zip_code_town(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html(operator_zip="81000 Podgorica"))
        detail = _fetch_detail_page(session, "4O-AOA")
        assert detail["Zip code, town"] == "81000 Podgorica"

    def test_extracts_country(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html(operator_country="Montenegro"))
        detail = _fetch_detail_page(session, "4O-AOA")
        assert detail["Country"] == "Montenegro"

    def test_returns_none_on_non_retriable_http_error(self):
        session = MagicMock()
        session.get.return_value = _make_response("", 404)
        detail = _fetch_detail_page(session, "4O-AOA")
        assert detail is None

    def test_retries_on_403(self):
        session = MagicMock()
        session.get.side_effect = [_make_response("", 403), _make_response(_detail_html())]
        with patch("me_caa_main.time.sleep"):
            detail = _fetch_detail_page(session, "4O-AOA")
        assert detail is not None
        assert session.get.call_count == 2

    def test_retries_on_429(self):
        session = MagicMock()
        session.get.side_effect = [_make_response("", 429), _make_response(_detail_html())]
        with patch("me_caa_main.time.sleep"):
            detail = _fetch_detail_page(session, "4O-AOA")
        assert detail is not None
        assert session.get.call_count == 2

    def test_retries_on_503(self):
        session = MagicMock()
        session.get.side_effect = [_make_response("", 503), _make_response(_detail_html())]
        with patch("me_caa_main.time.sleep"):
            detail = _fetch_detail_page(session, "4O-AOA")
        assert detail is not None
        assert session.get.call_count == 2

    def test_returns_none_after_retries_exhausted(self):
        session = MagicMock()
        session.get.return_value = _make_response("", 403)
        with patch("me_caa_main.time.sleep"):
            detail = _fetch_detail_page(session, "4O-AOA")
        assert detail is None
        assert session.get.call_count == 3  # initial + 2 retries

    def test_retries_on_connection_error(self):
        session = MagicMock()
        session.get.side_effect = [
            requests.exceptions.ConnectionError("dropped"),
            _make_response(_detail_html()),
        ]
        with patch("me_caa_main.time.sleep"):
            detail = _fetch_detail_page(session, "4O-AOA")
        assert detail is not None

    def test_returns_none_after_connection_error_retries_exhausted(self):
        session = MagicMock()
        session.get.side_effect = requests.exceptions.ConnectionError("dropped")
        with patch("me_caa_main.time.sleep"):
            detail = _fetch_detail_page(session, "4O-AOA")
        assert detail is None

    def test_url_uses_lowercase_registration(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html())
        _fetch_detail_page(session, "4O-AOA")
        called_url = session.get.call_args[0][0]
        assert "4o-aoa" in called_url

    def test_logs_url(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html())
        import logging
        with patch.object(logging.getLogger("me-caa"), "debug") as mock_log:
            _fetch_detail_page(session, "4O-AOA")
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert "4o-aoa" in logged


# ---------------------------------------------------------------------------
# Tests: download_and_parse
# ---------------------------------------------------------------------------

class TestDownloadAndParse:
    def _make_session(self, list_pages: list[list[dict]], detail_responses: list[str]):
        session = MagicMock()
        responses = []
        for page_rows in list_pages:
            responses.append(_make_response(_list_html(page_rows)))
        responses.append(_make_response(_list_html([])))
        for html in detail_responses:
            responses.append(_make_response(html))
        session.get.side_effect = responses
        return session

    def test_skips_non_4o_prefix(self):
        session = self._make_session(
            list_pages=[[
                {"Registarska oznaka": "N12345", "Ime": "Owner", "Tip": "C172"},
            ]],
            detail_responses=[],
        )
        records = download_and_parse(session)
        assert records == []

    def test_paginates_until_empty(self):
        session = self._make_session(
            list_pages=[
                [{"Registarska oznaka": "4O-AAA", "Ime": "Owner A", "Tip": "C172"}],
                [{"Registarska oznaka": "4O-BBB", "Ime": "Owner B", "Tip": "PA28"}],
            ],
            detail_responses=[_detail_html(), _detail_html()],
        )
        records = download_and_parse(session)
        assert len(records) == 2

    def test_prefers_detail_model_over_list_tip(self):
        session = self._make_session(
            list_pages=[[
                {"Registarska oznaka": "4O-AOA", "Ime": "Owner", "Tip": "ERJ190"},
            ]],
            detail_responses=[_detail_html(model="ERJ 190-200 LR")],
        )
        records = download_and_parse(session)
        assert records[0]["model"] == "ERJ 190-200 LR"

    def test_skips_record_when_detail_fetch_fails(self):
        """A None detail response must not produce a sparse Redis write."""
        session = MagicMock()
        responses = [
            _make_response(_list_html([
                {"Registarska oznaka": "4O-AAA", "Ime": "Owner A", "Tip": "C172"},
                {"Registarska oznaka": "4O-BBB", "Ime": "Owner B", "Tip": "PA28"},
            ])),
            _make_response(_list_html([])),
            _make_response("", 404),         # 4O-AAA detail fails
            _make_response(_detail_html()),  # 4O-BBB detail succeeds
        ]
        session.get.side_effect = responses
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "4O-BBB"

    def test_maps_all_fields(self):
        session = self._make_session(
            list_pages=[[
                {"Registarska oznaka": "4O-AOA", "Ime": "ToMontenegro DOO", "Tip": "ERJ190"},
            ]],
            detail_responses=[_detail_html()],
        )
        records = download_and_parse(session)
        r = records[0]
        assert r["registration"] == "4O-AOA"
        assert r["model"] == "ERJ 190-200 LR"
        assert r["category"] == "Transport – Airplane"
        assert r["serial_number"] == "19000180"
        assert r["manufacturer"] == "Embraer"
        assert r["year_built"] == "2008"
        assert r["operator_name"] == "ToMontenegro DOO"
        assert r["operator_address"] == "Bulevar Džordža Vašingtona broj 98"
        assert r["operator_zip"] == "81000 Podgorica"
        assert r["operator_country"] == "Montenegro"


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "4C0100", "4O-AOA")
        assert record["icao_hex"] == "4C0100"
        assert record["registration"] == "4O-AOA"
        assert record["source"] == "me-caa"
        assert record["military"] is False
        assert record["aircraft"]["model"] == "ERJ 190-200 LR"
        assert record["aircraft"]["type"] == "Airplane"
        assert record["aircraft"]["serial_number"] == "19000180"
        assert record["aircraft"]["manufacturer"] == "Embraer"
        assert record["aircraft"]["manufactured_date"] == "2008-01-01"
        assert record["registrant"]["names"] == ["ToMontenegro DOO"]
        assert record["registrant"]["street"] == "Bulevar Džordža Vašingtona broj 98"
        assert record["registrant"]["postal_code"] == "81000"
        assert record["registrant"]["city"] == "Podgorica"
        assert record["registrant"]["country"] == "Montenegro"

    def test_zip_town_split_postal_code(self):
        row = _make_row(operator_zip="85310 Budva")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert record["registrant"]["postal_code"] == "85310"

    def test_zip_town_split_city(self):
        row = _make_row(operator_zip="85310 Budva")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert record["registrant"]["city"] == "Budva"

    def test_zip_town_no_space_stored_as_postal_code_only(self):
        row = _make_row(operator_zip="81000")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert record["registrant"]["postal_code"] == "81000"
        assert "city" not in record["registrant"]

    def test_zip_town_city_with_spaces(self):
        row = _make_row(operator_zip="20000 Bar City")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert record["registrant"]["postal_code"] == "20000"
        assert record["registrant"]["city"] == "Bar City"

    def test_category_decoded_to_type(self):
        row = _make_row(category="General Aviation – Helicopter")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert record["aircraft"]["type"] == "Helicopter"

    def test_year_stored_as_date(self):
        row = _make_row(year_built="1999")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert record["aircraft"]["manufactured_date"] == "1999-01-01"

    def test_non_digit_year_omitted(self):
        row = _make_row(year_built="unknown")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_empty_model_omitted(self):
        row = _make_row(model="")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert "model" not in record.get("aircraft", {})

    def test_empty_category_omits_type(self):
        row = _make_row(category="")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert "type" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial_number="")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert "serial_number" not in record.get("aircraft", {})

    def test_empty_manufacturer_omitted(self):
        row = _make_row(manufacturer="")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert "manufacturer" not in record.get("aircraft", {})

    def test_empty_operator_name_omits_names(self):
        row = _make_row(operator_name="")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert "names" not in record.get("registrant", {})

    def test_empty_address_omits_street(self):
        row = _make_row(operator_address="")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert "street" not in record.get("registrant", {})

    def test_empty_zip_omits_postal_code(self):
        row = _make_row(operator_zip="")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert "postal_code" not in record.get("registrant", {})

    def test_empty_country_omits_country(self):
        row = _make_row(operator_country="")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert "country" not in record.get("registrant", {})

    def test_all_aircraft_empty_omits_aircraft_key(self):
        row = _make_row(model="", category="", serial_number="", manufacturer="", year_built="")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert "aircraft" not in record

    def test_all_registrant_empty_omits_registrant_key(self):
        row = _make_row(operator_name="", operator_address="", operator_zip="", operator_country="")
        record = _build_record(row, "4C0100", "4O-AOA")
        assert "registrant" not in record


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("4OAOA") == "4OAOA"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("4O-AOA")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4C0100", registration="4O-AOA")
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
        r = _make_redis_with_search(icao_hex="4C0100", registration="4O-AOA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:4C0100"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4C0100", registration="4O-AOA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "me-caa"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4C0100", registration="4O-AOA")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:4C0100", REDIS_TTL)

    def test_null_fields_omitted_from_written_record(self):
        rows = [_make_row(manufacturer="", operator_address="")]
        r = _make_redis_with_search(icao_hex="4C0100", registration="4O-AOA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        record = set_call[0][2]
        assert "manufacturer" not in record.get("aircraft", {})
        assert "street" not in record.get("registrant", {})

    def test_multiple_records(self):
        rows = [_make_row(registration="4O-AOA"), _make_row(registration="4O-ABC")]
        r = MagicMock()
        doc_a = MagicMock()
        doc_a.id = "aircraft:mictronics:4C0100"
        doc_a.registration = "4O-AOA"
        doc_b = MagicMock()
        doc_b.id = "aircraft:mictronics:4C0101"
        doc_b.registration = "4O-ABC"
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
        with patch("me_caa_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("me_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 33, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("me_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 33, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "33"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("me_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/me-caa"
