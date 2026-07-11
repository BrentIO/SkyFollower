"""Tests for the Kyrgyzstan CAA data runner."""

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
        "kg_caa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["kg_caa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_parse_manufacture_date = _mod._parse_manufacture_date
_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
download_and_parse = _mod.download_and_parse
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
_PAGE_URL = _mod._PAGE_URL


# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

def _make_table_html(rows_html: str) -> str:
    return f"<html><body><table>{rows_html}</table></body></html>"


def _header_row() -> str:
    """7-column header matching the real page structure."""
    return (
        "<tr>"
        "<th>№ п/п</th>"
        "<th>Эксплуатант/Operator</th>"
        "<th>Тип ВС/Type</th>"
        "<th>Регистрац. номер</th>"
        "<th>Дата регистрации</th>"
        "<th>Серийный №</th>"
        "<th>Дата производства</th>"
        "</tr>"
    )


def _full_row(
    operator="Air Manas",
    model="Boeing 737-300",
    registration="EX-001",
    date_reg="01.01.2000",
    serial="23456",
    mfr_date="15.06.1998",
    operator_rowspan=1,
    model_rowspan=1,
) -> str:
    """Full 7-cell data row; col 0 is always empty (row-number placeholder)."""
    op_rs = f' rowspan="{operator_rowspan}"' if operator_rowspan > 1 else ""
    mdl_rs = f' rowspan="{model_rowspan}"' if model_rowspan > 1 else ""
    return (
        f"<tr>"
        f"<td></td>"
        f"<td{op_rs}>{operator}</td>"
        f"<td{mdl_rs}>{model}</td>"
        f"<td>{registration}</td>"
        f"<td>{date_reg}</td>"
        f"<td>{serial}</td>"
        f"<td>{mfr_date}</td>"
        f"</tr>"
    )


def _continuation_row(
    model="Airbus A320",
    registration="EX-002",
    date_reg="01.02.2001",
    serial="99999",
    mfr_date="декабрь 1998",
) -> str:
    """6-cell row: operator rowspanned away, model present; col 0 still empty."""
    return (
        f"<tr>"
        f"<td></td>"
        f"<td>{model}</td>"
        f"<td>{registration}</td>"
        f"<td>{date_reg}</td>"
        f"<td>{serial}</td>"
        f"<td>{mfr_date}</td>"
        f"</tr>"
    )


def _double_continuation_row(
    registration="EX-003",
    date_reg="01.03.2002",
    serial="11111",
    mfr_date="01.01.2000",
) -> str:
    """5-cell row: both operator and model rowspanned away; col 0 still empty."""
    return (
        f"<tr>"
        f"<td></td>"
        f"<td>{registration}</td>"
        f"<td>{date_reg}</td>"
        f"<td>{serial}</td>"
        f"<td>{mfr_date}</td>"
        f"</tr>"
    )


def _make_response(html: str, status_code: int = 200):
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.text = html
    return resp


def _make_row(
    registration="EX-001",
    operator="Air Manas",
    model="Boeing 737-300",
    serial="23456",
    manufacture_date_raw="15.06.1998",
) -> dict:
    return {
        "registration": registration,
        "operator": operator,
        "model": model,
        "serial": serial,
        "manufacture_date_raw": manufacture_date_raw,
    }


def _make_redis_with_search(icao_hex="458100", registration="EX-001"):
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
# Tests: _parse_manufacture_date
# ---------------------------------------------------------------------------

class TestParseManufactureDate:
    def test_dmy_format(self):
        assert _parse_manufacture_date("15.06.1998") == "1998-06-15"

    def test_dmy_single_digit_day_month(self):
        assert _parse_manufacture_date("5.6.1998") == "1998-06-05"

    def test_english_month_year(self):
        assert _parse_manufacture_date("December 1998") == "1998-12-01"

    def test_russian_month_year(self):
        assert _parse_manufacture_date("декабрь 1998") == "1998-12-01"

    def test_russian_month_case_insensitive(self):
        assert _parse_manufacture_date("Декабрь 1998") == "1998-12-01"

    def test_all_russian_months(self):
        expected = [
            ("январь", "01"), ("февраль", "02"), ("март", "03"), ("апрель", "04"),
            ("май", "05"), ("июнь", "06"), ("июль", "07"), ("август", "08"),
            ("сентябрь", "09"), ("октябрь", "10"), ("ноябрь", "11"), ("декабрь", "12"),
        ]
        for month, num in expected:
            assert _parse_manufacture_date(f"{month} 2000") == f"2000-{num}-01"

    def test_year_only(self):
        assert _parse_manufacture_date("1998") == "1998-01-01"

    def test_empty_string(self):
        assert _parse_manufacture_date("") == ""

    def test_unknown_format(self):
        assert _parse_manufacture_date("sometime in 1998") == ""

    def test_whitespace_normalised(self):
        assert _parse_manufacture_date("  декабрь  1998  ") == "1998-12-01"


# ---------------------------------------------------------------------------
# Tests: download_and_parse
# ---------------------------------------------------------------------------

class TestDownloadAndParse:
    def test_raises_on_http_error(self):
        session = MagicMock()
        session.get.return_value = _make_response("", 503)
        with pytest.raises(RuntimeError, match="HTTP 503"):
            download_and_parse(session)

    def test_raises_when_no_table(self):
        session = MagicMock()
        session.get.return_value = _make_response("<html><body><p>No table</p></body></html>")
        with pytest.raises(RuntimeError, match="No table"):
            download_and_parse(session)

    def test_logs_url(self):
        session = MagicMock()
        session.get.return_value = _make_response("", 503)
        import logging
        with patch.object(logging.getLogger("kg-caa"), "info") as mock_log:
            with pytest.raises(RuntimeError):
                download_and_parse(session)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert _PAGE_URL in logged

    def test_parses_full_row(self):
        html = _make_table_html(
            _header_row() + _full_row()
        )
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 1
        r = records[0]
        assert r["registration"] == "EX-001"
        assert r["operator"] == "Air Manas"
        assert r["model"] == "Boeing 737-300"
        assert r["serial"] == "23456"
        assert r["manufacture_date_raw"] == "15.06.1998"

    def test_operator_carried_forward_across_rowspan(self):
        html = _make_table_html(
            _header_row()
            + _full_row(operator="Air Manas", registration="EX-001", operator_rowspan=2)
            + _continuation_row(registration="EX-002")
        )
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 2
        assert records[0]["operator"] == "Air Manas"
        assert records[1]["operator"] == "Air Manas"

    def test_model_carried_forward_when_both_rowspanned(self):
        html = _make_table_html(
            _header_row()
            + _full_row(operator="Tez Jet", model="AVRO 146-RJ85", registration="EX-008",
                        operator_rowspan=2, model_rowspan=2)
            + _double_continuation_row(registration="EX-009")
        )
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 2
        assert records[0]["operator"] == "Tez Jet"
        assert records[0]["model"] == "AVRO 146-RJ85"
        assert records[1]["operator"] == "Tez Jet"
        assert records[1]["model"] == "AVRO 146-RJ85"

    def test_operator_changes_for_new_group(self):
        html = _make_table_html(
            _header_row()
            + _full_row(operator="Air Manas", registration="EX-001")
            + _full_row(operator="Avia Traffic", registration="EX-003")
        )
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 2
        assert records[0]["operator"] == "Air Manas"
        assert records[1]["operator"] == "Avia Traffic"

    def test_cyrillic_registration_normalised_to_latin(self):
        # Cyrillic Е (U+0415) and Х (U+0425) look identical to Latin E/X
        html = _make_table_html(
            _header_row() + _full_row(registration="ЕХ-00001")
        )
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "EX-00001"

    def test_internal_spaces_removed_from_registration(self):
        html = _make_table_html(
            _header_row() + _full_row(registration="EX-370 20")
        )
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "EX-37020"

    def test_cyrillic_with_space_normalised(self):
        html = _make_table_html(
            _header_row() + _full_row(registration="ЕХ- 80003")
        )
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "EX-80003"

    def test_cyrillic_in_serial_normalised_to_latin(self):
        html = _make_table_html(
            _header_row() + _full_row(serial="ЕХ1234")
        )
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["serial"] == "EX1234"

    def test_skips_non_ex_rows(self):
        html = _make_table_html(
            _header_row()
            + _full_row(registration="N12345")
        )
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert records == []

    def test_date_of_registration_not_stored(self):
        html = _make_table_html(_header_row() + _full_row())
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert "date_reg" not in records[0]
        assert "date_of_registration" not in records[0]


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "458100", "EX-001")
        assert record["icao_hex"] == "458100"
        assert record["registration"] == "EX-001"
        assert record["source"] == "kg-caa"
        assert record["military"] is False
        assert record["aircraft"]["model"] == "Boeing 737-300"
        assert record["aircraft"]["serial_number"] == "23456"
        assert record["aircraft"]["manufactured_date"] == "1998-06-15"
        assert record["registrant"]["names"] == ["Air Manas"]

    def test_empty_model_omitted(self):
        row = _make_row(model="")
        record = _build_record(row, "458100", "EX-001")
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial="")
        record = _build_record(row, "458100", "EX-001")
        assert "serial_number" not in record.get("aircraft", {})

    def test_unparseable_date_omitted(self):
        row = _make_row(manufacture_date_raw="unknown")
        record = _build_record(row, "458100", "EX-001")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_empty_operator_omits_registrant(self):
        row = _make_row(operator="")
        record = _build_record(row, "458100", "EX-001")
        assert "registrant" not in record

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(model="", serial="", manufacture_date_raw="")
        record = _build_record(row, "458100", "EX-001")
        assert "aircraft" not in record

    def test_russian_month_date_parsed(self):
        row = _make_row(manufacture_date_raw="декабрь 1998")
        record = _build_record(row, "458100", "EX-001")
        assert record["aircraft"]["manufactured_date"] == "1998-12-01"


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("EX001") == "EX001"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("EX-001")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="458100", registration="EX-001")
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
        r = _make_redis_with_search(icao_hex="458100", registration="EX-001")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:458100"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="458100", registration="EX-001")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "kg-caa"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="458100", registration="EX-001")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:458100", REDIS_TTL)

    def test_null_fields_omitted_from_written_record(self):
        rows = [_make_row(model="", serial="")]
        r = _make_redis_with_search(icao_hex="458100", registration="EX-001")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][1] == "$"
        record = set_call[0][2]
        assert "model" not in record["aircraft"]
        assert "serial_number" not in record["aircraft"]

    def test_multiple_records(self):
        rows = [_make_row(registration="EX-001"), _make_row(registration="EX-002")]
        r = MagicMock()
        doc_a = MagicMock()
        doc_a.id = "aircraft:mictronics:458100"
        doc_a.registration = "EX-001"
        doc_b = MagicMock()
        doc_b.id = "aircraft:mictronics:458101"
        doc_b.registration = "EX-002"
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
        with patch("kg_caa_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("kg_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 15, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("kg_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 15, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "15"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("kg_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/kg-caa"
