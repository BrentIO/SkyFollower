"""Tests for the Airport Webcams special liveries data runner."""

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
        "airportwebcams_special_liveries_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["airportwebcams_special_liveries_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

from shared.url_reachability import assert_url_reachable

_derive_special_livery = _mod._derive_special_livery
_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
download_and_parse = _mod.download_and_parse
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
SOURCE_URL = _mod.SOURCE_URL
TABLE_ID = _mod.TABLE_ID


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_page(rows: list[tuple[str, str, str, str, str]]) -> str:
    """Build a minimal TablePress-shaped HTML page from (country, airline,
    aircraft_type, registration, description) tuples."""
    body_rows = []
    for i, (country, airline, actype, reg, desc) in enumerate(rows, start=1):
        body_rows.append(
            f'<tr class="row-{i}">'
            f'<td class="column-1">{country}</td>'
            f'<td class="column-2">{airline}</td>'
            f'<td class="column-3">{actype}</td>'
            f'<td class="column-4"><a href="https://www.flightradar24.com/data/aircraft/{reg}">{reg}</a></td>'
            f'<td class="column-5">{desc}</td>'
            f'</tr>'
        )
    return (
        f'<html><body><table id="{TABLE_ID}">'
        f'<thead><tr><th>Country</th><th>Airline</th><th>Aircraft Type</th>'
        f'<th>Registration</th><th>Description</th></tr></thead>'
        f'<tbody>{"".join(body_rows)}</tbody>'
        f'</table></body></html>'
    )


def _make_session(html: str, status_code: int = 200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = html
    session = MagicMock()
    session.get.return_value = resp
    return session


def _make_redis_with_search(icao_hex="AA7C64", registration="N775JB"):
    """Redis mock that returns a single RediSearch match for `registration`."""
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
# Tests: _derive_special_livery
# ---------------------------------------------------------------------------

class TestDeriveSpecialLivery:
    def test_simple_no_annotation(self):
        assert _derive_special_livery("SkyTeam") == "SkyTeam"

    def test_compound_with_sticker_annotation(self):
        # Real row: JetBlue N775JB
        assert _derive_special_livery("Vets In Blue (2022) / America250 (sticker)") == "America250"

    def test_compound_with_stickers_plural(self):
        # Real row: Cargolux LX-VCC
        assert _derive_special_livery(
            "50th Anniversary / Air Silk Road / 1 Million Tons, 10 Years (stickers)"
        ) == "1 Million Tons, 10 Years"

    def test_sticker_and_new_marker_both_present_no_slash(self):
        # Real row: Citilink PK-GLW
        assert _derive_special_livery("Amar Bank (sticker) (#New at 17-Jul-26)") == "Amar Bank"

    def test_new_marker_only_with_slash(self):
        # Real row: Airlink ZS-YAE
        assert _derive_special_livery(
            "Rugby's Greatest Rivalry / 2026 NZ v SA rugby tour (#New at 17-Jul-26)"
        ) == "2026 NZ v SA rugby tour"

    def test_slash_inside_sticker_annotation_not_treated_as_separator(self):
        # Real row (FedEx): the annotation itself contains a "/" — must not
        # be misread as an extra compound-description segment.
        assert _derive_special_livery(
            "FedEx founder F W Smith (sticker; underside/belly)"
        ) == "FedEx founder F W Smith"

    def test_sticker_with_comma_qualifier(self):
        assert _derive_special_livery("FedEx 50 (sticker, underside)") == "FedEx 50"

    def test_year_parenthetical_preserved(self):
        assert _derive_special_livery("Vets In Blue (2022)") == "Vets In Blue (2022)"

    def test_case_insensitive_sticker(self):
        assert _derive_special_livery("Foo (STICKER)") == "Foo"

    def test_case_insensitive_new_marker(self):
        assert _derive_special_livery("Foo (#NEW at 1-Jan-26)") == "Foo"

    def test_collapses_internal_whitespace(self):
        assert _derive_special_livery("Foo   Bar") == "Foo Bar"


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_record_shape(self):
        record = _build_record("AA7C64", "N775JB", "America250")
        assert record == {
            "icao_hex": "AA7C64",
            "registration": "N775JB",
            "source": "airportwebcams-special-liveries",
            "special_livery": "America250",
        }


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("N775JB") == "N775JB"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("LX-VCC")


# ---------------------------------------------------------------------------
# Tests: download_and_parse
# ---------------------------------------------------------------------------

class TestDownloadAndParse:
    def test_parses_rows(self):
        html = _make_page([
            ("Argentina", "Aerolineas Argentinas", "Embraer ERJ190", "LV-FPS", "SkyTeam"),
            ("USA", "JetBlue", "Airbus A320", "N775JB", "Vets In Blue (2022) / America250 (sticker)"),
        ])
        session = _make_session(html)
        rows = download_and_parse(session)
        assert len(rows) == 2
        assert rows[0]["registration"] == "LV-FPS"
        assert rows[0]["description"] == "SkyTeam"
        assert rows[1]["registration"] == "N775JB"

    def test_non_200_raises(self):
        session = _make_session("", status_code=500)
        with pytest.raises(RuntimeError):
            download_and_parse(session)

    def test_missing_table_raises(self):
        session = _make_session("<html><body>no table here</body></html>")
        with pytest.raises(RuntimeError):
            download_and_parse(session)

    def test_falls_back_to_header_text_search_when_id_changes(self):
        html = (
            '<html><body><table id="tablepress-99">'
            '<thead><tr><th>Country</th><th>Airline</th><th>Aircraft Type</th>'
            '<th>Registration</th><th>Description</th></tr></thead>'
            '<tbody><tr class="row-1">'
            '<td class="column-1">USA</td><td class="column-2">JetBlue</td>'
            '<td class="column-3">Airbus A320</td>'
            '<td class="column-4"><a href="#">N775JB</a></td>'
            '<td class="column-5">America250</td>'
            '</tr></tbody></table></body></html>'
        )
        session = _make_session(html)
        rows = download_and_parse(session)
        assert len(rows) == 1
        assert rows[0]["registration"] == "N775JB"


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_matched_record_written(self):
        rows = [{"registration": "N775JB", "description": "Vets In Blue (2022) / America250 (sticker)"}]
        r = _make_redis_with_search(icao_hex="AA7C64", registration="N775JB")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_various_registration_skipped(self):
        rows = [{"registration": "Various", "description": "10 Years (sticker)"}]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0
        r.ft.return_value.search.assert_not_called()

    def test_various_case_insensitive_skipped(self):
        rows = [{"registration": "various", "description": "10 Years (sticker)"}]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_no_redis_match_not_written(self):
        rows = [{"registration": "N775JB", "description": "America250"}]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_empty_registration_skipped(self):
        rows = [{"registration": "", "description": "America250"}]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_writes_to_livery_key(self):
        rows = [{"registration": "N775JB", "description": "America250"}]
        r = _make_redis_with_search(icao_hex="AA7C64", registration="N775JB")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call is not None
        key_used = set_call[0][0]
        assert key_used == "aircraft:livery:AA7C64"

    def test_written_record_fields(self):
        rows = [{"registration": "N775JB", "description": "Vets In Blue (2022) / America250 (sticker)"}]
        r = _make_redis_with_search(icao_hex="AA7C64", registration="N775JB")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        written = set_call[0][2]
        assert written["special_livery"] == "America250"
        assert written["source"] == "airportwebcams-special-liveries"

    def test_fire_and_forget_no_read_before_write(self):
        rows = [{"registration": "N775JB", "description": "America250"}]
        r = _make_redis_with_search(icao_hex="AA7C64", registration="N775JB")
        write_to_redis(rows, r, REDIS_TTL)
        r.json.return_value.get.assert_not_called()

    def test_empty_special_livery_after_transform_skipped(self):
        rows = [{"registration": "N775JB", "description": "(sticker)"}]
        r = _make_redis_with_search(icao_hex="AA7C64", registration="N775JB")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_empty_list_returns_zero(self):
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

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("airportwebcams_special_liveries_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("airportwebcams_special_liveries_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 77, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "77"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/airportwebcams-special-liveries"


# ---------------------------------------------------------------------------
# Tests: network (real outbound HTTP call — see #405)
# ---------------------------------------------------------------------------

class TestNetwork:
    @pytest.mark.network
    def test_url_reachable(self):
        assert_url_reachable(
            SOURCE_URL,
            "airportwebcams-special-liveries",
            headers={"User-Agent": "Mozilla/5.0 (compatible; P5Software SkyFollower)"},
        )
