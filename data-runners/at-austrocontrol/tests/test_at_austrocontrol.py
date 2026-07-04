"""Tests for the Austria Austrocontrol data runner."""

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
        "at_austrocontrol_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["at_austrocontrol_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_decode_aircraft_type = _mod._decode_aircraft_type
_decode_country = _mod._decode_country
_parse_halter = _mod._parse_halter
_build_record = _mod._build_record
_type_tokens = _mod._type_tokens
_type_check_passes = _mod._type_check_passes
_escape_tag = _mod._escape_tag
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_item(
    oid=296490,
    kennzeichen="ARG",
    seriennummer="38-81A0102",
    luftfahrzeugart="Flugzeug",
    hersteller="Piper Aircraft Corp.",
    baumuster="PA-38-112",
    halter="Josef Mustermann\r\n1010 Wien, Ringstraße 1\r\nÖsterreich",
    loeschung=None,
) -> dict:
    return {
        "oid": oid,
        "kennzeichen": kennzeichen,
        "ordnungszahl": 2373,
        "seriennummer": seriennummer,
        "luftfahrzeugart": luftfahrzeugart,
        "luftfahrzeugartOid": 167,
        "mtom": 758.0,
        "hersteller": hersteller,
        "baumuster": baumuster,
        "halter": halter,
        "loeschung": loeschung,
    }


def _make_redis(existing=None):
    r = MagicMock()
    mget_result = [[existing]] if existing is not None else [None]
    r.json.return_value.mget.return_value = mget_result
    r.json.return_value.get.return_value = None
    return r


def _make_redis_with_search(icao_hex="440123", registration="OE-ARG", simple_record=None):
    """Redis mock that returns a search result and a simple aircraft record for the type check."""
    r = _make_redis()
    doc = MagicMock()
    doc.id = f"aircraft:mictronics:{icao_hex}"
    doc.registration = registration
    results = MagicMock()
    results.docs = [doc]
    r.ft.return_value.search.return_value = results
    if simple_record is None:
        simple_record = {"icao_hex": icao_hex, "registration": registration}
    r.json.return_value.get.return_value = simple_record
    return r


# ---------------------------------------------------------------------------
# Tests: _decode_aircraft_type
# ---------------------------------------------------------------------------

class TestDecodeAircraftType:
    def test_flugzeug(self):
        assert _decode_aircraft_type("Flugzeug") == "Airplane"

    def test_hubschrauber(self):
        assert _decode_aircraft_type("Hubschrauber") == "Helicopter"

    def test_tragschrauber(self):
        assert _decode_aircraft_type("Tragschrauber") == "Gyroplane"

    def test_eigenstartfaehiger_motorsegler(self):
        assert _decode_aircraft_type("Eigenstartfähiger Motorsegler") == "Glider"

    def test_nicht_eigenstartfaehiger_motorsegler(self):
        assert _decode_aircraft_type("Nicht eigenstartfähiger Motorsegler") == "Glider"

    def test_unknown_passes_through(self):
        assert _decode_aircraft_type("Zeppelin") == "Zeppelin"

    def test_empty_returns_none(self):
        assert _decode_aircraft_type("") is None

    def test_whitespace_only_returns_none(self):
        assert _decode_aircraft_type("   ") is None

    def test_strips_whitespace(self):
        assert _decode_aircraft_type("  Flugzeug  ") == "Airplane"


# ---------------------------------------------------------------------------
# Tests: _decode_country
# ---------------------------------------------------------------------------

class TestDecodeCountry:
    def test_oesterreich(self):
        assert _decode_country("Österreich") == "AT"

    def test_deutschland(self):
        assert _decode_country("Deutschland") == "DE"

    def test_tschechische_republik(self):
        assert _decode_country("Tschechische Republik") == "CZ"

    def test_schweiz(self):
        assert _decode_country("Schweiz") == "CH"

    def test_spanien(self):
        assert _decode_country("Spanien") == "ES"

    def test_unknown_passes_through(self):
        assert _decode_country("Utopia") == "Utopia"

    def test_empty_returns_none(self):
        assert _decode_country("") is None

    def test_whitespace_only_returns_none(self):
        assert _decode_country("   ") is None

    def test_all_known_countries_mapped(self):
        known = [
            "Belgien", "Bulgarien", "Deutschland", "Dänemark", "Estland",
            "Finnland", "Frankreich", "Griechenland", "Irland", "Italien",
            "Kroatien", "Litauen", "Luxemburg", "Malta", "Monaco",
            "Niederlande", "Norwegen", "Polen", "Portugal", "Rumänien",
            "Schweden", "Schweiz", "Slowakei", "Slowenien", "Spanien",
            "Tschechische Republik", "Ungarn", "Zypern", "Österreich",
        ]
        for name in known:
            result = _decode_country(name)
            assert result is not None and len(result) == 2, f"{name!r} → {result!r}"


# ---------------------------------------------------------------------------
# Tests: _parse_halter
# ---------------------------------------------------------------------------

class TestParseHalter:
    def test_austrian_individual(self):
        raw = "Josef Mustermann\r\n1010 Wien, Ringstraße 1\r\nÖsterreich"
        result = _parse_halter(raw)
        assert result["names"] == ["Josef Mustermann"]
        assert result["street"] == ["Ringstraße 1"]
        assert result["city"] == "Wien"
        assert result["postal_code"] == "1010"
        assert result["country"] == "AT"

    def test_foreign_individual(self):
        raw = "Josef Mokry\r\n54401 Dvur Kralove n. Labem, Celakovskeho 886\r\nTschechische Republik"
        result = _parse_halter(raw)
        assert result["names"] == ["Josef Mokry"]
        assert result["street"] == ["Celakovskeho 886"]
        assert result["city"] == "Dvur Kralove n. Labem"
        assert result["postal_code"] == "54401"
        assert result["country"] == "CZ"

    def test_organisation_with_quotes(self):
        raw = '"Luftsportverband Salzburg"\r\n5020 Salzburg, Kendlerstrasse 90\r\nÖsterreich'
        result = _parse_halter(raw)
        assert result["names"] == ["Luftsportverband Salzburg"]
        assert result["street"] == ["Kendlerstrasse 90"]
        assert result["city"] == "Salzburg"
        assert result["postal_code"] == "5020"
        assert result["country"] == "AT"

    def test_multi_owner_takes_first(self):
        raw = (
            "Renate Wildberger\r\n4070 Eferding, Molkereistraße 23\r\nÖsterreich\r\n"
            "Alois Wildberger\r\n4082 Aschach, Siernerstraße 35\r\nÖsterreich"
        )
        result = _parse_halter(raw)
        assert result["names"] == ["Renate Wildberger"]
        assert result["city"] == "Eferding"

    def test_unknown_country_passes_through(self):
        raw = "Test Name\r\n1010 Wien, Hauptstrasse 1\r\nUnknownland"
        result = _parse_halter(raw)
        assert result["country"] == "Unknownland"

    def test_empty_returns_none(self):
        assert _parse_halter("") is None

    def test_none_returns_none(self):
        assert _parse_halter(None) is None

    def test_na_returns_none(self):
        assert _parse_halter("N/A") is None

    def test_fewer_than_three_lines_returns_none(self):
        assert _parse_halter("Only one line") is None
        assert _parse_halter("Line 1\r\nLine 2") is None

    def test_address_without_comma(self):
        raw = "Test Name\r\n1010 Wien\r\nÖsterreich"
        result = _parse_halter(raw)
        assert result["names"] == ["Test Name"]
        assert result["postal_code"] == "1010"
        assert result["city"] == "Wien"
        assert result.get("street") is None

    def test_spanish_address(self):
        raw = "FIS ATO EUROPE SL\r\n11150 Vejer de la Frontera, Pago de Patria\r\nSpanien"
        result = _parse_halter(raw)
        assert result["names"] == ["FIS ATO EUROPE SL"]
        assert result["country"] == "ES"
        assert result["postal_code"] == "11150"
        assert result["city"] == "Vejer de la Frontera"
        assert result["street"] == ["Pago de Patria"]


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        item = _make_item()
        record = _build_record(item, "440123", "OE-ARG")
        assert record["icao_hex"] == "440123"
        assert record["registration"] == "OE-ARG"
        assert record["source"] == "at-austrocontrol"
        assert record["aircraft"]["type"] == "Airplane"
        assert record["aircraft"]["manufacturer"] == "Piper Aircraft Corp."
        assert record["aircraft"]["model"] == "PA-38-112"
        assert record["aircraft"]["serial_number"] == "38-81A0102"
        assert record["registrant"]["names"] == ["Josef Mustermann"]
        assert record["registrant"]["country"] == "AT"

    def test_helicopter_type(self):
        item = _make_item(luftfahrzeugart="Hubschrauber")
        record = _build_record(item, "440123", "OE-ARG")
        assert record["aircraft"]["type"] == "Helicopter"

    def test_glider_type(self):
        item = _make_item(luftfahrzeugart="Eigenstartfähiger Motorsegler")
        record = _build_record(item, "440123", "OE-9522")
        assert record["aircraft"]["type"] == "Glider"

    def test_numeric_kennzeichen(self):
        item = _make_item(kennzeichen="9522", luftfahrzeugart="Eigenstartfähiger Motorsegler")
        record = _build_record(item, "440456", "OE-9522")
        assert record["registration"] == "OE-9522"

    def test_empty_halter_omits_registrant(self):
        item = _make_item(halter="")
        record = _build_record(item, "440123", "OE-ARG")
        assert "registrant" not in record

    def test_empty_manufacturer_omitted(self):
        item = _make_item(hersteller="")
        record = _build_record(item, "440123", "OE-ARG")
        assert "manufacturer" not in record["aircraft"]

    def test_empty_serial_number_omitted(self):
        item = _make_item(seriennummer="")
        record = _build_record(item, "440123", "OE-ARG")
        assert "serial_number" not in record["aircraft"]


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("OEARG") == "OEARG"

    def test_hyphen_escaped(self):
        result = _escape_tag("OE-ARG")
        assert "\\-" in result

    def test_dot_escaped(self):
        result = _escape_tag("OE.ARG")
        assert "\\." in result


# ---------------------------------------------------------------------------
# Tests: _type_tokens
# ---------------------------------------------------------------------------

class TestTypeTokens:
    def test_plain_designator(self):
        assert _type_tokens("B737") == {"B737"}

    def test_hyphenated_designator_strips_suffix(self):
        # "B737-800" → findall finds "B737", split('-')[0] = "B737"
        assert _type_tokens("B737-800") == {"B737"}

    def test_no_digits_returns_empty(self):
        assert _type_tokens("Piper") == set()

    def test_hyphen_between_letters_and_digits_no_match(self):
        # "PA-38" has a hyphen between letters and digits; regex won't match across it
        assert _type_tokens("PA-38") == set()

    def test_compact_form_matches(self):
        assert _type_tokens("PA38") == {"PA38"}

    def test_multiple_tokens(self):
        tokens = _type_tokens("B737 A320")
        assert tokens == {"B737", "A320"}

    def test_case_insensitive(self):
        assert _type_tokens("b737") == {"B737"}

    def test_empty_string_returns_empty(self):
        assert _type_tokens("") == set()


# ---------------------------------------------------------------------------
# Tests: _type_check_passes
# ---------------------------------------------------------------------------

class TestTypeCheckPasses:
    def test_empty_detail_model_passes(self):
        assert _type_check_passes({"type_designator": "B737"}, "") is True

    def test_empty_simple_record_passes(self):
        # No type_designator or manufacturer_model → simple_tokens empty → pass
        assert _type_check_passes({}, "B737-800") is True

    def test_no_tokens_in_detail_str_passes(self):
        # "Piper Tomahawk" has no matching tokens → detail_tokens empty → pass
        assert _type_check_passes({"type_designator": "PA38"}, "Piper Tomahawk") is True

    def test_matching_token_passes(self):
        simple = {"type_designator": "B737", "manufacturer_model": "Boeing 737-800"}
        assert _type_check_passes(simple, "B737-800") is True

    def test_mismatched_tokens_fails(self):
        simple = {"type_designator": "A320", "manufacturer_model": "Airbus A320"}
        assert _type_check_passes(simple, "B737-800") is False

    def test_type_designator_only(self):
        simple = {"type_designator": "C172"}
        assert _type_check_passes(simple, "C172S") is True

    def test_manufacturer_model_only(self):
        simple = {"manufacturer_model": "Cessna C172"}
        assert _type_check_passes(simple, "C172S") is True

    def test_case_insensitive_model_str(self):
        simple = {"type_designator": "B737"}
        assert _type_check_passes(simple, "b737-800") is True


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_active_record_written(self):
        item = _make_item()
        r = _make_redis_with_search(icao_hex="440123", registration="OE-ARG")
        count = write_to_redis([item], r, REDIS_TTL)
        assert count == 1
        # fire-and-forget: no read-before-write
        r.json.return_value.mget.assert_not_called()
        # type sanity check: simple record must have been fetched
        r.json.return_value.get.assert_called_once()

    def test_deregistered_record_skipped(self):
        item = _make_item(loeschung="2024-01-15")
        r = _make_redis_with_search()
        count = write_to_redis([item], r, REDIS_TTL)
        assert count == 0
        r.ft.return_value.search.assert_not_called()

    def test_no_redis_match_not_written(self):
        item = _make_item()
        r = _make_redis()
        results = MagicMock()
        results.docs = []
        r.ft.return_value.search.return_value = results
        count = write_to_redis([item], r, REDIS_TTL)
        assert count == 0
        r.json.return_value.mget.assert_not_called()
        r.json.return_value.get.assert_not_called()

    def test_fire_and_forget_no_read_before_write(self):
        """write_to_redis must not read existing records before writing (fire-and-forget)."""
        item = _make_item()
        r = _make_redis_with_search(icao_hex="440123", registration="OE-ARG")
        write_to_redis([item], r, REDIS_TTL)
        r.json.return_value.mget.assert_not_called()

    def test_writes_to_detail_key(self):
        """Records must be written to aircraft:registry:{icao_hex}, not icao_hex:{icao_hex}."""
        item = _make_item()
        r = _make_redis_with_search(icao_hex="440123", registration="OE-ARG")
        write_to_redis([item], r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call is not None
        key_used = set_call[0][0]
        assert key_used == "aircraft:registry:440123"
        assert "icao_hex:440123" not in key_used

    def test_source_field_in_written_record(self):
        """Every record written to Redis must contain source='at-austrocontrol'."""
        item = _make_item()
        r = _make_redis_with_search(icao_hex="440123", registration="OE-ARG")
        write_to_redis([item], r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call is not None
        written = set_call[0][2]
        assert written.get("source") == "at-austrocontrol"

    def test_type_check_skips_when_simple_raw_none(self):
        """If the aircraft:simple key is missing (None), the record must be skipped."""
        item = _make_item()
        r = _make_redis_with_search(icao_hex="440123", registration="OE-ARG")
        r.json.return_value.get.return_value = None
        count = write_to_redis([item], r, REDIS_TTL)
        assert count == 0
        r.pipeline.return_value.json.return_value.set.assert_not_called()

    def test_registration_prefixed_with_oe(self):
        # Verify that at least one RediSearch query was issued for OE-ARG
        item = _make_item(kennzeichen="ARG")
        r = _make_redis_with_search(icao_hex="440123", registration="OE-ARG")
        count = write_to_redis([item], r, REDIS_TTL)
        # Registration OE-ARG matched → one record written
        assert count == 1
        # The built record carries the OE- prefixed registration and writes to detail key
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call is not None
        key_used, _, written = set_call[0]
        assert key_used == "aircraft:registry:440123"
        assert written.get("registration") == "OE-ARG"

    def test_numeric_kennzeichen_prefixed(self):
        item = _make_item(kennzeichen="9522", luftfahrzeugart="Eigenstartfähiger Motorsegler")
        r = _make_redis_with_search(icao_hex="440456", registration="OE-9522")
        count = write_to_redis([item], r, REDIS_TTL)
        assert count == 1

    def test_empty_kennzeichen_skipped(self):
        item = _make_item(kennzeichen="")
        r = _make_redis_with_search()
        count = write_to_redis([item], r, REDIS_TTL)
        assert count == 0

    def test_empty_list_returns_zero(self):
        r = _make_redis()
        results = MagicMock()
        results.docs = []
        r.ft.return_value.search.return_value = results
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_multiple_records_written(self):
        items = [
            _make_item(kennzeichen="ARG"),
            _make_item(kennzeichen="ARK", seriennummer="471"),
        ]
        r = _make_redis()
        # Return a valid simple record so type check passes
        r.json.return_value.get.return_value = {"icao_hex": "440121", "registration": "OE-ARG"}

        call_count = [0]

        def _search(query):
            call_count[0] += 1
            results = MagicMock()
            docs = []
            for reg in ["OE-ARG", "OE-ARK"]:
                doc = MagicMock()
                doc.id = f"aircraft:mictronics:44012{call_count[0]}"
                doc.registration = reg
                docs.append(doc)
            results.docs = docs
            return results

        r.ft.return_value.search.side_effect = _search
        count = write_to_redis(items, r, REDIS_TTL)
        assert count == 2

    def test_mixed_active_and_deregistered(self):
        items = [
            _make_item(kennzeichen="ARG"),
            _make_item(kennzeichen="OLD", loeschung="2023-06-01"),
        ]
        r = _make_redis_with_search(icao_hex="440123", registration="OE-ARG")
        count = write_to_redis(items, r, REDIS_TTL)
        assert count == 1

    def test_null_fields_omitted_from_written_record(self):
        """Optional fields with no source data must be absent from the written record, not None."""
        item = _make_item(hersteller="", seriennummer="")
        r = _make_redis_with_search(icao_hex="440123", registration="OE-ARG")
        write_to_redis([item], r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call is not None
        written = set_call[0][2]
        assert "manufacturer" not in written["aircraft"]
        assert "serial_number" not in written["aircraft"]


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
        with patch("at_austrocontrol_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("at_austrocontrol_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("at_austrocontrol_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 77, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "77"

    def test_mqtt_publishes_last_run_at(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("at_austrocontrol_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/last_run_at" in topics

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("at_austrocontrol_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/at-austrocontrol"
