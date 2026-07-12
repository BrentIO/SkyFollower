"""Tests for the Cayman Islands CAA data runner."""

from __future__ import annotations

import importlib.util
import io
import json
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
        "ky_caa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ky_caa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

from shared.url_reachability import assert_url_reachable

_normalise_header = _mod._normalise_header
_normalise_cell = _mod._normalise_cell
_build_record = _mod._build_record
_nationality_to_iso = _mod._nationality_to_iso
_type_tokens = _mod._type_tokens
_type_check_passes = _mod._type_check_passes
_ensure_search_index = _mod._ensure_search_index
_escape_tag = _mod._escape_tag
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
COL_REGISTRATION = _mod.COL_REGISTRATION
COL_OWNER = _mod.COL_OWNER
COL_ADDRESS = _mod.COL_ADDRESS
COL_NATIONALITY = _mod.COL_NATIONALITY
COL_SERIES_TYPE = _mod.COL_SERIES_TYPE
COL_SERIAL = _mod.COL_SERIAL

from shared.redis_keys import AIRCRAFT_REGISTRY_SEARCH_INDEX, AIRCRAFT_MICTRONICS_SEARCH_INDEX


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    registration="VP-CAD",
    owner="Castle Air Limited",
    address="Castle Air Limited, Head Office, Trebrown, Liskeard, Cornwall PL14 3PX United Kingdom",
    nationality="United Kingdom",
    series_type="Leonardo S.p.A. AW139",
    serial="31475",
) -> dict:
    return {
        COL_REGISTRATION: registration,
        COL_OWNER: owner,
        COL_ADDRESS: address,
        COL_NATIONALITY: nationality,
        COL_SERIES_TYPE: series_type,
        COL_SERIAL: serial,
    }


def _make_redis():
    """Return a bare MagicMock Redis client with no specific setup."""
    r = MagicMock()
    return r


def _make_redis_with_search(icao_hex="C00001", registration="VP-CAD", simple_record=None):
    """Return a mock Redis client that resolves one registration via the simple search index.

    ``simple_record`` is what ``mget`` returns for the aircraft:simple key.
    Defaults to an empty dict, which always passes the type sanity check.
    """
    r = _make_redis()
    doc = MagicMock()
    doc.id = f"aircraft:mictronics:{icao_hex}"
    doc.registration = registration
    results = MagicMock()
    results.docs = [doc]
    r.ft.return_value.search.return_value = results
    # mget is called with the list of simple keys — return one entry per key
    sr = simple_record if simple_record is not None else {}
    r.json.return_value.mget.return_value = [[sr]]
    return r


# ---------------------------------------------------------------------------
# Tests: _normalise_header
# ---------------------------------------------------------------------------

class TestNormaliseHeader:
    def test_plain_header_unchanged(self):
        assert _normalise_header("Series Type") == "Series Type"

    def test_newline_in_header_becomes_space(self):
        assert _normalise_header("Aircraft\nRegistration") == "Aircraft Registration"

    def test_serial_number_header(self):
        assert _normalise_header("Serial\nNumber") == "Serial Number"

    def test_date_registered_header(self):
        assert _normalise_header("Date\nRegistered") == "Date Registered"

    def test_strips_leading_trailing_whitespace(self):
        assert _normalise_header("  Series Type  ") == "Series Type"

    def test_multiple_newlines_collapsed(self):
        assert _normalise_header("Aircraft\n\nRegistration") == "Aircraft Registration"


# ---------------------------------------------------------------------------
# Tests: _normalise_cell
# ---------------------------------------------------------------------------

class TestNormaliseCell:
    def test_plain_value_unchanged(self):
        assert _normalise_cell("VP-CAD") == "VP-CAD"

    def test_newline_in_cell_becomes_space(self):
        assert _normalise_cell("Gulfstream Aerospace\nCorporation G-IV") == "Gulfstream Aerospace Corporation G-IV"

    def test_none_returns_empty_string(self):
        assert _normalise_cell(None) == ""

    def test_empty_string_returns_empty_string(self):
        assert _normalise_cell("") == ""

    def test_strips_whitespace(self):
        assert _normalise_cell("  VP-CAD  ") == "VP-CAD"

    def test_multiline_manufacturer_model(self):
        assert _normalise_cell("Airbus Helicopters\nDeutschland MBB-BK117 D-2") == "Airbus Helicopters Deutschland MBB-BK117 D-2"


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record("C00001", "VP-CAD", row)
        assert record["icao_hex"] == "C00001"
        assert record["registration"] == "VP-CAD"
        assert record["source"] == "ky-caa"
        assert record["military"] is False
        assert record["aircraft"]["model"] == "Leonardo S.p.A. AW139"
        assert record["aircraft"]["serial_number"] == "31475"
        assert record["registrant"]["names"] == ["Castle Air Limited"]
        assert record["registrant"]["street"] == ["Castle Air Limited", "Head Office", "Trebrown", "Liskeard", "Cornwall PL14 3PX"]
        assert record["registrant"]["country"] == "GB"

    def test_source_field_always_present(self):
        row = _make_row(series_type="", serial="", owner="", address="", nationality="")
        record = _build_record("C00001", "VP-CAD", row)
        assert record["source"] == "ky-caa"

    def test_combined_manufacturer_model_stored_as_model(self):
        row = _make_row(series_type="Gulfstream Aerospace Corporation G-IV")
        record = _build_record("C00002", "VP-CAI", row)
        assert record["aircraft"]["model"] == "Gulfstream Aerospace Corporation G-IV"

    def test_address_stored_as_street_without_country(self):
        row = _make_row(
            address="P.O. Box 309, Ugland House, Grand Cayman KY1-1104 Cayman Islands",
            nationality="Cayman Islands",
        )
        record = _build_record("C00002", "VP-CAF", row)
        assert record["registrant"]["street"] == ["P.O. Box 309", "Ugland House", "Grand Cayman KY1-1104"]

    def test_nationality_stored_as_iso_country(self):
        row = _make_row(nationality="United Kingdom")
        record = _build_record("C00001", "VP-CAD", row)
        assert record["registrant"]["country"] == "GB"

    def test_nationality_cayman_islands(self):
        row = _make_row(nationality="Cayman Islands")
        record = _build_record("C00001", "VP-CAD", row)
        assert record["registrant"]["country"] == "KY"

    def test_nationality_case_insensitive(self):
        # "Cayman islands" (lowercase 'i') appears in the source PDF
        row = _make_row(nationality="Cayman islands")
        record = _build_record("C00001", "VP-CAD", row)
        assert record["registrant"]["country"] == "KY"

    def test_nationality_irish_maps_to_ie(self):
        row = _make_row(nationality="Irish")
        record = _build_record("C00001", "VP-CAD", row)
        assert record["registrant"]["country"] == "IE"

    def test_unknown_nationality_omits_country(self):
        row = _make_row(nationality="Atlantis")
        record = _build_record("C00001", "VP-CAD", row)
        assert "country" not in record.get("registrant", {})

    def test_empty_nationality_omits_country(self):
        row = _make_row(nationality="")
        record = _build_record("C00001", "VP-CAD", row)
        assert "country" not in record.get("registrant", {})

    def test_country_only_creates_registrant(self):
        # Nationality alone (no owner or address) still produces a registrant block
        row = _make_row(owner="", address="", nationality="United Kingdom")
        record = _build_record("C00001", "VP-CAD", row)
        assert record["registrant"]["country"] == "GB"
        assert "names" not in record["registrant"]
        assert "street" not in record["registrant"]

    def test_empty_address_omits_street(self):
        row = _make_row(address="")
        record = _build_record("C00001", "VP-CAD", row)
        assert "street" not in record.get("registrant", {})

    def test_all_registrant_fields_empty_omits_registrant(self):
        row = _make_row(owner="", address="", nationality="")
        record = _build_record("C00001", "VP-CAD", row)
        assert "registrant" not in record

    def test_address_without_owner_still_stored(self):
        row = _make_row(owner="", address="Grand Cayman KY1-1104 Cayman Islands", nationality="Cayman Islands")
        record = _build_record("C00001", "VP-CAD", row)
        assert record["registrant"]["street"] == ["Grand Cayman KY1-1104"]  # no comma in this address
        assert "names" not in record["registrant"]

    def test_empty_series_type_omits_model(self):
        row = _make_row(series_type="")
        record = _build_record("C00001", "VP-CAD", row)
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omits_serial_number(self):
        row = _make_row(serial="")
        record = _build_record("C00001", "VP-CAD", row)
        assert "serial_number" not in record.get("aircraft", {})

    def test_empty_series_and_serial_omits_aircraft_key(self):
        row = _make_row(series_type="", serial="")
        record = _build_record("C00001", "VP-CAD", row)
        assert "aircraft" not in record

    def test_registration_preserved(self):
        row = _make_row(registration="VP-CBB")
        record = _build_record("C00003", "VP-CBB", row)
        assert record["registration"] == "VP-CBB"

    def test_multiline_owner_normalised(self):
        row = _make_row(owner="Cayman Airways Express Limited")
        record = _build_record("C00004", "VP-CAW", row)
        assert record["registrant"]["names"] == ["Cayman Airways Express Limited"]


# ---------------------------------------------------------------------------
# Tests: _nationality_to_iso
# ---------------------------------------------------------------------------

class TestNationalityToIso:
    def test_united_kingdom(self):
        assert _nationality_to_iso("United Kingdom") == "GB"

    def test_cayman_islands(self):
        assert _nationality_to_iso("Cayman Islands") == "KY"

    def test_lowercase_cayman_islands(self):
        assert _nationality_to_iso("cayman islands") == "KY"

    def test_mixed_case_pdf_typo(self):
        # "Cayman islands" (lowercase i) appears in actual PDF data
        assert _nationality_to_iso("Cayman islands") == "KY"

    def test_ireland(self):
        assert _nationality_to_iso("Ireland") == "IE"

    def test_irish(self):
        assert _nationality_to_iso("Irish") == "IE"

    def test_bermuda(self):
        assert _nationality_to_iso("Bermuda") == "BM"

    def test_british_virgin_islands(self):
        assert _nationality_to_iso("British Virgin Islands") == "VG"

    def test_isle_of_man(self):
        assert _nationality_to_iso("Isle of Man") == "IM"

    def test_unknown_returns_none(self):
        assert _nationality_to_iso("Atlantis") is None

    def test_empty_returns_none(self):
        assert _nationality_to_iso("") is None


# ---------------------------------------------------------------------------
# Tests: _type_tokens
# ---------------------------------------------------------------------------

class TestTypeTokens:
    def test_aw139_extracted(self):
        assert _type_tokens("Leonardo S.p.A. AW139") == {"AW139"}

    def test_b737_extracted_from_designator(self):
        # The regex requires a letter prefix immediately before the digits.
        # A standalone "B737" designator extracts correctly.
        assert _type_tokens("B737") == {"B737"}

    def test_b737_not_extracted_from_full_model(self):
        # "Boeing 737-800" has no letter immediately before "737" — no token.
        assert _type_tokens("Boeing 737-800") == set()

    def test_type_designator_alone(self):
        assert _type_tokens("AW139") == {"AW139"}

    def test_multiple_tokens(self):
        tokens = _type_tokens("AW139 AW109")
        assert "AW139" in tokens
        assert "AW109" in tokens

    def test_empty_string_returns_empty_set(self):
        assert _type_tokens("") == set()

    def test_no_matching_tokens(self):
        assert _type_tokens("Leonardo S.p.A.") == set()

    def test_case_insensitive_input(self):
        assert _type_tokens("aw139") == {"AW139"}

    def test_hyphen_stripped_from_token(self):
        # "B737-800" → token is "B737" (the part before the hyphen)
        assert _type_tokens("B737-800") == {"B737"}

    def test_bd700_extracted_from_designator(self):
        # Type designator "BD700" (no hyphen) extracts correctly.
        assert _type_tokens("BD700") == {"BD700"}

    def test_bd700_hyphenated_extracts_a10(self):
        # "BD-700-1A10" has a hyphen between "BD" and "700" so "BD700" is not
        # found; the regex does find "A10" from the "1A10" portion instead.
        assert _type_tokens("Bombardier Inc. BD-700-1A10") == {"A10"}


# ---------------------------------------------------------------------------
# Tests: _type_check_passes
# ---------------------------------------------------------------------------

class TestTypeCheckPasses:
    def test_empty_detail_model_always_passes(self):
        simple = {"type_designator": "AW109", "manufacturer_model": "Leonardo AW109"}
        assert _type_check_passes(simple, "") is True

    def test_none_detail_model_always_passes(self):
        simple = {"type_designator": "AW109", "manufacturer_model": "Leonardo AW109"}
        assert _type_check_passes(simple, None) is True

    def test_no_simple_tokens_always_passes(self):
        # Simple record has no recognisable type tokens
        simple = {"type_designator": "Unknown", "manufacturer_model": "No model here"}
        assert _type_check_passes(simple, "Leonardo AW139") is True

    def test_no_detail_tokens_always_passes(self):
        simple = {"type_designator": "AW139", "manufacturer_model": "Leonardo AW139"}
        assert _type_check_passes(simple, "Leonardo S.p.A.") is True

    def test_matching_tokens_passes(self):
        simple = {"type_designator": "AW139", "manufacturer_model": "Leonardo AW139"}
        assert _type_check_passes(simple, "Leonardo S.p.A. AW139") is True

    def test_mismatched_tokens_fails(self):
        simple = {"type_designator": "AW109", "manufacturer_model": "Leonardo AW109"}
        assert _type_check_passes(simple, "Leonardo S.p.A. AW139") is False

    def test_empty_simple_record_always_passes(self):
        # No type_designator or manufacturer_model → no tokens → passes
        assert _type_check_passes({}, "Leonardo AW139") is True

    def test_type_designator_only_in_simple(self):
        simple = {"type_designator": "AW139"}
        assert _type_check_passes(simple, "Leonardo AW139") is True

    def test_manufacturer_model_only_in_simple(self):
        simple = {"manufacturer_model": "Leonardo AW139"}
        assert _type_check_passes(simple, "Leonardo AW139") is True

    def test_bd700_designator_match(self):
        # Both sides have "BD700" as a token — passes.
        simple = {"type_designator": "BD700", "manufacturer_model": "Bombardier BD700"}
        assert _type_check_passes(simple, "Bombardier BD700") is True

    def test_bd700_vs_aw139_mismatch(self):
        simple = {"type_designator": "BD700", "manufacturer_model": "Bombardier BD700"}
        assert _type_check_passes(simple, "Leonardo AW139") is False


# ---------------------------------------------------------------------------
# Tests: _ensure_search_index
# ---------------------------------------------------------------------------

class TestEnsureSearchIndex:
    def test_creates_index_when_not_exists(self):
        r = MagicMock()
        r.ft.return_value.info.side_effect = Exception("Index does not exist")
        _ensure_search_index(r)
        r.ft.assert_called_with(AIRCRAFT_REGISTRY_SEARCH_INDEX)
        r.ft.return_value.create_index.assert_called_once()

    def test_skips_creation_when_index_exists(self):
        r = MagicMock()
        r.ft.return_value.info.return_value = {"some": "info"}
        _ensure_search_index(r)
        r.ft.return_value.create_index.assert_not_called()

    def test_creates_index_with_detail_prefix(self):
        r = MagicMock()
        r.ft.return_value.info.side_effect = Exception("Index does not exist")
        with patch("ky_caa_main.IndexDefinition") as mock_def:
            _ensure_search_index(r)
        mock_def.assert_called_once()
        call_kwargs = mock_def.call_args.kwargs
        assert call_kwargs.get("prefix") == ["aircraft:registry:"]


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("VPCAD") == "VPCAD"

    def test_hyphen_escaped(self):
        result = _escape_tag("VP-CAD")
        assert "\\-" in result

    def test_dot_escaped(self):
        result = _escape_tag("VP.CAD")
        assert "\\." in result


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_active_record_written(self):
        row = _make_row()
        r = _make_redis_with_search(icao_hex="C00001", registration="VP-CAD")
        count = write_to_redis([row], r, REDIS_TTL)
        assert count == 1
        r.json.return_value.mget.assert_called_once()

    def test_no_redis_match_not_written(self):
        row = _make_row()
        r = _make_redis()
        results = MagicMock()
        results.docs = []
        r.ft.return_value.search.return_value = results
        count = write_to_redis([row], r, REDIS_TTL)
        assert count == 0
        r.json.return_value.mget.assert_not_called()

    def test_no_simple_record_skipped(self):
        """Records where the simple key does not exist are skipped."""
        row = _make_row()
        # simple_record=None causes mget to return [None] → simple_raw_list is None
        r = _make_redis_with_search(icao_hex="C00001", registration="VP-CAD")
        r.json.return_value.mget.return_value = [None]  # key missing in Redis
        count = write_to_redis([row], r, REDIS_TTL)
        assert count == 0

    def test_type_check_mismatch_skips_record(self):
        """Records where the type check fails are skipped."""
        row = _make_row(series_type="Leonardo S.p.A. AW139")
        # Simple record says AW109 — mismatch with AW139
        simple = {"type_designator": "AW109", "manufacturer_model": "Leonardo AW109"}
        r = _make_redis_with_search(icao_hex="C00001", registration="VP-CAD", simple_record=simple)
        count = write_to_redis([row], r, REDIS_TTL)
        assert count == 0

    def test_type_check_match_writes_record(self):
        """Records where the type check passes are written."""
        row = _make_row(series_type="Leonardo S.p.A. AW139")
        simple = {"type_designator": "AW139", "manufacturer_model": "Leonardo AW139"}
        r = _make_redis_with_search(icao_hex="C00001", registration="VP-CAD", simple_record=simple)
        count = write_to_redis([row], r, REDIS_TTL)
        assert count == 1

    def test_record_written_without_reading_existing_detail(self):
        """write_to_redis does not read the existing aircraft:detail key before writing."""
        row = _make_row()
        r = _make_redis_with_search(icao_hex="C00001", registration="VP-CAD")
        write_to_redis([row], r, REDIS_TTL)
        # mget is called once — only for the simple key (type check), not for the detail key
        r.json.return_value.mget.assert_called_once()
        call_args = r.json.return_value.mget.call_args
        keys_arg = call_args[0][0]
        assert all("aircraft:mictronics:" in k for k in keys_arg)
        assert not any("aircraft:registry:" in k for k in keys_arg)

    def test_writes_to_detail_key(self):
        """Verified key written is aircraft:registry:{icao_hex}."""
        row = _make_row()
        r = _make_redis_with_search(icao_hex="C00001", registration="VP-CAD")
        write_to_redis([row], r, REDIS_TTL)
        pipe = r.pipeline.return_value
        set_call = pipe.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:C00001"

    def test_source_field_in_written_record(self):
        """Written records contain source='ky-caa'."""
        row = _make_row()
        r = _make_redis_with_search(icao_hex="C00001", registration="VP-CAD")
        write_to_redis([row], r, REDIS_TTL)
        pipe = r.pipeline.return_value
        set_call = pipe.json.return_value.set.call_args
        written_record = set_call[0][2]
        assert written_record["source"] == "ky-caa"

    def test_empty_registration_skipped(self):
        row = _make_row(registration="")
        r = _make_redis_with_search()
        count = write_to_redis([row], r, REDIS_TTL)
        assert count == 0

    def test_empty_list_returns_zero(self):
        r = _make_redis()
        results = MagicMock()
        results.docs = []
        r.ft.return_value.search.return_value = results
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_multiple_records_written(self):
        rows = [
            _make_row(registration="VP-CAD"),
            _make_row(registration="VP-CAF", series_type="Bombardier Inc. BD-700-1A10", serial="9686"),
        ]
        r = _make_redis()

        call_count = [0]

        def _search(query):
            call_count[0] += 1
            results = MagicMock()
            docs = []
            for reg in ["VP-CAD", "VP-CAF"]:
                doc = MagicMock()
                doc.id = f"aircraft:mictronics:C0000{call_count[0]}"
                doc.registration = reg
                docs.append(doc)
            results.docs = docs
            return results

        r.ft.return_value.search.side_effect = _search
        # Return an empty simple record for each — passes type check
        r.json.return_value.mget.return_value = [[{}], [{}]]
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 2

    def test_null_fields_omitted_from_written_record(self):
        """Optional fields with no source data are absent from the written record, not None."""
        row = _make_row(series_type="", serial="")
        r = _make_redis_with_search(icao_hex="C00001", registration="VP-CAD")
        write_to_redis([row], r, REDIS_TTL)
        pipe = r.pipeline.return_value
        set_call = pipe.json.return_value.set.call_args
        assert set_call[0][1] == "$"
        written_record = set_call[0][2]
        assert "model" not in written_record.get("aircraft", {})
        assert "serial_number" not in written_record.get("aircraft", {})

    def test_searches_simple_index(self):
        """Registration lookup queries the Mictronics simple search index."""
        row = _make_row()
        r = _make_redis_with_search(icao_hex="C00001", registration="VP-CAD")
        write_to_redis([row], r, REDIS_TTL)
        r.ft.assert_called_with(AIRCRAFT_MICTRONICS_SEARCH_INDEX)


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
        with patch("ky_caa_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ky_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ky_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 77, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "77"

    def test_mqtt_publishes_last_run_at(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ky_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/last_run_at" in topics

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ky_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/ky-caa"


# ---------------------------------------------------------------------------
# Tests: network (real outbound HTTP call — see #405)
# ---------------------------------------------------------------------------

class TestNetwork:
    @pytest.mark.network
    def test_url_reachable(self):
        assert_url_reachable(_mod.PDF_URL, "ky-caa", headers={"User-Agent": "P5Software SkyFollower"})
