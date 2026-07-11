"""Tests for the France DGAC data runner."""

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
        "fr_dgac_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["fr_dgac_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_parse_address = _mod._parse_address
_escape_tag = _mod._escape_tag
_type_tokens = _mod._type_tokens
_type_check_passes = _mod._type_check_passes
_build_record = _mod._build_record
_group_by_registration = _mod._group_by_registration
download_registry = _mod.download_registry
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
DOWNLOAD_URL = _mod.DOWNLOAD_URL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    immat="F-ABCD",
    proprietaire="DUPONT JEAN",
    adresse="12 RUE DE LA PAIX 75001 PARIS, FRANCE",
    constructeur="CESSNA",
    modele="172S",
    numero_serie="172S12345",
) -> dict:
    return {
        "IMMATRICULATION": immat,
        "PROPRIETAIRE": proprietaire,
        "ADRESSE_PROPRIETAIRE": adresse,
        "CONSTRUCTEUR": constructeur,
        "MODELE": modele,
        "NUMERO_SERIE": numero_serie,
    }


def _make_redis():
    r = MagicMock()
    return r


def _make_redis_with_search(icao_hex="4B1234", registration="F-ABCD", simple_record=None):
    """Mock Redis client that resolves one registration via the Mictronics search index.

    ``simple_record`` is what ``r.json().get(...)`` returns for the
    aircraft:mictronics key used by the type sanity check. ``None`` (the
    default) simulates the key not existing, which does NOT block the write
    in this runner (unlike some others).
    """
    r = _make_redis()
    doc = MagicMock()
    doc.id = f"aircraft:mictronics:{icao_hex}"
    doc.registration = registration
    results = MagicMock()
    results.docs = [doc]
    r.ft.return_value.search.return_value = results
    r.json.return_value.get.return_value = simple_record
    return r


def _make_redis_no_match():
    r = _make_redis()
    results = MagicMock()
    results.docs = []
    r.ft.return_value.search.return_value = results
    return r


# ---------------------------------------------------------------------------
# Tests: _parse_address
# ---------------------------------------------------------------------------

class TestParseAddress:
    def test_empty_string(self):
        assert _parse_address("") == (None, None, None, None)

    def test_whitespace_only(self):
        assert _parse_address("   ") == (None, None, None, None)

    def test_street_city_postcode_country(self):
        street, city, postal, country = _parse_address("12 RUE DE LA PAIX 75001 PARIS, FRANCE")
        assert street == ["12 RUE DE LA PAIX"]
        assert city == "PARIS"
        assert postal == "75001"
        assert country == "FR"

    def test_multi_word_country_pays_bas(self):
        street, city, postal, country = _parse_address("1 RUE PRINCIPALE 75001 PARIS, PAYS BAS")
        assert country == "NL"
        assert postal == "75001"

    def test_multi_word_country_checked_before_single_word(self):
        """ETATS UNIS D'AMERIQUE must not be mis-split by a single-word match
        on its last token."""
        street, city, postal, country = _parse_address("1 MAIN ST 75001 PARIS, ETATS UNIS D'AMERIQUE")
        assert country == "US"

    def test_no_postal_code_country_only(self):
        street, city, postal, country = _parse_address("SOME ADDRESS WITH NO DIGITS, FRANCE")
        assert street is None
        assert city is None
        assert postal is None
        assert country == "FR"

    def test_no_country_recognized_still_finds_postal_code(self):
        street, city, postal, country = _parse_address("12 RUE INCONNUE 75001 PARIS")
        assert street == ["12 RUE INCONNUE"]
        assert city == "PARIS"
        assert postal == "75001"
        assert country is None

    def test_no_country_no_postal_code(self):
        assert _parse_address("SOME UNPARSEABLE TEXT") == (None, None, None, None)

    def test_four_digit_number_not_treated_as_postal_code(self):
        street, city, postal, country = _parse_address("BUILDING 1234, FRANCE")
        assert postal is None
        assert country == "FR"

    def test_unknown_country_word_ignored(self):
        street, city, postal, country = _parse_address("12 RUE DE LA PAIX 75001 PARIS, UTOPIA")
        assert country is None
        assert postal == "75001"


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("FABCD") == "FABCD"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("F-ABCD")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: _type_tokens / _type_check_passes
# ---------------------------------------------------------------------------

class TestTypeTokens:
    def test_extracts_type_designator(self):
        assert _type_tokens("C172") == {"C172"}

    def test_extracts_from_full_model_string(self):
        assert "B738" in _type_tokens("BOEING 737-800 B738")

    def test_empty_string_returns_empty_set(self):
        assert _type_tokens("") == set()

    def test_no_matching_tokens(self):
        assert _type_tokens("SOME PLAIN TEXT") == set()


class TestTypeCheckPasses:
    def test_empty_detail_model_always_passes(self):
        assert _type_check_passes({"type_designator": "C172"}, "") is True

    def test_no_simple_tokens_passes(self):
        assert _type_check_passes({}, "C172") is True

    def test_no_detail_tokens_passes(self):
        simple = {"type_designator": "C172"}
        assert _type_check_passes(simple, "SOME PLAIN TEXT") is True

    def test_matching_tokens_pass(self):
        simple = {"type_designator": "C172"}
        assert _type_check_passes(simple, "CESSNA 172S") is True

    def test_mismatched_tokens_fail(self):
        simple = {"type_designator": "AW109"}
        assert _type_check_passes(simple, "LEONARDO AW139") is False


# ---------------------------------------------------------------------------
# Tests: _group_by_registration
# ---------------------------------------------------------------------------

class TestGroupByRegistration:
    def test_single_row_group(self):
        groups = _group_by_registration([_make_row()])
        assert list(groups.keys()) == ["F-ABCD"]
        assert groups["F-ABCD"]["names"] == ["DUPONT JEAN"]

    def test_co_ownership_names_collected_in_order(self):
        rows = [
            _make_row(proprietaire="ALICE"),
            _make_row(proprietaire="BOB"),
        ]
        groups = _group_by_registration(rows)
        assert groups["F-ABCD"]["names"] == ["ALICE", "BOB"]

    def test_duplicate_names_deduplicated(self):
        rows = [
            _make_row(proprietaire="ALICE"),
            _make_row(proprietaire="ALICE"),
        ]
        groups = _group_by_registration(rows)
        assert groups["F-ABCD"]["names"] == ["ALICE"]

    def test_blank_registration_skipped(self):
        rows = [_make_row(immat="")]
        groups = _group_by_registration(rows)
        assert groups == {}

    def test_blank_name_not_added(self):
        rows = [_make_row(proprietaire="")]
        groups = _group_by_registration(rows)
        assert groups["F-ABCD"]["names"] == []

    def test_address_taken_from_first_non_blank_row(self):
        rows = [
            _make_row(proprietaire="ALICE", adresse=""),
            _make_row(proprietaire="BOB", adresse="1 RUE X 75001 PARIS, FRANCE"),
        ]
        groups = _group_by_registration(rows)
        assert groups["F-ABCD"]["address_raw"] == "1 RUE X 75001 PARIS, FRANCE"

    def test_aircraft_fields_from_first_row(self):
        rows = [
            _make_row(proprietaire="ALICE", modele="172S"),
            _make_row(proprietaire="BOB", modele="DIFFERENT MODEL"),
        ]
        groups = _group_by_registration(rows)
        assert groups["F-ABCD"]["first_row"]["MODELE"] == "172S"

    def test_separate_registrations_grouped_independently(self):
        rows = [_make_row(immat="F-ABCD"), _make_row(immat="F-XYZW")]
        groups = _group_by_registration(rows)
        assert set(groups.keys()) == {"F-ABCD", "F-XYZW"}


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(
            icao_hex="4B1234",
            registration="F-ABCD",
            first_row=row,
            names=["DUPONT JEAN"],
            address_raw="12 RUE DE LA PAIX 75001 PARIS, FRANCE",
        )
        assert record["icao_hex"] == "4B1234"
        assert record["registration"] == "F-ABCD"
        assert record["military"] is False
        assert record["aircraft"]["manufacturer"] == "CESSNA"
        assert record["aircraft"]["model"] == "172S"
        assert record["aircraft"]["serial_number"] == "172S12345"
        assert record["registrant"]["names"] == ["DUPONT JEAN"]
        assert record["registrant"]["street"] == ["12 RUE DE LA PAIX"]
        assert record["registrant"]["city"] == "PARIS"
        assert record["registrant"]["postal_code"] == "75001"
        assert record["registrant"]["country"] == "FR"

    def test_no_names_or_address_omits_registrant(self):
        row = _make_row()
        record = _build_record(
            icao_hex="4B1234", registration="F-ABCD", first_row=row, names=[], address_raw=None,
        )
        assert "registrant" not in record

    def test_names_only_no_address(self):
        row = _make_row()
        record = _build_record(
            icao_hex="4B1234", registration="F-ABCD", first_row=row, names=["ALICE"], address_raw=None,
        )
        assert record["registrant"] == {"names": ["ALICE"]}

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(constructeur="", modele="", numero_serie="")
        record = _build_record(
            icao_hex="4B1234", registration="F-ABCD", first_row=row, names=[], address_raw=None,
        )
        assert "aircraft" not in record

    def test_empty_manufacturer_omitted(self):
        row = _make_row(constructeur="")
        record = _build_record(
            icao_hex="4B1234", registration="F-ABCD", first_row=row, names=[], address_raw=None,
        )
        assert "manufacturer" not in record.get("aircraft", {})


# ---------------------------------------------------------------------------
# Tests: download_registry
# ---------------------------------------------------------------------------

class TestDownloadRegistry:
    def test_parses_semicolon_delimited_rows(self):
        with patch("fr_dgac_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 200
            resp.content = (
                "IMMATRICULATION;PROPRIETAIRE\r\n"
                "F-ABCD;DUPONT JEAN\r\n"
            ).encode("utf-8-sig")
            mock_get.return_value = resp
            rows = download_registry(DOWNLOAD_URL)
        assert len(rows) == 1
        assert rows[0]["IMMATRICULATION"] == "F-ABCD"
        assert rows[0]["PROPRIETAIRE"] == "DUPONT JEAN"

    def test_raises_on_http_error(self):
        with patch("fr_dgac_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 503
            mock_get.return_value = resp
            with pytest.raises(RuntimeError, match="HTTP 503"):
                download_registry(DOWNLOAD_URL)


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4B1234", registration="F-ABCD")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_no_redis_match_not_written(self):
        rows = [_make_row()]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_missing_simple_record_still_written(self):
        """Unlike some other runners, a missing Mictronics simple record does
        not block the write here — only an active type mismatch does."""
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4B1234", registration="F-ABCD", simple_record=None)
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_type_check_mismatch_skips_record(self):
        rows = [_make_row(modele="LEONARDO AW139")]
        simple = {"type_designator": "AW109", "manufacturer_model": "Leonardo AW109"}
        r = _make_redis_with_search(icao_hex="4B1234", registration="F-ABCD", simple_record=simple)
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_type_check_match_writes_record(self):
        rows = [_make_row(modele="C172")]
        simple = {"type_designator": "C172", "manufacturer_model": "Cessna 172"}
        r = _make_redis_with_search(icao_hex="4B1234", registration="F-ABCD", simple_record=simple)
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_writes_to_registry_key(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4B1234", registration="F-ABCD")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:4B1234"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4B1234", registration="F-ABCD")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "fr-dgac"

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4B1234", registration="F-ABCD")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:4B1234", REDIS_TTL)

    def test_empty_rows_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_co_ownership_names_all_present_in_written_record(self):
        rows = [
            _make_row(proprietaire="ALICE"),
            _make_row(proprietaire="BOB"),
        ]
        r = _make_redis_with_search(icao_hex="4B1234", registration="F-ABCD")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        record = set_call[0][2]
        assert record["registrant"]["names"] == ["ALICE", "BOB"]


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
        with patch("fr_dgac_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("fr_dgac_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 366, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("fr_dgac_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 366, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "366"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("fr_dgac_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/fr-dgac"
