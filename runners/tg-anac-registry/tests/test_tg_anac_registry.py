"""Tests for the Togo ANAC data runner."""

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
        "tg_anac_registry_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["tg_anac_registry_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()


download_and_parse = _mod.download_and_parse
_build_record = _mod._build_record
_clean = _mod._clean
_escape_tag = _mod._escape_tag
_build_registration_map = _mod._build_registration_map
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
_INDEX_URL = _mod._INDEX_URL
_REG_RE = _mod._REG_RE
MQTT_ROOT = _mod.MQTT_ROOT

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HEADER_ROW = "<tr><th>N&deg; Ordre</th><th>type</th><th>Immatriculation</th><th>Constructeur</th><th>N&deg; de serie</th><th>Radiation</th><th>Nom propri&eacute;taire</th><th>Adresse propri&eacute;taire</th></tr>"


def _row_html(num, model, reg, manufacturer, serial, radiation, owner="", address=""):
    return f"<tr><td>{num}</td><td>{model}</td><td>{reg}</td><td>{manufacturer}</td><td>{serial}</td><td>{radiation}</td><td>{owner}</td><td>{address}</td></tr>"


def _make_html(rows: list[str]) -> bytes:
    return ("<html><body><table>" + _HEADER_ROW + "".join(rows) + "</table></body></html>").encode("utf-8")


def _make_session(content=b"", status_code=200):
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.content = content
    resp.text = content.decode("utf-8", errors="replace") if isinstance(content, bytes) else content
    session = MagicMock()
    session.get.return_value = resp
    return session


# ---------------------------------------------------------------------------
# _clean
# ---------------------------------------------------------------------------


class TestClean:
    def test_strips_whitespace(self):
        assert _clean("  hello  ") == "hello"

    def test_collapses_internal_whitespace(self):
        assert _clean("hello   world") == "hello world"

    def test_none_returns_empty(self):
        assert _clean(None) == ""

    def test_empty_returns_empty(self):
        assert _clean("") == ""


# ---------------------------------------------------------------------------
# _REG_RE
# ---------------------------------------------------------------------------


class TestRegRe:
    def test_three_char_suffix(self):
        assert _REG_RE.match("5V-TAI")

    def test_two_char_suffix(self):
        assert _REG_RE.match("5V-AB")

    def test_header_text_rejected(self):
        assert not _REG_RE.match("Immatriculation")

    def test_glitched_radie_suffix_rejected(self):
        """Real source has one row where every cell got '(Radié)' appended,
        including the registration itself -- must not match."""
        assert not _REG_RE.match("5V-TGF(Radié)")

    def test_empty_rejected(self):
        assert not _REG_RE.match("")

    def test_wrong_prefix_rejected(self):
        assert not _REG_RE.match("9Y-ABC")


# ---------------------------------------------------------------------------
# _escape_tag
# ---------------------------------------------------------------------------


class TestEscapeTag:
    def test_plain_unchanged(self):
        assert _escape_tag("5VTAI") == "5VTAI"

    def test_hyphen_escaped(self):
        assert _escape_tag("5V-TAI") == r"5V\-TAI"


# ---------------------------------------------------------------------------
# download_and_parse
# ---------------------------------------------------------------------------


class TestDownloadAndParse:
    def test_logs_url(self):
        session = _make_session(_make_html([]))
        with patch.object(_mod, "logger") as mock_logger:
            download_and_parse(session)
        mock_logger.info.assert_any_call(
            "Downloading Togo ANAC aircraft register from %s", _INDEX_URL
        )

    def test_http_error_raises(self):
        session = _make_session(status_code=404)
        with pytest.raises(RuntimeError, match="Register page request failed with HTTP 404"):
            download_and_parse(session)

    def test_no_table_raises(self):
        session = _make_session(b"<html><body>no table here</body></html>")
        with pytest.raises(RuntimeError, match="No table found"):
            download_and_parse(session)

    def test_active_row_kept(self):
        rows = [_row_html("002", "F28 MK 1000", "5V-TAI", "FOKKER BV Services", "11079", "NON")]
        session = _make_session(_make_html(rows))
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "5V-TAI"

    def test_deregistered_row_dropped(self):
        """Radiation=OUI is the majority case in the real register (~70% of
        rows), not an edge case -- must be filtered, not just tolerated."""
        rows = [_row_html("015", "G1159 Dll", "5V-TAA", "GRUMMAN", "149", "OUI")]
        session = _make_session(_make_html(rows))
        records = download_and_parse(session)
        assert len(records) == 0

    def test_glitched_radie_row_dropped(self):
        """One real row has '(Radié)' appended to every cell including the
        registration and Radiation value itself -- rejected by the
        registration regex before the Radiation check even runs."""
        rows = [_row_html("001", "DC8 62(Radié)", "5V-TGF(Radié)", "Mc DONELL DOUGLAS(Radié)", "46071(Radié)", "NON(Radié)")]
        session = _make_session(_make_html(rows))
        records = download_and_parse(session)
        assert len(records) == 0

    def test_mixed_active_and_deregistered(self):
        rows = [
            _row_html("002", "F28 MK 1000", "5V-TAI", "FOKKER BV Services", "11079", "NON"),
            _row_html("015", "G1159 Dll", "5V-TAA", "GRUMMAN", "149", "OUI"),
            _row_html("003", "PA 31T", "5V-TPT", "PIPER AIRCRAFT", "7820013", "NON", "Mr SITTERLIN", "BP 10019 Lomé TOGO"),
        ]
        session = _make_session(_make_html(rows))
        records = download_and_parse(session)
        assert len(records) == 2
        assert {r["registration"] for r in records} == {"5V-TAI", "5V-TPT"}

    def test_radiation_case_insensitive(self):
        rows = [_row_html("002", "F28 MK 1000", "5V-TAI", "FOKKER BV Services", "11079", "non")]
        session = _make_session(_make_html(rows))
        records = download_and_parse(session)
        assert len(records) == 1

    def test_blank_owner_and_address_handled(self):
        rows = [_row_html("002", "F28 MK 1000", "5V-TAI", "FOKKER BV Services", "11079", "NON")]
        session = _make_session(_make_html(rows))
        records = download_and_parse(session)
        assert records[0]["owner"] == ""
        assert records[0]["address"] == ""

    def test_logs_counts(self):
        rows = [
            _row_html("002", "F28 MK 1000", "5V-TAI", "FOKKER BV Services", "11079", "NON"),
            _row_html("015", "G1159 Dll", "5V-TAA", "GRUMMAN", "149", "OUI"),
        ]
        session = _make_session(_make_html(rows))
        with patch.object(_mod, "logger") as mock_logger:
            download_and_parse(session)
        mock_logger.info.assert_any_call(
            "Parsed %d active 5V- records (%d deregistered rows skipped).", 1, 1
        )


# ---------------------------------------------------------------------------
# _build_record
# ---------------------------------------------------------------------------


class TestBuildRecord:
    def _row(self, **overrides):
        row = {
            "model": "PA 31T",
            "manufacturer": "PIPER AIRCRAFT",
            "serial": "7820013",
            "owner": "Mr SITTERLIN",
            "address": "BP 10019 Lomé TOGO",
        }
        row.update(overrides)
        return row

    def test_basic_fields(self):
        record = _build_record(self._row(), "500ABC", "5V-TPT")
        assert record["icao_hex"] == "500ABC"
        assert record["registration"] == "5V-TPT"
        assert record["source"] == "tg-anac-registry"
        assert record["military"] is False

    def test_model_from_type_column(self):
        record = _build_record(self._row(model="PA 31T"), "500ABC", "5V-TPT")
        assert record["aircraft"]["model"] == "PA 31T"

    def test_manufacturer(self):
        record = _build_record(self._row(manufacturer="PIPER AIRCRAFT"), "500ABC", "5V-TPT")
        assert record["aircraft"]["manufacturer"] == "PIPER AIRCRAFT"

    def test_serial_number(self):
        record = _build_record(self._row(serial="7820013"), "500ABC", "5V-TPT")
        assert record["aircraft"]["serial_number"] == "7820013"

    def test_registrant_names(self):
        record = _build_record(self._row(owner="Mr SITTERLIN"), "500ABC", "5V-TPT")
        assert record["registrant"]["names"] == ["Mr SITTERLIN"]

    def test_registrant_street_split_by_comma(self):
        record = _build_record(self._row(address="BP 10007 Lomé, TOGO"), "500ABC", "5V-TPT")
        assert record["registrant"]["street"] == ["BP 10007 Lomé", "TOGO"]

    def test_empty_owner_omits_names(self):
        record = _build_record(self._row(owner=""), "500ABC", "5V-TPT")
        assert "names" not in record.get("registrant", {})

    def test_empty_address_omits_street(self):
        record = _build_record(self._row(address=""), "500ABC", "5V-TPT")
        assert "street" not in record.get("registrant", {})

    def test_empty_owner_and_address_omits_registrant(self):
        record = _build_record(self._row(owner="", address=""), "500ABC", "5V-TPT")
        assert "registrant" not in record

    def test_empty_fields_omit_aircraft(self):
        record = _build_record(self._row(model="", manufacturer="", serial=""), "500ABC", "5V-TPT")
        assert "aircraft" not in record

    def test_source_is_tg_anac(self):
        record = _build_record(self._row(), "500ABC", "5V-TPT")
        assert record["source"] == "tg-anac-registry"


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
        r = self._make_redis([("500ABC", "5V-TAI"), ("500ABD", "5V-TPT")])
        reg_map = _build_registration_map(["5V-TAI", "5V-TPT"], r)
        assert reg_map["5V-TAI"] == "500ABC"
        assert reg_map["5V-TPT"] == "500ABD"

    def test_redis_failure_logs_warning(self):
        r = MagicMock()
        r.ft.return_value.search.side_effect = Exception("connection refused")
        with patch.object(_mod, "logger") as mock_logger:
            result = _build_registration_map(["5V-TAI"], r)
        assert result == {}
        mock_logger.warning.assert_called_once()

    def test_empty_registrations_returns_empty(self):
        r = MagicMock()
        result = _build_registration_map([], r)
        assert result == {}
        r.ft.return_value.search.assert_not_called()

    def test_batches_large_lists(self):
        docs = [(f"50{i:04d}", f"5V-{i:03d}") for i in range(150)]
        r = self._make_redis(docs)
        regs = [f"5V-{i:03d}" for i in range(150)]
        _build_registration_map(regs, r)
        assert r.ft.return_value.search.call_count == 2


# ---------------------------------------------------------------------------
# write_to_redis
# ---------------------------------------------------------------------------


class TestWriteToRedis:
    def _row(self, registration="5V-TPT"):
        return {
            "registration": registration,
            "model": "PA 31T",
            "manufacturer": "PIPER AIRCRAFT",
            "serial": "7820013",
            "owner": "Mr SITTERLIN",
            "address": "BP 10019 Lomé TOGO",
        }

    def test_writes_found_registrations(self):
        rows = [self._row()]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"5V-TPT": "500ABC"}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 1
        pipe.json.return_value.set.assert_called_once()
        pipe.expire.assert_called_once()

    def test_skips_no_redis_match(self):
        rows = [self._row()]
        r = MagicMock()
        r.pipeline.return_value = MagicMock()
        with patch.object(_mod, "_build_registration_map", return_value={}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 0

    def test_uses_aircraft_registry_key(self):
        rows = [self._row()]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"5V-TPT": "500ABC"}):
            write_to_redis(rows, r, 1209600)
        set_call = pipe.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:500ABC"

    def test_pipeline_error_logs_warning(self):
        rows = [self._row()]
        r = MagicMock()
        pipe = MagicMock()
        pipe.execute.side_effect = Exception("Redis timeout")
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"5V-TPT": "500ABC"}):
            with patch.object(_mod, "logger") as mock_logger:
                write_to_redis(rows, r, 1209600)
        mock_logger.warning.assert_called()


# ---------------------------------------------------------------------------
# publish_completion_stats
# ---------------------------------------------------------------------------


class TestPublishCompletionStats:
    def _setup_mock_client(self):
        mock_client = MagicMock()

        def fake_connect(host, port, keepalive):
            mock_client.on_connect(mock_client, None, None, 0, None)

        mock_client.connect.side_effect = fake_connect
        return mock_client

    def test_missing_mqtt_block_skips(self):
        publish_completion_stats({}, 100, "success")

    def test_blank_host_skips_without_crashing(self):
        cfg = {"mqtt": {"host": "", "port": 1883, "username": "", "password": ""}}
        publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch.object(_mod.mqtt, "Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 13, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch.object(_mod.mqtt, "Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "Failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/tg-anac-registry"
