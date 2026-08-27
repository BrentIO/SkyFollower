"""
Tests for message-processor/main.py components that don't require live
infrastructure.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import sqlite3
import sys
import tempfile
import threading
import time
from datetime import timezone
from unittest.mock import MagicMock, patch

import pytest

# message-processor/ can't be imported as a normal package -- the hyphen in
# the directory name isn't a valid Python identifier -- so register it under
# the dotted name 'message_processor' via importlib, the same workaround
# archive-processor/tests/conftest.py uses. This has to live inline here
# (not in a conftest.py) because pytest derives every conftest.py's plugin
# name from its "tests/conftest.py" path once the hyphenated parent breaks
# the dotted-name walk, so a second same-named conftest.py collides with
# archive-processor's at collection time.
_HERE = os.path.dirname(os.path.abspath(__file__))
_MESSAGE_PROCESSOR_DIR = os.path.dirname(_HERE)
_REPO_ROOT = os.path.dirname(_MESSAGE_PROCESSOR_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

if "message_processor" not in sys.modules:
    _spec = importlib.util.spec_from_file_location(
        "message_processor",
        os.path.join(_MESSAGE_PROCESSOR_DIR, "__init__.py"),
        submodule_search_locations=[_MESSAGE_PROCESSOR_DIR],
    )
    _pkg = importlib.util.module_from_spec(_spec)
    _pkg.__path__ = [_MESSAGE_PROCESSOR_DIR]
    _pkg.__package__ = "message_processor"
    sys.modules["message_processor"] = _pkg
    _spec.loader.exec_module(_pkg)

from message_processor.main import (  # noqa: E402  (after sys.path/package setup)
    Flight,
    MessageProcessor,
    _DepthHWM,
    _RateTracker,
    _TimeTracker,
    _SCHEMA,
    _migrate_schema,
    _confirm_after_repeated_sightings,
    _PARITY_ERROR_CONFIRM_COUNT,
    _PARITY_ERROR_CONFIRM_WINDOW_SECONDS,
    main as processor_main,
)
from shared.models import InboundMessage, Position, Velocity
from shared.redis_keys import message_processor_heartbeat_key, operator_key


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:", check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.executescript(_SCHEMA)
    return db


def _make_migrated_db() -> sqlite3.Connection:
    """Like _make_db(), but also runs _migrate_schema() -- needed for
    anything exercising the positions/velocities unique index, which is
    created there rather than in _SCHEMA (see _migrate_schema's docstring)."""
    db = _make_db()
    _migrate_schema(db)
    return db


def _minimal_config() -> dict:
    return {
        "redis": {"host": "localhost"},
        "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        "telemetry_interval_seconds": 30,
    }


def _make_processor(
    cfg: dict | None = None,
    message_processor_id: str = "0",
    data_dir: str | None = None,
) -> tuple[MessageProcessor, MagicMock]:
    """Construct a real MessageProcessor (file-backed active store) with Redis/rules/
    processor-ID-claim mocked out, matching TestProcessorEnrichment's pattern
    but keeping the real on-disk DB instead of swapping in an in-memory one —
    needed for the crash-recovery/message-clock tests below."""
    cfg = cfg or _minimal_config()
    with patch("message_processor.main.DATA_DIR", data_dir or tempfile.mkdtemp()), \
         patch("message_processor.main.redis_lib.Redis") as MockRedis, \
         patch("message_processor.main.RulesEngine"), \
         patch("message_processor.main.pathlib.Path"), \
         patch.object(MessageProcessor, "_claim_message_processor_id"):
        mock_redis = MagicMock()
        mock_redis.script_load.return_value = "abc123sha"
        # Default RedisJSON reads (e.g. _enrich_operator's .json().get()) to
        # "no data", matching a real empty Redis -- an unconfigured MagicMock
        # is truthy and not JSON-serializable, which breaks flight.save() in
        # any test that incidentally triggers operator enrichment without
        # caring about it.
        mock_redis.json.return_value.get.return_value = None
        MockRedis.return_value = mock_redis
        p = MessageProcessor(cfg, message_processor_id=message_processor_id)
        p._redis = mock_redis
        p._merge_sha = "abc123sha"
        p._route_sha = "routesha123"
        p._rules_engine.evaluate.return_value = []
        return p, mock_redis


# ---------------------------------------------------------------------------
# _decode_1090 (pyModeS 3.x migration)
#
# These hex frames are hand-crafted with pyModeS's own CRC function
# (pyModeS._bits.crc_remainder) rather than copy-pasted from elsewhere, so
# each one is deliberately built to exercise exactly one field combination
# and independently verified against pms.decode() directly before being
# used here. This is the coverage that would have caught a pyModeS 3.x
# migration bug (pms.df() raising V2APIRemovedError on every message) —
# every other test in this file calls _update_flight directly with
# hand-built dicts and never touches real decode at all.
# ---------------------------------------------------------------------------

class TestDecode1090:
    def test_ident_and_wake_turbulence_category(self):
        # TC=4, category=5 (Heavy), callsign "TESTHVY1"
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8DA8AE7F255054D42166710A1432",
            icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["ident"] == "TESTHVY1"
        assert data["wake_turbulence_category"] == "heavy"

    def test_gps_velocity(self):
        # TC=19 subtype 1 (GPS): groundspeed=159, track≈182.88, vertical_rate=-832
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8D485020994409940838175B284F",
            icao_hex="485020", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["velocity"] == 159
        assert data["heading"] == pytest.approx(182.88, abs=0.01)
        assert data["vertical_speed"] == -832

    def test_airspeed_velocity(self):
        # TC=19 subtype 3 (airspeed): airspeed=250 IAS, heading=90.0
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8DA8AE7F9B05001F600000533884",
            icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["velocity"] == 250
        assert data["heading"] == 90.0

    def test_position_with_configured_reference(self):
        p, _ = _make_processor(_minimal_config() | {"latitude": 52.2572, "longitude": 3.9198})
        msg = InboundMessage(
            raw="8D40621D58C382D690C8AC2863A7",
            icao_hex="40621D", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["latitude"] == pytest.approx(52.2572, abs=0.001)
        assert data["longitude"] == pytest.approx(3.9198, abs=0.001)
        assert data["altitude"] == 38000

    def test_position_without_configured_reference(self):
        # No latitude/longitude in config — altitude still decodes, but
        # position can't be resolved from a single message without one.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8D40621D58C382D690C8AC2863A7",
            icao_hex="40621D", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert "latitude" not in data
        assert data["altitude"] == 38000

    def test_squawk_decoded_from_df21(self):
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="A800030F992252CD453820AD87FB",
            icao_hex="71BE3A", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["squawk"] == "2646"

    def test_crc_valid_is_a_no_op_for_df21(self):
        """Documents real pyModeS behavior (verified by reading message.py):
        crc_valid is hardcoded True for DF0/4/5/11/16/20/21 regardless of
        the actual message content — their CRC field encodes the ICAO
        itself, so there's no single-message corruption signal for these
        DF types at all. A squawk is trusted once decoded; there's nothing
        else to check it against in single-message mode."""
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="A800030F992252CD453820AD87FB",
            icao_hex="000000", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["squawk"] == "2646"

    def test_adsb_version(self):
        # TC=31 subtype 0, version=2
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8DA8AE7FF8000000004000F9567C",
            icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["adsb_version"] == 2

    def test_corrupted_crc_rejected(self):
        # Same position message as above with the last hex char flipped —
        # pyModeS still returns the (now-untrustworthy) decoded fields with
        # crc_valid=False; the message must still be rejected.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8D40621D58C382D690C8AC2863A0",
            icao_hex="40621D", received_at=1.0, source="1090",
        )
        assert p._decode_1090(msg) is None

    def test_message_type_with_no_tracked_fields_dropped(self):
        # TC=28 (ACAS RA broadcast) — no df/typecode filtering exists
        # anymore, so this relies purely on the message not populating any
        # field _decode_1090 extracts.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8DA8AE7FE00000000000005E3ED8",
            icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        assert p._decode_1090(msg) is None

    def test_garbage_input_dropped(self):
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="not-valid-hex", icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        assert p._decode_1090(msg) is None

    def test_too_short_input_dropped(self):
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8D4B19", icao_hex="4B1900", received_at=1.0, source="1090",
        )
        assert p._decode_1090(msg) is None

    # DF5/DF21 (squawk-only replies) carry no explicit ICAO field, so
    # pyModeS can't compute a real crc_valid for them without an
    # independently-verified ICAO hint (which _decode_1090 doesn't supply)
    # — it reports crc_valid=None, which the `is False` check above lets
    # straight through. These are 19 real captured messages reported as
    # legacy "parity errors"; each one decodes without raising, but several
    # produce fabricated reserved/emergency squawks (7500/7600/7700/7777)
    # from corrupted bits. _decode_1090 still surfaces the (possibly
    # fabricated) value here -- deciding whether to trust it belongs to
    # _update_flight, which needs the raw value plus the `verified` flag
    # below to run the confirmation logic (see #900 and
    # TestSquawkConfirmation/TestIdentConfirmation).
    @pytest.mark.parametrize("raw,expected_squawk", [
        ("A8AE2ACA7DB5CA4AC22FCE4A0F04", "7600"),
        ("2CFB4A8ABA3544", "7600"),
        ("2D374A8AA103FF", "7600"),
        ("2E3FFFFF34D379", "7777"),
        ("AEE18AAAF390D1B076038A62D1A2", "7700"),
        ("2A000A8ABF824C", "7600"),
        ("AA000A8A0009F9327E6FC482913F", "7600"),
        ("AA000A8A00179B3C4004E841CAF5", "7600"),
        ("AA000A8A0009F7323E77D48A5561", "7600"),
        ("AA000A8A0009F731FE9FCE5D92FB", "7600"),
        ("AA000A8A203B3C0712082019F93B", "7600"),
        ("AA000A8A00179F3C0004E738B9D7", "7600"),
        ("AA000A8A0009F9323E4FD036F58A", "7600"),
        ("A9B16AE2248F41F8F8A505674BD4", "7500"),
        ("2FC26A8AE10854", "7600"),
        ("287A8ACA7DD46A", "7600"),
        ("AA3AEAAAE875E55C985DAB8B392E", "7700"),
        ("AC9B2AAA61141677E2DBD5ED6DFF", "7700"),
        ("2F3FFFFF73537B", "7777"),
    ])
    def test_captured_parity_error_messages_dont_crash(self, raw, expected_squawk):
        p, _ = _make_processor()
        msg = InboundMessage(
            raw=raw, icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data is not None
        assert data["squawk"] == expected_squawk
        # None of these 19 are DF17/18 -- pyModeS can't verify them, so
        # _update_flight must not trust this squawk on a single message.
        assert data["verified"] is False

    def test_verified_squawk_from_df17_aircraft_status_broadcast(self):
        # DF17 TC=28 subtype 1 ("Aircraft status") re-encodes the same
        # squawk inside a real extended-squitter message, which always
        # carries genuine CRC (unlike DF5/21's ICAO-derived kind) --
        # hand-crafted with pyModeS's own CRC function
        # (pyModeS._bits.crc_remainder) the same way the module docstring
        # above describes, encoding idcode 2730 (-> squawk "7700") at
        # TC=28/subtype=1/emergency_state=1, ICAO A8AE7F, independently
        # verified against pms.decode() directly: {'df': 17, 'icao':
        # 'A8AE7F', 'crc_valid': True, 'typecode': 28, 'bds': '6,1',
        # 'subtype': 1, 'emergency_state': 1, 'squawk': '7700'}.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8DA8AE7FE12AAA0000000060E34A",
            icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["squawk"] == "7700"
        assert data["verified"] is True

    def test_verified_ident_from_extended_squitter(self):
        # DF17 TC=4 ident broadcast -- real CRC, unlike DF20/21's BDS 2,0
        # Comm-B register. Same message as
        # test_ident_and_wake_turbulence_category above.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8DA8AE7F255054D42166710A1432",
            icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["ident"] == "TESTHVY1"
        assert data["verified"] is True

    def test_unverified_ident_from_comm_b_bds20(self):
        # DF21 Comm-B BDS 2,0 -- one of the 19 captured parity-error
        # messages, which decodes both a fabricated squawk *and* a
        # fabricated callsign ("N30GD") from the same corrupted, CRC-
        # unverifiable message.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="AA000A8A203B3C0712082019F93B",
            icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["ident"] == "N30GD"
        assert data["verified"] is False


# ---------------------------------------------------------------------------
# _decode_978 (pyModeS978 UAT decoding)
#
# These UAT frames are hand-crafted with a synthetic frame builder ported
# from pyModeS978's own test suite (tests/synth.py — test-only there, not
# shipped in the PyPI package) and independently verified against
# pyModeS978.decode() directly before being hardcoded here, matching the
# same verification discipline TestDecode1090 above uses for 1090 frames.
# ---------------------------------------------------------------------------

class TestDecode978:
    def test_ident_and_wake_turbulence_category(self):
        # payload_type=1 (long), category=4 (medium/large high vortex),
        # callsign "TEST978A"
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-08A3D3E335818151F32A59C9019432C0E01D96B3912D0A0800000210000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        data = p._decode_978(msg)
        assert data["ident"] == "TEST978A"
        assert data["wake_turbulence_category"] == "medium"

    def test_airborne_position_and_velocity(self):
        # Same frame as above: lat/lon/altitude + airborne groundspeed/track/vertical_rate
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-08A3D3E335818151F32A59C9019432C0E01D96B3912D0A0800000210000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        data = p._decode_978(msg)
        assert data["latitude"] == pytest.approx(37.6213, abs=0.001)
        assert data["longitude"] == pytest.approx(-122.3790, abs=0.001)
        assert data["altitude"] == 34875
        assert data["velocity"] == 141
        assert data["heading"] == 45.0
        assert data["vertical_speed"] == 832
        assert data["adsb_version"] == 2

    def test_squawk_decoded(self):
        # Same raw bits as callsign, CSID bit selects squawk instead — no
        # callsign, no category (NO_INFORMATION, not populated)
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-08A3D3E300000000000000000000000000002A0024E6C40000000000000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        data = p._decode_978(msg)
        assert data["squawk"] == "1200"
        assert "ident" not in data
        assert "wake_turbulence_category" not in data

    def test_no_information_category_not_populated(self):
        # callsign present, category=0 (NO_INFORMATION) — mirrors
        # _decode_1090's "No category information" guard
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-08A3D3E30000000000000000000000000003B04CAD40610000000200000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        data = p._decode_978(msg)
        assert data["ident"] == "NOCATAC1"
        assert "wake_turbulence_category" not in data

    def test_ground_heading(self):
        # On-ground frame, type code selects heading (not track); category=9
        # (glider/sailplane) -- not one of the 7 DO-260B wake-turbulence
        # categories, so it's left unset rather than mapped to anything.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-08A3D3E300000000000000008042C000003AD755D6B3890000000200000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        data = p._decode_978(msg)
        assert data["velocity"] == 15
        assert data["heading"] == 270.0
        assert "wake_turbulence_category" not in data

    def test_ground_track(self):
        # On-ground frame, type code selects track (not heading)
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-08A3D3E30000000000000000805540000005C4E6C4E6C40000000000000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        data = p._decode_978(msg)
        assert data["velocity"] == 20
        assert data["heading"] == 90.0

    def test_uplink_frame_returns_none(self):
        # FIS-B uplink frame (432-byte payload, '+' direction) carries no
        # traffic data — decode() returns None, not an error.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="+" + "00" * 432,
            icao_hex="000000", received_at=1.0, source="978",
        )
        assert p._decode_978(msg) is None

    def test_malformed_input_dropped(self):
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-ZZZZZZ", icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        assert p._decode_978(msg) is None

    def test_no_useful_fields_dropped(self):
        # payload_type=0 (short) — no Mode Status block present, so no
        # ident/squawk/category/version; every state-vector field left at
        # its "unavailable" raw value.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-00A3D3E30000000000000000000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        assert p._decode_978(msg) is None


class TestDecodeMessageRouting:
    def test_978_source_routes_to_decode_978(self):
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-08A3D3E335818151F32A59C9019432C0E01D96B3912D0A0800000210000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        assert p._decode_message(msg)["ident"] == "TEST978A"

    def test_1090_source_routes_to_decode_1090(self):
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8DA8AE7FF8000000004000F9567C",
            icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        assert p._decode_message(msg)["adsb_version"] == 2

    def test_external_source_routes_to_decode_1090(self):
        # EXTERNAL-tagged frames are still raw Mode-S hex — same path as
        # 1090, source was never branched on before this PR either.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8DA8AE7FF8000000004000F9567C",
            icao_hex="A8AE7F", received_at=1.0, source="EXTERNAL",
        )
        assert p._decode_message(msg)["adsb_version"] == 2


# ---------------------------------------------------------------------------
# Flight SQLite operations
# ---------------------------------------------------------------------------

class TestFlight:
    def test_load_returns_false_for_unknown(self):
        db = _make_db()
        f = Flight(db)
        assert f.load("AAAAAA") is False

    def test_save_and_reload(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1000.0
        f.last_message = 2000.0
        f.total_messages = 42
        f.aircraft = {"icao_hex": "A8AE7F", "registration": "N659DL"}
        f.ident = "DAL659"
        f.receiver_sources = ["1090"]
        f.save()

        f2 = Flight(db)
        assert f2.load("A8AE7F") is True
        assert f2.ident == "DAL659"
        assert f2.total_messages == 42
        assert f2.aircraft["registration"] == "N659DL"

    def test_receiver_sources_and_force_archive_round_trip(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1000.0
        f.last_message = 2000.0
        f.total_messages = 3
        f.receiver_sources = ["EXTERNAL", "1090"]
        f.force_archive = True
        f.save()

        f2 = Flight(db)
        assert f2.load("A8AE7F") is True
        assert f2.receiver_sources == ["EXTERNAL", "1090"]
        assert f2.force_archive is True

    def test_receiver_sources_and_force_archive_defaults(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1000.0
        f.last_message = 1000.0
        f.total_messages = 1
        f.save()

        f2 = Flight(db)
        f2.load("A8AE7F")
        assert f2.receiver_sources == []
        assert f2.force_archive is False

    def test_add_position(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1000.0
        f.last_message = 1000.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()
        f.add_position(Position(timestamp=1000.0, latitude=40.0, longitude=-73.0, altitude=5000))

        f2 = Flight(db)
        f2.load("A8AE7F")
        assert len(f2.positions) == 1
        assert f2.positions[0].altitude == 5000

    def test_add_velocity(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "BBBBBB"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()
        f.add_velocity(Velocity(timestamp=1.0, velocity=450.0, heading=270.0, vertical_speed=500))

        f2 = Flight(db)
        f2.load("BBBBBB")
        assert len(f2.velocities) == 1
        assert f2.velocities[0].velocity == 450.0

    def test_delete_removes_all_rows(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "CCCCCC"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()
        f.add_position(Position(timestamp=1.0, latitude=0.0, longitude=0.0))
        f.add_velocity(Velocity(timestamp=1.0, velocity=100.0))
        f.delete()

        f2 = Flight(db)
        assert f2.load("CCCCCC") is False
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM positions WHERE icao_hex='CCCCCC'")
        assert cur.fetchone()[0] == 0

    def test_to_completed_flight_shape(self):
        from datetime import datetime
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1717100000.0
        f.last_message = 1717100060.0
        f.total_messages = 10
        f.aircraft = {"icao_hex": "A8AE7F", "registration": "N659DL", "military": False}
        f.ident = "DAL659"
        f.operator = {"airline_designator": "DAL", "source": "mictronics"}
        f.squawk = "1234"
        f.origin = "KATL"
        f.destination = "KLAX"
        f.matched_rules = ["rule_1"]
        f.receiver_sources = ["1090"]
        f.save()

        cf = f.to_completed_flight()

        # _id is UUID-v7 string
        assert isinstance(cf.id, str)
        assert "-" in cf.id

        # military=False stripped
        assert "military" not in cf.aircraft

        # operator source key stripped
        assert "source" not in cf.operator

        # timestamps are UTC-aware datetimes
        assert cf.first_message.tzinfo is not None
        assert cf.destination == "KLAX"
        assert cf.matched_rules == ["rule_1"]
        assert cf.receiver_sources == ["1090"]
        assert cf.force_archive is False

    def test_to_completed_flight_no_aircraft_sets_icao_hex(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "FFFFFF"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["978"]
        f.save()
        cf = f.to_completed_flight()
        assert cf.aircraft["icao_hex"] == "FFFFFF"

    def test_to_completed_flight_force_archive_true(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["EXTERNAL"]
        f.force_archive = True
        f.save()
        cf = f.to_completed_flight()
        assert cf.receiver_sources == ["EXTERNAL"]
        assert cf.force_archive is True


class TestPositionVelocityDedup:
    """RabbitMQ redelivery (a normal at-least-once occurrence) can reprocess
    the same message twice; add_position()/add_velocity() must not leave
    duplicate rows behind, and the in-memory list must stay in sync with
    what's actually persisted."""

    def test_add_position_twice_same_timestamp_inserts_once(self):
        db = _make_migrated_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1000.0
        f.last_message = 1000.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()

        pos = Position(timestamp=1000.0, latitude=40.0, longitude=-73.0, altitude=5000)
        f.add_position(pos)
        f.add_position(pos)  # simulated redelivery of the identical message

        assert len(f.positions) == 1
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM positions WHERE icao_hex='A8AE7F'")
        assert cur.fetchone()[0] == 1

    def test_add_velocity_twice_same_timestamp_inserts_once(self):
        db = _make_migrated_db()
        f = Flight(db)
        f.icao_hex = "BBBBBB"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()

        vel = Velocity(timestamp=1.0, velocity=450.0, heading=270.0, vertical_speed=500)
        f.add_velocity(vel)
        f.add_velocity(vel)

        assert len(f.velocities) == 1
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM velocities WHERE icao_hex='BBBBBB'")
        assert cur.fetchone()[0] == 1

    def test_reload_matches_in_memory_list_after_duplicate_insert(self):
        """to_completed_flight()/to_dict() serialize self.positions directly
        rather than reloading -- a drift between the two would leak a
        duplicate into archived output even though the database correctly
        suppressed it."""
        db = _make_migrated_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1000.0
        f.last_message = 1000.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()
        pos = Position(timestamp=1000.0, latitude=40.0, longitude=-73.0, altitude=5000)
        f.add_position(pos)
        f.add_position(pos)

        f2 = Flight(db)
        f2.load("A8AE7F")
        assert len(f2.positions) == len(f.positions) == 1

    def test_different_timestamps_both_inserted(self):
        """Guards against an overly broad uniqueness key -- two genuinely
        distinct position reports must not be treated as duplicates."""
        db = _make_migrated_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1000.0
        f.last_message = 1001.0
        f.total_messages = 2
        f.receiver_sources = ["1090"]
        f.save()
        f.add_position(Position(timestamp=1000.0, latitude=40.0, longitude=-73.0, altitude=5000))
        f.add_position(Position(timestamp=1001.0, latitude=40.1, longitude=-73.1, altitude=5100))

        assert len(f.positions) == 2

    def test_migrate_schema_dedupes_preexisting_duplicate_rows(self):
        """A database from before this migration existed may already hold
        duplicate rows from past redeliveries -- CREATE UNIQUE INDEX would
        fail outright on those unless they're cleaned up first."""
        db = _make_db()  # schema only, no migration yet
        db.execute(
            "INSERT INTO positions (icao_hex, timestamp, latitude, longitude, altitude) "
            "VALUES ('A8AE7F', 1000.0, 40.0, -73.0, 5000)"
        )
        db.execute(
            "INSERT INTO positions (icao_hex, timestamp, latitude, longitude, altitude) "
            "VALUES ('A8AE7F', 1000.0, 40.0, -73.0, 5000)"
        )
        db.execute(
            "INSERT INTO velocities (icao_hex, timestamp, velocity, heading, vertical_speed) "
            "VALUES ('A8AE7F', 1000.0, 450.0, 270.0, 500)"
        )
        db.execute(
            "INSERT INTO velocities (icao_hex, timestamp, velocity, heading, vertical_speed) "
            "VALUES ('A8AE7F', 1000.0, 450.0, 270.0, 500)"
        )
        db.commit()

        _migrate_schema(db)  # must not raise, and must dedupe before indexing

        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM positions WHERE icao_hex='A8AE7F'")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM velocities WHERE icao_hex='A8AE7F'")
        assert cur.fetchone()[0] == 1

    def test_migrate_schema_is_idempotent_on_fresh_database(self):
        db = _make_db()
        _migrate_schema(db)
        _migrate_schema(db)  # must not raise on the second call


# ---------------------------------------------------------------------------
# MessageProcessor._archive — live-path publish, mirroring receiver._publish()'s
# rmq_connected reset on a basic_publish failure
# ---------------------------------------------------------------------------

class TestProcessorArchive:
    def _make_completed_flight(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()
        return f.to_completed_flight()

    def _connected_processor(self):
        """A processor with a mocked channel + connection, wired so
        add_callback_threadsafe invokes the scheduled callback immediately
        -- standing in for the connection's own thread actually running
        its ioloop, without needing a second real thread in every test."""
        p, _ = _make_processor()
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.add_callback_threadsafe.side_effect = lambda cb: cb()
        p._rmq_channel = mock_channel
        p._rmq_connection = mock_connection
        p._rmq_connected = True
        return p, mock_channel, mock_connection

    def test_archive_publishes_when_connected(self):
        p, mock_channel, mock_connection = self._connected_processor()

        p._archive(self._make_completed_flight())

        mock_connection.add_callback_threadsafe.assert_called_once()
        mock_channel.basic_publish.assert_called_once()
        assert mock_channel.basic_publish.call_args.kwargs["routing_key"] == "archive"
        assert p._fallback.depth() == 0
        assert p._rmq_connected is True

    def test_archive_schedules_via_add_callback_threadsafe_not_channel_directly(self):
        """self._rmq_channel must only ever be touched by the thread
        running start_consuming() -- the eviction thread calling _archive()
        must go through add_callback_threadsafe, never basic_publish
        directly, even before the scheduled callback has actually run."""
        p, _ = _make_processor()
        mock_channel = MagicMock()
        mock_connection = MagicMock()  # add_callback_threadsafe never invokes its callback here
        p._rmq_channel = mock_channel
        p._rmq_connection = mock_connection
        p._rmq_connected = True

        p._archive(self._make_completed_flight())

        mock_connection.add_callback_threadsafe.assert_called_once()
        mock_channel.basic_publish.assert_not_called()
        # Not yet queued to fallback either -- the callback hasn't run to
        # decide success/failure.
        assert p._fallback.depth() == 0

    def test_archive_falls_back_when_not_connected(self):
        p, _ = _make_processor()
        p._rmq_connected = False

        p._archive(self._make_completed_flight())

        assert p._fallback.depth() == 1
        assert p._rmq_connected is False

    def test_archive_falls_back_when_scheduling_fails(self):
        p, _ = _make_processor()
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.add_callback_threadsafe.side_effect = RuntimeError("connection closed")
        p._rmq_channel = mock_channel
        p._rmq_connection = mock_connection
        p._rmq_connected = True

        p._archive(self._make_completed_flight())

        assert p._rmq_connected is False
        assert p._fallback.depth() == 1

    def test_archive_callback_resets_rmq_connected_and_falls_back_on_publish_failure(self):
        """A live basic_publish failure is the only self-correcting path
        this component has — unlike the receiver, nothing else in
        _archive() ever flips rmq_connected back, so a failure inside the
        scheduled callback must set it False and queue the payload to the
        fallback itself, since the original caller can no longer observe
        the outcome synchronously."""
        p, mock_channel, mock_connection = self._connected_processor()
        mock_channel.basic_publish.side_effect = RuntimeError("boom")

        p._archive(self._make_completed_flight())

        assert p._rmq_connected is False
        assert p._fallback.depth() == 1


# Fallback queue put/drain/depth/dead-lettering is now covered by
# shared/tests/test_fallback_queue.py -- MessageProcessor just wires
# shared.FallbackQueue in, tested below via TestProcessorDrainFallback.


# ---------------------------------------------------------------------------
# MessageProcessor._drain_fallback — and its periodic-tick trigger from
# _telemetry_loop, alongside the existing RabbitMQ-reconnect trigger
# ---------------------------------------------------------------------------

def _synchronous_drain_thread():
    """Patch threading.Thread so drain_in_background's spawned thread runs
    synchronously in the caller's thread instead of racing the test's own
    assertions against a real background thread. Production code still
    spawns a genuine thread; this only affects the test."""
    class _ImmediateThread:
        def __init__(self, target=None, daemon=None, name=None):
            self._target = target

        def start(self):
            if self._target:
                self._target()

    return patch("message_processor.main.threading.Thread", _ImmediateThread)


class TestProcessorDrainFallback:
    def _connected_processor_with_queued_item(self):
        """A processor with one fallback row queued and a mocked
        channel + connection, wired so add_callback_threadsafe invokes the
        scheduled callback immediately -- standing in for the connection's
        own thread actually running its ioloop."""
        p, _ = _make_processor()
        p._fallback.put('{"_id": "a"}')
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.add_callback_threadsafe.side_effect = lambda cb: cb()
        p._rmq_channel = mock_channel
        p._rmq_connection = mock_connection
        return p, mock_channel, mock_connection

    def test_drain_fallback_publishes_queued_items(self):
        p, mock_channel, mock_connection = self._connected_processor_with_queued_item()

        with _synchronous_drain_thread():
            p._drain_fallback()

        assert p._fallback.depth() == 0
        mock_connection.add_callback_threadsafe.assert_called_once()
        mock_channel.basic_publish.assert_called_once()
        assert mock_channel.basic_publish.call_args.kwargs["routing_key"] == "archive"

    def test_drain_fallback_leaves_items_queued_on_publish_error(self):
        p, mock_channel, _ = self._connected_processor_with_queued_item()
        mock_channel.basic_publish.side_effect = ConnectionError("gone")

        with _synchronous_drain_thread():
            p._drain_fallback()

        assert p._fallback.depth() == 1

    def test_drain_fallback_resets_rmq_connected_on_publish_failure(self):
        """A failed basic_publish during draining is just as much evidence
        the connection is broken as a failed live-path publish — mirror
        _archive()'s handling (and the receiver's equivalent fix)."""
        p, mock_channel, _ = self._connected_processor_with_queued_item()
        mock_channel.basic_publish.side_effect = RuntimeError("boom")
        p._rmq_connected = True

        with _synchronous_drain_thread():
            p._drain_fallback()

        assert p._rmq_connected is False
        assert p._fallback.depth() == 1

    def test_drain_fallback_falls_back_when_no_connection(self):
        """No RabbitMQ connection at all (never connected yet) must behave
        like any other publish failure -- leave the row queued -- rather
        than raising out of drain_in_background's background thread."""
        p, _ = _make_processor()
        p._fallback.put('{"_id": "a"}')
        p._rmq_connection = None

        with _synchronous_drain_thread():
            p._drain_fallback()

        assert p._fallback.depth() == 1

    def test_publish_schedules_via_add_callback_threadsafe_and_waits_for_it(self):
        """The drain thread's publish() closure must never touch
        self._rmq_channel directly -- only the connection's own thread may
        (see _rmq_channel's threading contract) -- and must block until
        the scheduled callback has actually run before returning, per
        drain()'s synchronous per-row contract. Verified here with a real
        background drain thread (not the synchronous-thread patch) and a
        callback held back until the test explicitly releases it."""
        p, _ = _make_processor()
        p._fallback.put('{"_id": "a"}')
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        captured: dict = {}
        mock_connection.add_callback_threadsafe.side_effect = lambda cb: captured.setdefault("cb", cb)
        p._rmq_channel = mock_channel
        p._rmq_connection = mock_connection

        p._drain_fallback()  # spawns a real background drain thread

        deadline = time.monotonic() + 2.0
        while "cb" not in captured and time.monotonic() < deadline:
            time.sleep(0.01)
        assert "cb" in captured, "publish() never scheduled via add_callback_threadsafe"
        mock_channel.basic_publish.assert_not_called()
        assert p._fallback.depth() == 1  # not yet removed -- callback hasn't run

        captured["cb"]()  # simulate the connection thread running the callback

        deadline = time.monotonic() + 2.0
        while p._fallback.depth() != 0 and time.monotonic() < deadline:
            time.sleep(0.01)
        mock_channel.basic_publish.assert_called_once()
        assert p._fallback.depth() == 0

    def _run_one_telemetry_tick(self, p) -> None:
        """Run the real _telemetry_loop for exactly one iteration, by
        making the mocked time.sleep set _shutdown so the loop body runs
        once and then exits — rather than re-implementing the loop's
        conditional in the test, which wouldn't actually exercise it."""
        def fake_sleep(_seconds):
            p._shutdown.set()

        with patch("message_processor.main.time.sleep", side_effect=fake_sleep), \
             patch.object(p, "_publish_telemetry"):
            p._telemetry_loop()

    def test_telemetry_loop_drains_when_connected(self):
        """The periodic tick must attempt a drain when RabbitMQ is
        connected — this is what lets a stuck/missed reconnect-triggered
        drain still recover on the next tick, since a publish
        failure can pin _rmq_connected False without the underlying
        connection ever raising AMQPConnectionError to re-enter
        _consume_loop's own reconnect-triggered drain."""
        p, _ = _make_processor()
        p._fallback.put('{"_id": "a"}')
        p._rmq_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.add_callback_threadsafe.side_effect = lambda cb: cb()
        p._rmq_connection = mock_connection
        p._rmq_connected = True

        with _synchronous_drain_thread():
            self._run_one_telemetry_tick(p)

        assert p._fallback.depth() == 0

    def test_telemetry_tick_does_not_drain_when_disconnected(self):
        p, _ = _make_processor()
        p._fallback.put('{"_id": "a"}')
        p._rmq_connected = False

        self._run_one_telemetry_tick(p)

        assert p._fallback.depth() == 1


# ---------------------------------------------------------------------------
# _RateTracker
# ---------------------------------------------------------------------------

class TestRateTracker:
    def test_zero_rate_when_empty(self):
        rt = _RateTracker(window=30)
        assert rt.rate() == 0.0

    def test_rate_reflects_recent_messages(self):
        rt = _RateTracker(window=30)
        for _ in range(30):
            rt.record()
        assert 0.9 <= rt.rate() <= 1.1  # ~1/s


# ---------------------------------------------------------------------------
# _TimeTracker
# ---------------------------------------------------------------------------

class TestTimeTracker:
    def test_avg(self):
        tt = _TimeTracker()
        tt.record(100.0)
        tt.record(200.0)
        assert tt.avg_ms() == 150.0

    def test_hwm_resets_on_read(self):
        tt = _TimeTracker()
        tt.record_hwm(500)
        tt.record_hwm(200)
        assert tt.hwm_ms_and_reset() == 500
        assert tt.hwm_ms_and_reset() == 0  # reset

    def test_reset_clears_avg(self):
        tt = _TimeTracker()
        tt.record(100.0)
        tt.reset()
        assert tt.avg_ms() == 0.0

    def test_hwm_preserves_full_float_precision(self):
        """record_hwm() -> hwm_ms_and_reset() must not round or truncate
        anywhere in the path -- processing_time_hwm_ms publishes this
        value as-is, with any display-side rounding left to Home
        Assistant's suggested_display_precision, not Python."""
        tt = _TimeTracker()
        tt.record_hwm(2.5583304843038)
        assert tt.hwm_ms_and_reset() == 2.5583304843038

    def test_hwm_and_reset_return_type_is_float(self):
        tt = _TimeTracker()
        tt.record_hwm(3.0)
        assert isinstance(tt.hwm_ms_and_reset(), float)
        # And the post-reset zero value is a float too, not an int --
        # matches hwm_ms_and_reset()'s -> float type hint.
        assert isinstance(tt.hwm_ms_and_reset(), float)


# ---------------------------------------------------------------------------
# _DepthHWM — high-water-mark tracker backing rabbitmq_input_queue_depth_hwm
# ---------------------------------------------------------------------------

class TestDepthHWM:
    def test_starts_at_negative_one(self):
        assert _DepthHWM().value_and_reset() == -1

    def test_tracks_max_of_recorded_values(self):
        hwm = _DepthHWM()
        hwm.record(3)
        hwm.record(9)
        hwm.record(5)
        assert hwm.value_and_reset() == 9

    def test_resets_after_read(self):
        hwm = _DepthHWM()
        hwm.record(9)
        assert hwm.value_and_reset() == 9
        assert hwm.value_and_reset() == -1

    def test_error_reading_does_not_clobber_a_prior_valid_max(self):
        """A -1 (error/no-sample) reading is still passed to record() by the
        sampler loop — it must never win against a real depth already seen
        this window, since -1 always loses the max() comparison."""
        hwm = _DepthHWM()
        hwm.record(4)
        hwm.record(-1)
        assert hwm.value_and_reset() == 4

    def test_all_error_readings_report_negative_one(self):
        hwm = _DepthHWM()
        hwm.record(-1)
        hwm.record(-1)
        assert hwm.value_and_reset() == -1


# ---------------------------------------------------------------------------
# MESSAGE_PROCESSOR_ID and the consistent-hash exchange binding
# ---------------------------------------------------------------------------

class TestMessageProcessorIdentity:
    """MESSAGE_PROCESSOR_ID is any string unique across the deployment, and
    it names both the input queue and the Redis duplicate-ID guard key."""

    def test_queue_name_uses_the_id_verbatim(self):
        p, _ = _make_processor(message_processor_id="7")
        assert p._queue_name == "skyfollower-message-processor-7"

    def test_heartbeat_key_uses_the_id_verbatim(self):
        p, mock_redis = _make_processor(message_processor_id="7")
        mock_redis.set.return_value = True

        p._claim_message_processor_id()

        assert mock_redis.set.call_args.args[0] == message_processor_heartbeat_key(
            "7"
        )

    def test_duplicate_id_still_exits(self):
        p, mock_redis = _make_processor(message_processor_id="7")
        mock_redis.set.return_value = None

        with pytest.raises(SystemExit):
            p._claim_message_processor_id()

    def test_main_passes_the_id_through_without_coercion(self):
        cfg = _minimal_config()
        cfg["message_processor_id"] = "7"
        with patch("message_processor.main.load_config", return_value=cfg), \
             patch("message_processor.main.MessageProcessor") as MockProcessor, \
             patch("message_processor.main.signal.signal"):
            processor_main()

        assert MockProcessor.call_args.args[1] == "7"

    def test_main_requires_the_id(self):
        # Everything else present, so the exit can only be about the ID.
        env = {
            "RABBITMQ_HOST": "localhost",
            "RABBITMQ_USERNAME": "u",
            "RABBITMQ_PASSWORD": "p",
            "REDIS_HOST": "localhost",
            "REDIS_PASSWORD": "p",
            "MQTT_HOST": "localhost",
            "MQTT_USERNAME": "u",
            "MQTT_PASSWORD": "p",
            "LATITUDE": "0",
            "LONGITUDE": "0",
            "MESSAGE_PROCESSOR_ID": "",
        }
        with patch.dict(os.environ, env), pytest.raises(SystemExit):
            processor_main()


class TestConsumeLoopExchangeBinding:
    """Each processor owns its queue: it declares the shared exchange
    topology and binds its own queue to it, rather than consuming from a
    queue some receiver pre-declared."""

    def _run_one_connect(self, p) -> MagicMock:
        channel = MagicMock()
        channel.start_consuming.side_effect = lambda: p._shutdown.set()
        with patch("message_processor.main.pika.BlockingConnection") as MockConnection:
            MockConnection.return_value.channel.return_value = channel
            p._consume_loop()
        return channel

    def test_declares_the_hash_exchange_with_its_alternate_exchange(self):
        p, _ = _make_processor()
        channel = self._run_one_connect(p)

        channel.exchange_declare.assert_any_call(
            exchange="adsb",
            exchange_type="x-consistent-hash",
            durable=True,
            arguments={"alternate-exchange": "adsb-unroutable"},
        )

    def test_binds_its_own_queue_with_weight_one(self):
        p, _ = _make_processor(message_processor_id="7")
        channel = self._run_one_connect(p)

        channel.queue_declare.assert_any_call(
            queue="skyfollower-message-processor-7", durable=True
        )
        channel.queue_bind.assert_any_call(
            queue="skyfollower-message-processor-7", exchange="adsb", routing_key="1"
        )
        channel.basic_consume.assert_called_once_with(
            queue="skyfollower-message-processor-7", on_message_callback=p._on_message
        )

    def test_sets_prefetch_count_above_one(self):
        """Each queue has exactly one consumer here (bound via the
        consistent-hash exchange), so prefetch_count=1's fair-dispatch
        rationale doesn't apply -- it was pure per-message round-trip
        serialization capping throughput at ~200 msg/sec. Pin >1 rather
        than the exact tuned value, so this doesn't churn on every future
        retune."""
        p, _ = _make_processor()
        channel = self._run_one_connect(p)

        channel.basic_qos.assert_called_once()
        _, kwargs = channel.basic_qos.call_args
        assert kwargs["prefetch_count"] > 1


# ---------------------------------------------------------------------------
# MessageProcessor._sample_rmq_queue_depth — passive queue_declare on this
# processor's own input queue, reusing the existing consumer channel but
# scheduled onto the connection's own thread via add_callback_threadsafe
# (self._rmq_channel must only ever be touched by the thread running
# start_consuming())
# ---------------------------------------------------------------------------

class TestSampleRmqQueueDepth:
    def test_no_channel_records_negative_one(self):
        p, _ = _make_processor()
        p._rmq_channel = None
        p._rmq_connection = MagicMock()

        p._sample_rmq_queue_depth()

        assert p._rmq_queue_depth_hwm.value_and_reset() == -1

    def test_no_connection_records_negative_one(self):
        p, _ = _make_processor()
        p._rmq_channel = MagicMock()
        p._rmq_connection = None

        p._sample_rmq_queue_depth()

        assert p._rmq_queue_depth_hwm.value_and_reset() == -1

    def test_schedules_via_add_callback_threadsafe_not_channel_directly(self):
        p, _ = _make_processor()
        mock_channel = MagicMock()
        mock_connection = MagicMock()  # never invokes its callback here
        p._rmq_channel = mock_channel
        p._rmq_connection = mock_connection

        p._sample_rmq_queue_depth()

        mock_connection.add_callback_threadsafe.assert_called_once()
        mock_channel.queue_declare.assert_not_called()

    def test_callback_records_message_count_from_passive_declare(self):
        p, _ = _make_processor()
        mock_channel = MagicMock()
        mock_channel.queue_declare.return_value.method.message_count = 7
        mock_connection = MagicMock()
        mock_connection.add_callback_threadsafe.side_effect = lambda cb: cb()
        p._rmq_channel = mock_channel
        p._rmq_connection = mock_connection

        p._sample_rmq_queue_depth()

        mock_channel.queue_declare.assert_called_once_with(
            queue=p._queue_name, durable=True, passive=True
        )
        assert p._rmq_queue_depth_hwm.value_and_reset() == 7

    def test_callback_records_negative_one_on_declare_error(self):
        p, _ = _make_processor()
        mock_channel = MagicMock()
        mock_channel.queue_declare.side_effect = ConnectionError("gone")
        mock_connection = MagicMock()
        mock_connection.add_callback_threadsafe.side_effect = lambda cb: cb()
        p._rmq_channel = mock_channel
        p._rmq_connection = mock_connection

        p._sample_rmq_queue_depth()

        assert p._rmq_queue_depth_hwm.value_and_reset() == -1

    def test_scheduling_failure_records_negative_one(self):
        p, _ = _make_processor()
        mock_channel = MagicMock()
        mock_connection = MagicMock()
        mock_connection.add_callback_threadsafe.side_effect = RuntimeError("connection closed")
        p._rmq_channel = mock_channel
        p._rmq_connection = mock_connection

        p._sample_rmq_queue_depth()

        assert p._rmq_queue_depth_hwm.value_and_reset() == -1


# ---------------------------------------------------------------------------
# MessageProcessor._rmq_queue_depth_sampler_loop — polls at most once every 10
# seconds, independent of telemetry_interval_seconds
# ---------------------------------------------------------------------------

class TestRmqQueueDepthSamplerLoop:
    def _run_one_sample_tick(self, p) -> None:
        """Run the real _rmq_queue_depth_sampler_loop for exactly one
        iteration, by making the mocked time.sleep set _shutdown so the
        loop body runs once and then exits — mirrors
        _run_one_telemetry_tick's approach for _telemetry_loop."""
        def fake_sleep(_seconds):
            p._shutdown.set()

        with patch("message_processor.main.time.sleep", side_effect=fake_sleep):
            p._rmq_queue_depth_sampler_loop()

    def test_sleeps_ten_seconds_between_samples(self):
        p, _ = _make_processor()
        mock_channel = MagicMock()
        mock_channel.queue_declare.return_value.method.message_count = 0
        mock_connection = MagicMock()
        mock_connection.add_callback_threadsafe.side_effect = lambda cb: cb()
        p._rmq_channel = mock_channel
        p._rmq_connection = mock_connection

        with patch("message_processor.main.time.sleep") as mock_sleep:
            mock_sleep.side_effect = lambda _s: p._shutdown.set()
            p._rmq_queue_depth_sampler_loop()

        mock_sleep.assert_called_once_with(10)

    def test_one_tick_records_sampled_depth_into_hwm(self):
        p, _ = _make_processor()
        mock_channel = MagicMock()
        mock_channel.queue_declare.return_value.method.message_count = 12
        mock_connection = MagicMock()
        mock_connection.add_callback_threadsafe.side_effect = lambda cb: cb()
        p._rmq_channel = mock_channel
        p._rmq_connection = mock_connection

        self._run_one_sample_tick(p)

        assert p._rmq_queue_depth_hwm.value_and_reset() == 12

    def test_one_tick_with_no_channel_records_negative_one(self):
        p, _ = _make_processor()
        p._rmq_channel = None
        p._rmq_connection = MagicMock()

        self._run_one_sample_tick(p)

        assert p._rmq_queue_depth_hwm.value_and_reset() == -1


# ---------------------------------------------------------------------------
# MessageProcessor enrichment logic (unit tests with mocked Redis)
# ---------------------------------------------------------------------------

class TestProcessorEnrichment:
    def _make_processor(self):
        cfg = _minimal_config()
        with patch("message_processor.main.DATA_DIR", tempfile.mkdtemp()), \
             patch("message_processor.main.redis_lib.Redis") as MockRedis, \
             patch("message_processor.main.RulesEngine"), \
             patch("message_processor.main.pathlib.Path"), \
             patch.object(MessageProcessor, "_claim_message_processor_id"):
            mock_redis = MagicMock()
            mock_redis.script_load.return_value = "abc123sha"
            MockRedis.return_value = mock_redis
            p = MessageProcessor(cfg, message_processor_id="0")
            p._redis = mock_redis
            p._merge_sha = "abc123sha"
            p._db = _make_db()
            return p, mock_redis

    def test_enrich_aircraft_populates_from_redis(self):
        p, mock_redis = self._make_processor()
        aircraft_data = {"icao_hex": "A8AE7F", "registration": "N659DL", "type_designator": "B763"}
        mock_redis.evalsha.return_value = json.dumps(aircraft_data)

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        p._enrich_aircraft(f)

        assert f.aircraft["registration"] == "N659DL"
        mock_redis.evalsha.assert_called_once_with("abc123sha", 0, "A8AE7F")

    def test_enrich_aircraft_increments_miss_on_cache_miss(self):
        p, mock_redis = self._make_processor()
        mock_redis.evalsha.return_value = None

        f = Flight(p._db)
        f.icao_hex = "ZZZZZZ"
        p._enrich_aircraft(f)

        mock_redis.incr.assert_called()
        assert f.aircraft.get("icao_hex") == "ZZZZZZ"

    def test_enrich_aircraft_strips_registry_wake_turbulence_category(self):
        # wake_turbulence_category is receiver-decode-only -- a merged Redis
        # document (still possibly carrying a registry-sourced value, until
        # the data-runner side stops writing it) must never seed the field.
        p, mock_redis = self._make_processor()
        aircraft_data = {
            "icao_hex": "A8AE7F", "registration": "N659DL",
            "wake_turbulence_category": "heavy",
        }
        mock_redis.evalsha.return_value = json.dumps(aircraft_data)

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        p._enrich_aircraft(f)

        assert f.aircraft["registration"] == "N659DL"
        assert "wake_turbulence_category" not in f.aircraft

    def test_enrich_aircraft_no_registration_key_written(self):
        p, mock_redis = self._make_processor()
        aircraft_data = {"icao_hex": "A8AE7F", "registration": "N659DL"}
        mock_redis.evalsha.return_value = json.dumps(aircraft_data)

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        p._enrich_aircraft(f)

        for call in mock_redis.set.call_args_list:
            assert not str(call).startswith("registration:")

    def test_enrich_operator_skips_us_tail_number(self):
        p, mock_redis = self._make_processor()
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.ident = "N12345"  # US registration
        f.aircraft = {}
        p._enrich_operator(f)
        mock_redis.json.return_value.get.assert_not_called()

    def test_enrich_operator_skips_military(self):
        p, mock_redis = self._make_processor()
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.ident = "DAL659"
        f.aircraft = {"military": True}
        p._enrich_operator(f)
        mock_redis.json.return_value.get.assert_not_called()

    def test_enrich_operator_extracts_prefix(self):
        # operator:{designator} is a RedisJSON document -- .json().get()
        # returns the decoded dict directly, not a JSON string (unlike the
        # plain GET this replaced, which raised WRONGTYPE against a real
        # instance; a test mocking plain .get() never caught that).
        p, mock_redis = self._make_processor()
        mock_redis.json.return_value.get.return_value = {"airline_designator": "DAL", "name": "Delta"}

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.ident = "DAL659"
        f.aircraft = {}
        f.operator = {}
        p._enrich_operator(f)

        assert f.operator["airline_designator"] == "DAL"
        mock_redis.json.return_value.get.assert_called_once_with(operator_key("DAL"))


# ---------------------------------------------------------------------------
# _route_ready / _maybe_resolve_route — route:{ident} leg resolution
#
# The heuristics themselves (proximity, heading-vs-bearing, cross-track
# sanity check) are unit tested in isolation in test_route_resolver.py;
# these tests cover the message-processor-side wiring: resolution runs the
# moment ident/position/altitude/heading are all known for a flight (not at
# archive time), at most once per flight, and never for an ident that's
# just the aircraft's own tail number.
# ---------------------------------------------------------------------------

def _evalsha_dispatch(route_sha: str, route_return=None, aircraft_return=None):
    """mock_redis.evalsha is shared between merge_aircraft.lua (icao_hex)
    and route_airports.lua (ident) calls -- dispatch on which sha was
    passed so a test can control each independently."""
    def _side_effect(sha, _numkeys, *_args):
        if sha == route_sha:
            return route_return
        return aircraft_return

    return _side_effect


class TestRouteReady:
    def _make_flight(self, p, ident="", registration=None) -> Flight:
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "fid-route-ready"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.ident = ident
        if registration is not None:
            f.aircraft = {"icao_hex": "A8AE7F", "registration": registration}
        f.save()
        return f

    def test_not_ready_without_ident(self):
        p, _ = _make_processor()
        f = self._make_flight(p, ident="")
        assert p._route_ready(f) is False

    def test_not_ready_when_ident_matches_registration(self):
        p, _ = _make_processor()
        f = self._make_flight(p, ident="VPCKA", registration="VP-CKA")
        f.add_position(Position(timestamp=1.0, latitude=1.0, longitude=1.0, altitude=1000))
        f.add_velocity(Velocity(timestamp=1.0, heading=90))
        assert p._route_ready(f) is False

    def test_not_ready_without_altitude(self):
        p, _ = _make_processor()
        f = self._make_flight(p, ident="DAL659")
        f.add_position(Position(timestamp=1.0, latitude=1.0, longitude=1.0, altitude=None))
        f.add_velocity(Velocity(timestamp=1.0, heading=90))
        assert p._route_ready(f) is False

    def test_not_ready_without_heading(self):
        p, _ = _make_processor()
        f = self._make_flight(p, ident="DAL659")
        f.add_position(Position(timestamp=1.0, latitude=1.0, longitude=1.0, altitude=1000))
        f.add_velocity(Velocity(timestamp=1.0, heading=None))
        assert p._route_ready(f) is False

    def test_ready_when_all_conditions_met(self):
        p, _ = _make_processor()
        f = self._make_flight(p, ident="DAL659")
        f.add_position(Position(timestamp=1.0, latitude=1.0, longitude=1.0, altitude=1000))
        f.add_velocity(Velocity(timestamp=1.0, heading=90))
        assert p._route_ready(f) is True

    def test_ready_regardless_of_arrival_order(self):
        """Altitude/heading recorded on earlier messages than the ident
        still counts -- readiness is checked against the flight's full
        history, not just this message's fields."""
        p, _ = _make_processor()
        f = self._make_flight(p, ident="")
        f.add_position(Position(timestamp=1.0, latitude=1.0, longitude=1.0, altitude=1000))
        f.add_velocity(Velocity(timestamp=1.0, heading=90))
        assert p._route_ready(f) is False  # no ident yet
        f.ident = "DAL659"
        assert p._route_ready(f) is True


class TestMaybeResolveRoute:
    def _make_ready_flight(self, p, ident="DAL659", positions=None, velocities=None) -> Flight:
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "fid-resolve-route"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.ident = ident
        f.save()
        for pos in positions or [(37.0, -79.0, 35000)]:
            f.add_position(Position(timestamp=1.0, latitude=pos[0], longitude=pos[1], altitude=pos[2]))
        for vel in velocities or [(90,)]:
            f.add_velocity(Velocity(timestamp=1.0, heading=vel[0]))
        return f

    def test_not_triggered_until_ready(self):
        p, mock_redis = _make_processor()
        f = self._make_ready_flight(p, positions=[(37.0, -79.0, None)])  # no altitude yet

        p._maybe_resolve_route(f)

        mock_redis.evalsha.assert_not_called()
        assert f.route_resolution_attempted is False

    def test_resolves_direct_two_airport_route(self):
        p, mock_redis = _make_processor()
        airports = [
            {"icao_code": "KJFK", "latitude": 40.6398, "longitude": -73.7789},
            {"icao_code": "KATL", "latitude": 33.6367, "longitude": -84.4281},
        ]
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return=json.dumps(airports))
        f = self._make_ready_flight(p)

        p._maybe_resolve_route(f)

        mock_redis.evalsha.assert_called_once_with(p._route_sha, 0, "DAL659")
        assert f.origin == "KJFK"
        assert f.destination == "KATL"
        assert f.route_resolution_attempted is True

    def test_leaves_unset_when_no_route_known(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return="[]")
        f = self._make_ready_flight(p)

        p._maybe_resolve_route(f)

        assert f.origin is None
        assert f.destination is None
        assert f.route_resolution_attempted is True

    def test_no_route_known_logs_debug_with_redis_response(self, caplog):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return="[]")
        f = self._make_ready_flight(p, ident="DAL659")

        with caplog.at_level(logging.DEBUG, logger="message_processor"):
            p._maybe_resolve_route(f)

        assert "DAL659" in caplog.text
        assert "A8AE7F" in caplog.text
        assert "[]" in caplog.text

    def test_sanity_check_rejection_leaves_unset(self):
        """Real-world case: a flight's actual track ran ~800nm from the
        KMSP-KMKE great-circle line -- the route entry was bogus for this
        flight, and neither field should be set."""
        p, mock_redis = _make_processor()
        airports = [
            {"icao_code": "KMSP", "latitude": 44.882, "longitude": -93.222},
            {"icao_code": "KMKE", "latitude": 42.947, "longitude": -87.897},
        ]
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return=json.dumps(airports))
        f = self._make_ready_flight(p, ident="BOGUS1", positions=[(25.0, -90.0, 35000)])

        p._maybe_resolve_route(f)

        assert f.origin is None
        assert f.destination is None

    def test_sanity_check_rejection_logs_debug_with_reason_and_redis_response(self, caplog):
        """The requested DEBUG log: what route_airports.lua returned, and
        why the candidate was rejected."""
        p, mock_redis = _make_processor()
        airports = [
            {"icao_code": "KMSP", "latitude": 44.882, "longitude": -93.222},
            {"icao_code": "KMKE", "latitude": 42.947, "longitude": -87.897},
        ]
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return=json.dumps(airports))
        f = self._make_ready_flight(p, ident="BOGUS1", positions=[(25.0, -90.0, 35000)])

        with caplog.at_level(logging.DEBUG, logger="message_processor"):
            p._maybe_resolve_route(f)

        assert "BOGUS1" in caplog.text
        assert "KMSP" in caplog.text and "KMKE" in caplog.text
        assert "sanity check" in caplog.text

    def test_redis_error_leaves_unset_but_marks_attempted(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.side_effect = RuntimeError("boom")
        f = self._make_ready_flight(p)

        p._maybe_resolve_route(f)

        assert f.origin is None
        assert f.destination is None
        assert f.route_resolution_attempted is True

    def test_does_not_requery_redis_once_attempted(self):
        """The one-shot guard: a valid ident with no known route must not
        be re-queried against Redis on every subsequent message."""
        p, mock_redis = _make_processor()
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return="[]")
        f = self._make_ready_flight(p)

        p._maybe_resolve_route(f)
        p._maybe_resolve_route(f)
        p._maybe_resolve_route(f)

        mock_redis.evalsha.assert_called_once()


class TestMaybeResolveRouteHeadingStability:
    """A multi-leg route whose heading hasn't stabilized yet (a single
    reading, or a genuinely circling/holding aircraft) must not be marked
    route_resolution_attempted -- it's re-evaluated on later messages once
    more heading samples arrive -- but the fetched airport records are
    cached so the retry never repeats the Redis round trip (the
    storm-holding-pattern scenario)."""

    KJFK = {"icao_code": "KJFK", "latitude": 40.6398, "longitude": -73.7789}
    KMIA = {"icao_code": "KMIA", "latitude": 25.7959, "longitude": -80.2870}
    KMCO = {"icao_code": "KMCO", "latitude": 28.4294, "longitude": -81.3089}

    def _make_flight(self, p, ident="DAL659") -> Flight:
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "fid-heading-stability"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.ident = ident
        f.save()
        return f

    def test_single_heading_sample_defers_without_marking_attempted(self):
        p, mock_redis = _make_processor()
        airports = [self.KJFK, self.KMIA, self.KMCO, self.KJFK]
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return=json.dumps(airports))
        f = self._make_flight(p)
        f.add_position(Position(timestamp=1.0, latitude=30.3, longitude=-81.0, altitude=22000))
        f.add_velocity(Velocity(timestamp=1.0, heading=350))

        p._maybe_resolve_route(f)

        assert f.route_resolution_attempted is False
        assert f.origin is None
        assert f.destination is None
        mock_redis.evalsha.assert_called_once()  # fetched once, cached
        assert f.route_candidate_airports == json.dumps(airports)

    def test_retry_uses_cached_airports_not_a_second_redis_call(self):
        p, mock_redis = _make_processor()
        airports = [self.KJFK, self.KMIA, self.KMCO, self.KJFK]
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return=json.dumps(airports))
        f = self._make_flight(p)
        f.add_position(Position(timestamp=1.0, latitude=30.3, longitude=-81.0, altitude=22000))
        f.add_velocity(Velocity(timestamp=1.0, heading=350))

        p._maybe_resolve_route(f)  # first call: fetches + caches, defers
        p._maybe_resolve_route(f)  # retry: still only one sample -- still deferred
        p._maybe_resolve_route(f)

        assert f.route_resolution_attempted is False
        mock_redis.evalsha.assert_called_once()

    def test_stabilizes_after_more_samples_without_a_second_redis_call(self):
        """Once enough consistent headings accumulate, the same holding
        heading (~350deg) is trusted -- but the along-track sanity check
        still rejects the wrong leg it would otherwise match, all without
        ever calling Redis a second time."""
        p, mock_redis = _make_processor()
        airports = [self.KJFK, self.KMIA, self.KMCO, self.KJFK]
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return=json.dumps(airports))
        f = self._make_flight(p)
        f.add_position(Position(timestamp=1.0, latitude=30.3, longitude=-81.0, altitude=22000))
        f.add_velocity(Velocity(timestamp=1.0, heading=350))

        p._maybe_resolve_route(f)
        assert f.route_resolution_attempted is False

        for t, heading in enumerate((349, 351, 350), start=2):
            f.add_velocity(Velocity(timestamp=float(t), heading=heading))
            p._maybe_resolve_route(f)

        assert f.route_resolution_attempted is True
        assert f.origin is None
        assert f.destination is None  # along-track check rejects KMIA->KMCO
        mock_redis.evalsha.assert_called_once()


class TestWakeTurbulenceCategoryLiveOverwrite:
    """wake_turbulence_category has exactly one writer (live decode) --
    each new reading replaces the last one outright, no first-wins
    protection (unlike the old setdefault-based merge)."""

    def test_later_message_overwrites_earlier_value(self):
        p, mock_redis = _make_processor()
        icao_hex = "A8AE7F"
        t = 1_700_000_000.0

        with p._db_lock:
            p._update_flight(
                {"icao_hex": icao_hex, "wake_turbulence_category": "medium"},
                InboundMessage(raw="00" * 14, icao_hex=icao_hex, received_at=t, source="1090"),
            )
        f = Flight(p._db)
        f.load(icao_hex)
        assert f.aircraft["wake_turbulence_category"] == "medium"

        with p._db_lock:
            p._update_flight(
                {"icao_hex": icao_hex, "wake_turbulence_category": "heavy"},
                InboundMessage(raw="00" * 14, icao_hex=icao_hex, received_at=t + 1, source="1090"),
            )
        f = Flight(p._db)
        f.load(icao_hex)
        assert f.aircraft["wake_turbulence_category"] == "heavy"


class TestRulesEngineHwmNanoseconds:
    """rules_engine_hwm_ns measures a single evaluate() call in
    nanoseconds, not milliseconds -- ms resolution would mostly read 0-1
    for this in-process, no-I/O call."""

    def test_evaluate_duration_recorded_in_nanoseconds(self):
        p, _ = _make_processor()
        icao_hex = "A8AE7F"
        msg = InboundMessage(
            raw="00" * 14, icao_hex=icao_hex, received_at=1_700_000_000.0, source="1090"
        )

        # 1 microsecond of "elapsed" monotonic time -> 1000 nanoseconds.
        # _update_flight's rules-timing block is the only caller of
        # time.monotonic() reached here (route resolution never fires --
        # no ident on this message).
        times = iter([100.0, 100.000001])
        with patch("message_processor.main.time.monotonic", side_effect=lambda: next(times)):
            with p._db_lock:
                p._update_flight({"icao_hex": icao_hex}, msg)

        hwm_ns = p._rules_time.hwm_ms_and_reset()
        assert hwm_ns == pytest.approx(1000.0, rel=1e-6)


class TestMessageLatencyHwmEndToEnd:
    """message_latency_hwm_ms is receipt-through-processed (wall clock,
    time.time() - msg.received_at) -- a superset of processing_time_hwm_ms
    (message-processor-internal decode+state-update work only, monotonic
    clock), so it must never read narrower, and must surface RabbitMQ
    queue wait time processing_time_hwm_ms can't see at all."""

    def _run_on_message(self, p, received_at: float) -> None:
        # raw="00"*14 decodes to DF0 with no tracked fields populated (see
        # TestDecode1090.test_message_type_with_no_tracked_fields_dropped),
        # so _process() returns immediately without touching Redis/the
        # rules engine -- keeps processing_time_hwm_ms genuinely small
        # without needing to mock enrichment for this test.
        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=received_at, source="1090")
        method = MagicMock(delivery_tag=1)
        p._on_message(MagicMock(), method, None, msg.model_dump_json().encode())

    def test_latency_is_at_least_processing_time_under_normal_operation(self):
        p, _ = _make_processor()
        self._run_on_message(p, time.time())  # received "just now" -- no queue backlog

        latency_hwm = p._message_latency.hwm_ms_and_reset()
        processing_hwm = p._processing_time.hwm_ms_and_reset()
        assert latency_hwm >= processing_hwm

    def test_elevated_latency_under_simulated_queue_backlog_while_processing_time_stays_low(self):
        """A message stamped as received several seconds in the past
        (simulating time spent queued in RabbitMQ under backpressure) must
        show elevated message_latency_hwm_ms, while processing_time_hwm_ms
        -- which only measures work done after delivery -- stays low."""
        p, _ = _make_processor()
        backlog_seconds = 5.0
        self._run_on_message(p, time.time() - backlog_seconds)

        latency_hwm = p._message_latency.hwm_ms_and_reset()
        processing_hwm = p._processing_time.hwm_ms_and_reset()

        assert latency_hwm >= backlog_seconds * 1000
        assert processing_hwm < 1000  # genuinely fast -- no real backlog in decode/state-update
        assert latency_hwm > processing_hwm


class TestUpdateFlightTriggersRouteResolution:
    """End-to-end through _update_flight: resolution fires mid-flight, the
    moment the last of ident/position/altitude/heading arrives -- not at
    archive time -- and never re-fires afterward."""

    def test_resolves_as_soon_as_all_fields_present_across_messages(self):
        p, mock_redis = _make_processor()
        airports = [
            {"icao_code": "KJFK", "latitude": 40.6398, "longitude": -73.7789},
            {"icao_code": "KATL", "latitude": 33.6367, "longitude": -84.4281},
        ]
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return=json.dumps(airports))

        icao_hex = "A8AE7F"
        t = 1_700_000_000.0

        # Message 1: ident only.
        with p._db_lock:
            p._update_flight({"icao_hex": icao_hex, "ident": "DAL659"},
                              InboundMessage(raw="00" * 14, icao_hex=icao_hex, received_at=t, source="1090"))
        mock_redis.evalsha.assert_called_once()  # aircraft enrichment only

        # Message 2: position + altitude, still no heading.
        with p._db_lock:
            p._update_flight(
                {"icao_hex": icao_hex, "latitude": 37.0, "longitude": -79.0, "altitude": 35000},
                InboundMessage(raw="00" * 14, icao_hex=icao_hex, received_at=t + 1, source="1090"),
            )
        f = Flight(p._db)
        f.load(icao_hex)
        assert f.route_resolution_attempted is False

        # Message 3: heading arrives (alongside velocity, as a real airborne
        # velocity message would) -- all four conditions now satisfied.
        with p._db_lock:
            p._update_flight(
                {"icao_hex": icao_hex, "velocity": 450, "heading": 20},
                InboundMessage(raw="00" * 14, icao_hex=icao_hex, received_at=t + 2, source="1090"),
            )

        f = Flight(p._db)
        f.load(icao_hex)
        assert f.route_resolution_attempted is True
        assert f.origin == "KJFK"
        assert f.destination == "KATL"

    def test_never_triggers_for_tail_number_ident(self):
        p, mock_redis = _make_processor()
        aircraft = {"icao_hex": "A8AE7F", "registration": "VP-CKA"}
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, aircraft_return=json.dumps(aircraft))

        icao_hex = "A8AE7F"
        t = 1_700_000_000.0
        with p._db_lock:
            p._update_flight(
                {"icao_hex": icao_hex, "ident": "VPCKA", "latitude": 1.0, "longitude": 1.0,
                 "altitude": 1000, "velocity": 200, "heading": 90},
                InboundMessage(raw="00" * 14, icao_hex=icao_hex, received_at=t, source="1090"),
            )

        f = Flight(p._db)
        f.load(icao_hex)
        assert f.route_resolution_attempted is False
        mock_redis.evalsha.assert_called_once()  # aircraft enrichment only, no route lookup


# ---------------------------------------------------------------------------
# Telemetry — one retained topic per stat
# ---------------------------------------------------------------------------

class TestTelemetryPayload:
    """Tests for _publish_telemetry()'s one-retained-topic-per-stat behaviour."""

    def _make_processor(self) -> MessageProcessor:
        with patch("message_processor.main.DATA_DIR", tempfile.mkdtemp()), \
             patch("message_processor.main.redis_lib.Redis"):
            p = MessageProcessor(_minimal_config(), message_processor_id="0")
        return p

    def test_correct_base_topic(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_telemetry()
        topics = [c.args[0] for c in mock_mqtt.publish.call_args_list]
        assert all(t.startswith("SkyFollower/message-processor/0/statistic/") for t in topics)

    def test_retained(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_telemetry()
        for call in mock_mqtt.publish.call_args_list:
            assert call.kwargs.get("retain") is True

    def test_payload_fields(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        # A fresh _DepthHWM starts at -1 ("no valid sample landed this
        # window") -- record a real sample so rabbitmq_input_queue_depth_hwm
        # is actually published this tick (see #981: -1 is never
        # published, so it'd otherwise be silently absent from topics
        # below and this test wouldn't cover it at all).
        p._rmq_queue_depth_hwm.record(0)
        p._publish_telemetry()
        base = "SkyFollower/message-processor/0/statistic"
        topics = {c.args[0] for c in mock_mqtt.publish.call_args_list}
        expected = {
            "started_at", "messages_per_second", "processing_time_hwm_ms",
            "message_latency_hwm_ms", "rules_engine_hwm_ns", "rabbitmq_input_queue_depth_hwm",
            "local_archive_queue_depth", "active_flights",
            "registration_misses_hour", "registration_misses_today",
            "aircraft_type_misses_hour", "aircraft_type_misses_today",
        }
        assert {f"{base}/{name}" for name in expected}.issubset(topics)

    def test_processing_time_hwm_not_avg(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._processing_time.record(10.0)
        p._processing_time.record_hwm(10.0)
        p._processing_time.record(50.0)
        p._processing_time.record_hwm(50.0)
        p._publish_telemetry()
        calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        assert calls["SkyFollower/message-processor/0/statistic/processing_time_hwm_ms"] == "50.0"

    def test_processing_time_hwm_publishes_full_float_precision(self):
        """No Python-side rounding anywhere between record_hwm() and the
        published MQTT payload -- rounding is display-side only (Home
        Assistant's suggested_display_precision on the HA discovery
        entry, asserted separately below)."""
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._processing_time.record_hwm(2.5583304843038)
        p._publish_telemetry()
        calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        assert calls["SkyFollower/message-processor/0/statistic/processing_time_hwm_ms"] == \
            "2.5583304843038"

    def test_processing_time_ha_discovery_sets_suggested_display_precision(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_ha_autodiscovery()
        configs = {
            c.args[0]: json.loads(c.args[1])
            for c in mock_mqtt.publish.call_args_list
            if c.args[0].startswith("homeassistant/")
        }
        cfg = configs["homeassistant/sensor/SkyFollower_message_processor_0_processing_time_hwm_ms/config"]
        assert cfg["suggested_display_precision"] == 1

    def test_message_latency_hwm_publishes_full_float_precision(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._message_latency.record_hwm(340.71234567)
        p._publish_telemetry()
        calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        assert calls["SkyFollower/message-processor/0/statistic/message_latency_hwm_ms"] == \
            "340.71234567"

    def test_message_latency_ha_discovery_alongside_processing_time(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_ha_autodiscovery()
        configs = {
            c.args[0]: json.loads(c.args[1])
            for c in mock_mqtt.publish.call_args_list
            if c.args[0].startswith("homeassistant/")
        }
        cfg = configs["homeassistant/sensor/SkyFollower_message_processor_0_message_latency_hwm_ms/config"]
        assert cfg["unit_of_measurement"] == "ms"
        assert cfg["suggested_display_precision"] == 1
        assert cfg["state_topic"] == "SkyFollower/message-processor/0/statistic/message_latency_hwm_ms"

    def test_rules_engine_hwm_ha_discovery_uses_ns_unit_and_precision(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_ha_autodiscovery()
        configs = {
            c.args[0]: json.loads(c.args[1])
            for c in mock_mqtt.publish.call_args_list
            if c.args[0].startswith("homeassistant/")
        }
        cfg = configs["homeassistant/sensor/SkyFollower_message_processor_0_rules_engine_hwm_ns/config"]
        assert cfg["unit_of_measurement"] == "ns"
        assert cfg["suggested_display_precision"] == 0
        assert cfg["state_topic"] == "SkyFollower/message-processor/0/statistic/rules_engine_hwm_ns"
        assert "homeassistant/sensor/SkyFollower_message_processor_0_rules_engine_hwm_ms/config" \
            not in configs

    def test_rmq_queue_depth_hwm_ha_discovery_sets_expire_after(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_ha_autodiscovery()
        configs = {
            c.args[0]: json.loads(c.args[1])
            for c in mock_mqtt.publish.call_args_list
            if c.args[0].startswith("homeassistant/")
        }
        cfg = configs["homeassistant/sensor/SkyFollower_message_processor_0_rabbitmq_input_queue_depth_hwm/config"]
        # telemetry_interval_seconds=30 (see _minimal_config) x 3.
        assert cfg["expire_after"] == 90

    def test_rmq_queue_depth_hwm_publishes_recorded_max_then_resets(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._rmq_queue_depth_hwm.record(3)
        p._rmq_queue_depth_hwm.record(15)
        p._rmq_queue_depth_hwm.record(8)
        topic = "SkyFollower/message-processor/0/statistic/rabbitmq_input_queue_depth_hwm"

        p._publish_telemetry()
        first_calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        assert first_calls[topic] == "15"

        # Second tick: nothing recorded since the reset above, so
        # value_and_reset() is back to -1 ("no valid sample landed this
        # window") -- must never be published as -1 on the wire (#981);
        # the retained topic simply keeps its last known-good value ("15"
        # from the first tick) by not being touched at all this cycle.
        mock_mqtt.reset_mock()
        p._publish_telemetry()
        second_topics = {c.args[0] for c in mock_mqtt.publish.call_args_list}
        assert topic not in second_topics

    def test_rmq_queue_depth_hwm_never_publishes_negative_one(self):
        """Regression test (#981): feed _DepthHWM only error samples
        across a window, assert value_and_reset() still internally
        returns -1 (tracker behavior unchanged) and that
        _publish_telemetry() does not call mqtt.publish for this specific
        topic on that tick."""
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._rmq_queue_depth_hwm.record(-1)
        p._rmq_queue_depth_hwm.record(-1)
        assert p._rmq_queue_depth_hwm.value_and_reset() == -1  # unchanged tracker behavior

        # value_and_reset() above already consumed/reset the tracker back
        # to -1 -- record the same error-only pattern again for the
        # publish-time assertion below.
        p._rmq_queue_depth_hwm.record(-1)
        topic = "SkyFollower/message-processor/0/statistic/rabbitmq_input_queue_depth_hwm"

        p._publish_telemetry()

        published_topics = {c.args[0] for c in mock_mqtt.publish.call_args_list}
        assert topic not in published_topics
        # Every other statistic still publishes normally this cycle --
        # only this one topic is skipped.
        assert "SkyFollower/message-processor/0/statistic/active_flights" in published_topics

    def test_no_publish_when_mqtt_not_connected(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = False
        p._publish_telemetry()
        mock_mqtt.publish.assert_not_called()

    def test_ha_autodiscovery_uses_direct_state_topic(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_ha_autodiscovery()
        base = "SkyFollower/message-processor/0/statistic/"
        for call in mock_mqtt.publish.call_args_list:
            if call.args[0].startswith("homeassistant/"):
                cfg = json.loads(call.args[1])
                assert "value_template" not in cfg
                assert cfg["state_topic"].startswith(base)

    def test_ha_autodiscovery_no_avg_processing_time(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_ha_autodiscovery()
        for call in mock_mqtt.publish.call_args_list:
            assert "avg_processing_time_ms" not in call[0][0]


# ---------------------------------------------------------------------------
# Telemetry -- active_flights COUNT(*) must not hold self._db_lock
#
# self._db_lock also guards every _update_flight() call on the main
# consuming thread (see _process()), so a slow COUNT(*) held under that
# same lock stalls per-message processing for the query's full duration.
# WAL mode gives the read snapshot isolation without the lock, so
# _publish_telemetry() must not acquire self._db_lock around this query.
# ---------------------------------------------------------------------------

class TestTelemetryActiveFlightsNoLock:
    def _make_processor(self) -> MessageProcessor:
        with patch("message_processor.main.DATA_DIR", tempfile.mkdtemp()), \
             patch("message_processor.main.redis_lib.Redis"):
            p = MessageProcessor(_minimal_config(), message_processor_id="0")
        return p

    def test_active_flights_query_never_acquires_db_lock(self):
        """Direct check of the acceptance criterion: swap in a spy lock and
        confirm _publish_telemetry() never touches it at all."""
        p = self._make_processor()
        p._mqtt = MagicMock()
        p._mqtt_connected = True
        spy_lock = MagicMock(wraps=p._db_lock)
        p._db_lock = spy_lock

        p._publish_telemetry()

        spy_lock.acquire.assert_not_called()
        spy_lock.__enter__.assert_not_called()

    def test_message_thread_not_stalled_by_slow_active_flights_query(self):
        """Soak-style regression test for the reported symptom: with a
        deliberately slow COUNT(*) in flight on the telemetry thread (stood
        in for a large `flights` table so the test is fast and
        deterministic rather than depending on actually inserting enough
        rows to measure), the main thread must still be able to acquire
        self._db_lock -- exactly what _update_flight() does for every
        message -- essentially immediately instead of waiting on the
        query."""
        p = self._make_processor()
        p._mqtt = MagicMock()
        p._mqtt_connected = True

        query_started = threading.Event()
        release_query = threading.Event()

        # sqlite3.Connection/Cursor are immutable C types -- neither one's
        # methods can be patched in place -- so stand a thin proxy pair in
        # front of the real connection/cursor instead, swapped onto
        # self._db only for the telemetry thread's duration, that stalls
        # specifically on the active_flights query.
        class _SlowCountCursor:
            def __init__(self, real_cursor):
                self._real = real_cursor

            def execute(self, sql, *args, **kwargs):
                if sql.strip() == "SELECT COUNT(*) FROM flights":
                    query_started.set()
                    release_query.wait(timeout=5)
                self._real.execute(sql, *args, **kwargs)
                return self

            def __getattr__(self, name):
                return getattr(self._real, name)

        class _SlowCountConnection:
            def __init__(self, real_db):
                self._real = real_db

            def cursor(self):
                return _SlowCountCursor(self._real.cursor())

            def __getattr__(self, name):
                return getattr(self._real, name)

        real_db = p._db

        def run_telemetry():
            p._db = _SlowCountConnection(real_db)
            try:
                p._publish_telemetry()
            finally:
                p._db = real_db

        telemetry_thread = threading.Thread(target=run_telemetry)
        telemetry_thread.start()
        try:
            assert query_started.wait(timeout=2), "COUNT(*) never started"

            # The slow COUNT(*) is now deliberately stalled mid-query on the
            # telemetry thread. Acquiring self._db_lock here simulates the
            # main thread's per-message _update_flight() call -- it must
            # not be blocked waiting on the telemetry thread's read.
            acquire_start = time.monotonic()
            with p._db_lock:
                pass
            acquire_elapsed = time.monotonic() - acquire_start
        finally:
            release_query.set()
            telemetry_thread.join(timeout=5)

        assert acquire_elapsed < 0.5, (
            f"self._db_lock acquisition took {acquire_elapsed:.3f}s while the "
            "active_flights COUNT(*) was in flight -- _publish_telemetry() "
            "must not hold self._db_lock around that query"
        )


# ---------------------------------------------------------------------------
# Crash-durable active store
# ---------------------------------------------------------------------------

class TestCrashRecovery:
    """Active store is file-backed; a process restart (crash or deliberate
    stop, handled identically) must recover it without eagerly archiving
    based on wall-clock time elapsed while the process was down."""

    def _write_active_flights_db(self, data_dir, icao_hex, last_message, flight_id="pre-crash-id"):
        path = os.path.join(data_dir, "active_flights.db")
        db = sqlite3.connect(path)
        db.executescript(_SCHEMA)
        db.execute(
            "INSERT INTO flights (icao_hex, flight_id, first_message, last_message, "
            "total_messages, aircraft, ident, operator, squawk, origin, destination, "
            "matched_rules, receiver_sources, force_archive) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (icao_hex, flight_id, last_message - 10, last_message, 5,
             "{}", "", "{}", "", None, None, '["rule_a"]', '["1090"]', 0),
        )
        db.commit()
        db.close()

    def test_recovers_flight_without_archiving(self):
        data_dir = tempfile.mkdtemp()
        # 10 minutes old — would look wall-clock-stale against a 300s TTL,
        # the exact scenario a naive wall-clock eviction check gets wrong.
        old_last_message = time.time() - 600
        self._write_active_flights_db(data_dir, "A8AE7F", old_last_message)

        p, _ = _make_processor(_minimal_config(), data_dir=data_dir)

        # message_clock floors at the recovered flight's last_message, not
        # at wall-clock "now" — see MessageProcessor.__init__.
        assert p._message_clock == pytest.approx(old_last_message)

        f = Flight(p._db)
        assert f.load("A8AE7F") is True
        assert f.matched_rules == ["rule_a"]
        assert f.flight_id == "pre-crash-id"

        # A periodic eviction sweep right after startup must not archive it
        # — message_clock hasn't advanced past its TTL window yet.
        p._evict_stale()
        f2 = Flight(p._db)
        assert f2.load("A8AE7F") is True

    def test_empty_store_uses_wall_clock(self):
        cfg = _minimal_config()
        p, _ = _make_processor(cfg)
        assert p._message_clock == pytest.approx(time.time(), abs=5)


# ---------------------------------------------------------------------------
# Per-message gap check
# ---------------------------------------------------------------------------

class TestPerMessageGapCheck:
    def test_gap_beyond_ttl_archives_old_and_starts_new(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None  # aircraft enrichment miss

        old_time = 1_700_000_000.0
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "old-flight-id"
        f.first_message = old_time - 100
        f.last_message = old_time
        f.total_messages = 5
        f.matched_rules = ["rule_a"]
        f.receiver_sources = ["1090"]
        f.save()

        ttl = p._flight_ttl_seconds
        new_time = old_time + ttl + 50  # gap exceeds ttl

        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=new_time, source="1090")
        data = {"icao_hex": "A8AE7F"}

        with p._db_lock:
            p._update_flight(data, msg)

        # Old flight landed in the local fallback (no RabbitMQ connected in
        # this test — MessageProcessor was never start()ed).
        assert p._fallback.depth() == 1

        # A fresh row now exists for the same icao_hex, not an extension of
        # the old one.
        f2 = Flight(p._db)
        assert f2.load("A8AE7F") is True
        assert f2.flight_id != "old-flight-id"
        assert f2.matched_rules == []
        assert f2.total_messages == 1
        assert f2.first_message == new_time

    def test_gap_within_ttl_extends_existing_flight(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None

        old_time = 1_700_000_000.0
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "same-flight-id"
        f.first_message = old_time - 100
        f.last_message = old_time
        f.total_messages = 5
        f.receiver_sources = ["1090"]
        f.save()

        new_time = old_time + 10  # well within the default 300s ttl
        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=new_time, source="1090")
        data = {"icao_hex": "A8AE7F"}

        with p._db_lock:
            p._update_flight(data, msg)

        assert p._fallback.depth() == 0
        f2 = Flight(p._db)
        assert f2.load("A8AE7F") is True
        assert f2.flight_id == "same-flight-id"
        assert f2.total_messages == 6


# ---------------------------------------------------------------------------
# receiver_sources accumulation + force_archive
# ---------------------------------------------------------------------------

class TestReceiverSourcesAccumulation:
    def test_accumulates_across_messages_with_different_sources(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None

        msg1 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="EXTERNAL")
        msg2 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=2.0, source="1090")

        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg1)
            p._update_flight({"icao_hex": "A8AE7F"}, msg2)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.receiver_sources == ["EXTERNAL", "1090"]

    def test_dedupes_repeated_same_source(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None

        msg1 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="1090")
        msg2 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=2.0, source="1090")

        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg1)
            p._update_flight({"icao_hex": "A8AE7F"}, msg2)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.receiver_sources == ["1090"]

    def test_not_only_set_on_first_message(self):
        """Regression guard: source used to be set only inside the
        `if not exists:` branch, so a flight created on EXTERNAL that later
        got picked up on 1090 never reflected the second source."""
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None

        msg1 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="EXTERNAL")
        p_db_lock_msg2 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=2.0, source="978")

        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg1)
        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.receiver_sources == ["EXTERNAL"]

        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, p_db_lock_msg2)
        f2 = Flight(p._db)
        f2.load("A8AE7F")
        assert f2.receiver_sources == ["EXTERNAL", "978"]


class TestConfirmAfterRepeatedSightings:
    """Pure-function tests for the #900 debounce helper, independent of
    Flight/SQLite -- TestSquawkConfirmation/TestIdentConfirmation below
    cover its integration into _update_flight."""

    def test_first_sighting_not_confirmed(self):
        pending, confirmed = _confirm_after_repeated_sightings(None, "7700", 100.0)
        assert confirmed is False
        assert pending == {"value": "7700", "sightings": [100.0]}

    def test_confirms_on_the_configured_count(self):
        pending, confirmed = None, False
        for i in range(_PARITY_ERROR_CONFIRM_COUNT):
            pending, confirmed = _confirm_after_repeated_sightings(pending, "7700", 100.0 + i)
        assert confirmed is True

    def test_not_confirmed_one_short_of_the_count(self):
        pending, confirmed = None, False
        for i in range(_PARITY_ERROR_CONFIRM_COUNT - 1):
            pending, confirmed = _confirm_after_repeated_sightings(pending, "7700", 100.0 + i)
        assert confirmed is False
        assert len(pending["sightings"]) == _PARITY_ERROR_CONFIRM_COUNT - 1

    def test_sightings_outside_window_are_pruned(self):
        pending = {"value": "7700", "sightings": [0.0, 1.0, 2.0, 3.0]}
        new_received_at = _PARITY_ERROR_CONFIRM_WINDOW_SECONDS + 10
        pending, confirmed = _confirm_after_repeated_sightings(
            pending, "7700", new_received_at,
        )
        # All 4 prior sightings are now outside the trailing window --
        # only this new one remains, nowhere near the confirm threshold.
        assert pending["sightings"] == [new_received_at]
        assert confirmed is False

    def test_differing_value_resets_the_candidate(self):
        # Mirrors real usage: flight.pending_squawk/pending_ident is a
        # single slot, reassigned on every call -- a differing value
        # discards the previous candidate's progress rather than tracking
        # multiple candidates in parallel (range-boundary noise producing
        # two different fabricated values in a row still shouldn't confirm
        # either one quickly).
        pending, _ = _confirm_after_repeated_sightings(None, "7700", 0.0)
        pending, _ = _confirm_after_repeated_sightings(pending, "7700", 1.0)
        pending, confirmed = _confirm_after_repeated_sightings(pending, "7600", 2.0)
        assert pending == {"value": "7600", "sightings": [2.0]}
        assert confirmed is False


class TestSquawkConfirmation:
    """_update_flight's #900 two-tier squawk trust: verified source or an
    ordinary value trusts immediately; a reserved code from an unverified
    source needs _PARITY_ERROR_CONFIRM_COUNT sightings of the same value
    within _PARITY_ERROR_CONFIRM_WINDOW_SECONDS."""

    def test_ordinary_squawk_from_unverified_source_trusts_immediately(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None
        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="1090")

        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F", "squawk": "1200", "verified": False}, msg)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.squawk == "1200"
        assert f.pending_squawk is None

    def test_reserved_squawk_from_verified_source_trusts_immediately(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None
        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="1090")

        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F", "squawk": "7700", "verified": True}, msg)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.squawk == "7700"

    def test_reserved_squawk_from_unverified_source_requires_confirmation(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None

        for i in range(_PARITY_ERROR_CONFIRM_COUNT - 1):
            msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=float(i), source="1090")
            with p._db_lock:
                p._update_flight({"icao_hex": "A8AE7F", "squawk": "7700", "verified": False}, msg)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.squawk == ""
        assert f.pending_squawk["value"] == "7700"
        assert len(f.pending_squawk["sightings"]) == _PARITY_ERROR_CONFIRM_COUNT - 1

        msg = InboundMessage(
            raw="00" * 14, icao_hex="A8AE7F",
            received_at=float(_PARITY_ERROR_CONFIRM_COUNT - 1), source="1090",
        )
        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F", "squawk": "7700", "verified": False}, msg)

        f2 = Flight(p._db)
        f2.load("A8AE7F")
        assert f2.squawk == "7700"
        assert f2.pending_squawk is None

    def test_sightings_outside_window_never_confirm(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None

        # Same reserved value, but each sighting is spaced further apart
        # than the confirmation window -- every call effectively restarts
        # the count at 1.
        for i in range(_PARITY_ERROR_CONFIRM_COUNT + 5):
            t = i * (_PARITY_ERROR_CONFIRM_WINDOW_SECONDS + 1)
            msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=float(t), source="1090")
            with p._db_lock:
                p._update_flight({"icao_hex": "A8AE7F", "squawk": "7700", "verified": False}, msg)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.squawk == ""

    def test_alternating_reserved_values_never_confirm(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None
        values = ["7700", "7600"] * _PARITY_ERROR_CONFIRM_COUNT

        for i, value in enumerate(values):
            msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=float(i), source="1090")
            with p._db_lock:
                p._update_flight({"icao_hex": "A8AE7F", "squawk": value, "verified": False}, msg)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.squawk == ""

    def test_pending_state_does_not_block_a_later_ordinary_squawk(self):
        # An in-progress (unconfirmed) reserved-squawk candidate must not
        # itself count as "already set" -- flight.squawk is still "",
        # so a later ordinary reading should still commit immediately.
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None
        msg1 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="1090")
        msg2 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=2.0, source="1090")

        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F", "squawk": "7700", "verified": False}, msg1)
            p._update_flight({"icao_hex": "A8AE7F", "squawk": "2646", "verified": False}, msg2)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.squawk == "2646"


class TestIdentConfirmation:
    """_update_flight's #900 ident extension: verified source (DF17/18)
    trusts immediately; unverified source (DF20/21 Comm-B BDS 2,0) needs
    the same confirmation treatment as reserved squawks, but for every
    value -- there's no "safe" ident subset the way ordinary squawks are."""

    def test_verified_ident_trusts_immediately(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None
        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="1090")

        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F", "ident": "DAL123", "verified": True}, msg)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.ident == "DAL123"
        assert f.pending_ident is None

    def test_unverified_ident_requires_confirmation(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None

        for i in range(_PARITY_ERROR_CONFIRM_COUNT - 1):
            msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=float(i), source="1090")
            with p._db_lock:
                p._update_flight({"icao_hex": "A8AE7F", "ident": "N30GD", "verified": False}, msg)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.ident == ""
        assert f.pending_ident["value"] == "N30GD"

        msg = InboundMessage(
            raw="00" * 14, icao_hex="A8AE7F",
            received_at=float(_PARITY_ERROR_CONFIRM_COUNT - 1), source="1090",
        )
        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F", "ident": "N30GD", "verified": False}, msg)

        f2 = Flight(p._db)
        f2.load("A8AE7F")
        assert f2.ident == "N30GD"
        assert f2.pending_ident is None

    def test_alternating_unverified_idents_never_confirm(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None
        values = ["N30GD", "ABCDEF"] * _PARITY_ERROR_CONFIRM_COUNT

        for i, value in enumerate(values):
            msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=float(i), source="1090")
            with p._db_lock:
                p._update_flight({"icao_hex": "A8AE7F", "ident": value, "verified": False}, msg)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.ident == ""


class TestForceArchiveFromRules:
    def test_set_when_matched_rule_has_force_archive(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None
        p._rules_engine.evaluate.return_value = [
            {"identifier": "rule1", "name": "", "description": "", "force_archive": True, "conditions": []},
        ]

        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="EXTERNAL")
        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.force_archive is True
        assert f.matched_rules == ["rule1"]

    def test_not_set_when_matched_rule_lacks_force_archive(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None
        p._rules_engine.evaluate.return_value = [
            {"identifier": "rule1", "name": "", "description": "", "force_archive": False, "conditions": []},
        ]

        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="EXTERNAL")
        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.force_archive is False

    def test_sticky_once_set_stays_set(self):
        """A later message with no matching rules must not clear a
        force_archive already set by an earlier match — only one persist-
        worthy match ever needs to have happened for the flight."""
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None

        p._rules_engine.evaluate.return_value = [
            {"identifier": "rule1", "name": "", "description": "", "force_archive": True, "conditions": []},
        ]
        msg1 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="EXTERNAL")
        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg1)

        # Second message: rule already in matched_rules, so evaluate()
        # would naturally skip it in the real engine — simulate that here.
        p._rules_engine.evaluate.return_value = []
        msg2 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=2.0, source="EXTERNAL")
        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg2)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.force_archive is True


# ---------------------------------------------------------------------------
# Flight ID assigned at creation, not archive time
# ---------------------------------------------------------------------------

class TestFlightIdStability:
    def test_persists_across_save_and_reload(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "abc-123"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()

        f2 = Flight(db)
        f2.load("A8AE7F")
        assert f2.flight_id == "abc-123"

    def test_to_completed_flight_reuses_flight_id(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "abc-123"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()

        # A duplicate archive attempt (e.g. a crash between the archive
        # commit and the active-store delete) reuses the same _id, landing
        # as an idempotent overwrite of the same S3 object rather than a
        # duplicate record.
        cf1 = f.to_completed_flight()
        cf2 = f.to_completed_flight()
        assert cf1.id == "abc-123"
        assert cf2.id == "abc-123"


# ---------------------------------------------------------------------------
# message_clock gates eviction, not wall-clock time
# ---------------------------------------------------------------------------

class TestMessageClockGatesEviction:
    def test_does_not_evict_ahead_of_message_clock(self):
        p, _ = _make_processor()
        ttl = p._flight_ttl_seconds

        last_message = time.time() - 600  # 10 minutes old by wall-clock

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "fid-1"
        f.first_message = last_message - 10
        f.last_message = last_message
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()

        # Backlog replay has only reached 100s past this flight's last
        # message so far — well within the ttl window, even though real
        # wall-clock time has moved on much further.
        p._message_clock = last_message + 100
        p._evict_stale()
        f2 = Flight(p._db)
        assert f2.load("A8AE7F") is True  # not evicted

        # Once message_clock actually catches up past the ttl window, the
        # normal eviction outcome applies.
        p._message_clock = last_message + ttl + 1
        p._evict_stale()
        f3 = Flight(p._db)
        assert f3.load("A8AE7F") is False  # now evicted


# ---------------------------------------------------------------------------
# MQTT rule-notification flood guard
# ---------------------------------------------------------------------------

class TestMqttLagGuard:
    def _make_flight(self, p) -> Flight:
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "fid-1"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()
        return f

    def test_suppresses_backlogged_message_notification(self, caplog):
        p, _ = _make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        f = self._make_flight(p)

        old_received_at = time.time() - 3600  # an hour old — backlog replay
        with caplog.at_level(logging.DEBUG, logger="message_processor"):
            p._publish_rule_notification(f, {"identifier": "rule_a"}, old_received_at)

        mock_mqtt.publish.assert_not_called()
        assert "rule_a" in caplog.text
        assert "A8AE7F" in caplog.text

    def test_publishes_recent_message_notification(self):
        p, _ = _make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        f = self._make_flight(p)

        recent_received_at = time.time() - 1
        p._publish_rule_notification(f, {"identifier": "rule_a"}, recent_received_at)
        mock_mqtt.publish.assert_called_once()


# ---------------------------------------------------------------------------
# flight_ttl_seconds: shared Redis config
# ---------------------------------------------------------------------------

class TestFlightTtlLoad:
    def test_defaults_to_300_when_unset(self):
        p, mock_redis = _make_processor()
        mock_redis.get.return_value = None
        p._load_flight_ttl_seconds()
        assert p._flight_ttl_seconds == 300

    def test_loads_from_redis_value(self):
        p, mock_redis = _make_processor()
        mock_redis.get.return_value = "600"
        p._load_flight_ttl_seconds()
        assert p._flight_ttl_seconds == 600

    def test_keeps_default_on_redis_error(self):
        p, mock_redis = _make_processor()
        mock_redis.get.side_effect = ConnectionError("redis down")
        p._load_flight_ttl_seconds()
        assert p._flight_ttl_seconds == 300

    def test_gap_check_uses_loaded_value_not_config(self):
        """The per-message gap check must read the cached attribute —
        config no longer carries flight_ttl_seconds at all."""
        p, mock_redis = _make_processor()
        mock_redis.get.return_value = "10"
        mock_redis.evalsha.return_value = None
        p._load_flight_ttl_seconds()
        assert "flight_ttl_seconds" not in p._cfg

        old_time = 1_700_000_000.0
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "old-flight-id"
        f.first_message = old_time
        f.last_message = old_time
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()

        # Gap of 20s exceeds the refreshed 10s ttl, so this should split.
        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=old_time + 20, source="1090")
        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg)

        f2 = Flight(p._db)
        f2.load("A8AE7F")
        assert f2.flight_id != "old-flight-id"
