"""
Tests for archive-processor/main.py components that don't require live
infrastructure.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest

# Make sure the archive-processor package is importable when running from the
# repo root.
_HERE = os.path.dirname(os.path.abspath(__file__))
_ARCHIVE_PROCESSOR_DIR = os.path.dirname(_HERE)
_REPO_ROOT = os.path.dirname(_ARCHIVE_PROCESSOR_DIR)
for _p in (_REPO_ROOT, _ARCHIVE_PROCESSOR_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import gzip

from archive_processor.main import (  # noqa: E402  (after sys.path manipulation)
    ArchiveProcessor,
    _S3FallbackQueue,
    _interpolate_altitudes,
    _merge_segments,
    _normalize_timestamps,
    build_geojson_feature,
    build_s3_key,
)
from shared.models import CompletedFlight
from shared.redis_keys import archive_last_segment_key


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_flight(**overrides) -> CompletedFlight:
    """Return a minimal CompletedFlight suitable for unit tests."""
    defaults = {
        "_id": "018f1234-5678-7abc-def0-123456789abc",
        "first_message": datetime(2024, 5, 31, 12, 0, 0, tzinfo=timezone.utc),
        "last_message": datetime(2024, 5, 31, 13, 0, 0, tzinfo=timezone.utc),
        "total_messages": 100,
        "source": "1090",
        "aircraft": {"icao_hex": "A8AE7F", "registration": "N659DL"},
        "ident": "DAL659",
        "positions": [
            {"timestamp": datetime(2024, 5, 31, 12, 0, 0, tzinfo=timezone.utc),
             "latitude": 33.6367, "longitude": -84.4281, "altitude": 0},
            {"timestamp": datetime(2024, 5, 31, 12, 30, 0, tzinfo=timezone.utc),
             "latitude": 36.0, "longitude": -87.0, "altitude": 35000},
            {"timestamp": datetime(2024, 5, 31, 13, 0, 0, tzinfo=timezone.utc),
             "latitude": 33.9425, "longitude": -118.4081, "altitude": 0},
        ],
        "velocities": [],
    }
    defaults.update(overrides)
    return CompletedFlight(**defaults)


def _make_processor(tmp_dir: str, flight_ttl_seconds: int = 300):
    """Build an ArchiveProcessor with mocked Redis and boto3."""
    config = {
        "s3": {"region": "us-east-1", "bucket": "test-bucket",
               "access_key_id": "x", "secret_access_key": "x"},
        "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        "redis": {"host": "localhost"},
        "mqtt": None,
        "flight_ttl_seconds": flight_ttl_seconds,
        "telemetry_interval_seconds": 30,
        "data_dir": tmp_dir,
    }

    with patch("archive_processor.main.redis_lib.Redis") as MockRedis, \
         patch("archive_processor.main.boto3.Session"):
        mock_redis = MagicMock()
        MockRedis.return_value = mock_redis
        processor = ArchiveProcessor(config)
        processor._redis = mock_redis
        processor._s3_connected = True
        return processor, mock_redis


class _FakeS3:
    """In-memory stand-in for the pieces of the boto3 S3 client this module
    uses (put_object / get_object), so stitching tests can round-trip a
    "previously archived" object without touching real S3."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(f"no such key: {Key}")
        body = MagicMock()
        body.read.return_value = self.objects[Key]
        return {"Body": body}

    def read_json(self, key: str) -> dict:
        return json.loads(gzip.decompress(self.objects[key]))


# ---------------------------------------------------------------------------
# S3 key generation
# ---------------------------------------------------------------------------

class TestBuildS3Key:
    def test_basic_structure(self):
        flight = _make_flight()
        key = build_s3_key(flight)
        # Should be: flights/2024/05/31/A8AE7F_DAL659_{uuid}.json.gz
        assert key.startswith("flights/2024/05/31/")
        assert "A8AE7F_DAL659_" in key
        assert key.endswith(".json.gz")

    def test_non_alphanumeric_ident_stripped(self):
        flight = _make_flight(ident="UAL-123/A")
        key = build_s3_key(flight)
        assert "UAL123A_" in key or "_UAL123A" in key or "UAL123A" in key
        assert "-" not in key.split("/")[-1].split("_")[1]
        assert "/" not in key.split("/")[-1]

    def test_none_ident_becomes_unknown(self):
        flight = _make_flight(ident=None)
        key = build_s3_key(flight)
        assert "_unknown_" in key

    def test_unknown_icao_hex_when_missing(self):
        flight = _make_flight(aircraft={})
        key = build_s3_key(flight)
        assert key.startswith("flights/2024/05/31/unknown_")

    def test_uuid_in_key(self):
        flight = _make_flight()
        key = build_s3_key(flight)
        assert flight.id in key

    def test_date_from_last_message_utc(self):
        # last_message at 2023-12-01T23:59:00Z
        flight = _make_flight(
            last_message=datetime(2023, 12, 1, 23, 59, 0, tzinfo=timezone.utc)
        )
        key = build_s3_key(flight)
        assert key.startswith("flights/2023/12/01/")


# ---------------------------------------------------------------------------
# Altitude interpolation
# ---------------------------------------------------------------------------

class TestInterpolateAltitudes:
    def test_no_nones_unchanged(self):
        positions = [
            {"altitude": 1000},
            {"altitude": 2000},
            {"altitude": 3000},
        ]
        result = _interpolate_altitudes(positions)
        assert result == [1000, 2000, 3000]

    def test_single_missing_middle_interpolated(self):
        positions = [
            {"altitude": 0},
            {"altitude": None},
            {"altitude": 4000},
        ]
        result = _interpolate_altitudes(positions)
        assert result[1] == 2000

    def test_multiple_missing_middle_interpolated(self):
        positions = [
            {"altitude": 0},
            {"altitude": None},
            {"altitude": None},
            {"altitude": 3000},
        ]
        result = _interpolate_altitudes(positions)
        assert result[1] == 1000
        assert result[2] == 2000

    def test_missing_at_start_stays_none(self):
        # No preceding known altitude — can't interpolate
        positions = [
            {"altitude": None},
            {"altitude": 5000},
        ]
        result = _interpolate_altitudes(positions)
        assert result[0] is None
        assert result[1] == 5000

    def test_missing_at_end_stays_none(self):
        positions = [
            {"altitude": 5000},
            {"altitude": None},
        ]
        result = _interpolate_altitudes(positions)
        assert result[0] == 5000
        assert result[1] is None

    def test_all_none_stays_none(self):
        positions = [{"altitude": None}, {"altitude": None}]
        result = _interpolate_altitudes(positions)
        assert result == [None, None]

    def test_rounding(self):
        # 0 -> None -> 3 should give 1 or 2 (rounded)
        positions = [
            {"altitude": 0},
            {"altitude": None},
            {"altitude": 3},
        ]
        result = _interpolate_altitudes(positions)
        assert isinstance(result[1], int)


# ---------------------------------------------------------------------------
# GeoJSON builder
# ---------------------------------------------------------------------------

class TestBuildGeoJsonFeature:
    def test_returns_none_for_zero_positions(self):
        flight = _make_flight(positions=[])
        assert build_geojson_feature(flight) is None

    def test_returns_none_for_one_position(self):
        flight = _make_flight(positions=[
            {"timestamp": datetime(2024, 5, 31, 12, 0, 0, tzinfo=timezone.utc),
             "latitude": 33.0, "longitude": -84.0, "altitude": 1000},
        ])
        assert build_geojson_feature(flight) is None

    def test_valid_feature_structure(self):
        flight = _make_flight()
        feature = build_geojson_feature(flight)
        assert feature is not None
        assert feature["type"] == "Feature"
        assert feature["geometry"]["type"] == "LineString"
        assert "coordinates" in feature["geometry"]
        assert feature["properties"] == {}

    def test_coordinates_have_correct_lon_lat_order(self):
        flight = _make_flight()
        feature = build_geojson_feature(flight)
        coords = feature["geometry"]["coordinates"]
        # GeoJSON: [longitude, latitude, altitude]
        # First position: lat=33.6367, lon=-84.4281
        assert coords[0][0] == pytest.approx(-84.4281)
        assert coords[0][1] == pytest.approx(33.6367)

    def test_3d_coordinates_when_altitude_present(self):
        flight = _make_flight()
        feature = build_geojson_feature(flight)
        coords = feature["geometry"]["coordinates"]
        # All positions have altitude so all coords are 3D
        for c in coords:
            assert len(c) == 3

    def test_2d_coordinates_when_altitude_none_and_uninterpolatable(self):
        # Two positions, both without altitude and no surrounding known alt
        flight = _make_flight(positions=[
            {"timestamp": datetime(2024, 5, 31, 12, 0, 0, tzinfo=timezone.utc),
             "latitude": 33.0, "longitude": -84.0, "altitude": None},
            {"timestamp": datetime(2024, 5, 31, 12, 30, 0, tzinfo=timezone.utc),
             "latitude": 34.0, "longitude": -85.0, "altitude": None},
        ])
        feature = build_geojson_feature(flight)
        coords = feature["geometry"]["coordinates"]
        for c in coords:
            assert len(c) == 2

    def test_mixed_altitude_interpolated(self):
        # Middle position altitude=None, should be interpolated
        flight = _make_flight(positions=[
            {"timestamp": datetime(2024, 5, 31, 12, 0, 0, tzinfo=timezone.utc),
             "latitude": 33.0, "longitude": -84.0, "altitude": 0},
            {"timestamp": datetime(2024, 5, 31, 12, 30, 0, tzinfo=timezone.utc),
             "latitude": 34.0, "longitude": -85.0, "altitude": None},
            {"timestamp": datetime(2024, 5, 31, 13, 0, 0, tzinfo=timezone.utc),
             "latitude": 35.0, "longitude": -86.0, "altitude": 4000},
        ])
        feature = build_geojson_feature(flight)
        coords = feature["geometry"]["coordinates"]
        # Middle coord should be 3D with interpolated altitude
        assert len(coords[1]) == 3
        assert coords[1][2] == 2000


# ---------------------------------------------------------------------------
# SQLite fallback queue
# ---------------------------------------------------------------------------

class TestS3FallbackQueue:
    def test_put_increases_depth(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            q = _S3FallbackQueue(tmp.name)
            assert q.depth() == 0
            q.put('{"test": 1}')
            assert q.depth() == 1
            q.put('{"test": 2}')
            assert q.depth() == 2

    def test_drain_oldest_first(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            q = _S3FallbackQueue(tmp.name)
            q.put("first")
            q.put("second")
            q.put("third")
            received = []
            q.drain(received.append)
            assert received == ["first", "second", "third"]
            assert q.depth() == 0

    def test_drain_stops_on_exception(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            q = _S3FallbackQueue(tmp.name)
            q.put("item1")
            q.put("item2")

            def fail(_):
                raise ConnectionError("S3 gone")

            q.drain(fail)
            assert q.depth() == 2  # nothing removed

    def test_survives_reopen(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            path = tmp.name
        q = _S3FallbackQueue(path)
        q.put('{"persist": true}')
        del q
        q2 = _S3FallbackQueue(path)
        assert q2.depth() == 1
        os.unlink(path)

    def test_partial_drain_leaves_rest(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            q = _S3FallbackQueue(tmp.name)
            q.put("a")
            q.put("b")
            q.put("c")

            call_count = [0]

            def process_one(payload):
                call_count[0] += 1
                if call_count[0] >= 2:
                    raise RuntimeError("stop")

            q.drain(process_one)
            assert q.depth() == 2  # only "a" was processed


# ---------------------------------------------------------------------------
# Redis counter increments
# ---------------------------------------------------------------------------

class TestRedisCounterIncrements:
    def _make_processor(self, tmp_dir: str):
        """Build an ArchiveProcessor with a mocked Redis client."""
        # Import here so sys.path is already configured
        from archive_processor.main import ArchiveProcessor

        config = {
            "s3": {"region": "us-east-1", "bucket": "test-bucket",
                   "access_key_id": "x", "secret_access_key": "x"},
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "redis": {"host": "localhost"},
            "mqtt": None,
            "telemetry_interval_seconds": 30,
            "data_dir": tmp_dir,
        }

        with patch("archive_processor.main.redis_lib.Redis") as MockRedis, \
             patch("archive_processor.main.boto3.Session"):
            mock_redis = MagicMock()
            MockRedis.return_value = mock_redis
            processor = ArchiveProcessor(config)
            processor._redis = mock_redis
            return processor, mock_redis

    def test_redis_incr_called_after_successful_write(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)

            flight = _make_flight()

            # Mock out S3 write and Parquet index
            with patch.object(processor, "_write_to_s3") as mock_s3, \
                 patch("archive_processor.main.append_to_parquet_index"):
                processor._s3_connected = True
                processor._post_write_success(flight, "flights/2024/05/31/key.json.gz")

            from shared.redis_keys import metrics_flights_archived_key
            mock_redis.incr.assert_any_call(metrics_flights_archived_key("hour"))
            mock_redis.incr.assert_any_call(metrics_flights_archived_key("today"))
            assert mock_redis.incr.call_count >= 2

    def test_redis_not_incremented_on_s3_unavailable(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)
            processor._s3_connected = False

            flight = _make_flight()
            processor._process_flight(flight)

            mock_redis.incr.assert_not_called()
            assert processor._fallback.depth() == 1


# ---------------------------------------------------------------------------
# Split-flight stitching
# ---------------------------------------------------------------------------

class TestNormalizeTimestamps:
    def test_string_timestamps_parsed(self):
        items = [{"timestamp": "2024-05-31 12:00:00+00:00", "latitude": 1.0}]
        result = _normalize_timestamps(items)
        assert isinstance(result[0]["timestamp"], datetime)

    def test_isoformat_with_t_separator_parsed(self):
        items = [{"timestamp": "2024-05-31T12:00:00+00:00", "latitude": 1.0}]
        result = _normalize_timestamps(items)
        assert result[0]["timestamp"] == datetime(2024, 5, 31, 12, 0, 0, tzinfo=timezone.utc)

    def test_non_string_timestamp_left_alone(self):
        dt = datetime(2024, 5, 31, 12, 0, 0, tzinfo=timezone.utc)
        items = [{"timestamp": dt, "latitude": 1.0}]
        result = _normalize_timestamps(items)
        assert result[0]["timestamp"] is dt

    def test_does_not_mutate_input(self):
        items = [{"timestamp": "2024-05-31 12:00:00+00:00"}]
        _normalize_timestamps(items)
        assert isinstance(items[0]["timestamp"], str)


class TestMergeSegments:
    def _prev_dict(self, **overrides) -> dict:
        defaults = {
            "_id": "prev-uuid",
            "first_message": "2024-05-31 12:00:00+00:00",
            "last_message": "2024-05-31 12:30:00+00:00",
            "total_messages": 50,
            "matched_rules": ["rule_a"],
            "positions": [
                {"timestamp": "2024-05-31 12:00:00+00:00",
                 "latitude": 33.0, "longitude": -84.0, "altitude": 1000},
            ],
            "velocities": [],
        }
        defaults.update(overrides)
        return defaults

    def test_uses_original_id_and_first_message(self):
        new = _make_flight(_id="new-uuid")
        merged = _merge_segments(new, self._prev_dict())
        assert merged.id == "prev-uuid"
        assert merged.first_message == datetime(2024, 5, 31, 12, 0, 0, tzinfo=timezone.utc)

    def test_total_messages_summed(self):
        new = _make_flight(total_messages=25)
        merged = _merge_segments(new, self._prev_dict(total_messages=50))
        assert merged.total_messages == 75

    def test_positions_merged_and_sorted(self):
        new = _make_flight()  # 3 positions spanning 12:00-13:00
        merged = _merge_segments(new, self._prev_dict())
        timestamps = [p["timestamp"] for p in merged.positions]
        assert timestamps == sorted(timestamps)
        assert len(merged.positions) == 1 + len(new.positions)

    def test_matched_rules_unioned_and_deduped(self):
        new = _make_flight(matched_rules=["rule_a", "rule_b"])
        merged = _merge_segments(new, self._prev_dict(matched_rules=["rule_a"]))
        assert merged.matched_rules == ["rule_a", "rule_b"]

    def test_last_message_comes_from_new_segment(self):
        new = _make_flight(
            last_message=datetime(2024, 5, 31, 14, 0, 0, tzinfo=timezone.utc)
        )
        merged = _merge_segments(new, self._prev_dict())
        assert merged.last_message == datetime(2024, 5, 31, 14, 0, 0, tzinfo=timezone.utc)


class TestStitching:
    def test_no_pointer_writes_normally(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            mock_redis.get.return_value = None

            flight = _make_flight()
            with patch("archive_processor.main.append_to_parquet_index"):
                processor._archive_flight_to_s3(flight)

            assert len(processor._s3_client.objects) == 1
            key = next(iter(processor._s3_client.objects))
            assert processor._s3_client.read_json(key)["_id"] == flight.id

    def test_gap_beyond_ttl_writes_normally(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir, flight_ttl_seconds=300)
            processor._s3_client = _FakeS3()

            pointer = {
                "uuid": "prev-uuid",
                "first_message": 0.0,
                "last_message": 1000.0,
                "s3_key": "flights/2024/05/31/prev.json.gz",
            }
            mock_redis.get.return_value = json.dumps(pointer)

            # first_message far beyond 1000.0 + 300s ttl
            new_first = datetime.fromtimestamp(1000.0 + 301, tz=timezone.utc)
            flight = _make_flight(_id="new-uuid", first_message=new_first)

            with patch("archive_processor.main.append_to_parquet_index"):
                processor._archive_flight_to_s3(flight)

            # Wrote a fresh object under its own key, not the previous one
            assert len(processor._s3_client.objects) == 1
            key = next(iter(processor._s3_client.objects))
            assert processor._s3_client.read_json(key)["_id"] == "new-uuid"

    def test_gap_within_ttl_stitches(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir, flight_ttl_seconds=300)
            processor._s3_client = _FakeS3()

            # Archive segment 1 for real, so there's a genuine previous
            # object to fetch and merge into.
            seg1 = _make_flight(
                _id="seg1-uuid",
                first_message=datetime.fromtimestamp(0.0, tz=timezone.utc),
                last_message=datetime.fromtimestamp(1000.0, tz=timezone.utc),
                total_messages=10,
                matched_rules=["rule_a"],
            )
            mock_redis.get.return_value = None
            with patch("archive_processor.main.append_to_parquet_index"):
                processor._archive_flight_to_s3(seg1)

            assert len(processor._s3_client.objects) == 1
            seg1_key = next(iter(processor._s3_client.objects))

            # Pointer written for segment 1
            pointer_call = mock_redis.set.call_args
            pointer = json.loads(pointer_call.args[1])
            assert pointer["s3_key"] == seg1_key
            assert pointer["uuid"] == "seg1-uuid"

            # Segment 2 starts 50s after segment 1 ended — within the 300s ttl
            mock_redis.get.return_value = json.dumps(pointer)
            seg2 = _make_flight(
                _id="seg2-uuid",
                first_message=datetime.fromtimestamp(1050.0, tz=timezone.utc),
                last_message=datetime.fromtimestamp(1200.0, tz=timezone.utc),
                total_messages=5,
                matched_rules=["rule_a", "rule_b"],
            )
            with patch("archive_processor.main.append_to_parquet_index"):
                processor._archive_flight_to_s3(seg2)

            # Still only one S3 object — segment 2 was merged into segment 1's key
            assert len(processor._s3_client.objects) == 1
            merged = processor._s3_client.read_json(seg1_key)
            assert merged["_id"] == "seg1-uuid"
            assert merged["total_messages"] == 15
            assert merged["matched_rules"] == ["rule_a", "rule_b"]

            # Pointer now points at the merged (still seg1) key with the
            # extended last_message
            pointer_call = mock_redis.set.call_args
            pointer = json.loads(pointer_call.args[1])
            assert pointer["uuid"] == "seg1-uuid"
            assert pointer["s3_key"] == seg1_key
            assert pointer["last_message"] == pytest.approx(1200.0)

    def test_chained_three_segment_stitch(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir, flight_ttl_seconds=300)
            processor._s3_client = _FakeS3()

            mock_redis.get.return_value = None
            seg1 = _make_flight(
                _id="seg1-uuid",
                first_message=datetime.fromtimestamp(0.0, tz=timezone.utc),
                last_message=datetime.fromtimestamp(100.0, tz=timezone.utc),
                total_messages=1,
                matched_rules=["rule_a"],
                positions=[{"timestamp": datetime.fromtimestamp(0.0, tz=timezone.utc),
                            "latitude": 1.0, "longitude": 1.0, "altitude": 1000}],
            )
            with patch("archive_processor.main.append_to_parquet_index"):
                processor._archive_flight_to_s3(seg1)
            seg1_key = next(iter(processor._s3_client.objects))

            for i, (start, end, rule) in enumerate(
                [(150.0, 200.0, "rule_b"), (250.0, 300.0, "rule_c")], start=2
            ):
                pointer = json.loads(mock_redis.set.call_args.args[1])
                mock_redis.get.return_value = json.dumps(pointer)
                seg = _make_flight(
                    _id=f"seg{i}-uuid",
                    first_message=datetime.fromtimestamp(start, tz=timezone.utc),
                    last_message=datetime.fromtimestamp(end, tz=timezone.utc),
                    total_messages=1,
                    matched_rules=[rule],
                    positions=[{"timestamp": datetime.fromtimestamp(start, tz=timezone.utc),
                                "latitude": 1.0, "longitude": 1.0, "altitude": 1000}],
                )
                with patch("archive_processor.main.append_to_parquet_index"):
                    processor._archive_flight_to_s3(seg)

            # All three segments stitched into the original seg1 object —
            # never more than one S3 object for this aircraft.
            assert len(processor._s3_client.objects) == 1
            merged = processor._s3_client.read_json(seg1_key)
            assert merged["_id"] == "seg1-uuid"
            assert merged["total_messages"] == 3
            assert merged["matched_rules"] == ["rule_a", "rule_b", "rule_c"]
            assert len(merged["positions"]) == 3  # one per segment
