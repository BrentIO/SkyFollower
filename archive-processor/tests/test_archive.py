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

from archive_processor.main import (  # noqa: E402  (after sys.path manipulation)
    _S3FallbackQueue,
    _interpolate_altitudes,
    build_geojson_feature,
    build_s3_key,
)
from shared.models import CompletedFlight


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
