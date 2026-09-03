"""
Tests for archive-processor/main.py components that don't require live
infrastructure.
"""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pyarrow.parquet as pq
import pytest
import yaml

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
    _interpolate_altitudes,
    _merge_segments,
    _normalize_timestamps,
    build_geojson_feature,
    build_index_s3_key,
    build_parquet_index_row,
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
        "receiver_sources": ["1090"],
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
    }

    with patch("archive_processor.main.DATA_DIR", tmp_dir), \
         patch("archive_processor.main.redis_lib.Redis") as MockRedis, \
         patch("archive_processor.main.boto3.Session"):
        mock_redis = MagicMock()
        MockRedis.return_value = mock_redis
        processor = ArchiveProcessor(config)
        processor._redis = mock_redis
        processor._s3_connected = True
        processor._flight_ttl_seconds = flight_ttl_seconds
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

    def read_parquet_bytes(self, key: str) -> bytes:
        return self.objects[key]


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

    def test_date_from_first_message_utc(self):
        # first_message at 2023-12-01T23:59:00Z, last_message the next day —
        # the key must follow first_message, not last_message, so it stays
        # invariant across split-flight stitching (see _merge_segments,
        # which always preserves the original segment's first_message).
        flight = _make_flight(
            first_message=datetime(2023, 12, 1, 23, 59, 0, tzinfo=timezone.utc),
            last_message=datetime(2023, 12, 2, 0, 5, 0, tzinfo=timezone.utc),
        )
        key = build_s3_key(flight)
        assert key.startswith("flights/2023/12/01/")


class TestBuildIndexS3Key:
    def test_basic_structure(self):
        flight = _make_flight()
        key = build_index_s3_key(flight)
        assert key == f"index/year=2024/month=05/day=31/{flight.id}.parquet"

    def test_date_from_first_message_utc(self):
        # Same rationale as TestBuildS3Key.test_date_from_first_message_utc:
        # first_message, not last_message, is what stays invariant across a
        # stitch, so it's what both key builders must agree on.
        flight = _make_flight(
            first_message=datetime(2023, 12, 1, 23, 59, 0, tzinfo=timezone.utc),
            last_message=datetime(2023, 12, 2, 0, 5, 0, tzinfo=timezone.utc),
        )
        key = build_index_s3_key(flight)
        assert key.startswith("index/year=2023/month=12/day=01/")

    def test_key_matches_build_s3_key_date_across_simulated_stitch(self):
        """A stitched flight's first_message stays pinned to the original
        segment even as last_message advances into the next UTC day — the
        index key (recomputed on every stitch) must land in the same
        day-partition as the flight object's key (frozen at first archive),
        or a cross-midnight stitch silently orphans a stale index row."""
        original_first = datetime(2024, 3, 14, 23, 55, 0, tzinfo=timezone.utc)
        object_key = build_s3_key(_make_flight(
            first_message=original_first,
            last_message=datetime(2024, 3, 14, 23, 58, 0, tzinfo=timezone.utc),
        ))

        merged_after_stitch = _make_flight(
            first_message=original_first,  # _merge_segments always preserves this
            last_message=datetime(2024, 3, 15, 0, 10, 0, tzinfo=timezone.utc),
        )
        index_key = build_index_s3_key(merged_after_stitch)

        assert object_key.startswith("flights/2024/03/14/")
        assert index_key.startswith("index/year=2024/month=03/day=14/")


class TestBuildParquetIndexRow:
    def _read(self, payload_bytes: bytes) -> dict:
        table = pq.read_table(io.BytesIO(payload_bytes))
        rows = table.to_pylist()
        assert len(rows) == 1
        return rows[0]

    def test_schema_columns_match_data_dictionary(self):
        # specs/data-dictionary.yaml's archive_parquet_index.fields is the
        # authoritative, ordered column list -- read it here rather than
        # restating it, so a change there that isn't mirrored in
        # _PARQUET_INDEX_SCHEMA fails this test.
        dd_path = os.path.join(_REPO_ROOT, "specs", "data-dictionary.yaml")
        with open(dd_path) as f:
            data_dictionary = yaml.safe_load(f)
        expected_columns = list(
            data_dictionary["records"]["archive_parquet_index"]["fields"].keys()
        )

        flight = _make_flight()
        payload = build_parquet_index_row(flight, "flights/2024/05/31/key.json.gz")
        table = pq.read_table(io.BytesIO(payload))
        assert table.schema.names == expected_columns

    def test_basic_field_values(self):
        flight = _make_flight()
        row = self._read(build_parquet_index_row(flight, "flights/2024/05/31/key.json.gz"))
        assert row["icao_hex"] == "A8AE7F"
        assert row["registration"] == "N659DL"
        assert row["ident"] == "DAL659"
        assert row["s3_key"] == "flights/2024/05/31/key.json.gz"

    def test_military_absent_normalizes_to_false(self):
        flight = _make_flight(aircraft={"icao_hex": "A8AE7F"})
        row = self._read(build_parquet_index_row(flight, "k"))
        assert row["military"] is False

    def test_military_true_preserved(self):
        flight = _make_flight(aircraft={"icao_hex": "A8AE7F", "military": True})
        row = self._read(build_parquet_index_row(flight, "k"))
        assert row["military"] is True

    def test_type_designator_present(self):
        flight = _make_flight(aircraft={"icao_hex": "A8AE7F", "type_designator": "B763"})
        row = self._read(build_parquet_index_row(flight, "k"))
        assert row["type_designator"] == "B763"

    def test_type_designator_missing_becomes_empty_string(self):
        flight = _make_flight(aircraft={"icao_hex": "A8AE7F"})
        row = self._read(build_parquet_index_row(flight, "k"))
        assert row["type_designator"] == ""

    def test_operator_designator_from_operator_dict(self):
        flight = _make_flight(operator={"airline_designator": "DAL"})
        row = self._read(build_parquet_index_row(flight, "k"))
        assert row["operator_designator"] == "DAL"

    def test_operator_none_becomes_empty_string(self):
        flight = _make_flight(operator=None)
        row = self._read(build_parquet_index_row(flight, "k"))
        assert row["operator_designator"] == ""


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


# Fallback queue put/drain/depth/dead-lettering is now covered by
# shared/tests/test_fallback_queue.py -- ArchiveProcessor just wires
# shared.FallbackQueue in for both its tables (queue, index_queue).

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
            }

        with patch("archive_processor.main.DATA_DIR", tmp_dir), \
             patch("archive_processor.main.redis_lib.Redis") as MockRedis, \
             patch("archive_processor.main.boto3.Session"):
            mock_redis = MagicMock()
            MockRedis.return_value = mock_redis
            processor = ArchiveProcessor(config)
            processor._redis = mock_redis
            return processor, mock_redis

    def test_redis_incr_called_after_successful_write(self):
        # Both hour/today counters go through incr_period_counter.lua now
        # (see shared/lua/incr_period_counter.lua), not a plain INCR -- so
        # the real reset-at-boundary fix actually applies here.
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)

            flight = _make_flight()

            # Mock out S3 write and Parquet index
            with patch.object(processor, "_write_to_s3") as mock_s3, \
                 patch.object(processor, "_write_index_to_s3"):
                processor._s3_connected = True
                processor._post_write_success(flight, "flights/2024/05/31/key.json.gz")

            from shared.redis_keys import metrics_flights_archived_key
            evalsha_keys = {c.args[2] for c in mock_redis.evalsha.call_args_list}
            assert metrics_flights_archived_key("hour") in evalsha_keys
            assert metrics_flights_archived_key("today") in evalsha_keys
            mock_redis.incr.assert_not_called()

    def test_redis_not_incremented_on_s3_unavailable(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)
            processor._s3_connected = False

            flight = _make_flight()
            processor._process_flight(flight)

            mock_redis.incr.assert_not_called()
            mock_redis.evalsha.assert_not_called()
            assert processor._fallback.depth() == 1


class TestIncrPeriodCounters:
    """_incr_period_counters()'s own behavior -- which key/amount/boundary
    it hands evalsha. incr_period_counter.lua's own atomicity/expiry-on-
    creation semantics are covered live against real Redis by
    shared/tests/test_incr_period_counter_lua.py."""

    def _make_processor(self, tmp_dir: str):
        from archive_processor.main import ArchiveProcessor

        config = {
            "s3": {"region": "us-east-1", "bucket": "test-bucket",
                   "access_key_id": "x", "secret_access_key": "x"},
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "redis": {"host": "localhost"},
            "mqtt": None,
            }
        with patch("archive_processor.main.DATA_DIR", tmp_dir), \
             patch("archive_processor.main.redis_lib.Redis") as MockRedis, \
             patch("archive_processor.main.boto3.Session"):
            mock_redis = MagicMock()
            mock_redis.script_load.return_value = "incrsha123"
            MockRedis.return_value = mock_redis
            processor = ArchiveProcessor(config)
            processor._redis = mock_redis
            return processor, mock_redis

    def test_increments_each_period_by_one_via_evalsha(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)
            from shared.redis_keys import metrics_flights_archived_key

            processor._incr_period_counters(metrics_flights_archived_key, ("hour", "today"))

            assert mock_redis.evalsha.call_count == 2
            for c in mock_redis.evalsha.call_args_list:
                assert c.args[0] == "incrsha123"
                assert c.args[1] == 0
                assert c.args[3] == 1  # increment amount
            keys = {c.args[2] for c in mock_redis.evalsha.call_args_list}
            assert keys == {
                metrics_flights_archived_key("hour"), metrics_flights_archived_key("today"),
            }

    def test_hour_boundary_uses_real_utc_top_of_hour(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)
            from shared.redis_keys import metrics_flights_archived_key

            fixed_now = datetime(2026, 8, 23, 14, 37, 0, tzinfo=timezone.utc)
            with patch("archive_processor.main.datetime") as mock_dt:
                mock_dt.now.return_value = fixed_now
                processor._incr_period_counters(metrics_flights_archived_key, ("hour",))

            expected = int(datetime(2026, 8, 23, 15, 0, 0, tzinfo=timezone.utc).timestamp())
            assert mock_redis.evalsha.call_args.args[4] == expected

    def test_today_boundary_uses_real_utc_midnight(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)
            from shared.redis_keys import metrics_flights_archived_key

            fixed_now = datetime(2026, 8, 23, 23, 59, 0, tzinfo=timezone.utc)
            with patch("archive_processor.main.datetime") as mock_dt:
                mock_dt.now.return_value = fixed_now
                processor._incr_period_counters(metrics_flights_archived_key, ("today",))

            expected = int(datetime(2026, 8, 24, 0, 0, 0, tzinfo=timezone.utc).timestamp())
            assert mock_redis.evalsha.call_args.args[4] == expected

    def test_no_lifetime_period_used_for_archive_processor_counters(self):
        # Explicit scope note from the issue: no new "lifetime" period is
        # added to archive-processor's existing flights_archived/flights_skipped
        # counters -- only the reset-mechanism fix for hour/today.
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)
            from shared.redis_keys import metrics_flights_archived_key

            with pytest.raises(ValueError, match="period"):
                processor._incr_period_counters(metrics_flights_archived_key, ("lifetime",))


# ---------------------------------------------------------------------------
# External-only flight skip + force_archive override
# ---------------------------------------------------------------------------

class TestExternalOnlySkip:
    def _make_processor(self, tmp_dir: str):
        from archive_processor.main import ArchiveProcessor

        config = {
            "s3": {"region": "us-east-1", "bucket": "test-bucket",
                   "access_key_id": "x", "secret_access_key": "x"},
            "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
            "redis": {"host": "localhost"},
            "mqtt": None,
            }

        with patch("archive_processor.main.DATA_DIR", tmp_dir), \
             patch("archive_processor.main.redis_lib.Redis") as MockRedis, \
             patch("archive_processor.main.boto3.Session"):
            mock_redis = MagicMock()
            MockRedis.return_value = mock_redis
            processor = ArchiveProcessor(config)
            processor._redis = mock_redis
            processor._s3_connected = True
            return processor, mock_redis

    def test_external_only_flight_not_written_to_s3(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)
            flight = _make_flight(receiver_sources=["EXTERNAL"], force_archive=False)

            with patch.object(processor, "_archive_flight_to_s3") as mock_archive:
                processor._process_flight(flight)

            mock_archive.assert_not_called()

    def test_external_only_flight_not_queued_to_local_fallback(self):
        """Skip must happen before the S3-available branch — even when S3 is
        down, a skipped flight is dropped, not deferred."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)
            processor._s3_connected = False
            flight = _make_flight(receiver_sources=["EXTERNAL"], force_archive=False)

            processor._process_flight(flight)

            assert processor._fallback.depth() == 0

    def test_mixed_sources_archived_normally(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)
            flight = _make_flight(receiver_sources=["1090", "EXTERNAL"], force_archive=False)

            with patch.object(processor, "_archive_flight_to_s3") as mock_archive:
                processor._process_flight(flight)

            mock_archive.assert_called_once()

    def test_non_external_single_source_archived_normally(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)
            flight = _make_flight(receiver_sources=["1090"], force_archive=False)

            with patch.object(processor, "_archive_flight_to_s3") as mock_archive:
                processor._process_flight(flight)

            mock_archive.assert_called_once()

    def test_force_archive_overrides_external_only_skip(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)
            flight = _make_flight(receiver_sources=["EXTERNAL"], force_archive=True)

            with patch.object(processor, "_archive_flight_to_s3") as mock_archive:
                processor._process_flight(flight)

            mock_archive.assert_called_once()

    def test_external_only_skip_increments_skipped_metric(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)
            flight = _make_flight(receiver_sources=["EXTERNAL"], force_archive=False)

            processor._process_flight(flight)

            from shared.redis_keys import metrics_flights_skipped_key
            evalsha_keys = {c.args[2] for c in mock_redis.evalsha.call_args_list}
            assert metrics_flights_skipped_key("hour") in evalsha_keys
            assert metrics_flights_skipped_key("today") in evalsha_keys
            mock_redis.incr.assert_not_called()

    def test_archived_flight_does_not_increment_skipped_metric(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = self._make_processor(tmp_dir)
            flight = _make_flight(receiver_sources=["1090"], force_archive=False)

            with patch.object(processor, "_archive_flight_to_s3"):
                processor._process_flight(flight)

            from shared.redis_keys import metrics_flights_skipped_key
            skipped_calls = [
                c for c in mock_redis.evalsha.call_args_list
                if c.args[2] in (metrics_flights_skipped_key("hour"), metrics_flights_skipped_key("today"))
            ]
            assert skipped_calls == []


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

    def test_new_segment_with_string_timestamps_merges_without_crashing(self):
        """A live CompletedFlight's positions/velocities have string
        timestamps by the time they reach here -- CompletedFlight.positions
        is an untyped list[dict] (see shared/models.py), so pydantic never
        re-parses a "timestamp" string back into datetime on
        model_validate_json(), which is exactly what deserializes every
        real flight off RabbitMQ (or the local SQLite fallback queue).
        _make_flight() constructs positions with real datetime objects
        directly, which doesn't exercise this -- this test simulates the
        actual shape a live segment has instead."""
        new = _make_flight(
            positions=[
                {"timestamp": "2024-05-31T12:15:00+00:00",
                 "latitude": 34.0, "longitude": -85.0, "altitude": 2000},
                {"timestamp": "2024-05-31T12:45:00+00:00",
                 "latitude": 35.0, "longitude": -86.0, "altitude": 3000},
            ],
            velocities=[
                {"timestamp": "2024-05-31T12:15:00+00:00", "ground_speed": 400},
            ],
        )
        merged = _merge_segments(new, self._prev_dict())  # must not raise

        assert [p["timestamp"] for p in merged.positions] == [
            datetime(2024, 5, 31, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 31, 12, 15, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 31, 12, 45, 0, tzinfo=timezone.utc),
        ]
        assert merged.velocities[0]["timestamp"] == datetime(
            2024, 5, 31, 12, 15, 0, tzinfo=timezone.utc
        )

    def test_mixed_string_and_datetime_timestamps_sort_correctly(self):
        """A previously-archived (string-timestamp) segment and a live
        (also string-timestamp, post-round-trip) segment must interleave
        into true chronological order, not just "prev items then new
        items" -- proves the fix actually normalizes both sides rather
        than merely avoiding the crash."""
        new = _make_flight(
            positions=[
                # Earlier than the previous segment's own position below --
                # a correct merge must sort it first, not leave it trailing
                # just because it came from "new_flight".
                {"timestamp": "2024-05-31T11:00:00+00:00",
                 "latitude": 30.0, "longitude": -80.0, "altitude": 500},
            ],
            velocities=[],
        )
        merged = _merge_segments(new, self._prev_dict())  # prev position at 12:00:00
        assert [p["timestamp"] for p in merged.positions] == [
            datetime(2024, 5, 31, 11, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 5, 31, 12, 0, 0, tzinfo=timezone.utc),
        ]


class TestStitching:
    def test_no_pointer_writes_normally(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            mock_redis.get.return_value = None

            flight = _make_flight()
            with patch.object(processor, "_write_index_to_s3"):
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

            with patch.object(processor, "_write_index_to_s3"):
                processor._archive_flight_to_s3(flight)

            # Wrote a fresh object under its own key, not the previous one
            assert len(processor._s3_client.objects) == 1
            key = next(iter(processor._s3_client.objects))
            assert processor._s3_client.read_json(key)["_id"] == "new-uuid"

    def test_negative_gap_writes_normally_instead_of_merging_backwards(self):
        """A flight can arrive out of order relative to the pointer it
        finds -- e.g. it failed and sat in the local retry queue while a
        later continuation for the same aircraft raced ahead and archived
        first. The pointer's last_message then lands *after* this flight's
        own first_message (a negative gap), which the too-large-gap check
        alone doesn't catch. Before this guard, _merge_segments would
        still run and -- since it takes first_message from the pointed-to
        segment and leaves last_message from the segment being processed --
        silently produce (and write to S3, overwriting the correct object)
        a record with last_message before first_message.

        Archives the "later continuation" for real first (a genuine S3
        object + pointer to fetch and merge against), matching what
        _try_stitch actually does -- a pointer referencing a nonexistent
        key would short-circuit in _fetch_previous_segment before ever
        reaching the gap check this test is about, silently passing for
        the wrong reason."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir, flight_ttl_seconds=300)
            processor._s3_client = _FakeS3()

            later_continuation = _make_flight(
                _id="later-continuation-uuid",
                first_message=datetime.fromtimestamp(1050.0, tz=timezone.utc),
                last_message=datetime.fromtimestamp(1200.0, tz=timezone.utc),
            )
            mock_redis.get.return_value = None
            with patch.object(processor, "_write_index_to_s3"):
                processor._archive_flight_to_s3(later_continuation)
            later_key = next(iter(processor._s3_client.objects))
            pointer = json.loads(mock_redis.set.call_args.args[1])
            assert pointer["s3_key"] == later_key
            mock_redis.get.return_value = json.dumps(pointer)

            late_arriving_earlier_segment = _make_flight(
                _id="earlier-segment-uuid",
                first_message=datetime.fromtimestamp(0.0, tz=timezone.utc),
                last_message=datetime.fromtimestamp(1000.0, tz=timezone.utc),
            )
            with patch.object(processor, "_write_index_to_s3"):
                processor._archive_flight_to_s3(late_arriving_earlier_segment)

            # Archived as its own new object, not merged backwards into
            # the pointed-to (chronologically later) segment.
            assert len(processor._s3_client.objects) == 2
            new_key = next(k for k in processor._s3_client.objects if k != later_key)
            doc = processor._s3_client.read_json(new_key)
            assert doc["_id"] == "earlier-segment-uuid"
            assert doc["first_message"] < doc["last_message"]  # never inverted
            # The earlier-archived (later-continuation) object is untouched
            assert processor._s3_client.read_json(later_key)["_id"] == "later-continuation-uuid"

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
            with patch.object(processor, "_write_index_to_s3"):
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
            with patch.object(processor, "_write_index_to_s3"):
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
            with patch.object(processor, "_write_index_to_s3"):
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
                with patch.object(processor, "_write_index_to_s3"):
                    processor._archive_flight_to_s3(seg)

            # All three segments stitched into the original seg1 object —
            # never more than one S3 object for this aircraft.
            assert len(processor._s3_client.objects) == 1
            merged = processor._s3_client.read_json(seg1_key)
            assert merged["_id"] == "seg1-uuid"
            assert merged["total_messages"] == 3
            assert merged["matched_rules"] == ["rule_a", "rule_b", "rule_c"]
            assert len(merged["positions"]) == 3  # one per segment

    def test_stitch_survives_a_real_wire_round_trip(self):
        """End-to-end regression test for the crash a real continuation
        segment always hit: every other stitching test in this class
        constructs its "new" segment directly via _make_flight() and calls
        _archive_flight_to_s3() with it, which keeps real datetime
        objects the whole way through and never exercises the shape a
        live flight actually has. This test instead round-trips the
        continuation segment through model_dump_json()/model_validate_json()
        -- exactly what _on_message() does for every real RabbitMQ message
        -- and drives it through _process_flight(), the real entry point,
        not _archive_flight_to_s3() directly."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir, flight_ttl_seconds=300)
            processor._s3_client = _FakeS3()

            seg1 = _make_flight(
                _id="seg1-uuid",
                first_message=datetime.fromtimestamp(0.0, tz=timezone.utc),
                last_message=datetime.fromtimestamp(1000.0, tz=timezone.utc),
                total_messages=10,
            )
            mock_redis.get.return_value = None
            with patch.object(processor, "_write_index_to_s3"):
                processor._archive_flight_to_s3(seg1)
            seg1_key = next(iter(processor._s3_client.objects))
            pointer = json.loads(mock_redis.set.call_args.args[1])
            mock_redis.get.return_value = json.dumps(pointer)

            seg2 = _make_flight(
                _id="seg2-uuid",
                first_message=datetime.fromtimestamp(1050.0, tz=timezone.utc),
                last_message=datetime.fromtimestamp(1200.0, tz=timezone.utc),
                total_messages=5,
            )
            wire_payload = seg2.model_dump_json(by_alias=True)  # what RabbitMQ actually carries
            live_seg2 = CompletedFlight.model_validate_json(wire_payload)  # what _on_message actually does
            assert isinstance(live_seg2.positions[0]["timestamp"], str)  # confirms this reproduces the real shape

            with patch.object(processor, "_write_index_to_s3"):
                processor._process_flight(live_seg2)  # must not raise

            assert len(processor._s3_client.objects) == 1  # stitched, not split
            merged = processor._s3_client.read_json(seg1_key)
            assert merged["_id"] == "seg1-uuid"
            assert merged["total_messages"] == 15
            assert len(merged["positions"]) == len(seg1.positions) + len(seg2.positions)


# ---------------------------------------------------------------------------
# Parquet index write, alongside the flight object, and its retry queue
# ---------------------------------------------------------------------------

class TestArchiveWritesIndexToS3:
    def test_index_object_written_alongside_flight_object(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            mock_redis.get.return_value = None

            flight = _make_flight()
            processor._archive_flight_to_s3(flight)

            keys = list(processor._s3_client.objects)
            flight_keys = [k for k in keys if k.startswith("flights/")]
            index_keys = [k for k in keys if k.startswith("index/")]
            assert len(flight_keys) == 1
            assert len(index_keys) == 1
            assert index_keys[0] == f"index/year=2024/month=05/day=31/{flight.id}.parquet"

    def test_index_row_content_matches_flight(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            mock_redis.get.return_value = None

            flight = _make_flight()
            processor._archive_flight_to_s3(flight)

            index_key = next(k for k in processor._s3_client.objects if k.startswith("index/"))
            payload = processor._s3_client.read_parquet_bytes(index_key)
            table = pq.read_table(io.BytesIO(payload))
            row = table.to_pylist()[0]
            assert row["icao_hex"] == "A8AE7F"
            flight_key = next(k for k in processor._s3_client.objects if k.startswith("flights/"))
            assert row["s3_key"] == flight_key

    def test_index_stays_in_original_partition_across_midnight_stitch(self):
        """End-to-end regression test for the bug this design avoids: a
        stitch whose new segment's last_message falls on the next UTC day
        must NOT move the index row to a new partition or leave a stale
        one behind under the original day."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir, flight_ttl_seconds=600)
            processor._s3_client = _FakeS3()

            seg1 = _make_flight(
                _id="seg1-uuid",
                first_message=datetime(2024, 3, 14, 23, 55, 0, tzinfo=timezone.utc),
                last_message=datetime(2024, 3, 14, 23, 58, 0, tzinfo=timezone.utc),
            )
            mock_redis.get.return_value = None
            processor._archive_flight_to_s3(seg1)

            flight_keys = [k for k in processor._s3_client.objects if k.startswith("flights/")]
            index_keys = [k for k in processor._s3_client.objects if k.startswith("index/")]
            assert flight_keys == ["flights/2024/03/14/A8AE7F_DAL659_seg1-uuid.json.gz"]
            assert index_keys == ["index/year=2024/month=03/day=14/seg1-uuid.parquet"]

            pointer_call = mock_redis.set.call_args
            pointer = json.loads(pointer_call.args[1])
            mock_redis.get.return_value = json.dumps(pointer)

            # Segment 2 starts 5 minutes later — past midnight UTC, but
            # within the 600s ttl, so this stitches into segment 1.
            seg2 = _make_flight(
                _id="seg2-uuid",
                first_message=datetime(2024, 3, 15, 0, 3, 0, tzinfo=timezone.utc),
                last_message=datetime(2024, 3, 15, 0, 10, 0, tzinfo=timezone.utc),
            )
            processor._archive_flight_to_s3(seg2)

            # Still exactly one flight object and one index row — the
            # stitch overwrote both in place, neither moved to 03/15, and
            # no stale 03/14-dated leftover or duplicate 03/15 copy exists.
            flight_keys = [k for k in processor._s3_client.objects if k.startswith("flights/")]
            index_keys = [k for k in processor._s3_client.objects if k.startswith("index/")]
            assert flight_keys == ["flights/2024/03/14/A8AE7F_DAL659_seg1-uuid.json.gz"]
            assert index_keys == ["index/year=2024/month=03/day=14/seg1-uuid.parquet"]

            payload = processor._s3_client.read_parquet_bytes(index_keys[0])
            row = pq.read_table(io.BytesIO(payload)).to_pylist()[0]
            # The column value itself correctly reflects the real, extended
            # last_message — only the partition/key stays pinned to the
            # original day.
            assert row["last_message"] == datetime(2024, 3, 15, 0, 10, 0, tzinfo=timezone.utc)

    def test_index_write_failure_does_not_block_archiving(self):
        """Best-effort semantic: an exception building/uploading the index
        row must not prevent the flight from being archived, and must not
        be treated as a full-flight archive failure (no fallback.put)."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            mock_redis.get.return_value = None

            flight = _make_flight()
            with patch(
                "archive_processor.main.build_parquet_index_row",
                side_effect=RuntimeError("boom"),
            ):
                processor._archive_flight_to_s3(flight)  # must not raise

            flight_keys = [k for k in processor._s3_client.objects if k.startswith("flights/")]
            assert len(flight_keys) == 1
            index_keys = [k for k in processor._s3_client.objects if k.startswith("index/")]
            assert index_keys == []
            assert processor._fallback.depth() == 0  # not queued as a full re-archive


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

    return patch("archive_processor.main.threading.Thread", _ImmediateThread)


class TestIndexWriteRetryQueue:
    def test_failed_index_write_is_queued_for_retry(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            mock_redis.get.return_value = None

            flight = _make_flight()
            with patch(
                "archive_processor.main.build_parquet_index_row",
                side_effect=RuntimeError("boom"),
            ):
                processor._archive_flight_to_s3(flight)

            assert processor._index_fallback.depth() == 1
            assert processor._fallback.depth() == 0

    def test_drain_index_fallback_succeeds(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            mock_redis.get.return_value = None

            flight = _make_flight()
            with patch(
                "archive_processor.main.build_parquet_index_row",
                side_effect=RuntimeError("boom"),
            ):
                processor._archive_flight_to_s3(flight)
            assert processor._index_fallback.depth() == 1

            with _synchronous_drain_thread():
                processor._drain_index_fallback()

            assert processor._index_fallback.depth() == 0
            index_keys = [k for k in processor._s3_client.objects if k.startswith("index/")]
            assert index_keys == [f"index/year=2024/month=05/day=31/{flight.id}.parquet"]

    def test_drain_index_fallback_leaves_row_queued_on_repeat_failure(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            mock_redis.get.return_value = None

            flight = _make_flight()
            with patch(
                "archive_processor.main.build_parquet_index_row",
                side_effect=RuntimeError("boom"),
            ):
                processor._archive_flight_to_s3(flight)
            assert processor._index_fallback.depth() == 1

            with patch.object(
                processor, "_write_index_to_s3", side_effect=RuntimeError("still down")
            ), _synchronous_drain_thread():
                processor._drain_index_fallback()

            assert processor._index_fallback.depth() == 1

    def test_drain_all_fallbacks_drains_both_queues(self):
        """Both queues get both triggers (S3 reconnect and every telemetry
        tick) — _drain_all_fallbacks is what both loops call, so it must
        actually drain both, not just the index queue."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            mock_redis.get.return_value = None

            # Queue a full flight (as if S3 had been down) and an
            # index-only retry (as if just the index write had failed).
            processor._fallback.put(_make_flight(_id="queued-flight").model_dump_json(
                by_alias=True
            ))
            processor._index_fallback.put(json.dumps({
                "flight_json": _make_flight(_id="queued-index-only").model_dump_json(
                    by_alias=True
                ),
                "s3_key": "flights/2024/05/31/prebuilt-key.json.gz",
            }))
            assert processor._fallback.depth() == 1
            assert processor._index_fallback.depth() == 1

            with _synchronous_drain_thread():
                processor._drain_all_fallbacks()

            assert processor._fallback.depth() == 0
            assert processor._index_fallback.depth() == 0
            flight_keys = [k for k in processor._s3_client.objects if k.startswith("flights/")]
            index_keys = [k for k in processor._s3_client.objects if k.startswith("index/")]
            assert len(flight_keys) == 1  # the drained full flight
            assert len(index_keys) == 2  # one from that flight's own index write,
            # one from the index-only retry


# ---------------------------------------------------------------------------
# Local index cache: mirrors the Parquet index row to a shared volume
# archive-compaction reads from instead of re-downloading from S3.
# ---------------------------------------------------------------------------

class TestLocalIndexCache:
    def test_written_after_successful_index_write(self):
        with tempfile.TemporaryDirectory() as tmp_dir, \
             tempfile.TemporaryDirectory() as cache_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            mock_redis.get.return_value = None

            flight = _make_flight()
            with patch("archive_processor.main.INDEX_CACHE_DIR", cache_dir):
                processor._archive_flight_to_s3(flight)

            from shared.index_cache import local_index_path

            index_key = f"index/year=2024/month=05/day=31/{flight.id}.parquet"
            local_path = local_index_path(index_key, cache_dir)
            assert os.path.exists(local_path)
            with open(local_path, "rb") as f:
                local_bytes = f.read()
            assert local_bytes == processor._s3_client.read_parquet_bytes(index_key)

    def test_local_write_failure_does_not_affect_archiving_or_retry_queue(self):
        """Best-effort semantic, same as the S3 index write's own failure
        handling: a local cache write failure must never block archiving
        and must never be treated as an index-write failure requiring a
        retry -- the S3 write (the durable copy) already succeeded."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            mock_redis.get.return_value = None

            flight = _make_flight()
            with patch(
                "archive_processor.main.write_local_index",
                side_effect=OSError("disk full"),
            ):
                processor._archive_flight_to_s3(flight)  # must not raise

            index_keys = [k for k in processor._s3_client.objects if k.startswith("index/")]
            assert len(index_keys) == 1  # S3 write still succeeded
            assert processor._index_fallback.depth() == 0  # not treated as a failed index write
            assert processor._fallback.depth() == 0

    def test_not_written_when_s3_index_write_fails(self):
        """The local cache mirrors a write that already succeeded on S3 --
        it must not be populated from a row that failed to upload, since
        the failed row is still pending in the retry queue instead."""
        with tempfile.TemporaryDirectory() as tmp_dir, \
             tempfile.TemporaryDirectory() as cache_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            mock_redis.get.return_value = None

            flight = _make_flight()
            with patch("archive_processor.main.INDEX_CACHE_DIR", cache_dir), \
                 patch.object(processor, "_write_index_to_s3", side_effect=RuntimeError("down")):
                processor._archive_flight_to_s3(flight)

            assert processor._index_fallback.depth() == 1
            assert not os.listdir(cache_dir)

    def test_drain_index_fallback_writes_local_cache_too(self):
        with tempfile.TemporaryDirectory() as tmp_dir, \
             tempfile.TemporaryDirectory() as cache_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            mock_redis.get.return_value = None

            flight = _make_flight()
            with patch(
                "archive_processor.main.build_parquet_index_row",
                side_effect=RuntimeError("boom"),
            ):
                processor._archive_flight_to_s3(flight)
            assert processor._index_fallback.depth() == 1

            from shared.index_cache import local_index_path

            index_key = f"index/year=2024/month=05/day=31/{flight.id}.parquet"
            with patch("archive_processor.main.INDEX_CACHE_DIR", cache_dir), \
                 _synchronous_drain_thread():
                processor._drain_index_fallback()

            assert processor._index_fallback.depth() == 0
            assert os.path.exists(local_index_path(index_key, cache_dir))


# ---------------------------------------------------------------------------
# S3 reconnect: synchronous backlog drain gates _s3_connected
# ---------------------------------------------------------------------------

def _fake_redis_get_set(mock_redis) -> dict:
    """Wires a MagicMock's get/set to a real backing dict, so a pointer
    written by one _archive_flight_to_s3 call is actually visible to a
    later _try_stitch lookup within the same test -- the plain MagicMock
    used elsewhere in this file doesn't connect get() to a prior set()."""
    store: dict = {}

    def fake_set(key, value, **kwargs):
        store[key] = value

    def fake_get(key):
        return store.get(key)

    mock_redis.set.side_effect = fake_set
    mock_redis.get.side_effect = fake_get
    return store


class TestFinishS3Connect:
    def test_flips_s3_connected_only_after_backlog_drains(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            processor._s3_connected = False
            mock_redis.get.return_value = None

            processor._fallback.put(_make_flight(_id="queued").model_dump_json(by_alias=True))
            assert processor._fallback.depth() == 1

            processor._finish_s3_connect()

            assert processor._s3_connected is True
            assert processor._fallback.depth() == 0
            flight_keys = [k for k in processor._s3_client.objects if k.startswith("flights/")]
            assert len(flight_keys) == 1

    def test_stays_disconnected_if_drain_fails_partway(self):
        """S3 goes away again mid-drain: _s3_connected must stay False so
        the normal reconnect-loop retry cadence tries the whole sequence
        again later, rather than prematurely routing new live flights
        directly to a client that just proved unreliable."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            processor._s3_connected = False
            mock_redis.get.return_value = None

            processor._fallback.put(_make_flight(_id="queued").model_dump_json(by_alias=True))

            with patch.object(processor, "_write_to_s3", side_effect=ConnectionError("gone again")):
                processor._finish_s3_connect()

            assert processor._s3_connected is False
            assert processor._fallback.depth() == 1  # left queued, not lost

    def test_reconnect_drain_gate_prevents_live_flight_from_jumping_the_backlog(self):
        """The core reconnect-race regression test. Segment A (queued while
        S3 was down) is still being drained when segment B -- its continuation,
        same aircraft, within flight_ttl_seconds -- arrives on what would
        be the live RabbitMQ consumer path. Before the fix, B would see
        s3_connected already True and archive independently (missing A's
        not-yet-written stitch pointer), splitting one flight into two S3
        objects. With the fix, B must still see s3_connected as False at
        that moment and queue behind A instead -- so by the time B is
        actually processed (later in this same drain), A's pointer already
        exists and B correctly stitches into a single object."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir, flight_ttl_seconds=300)
            processor._s3_client = _FakeS3()
            processor._s3_connected = False
            _fake_redis_get_set(mock_redis)

            seg_a = _make_flight(
                _id="seg-a-uuid",
                aircraft={"icao_hex": "A8AE7F", "registration": "N659DL"},
                first_message=datetime.fromtimestamp(0.0, tz=timezone.utc),
                last_message=datetime.fromtimestamp(1000.0, tz=timezone.utc),
            )
            processor._fallback.put(seg_a.model_dump_json(by_alias=True))

            seg_b = _make_flight(
                _id="seg-b-uuid",
                aircraft={"icao_hex": "A8AE7F", "registration": "N659DL"},
                first_message=datetime.fromtimestamp(1050.0, tz=timezone.utc),  # 50s gap, within ttl
                last_message=datetime.fromtimestamp(1200.0, tz=timezone.utc),
                # Empty, not _make_flight's default positions -- this test
                # goes through the same JSON round-trip (fallback queue
                # put -> model_validate_json) that a real live flight does,
                # which turns positions[]/velocities[]' timestamps into
                # plain strings (untyped list[dict] fields). _merge_segments
                # doesn't normalize the *new* segment's timestamps back to
                # datetime (only the previously-archived one it's merging
                # against), so a non-empty positions list here would trip
                # that unrelated, separately-filed bug instead of exercising
                # what this test is actually about.
                positions=[],
                velocities=[],
            )

            s3_connected_when_b_arrived = []
            injected = {"done": False}
            original_process = processor._process_fallback_flight

            def intercept(payload):
                # Fires once, while draining segment A -- simulates B
                # arriving concurrently on the live consumer thread.
                if not injected["done"]:
                    injected["done"] = True
                    with processor._s3_lock:
                        s3_connected_when_b_arrived.append(processor._s3_connected)
                    processor._process_flight(seg_b)
                original_process(payload)

            with patch.object(processor, "_process_fallback_flight", side_effect=intercept):
                processor._finish_s3_connect()

            assert s3_connected_when_b_arrived == [False]  # B was gated, not routed live
            assert processor._s3_connected is True
            assert processor._fallback.depth() == 0  # both eventually drained

            flight_keys = [k for k in processor._s3_client.objects if k.startswith("flights/")]
            assert len(flight_keys) == 1  # stitched into one object, not split into two
            merged = processor._s3_client.read_json(flight_keys[0])
            assert merged["_id"] == "seg-a-uuid"
            assert merged["total_messages"] == seg_a.total_messages + seg_b.total_messages

    def test_new_arrivals_during_drain_queue_behind_not_ahead(self):
        """A flight for an unrelated aircraft arriving mid-drain must also
        still be gated (not just same-aircraft continuations) -- the gate
        is on s3_connected globally, not per-aircraft."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            processor._s3_client = _FakeS3()
            processor._s3_connected = False
            mock_redis.get.return_value = None

            seg_a = _make_flight(_id="seg-a", aircraft={"icao_hex": "AAAAAA"})
            processor._fallback.put(seg_a.model_dump_json(by_alias=True))
            unrelated = _make_flight(_id="unrelated", aircraft={"icao_hex": "BBBBBB"})

            injected = {"done": False}
            original_process = processor._process_fallback_flight

            def intercept(payload):
                if not injected["done"]:
                    injected["done"] = True
                    processor._process_flight(unrelated)
                original_process(payload)

            with patch.object(processor, "_process_fallback_flight", side_effect=intercept):
                processor._finish_s3_connect()

            assert processor._s3_connected is True
            assert processor._fallback.depth() == 0
            flight_keys = [k for k in processor._s3_client.objects if k.startswith("flights/")]
            assert len(flight_keys) == 2


# ---------------------------------------------------------------------------
# flight_ttl_seconds: shared Redis config
# ---------------------------------------------------------------------------

class TestFlightTtlLoad:
    def test_defaults_to_300_when_unset(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            mock_redis.get.return_value = None
            processor._load_flight_ttl_seconds()
            assert processor._flight_ttl_seconds == 300

    def test_loads_from_redis_value(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            mock_redis.get.return_value = "600"
            processor._load_flight_ttl_seconds()
            assert processor._flight_ttl_seconds == 600

    def test_keeps_default_on_redis_error(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir)
            mock_redis.get.side_effect = ConnectionError("redis down")
            processor._load_flight_ttl_seconds()
            assert processor._flight_ttl_seconds == 300

    def test_stitch_uses_loaded_value_not_config(self):
        """_try_stitch must read the cached attribute — config no longer
        carries flight_ttl_seconds at all."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, mock_redis = _make_processor(tmp_dir, flight_ttl_seconds=10)
            assert "flight_ttl_seconds" not in processor._cfg

            pointer = {
                "uuid": "prev-uuid",
                "first_message": 0.0,
                "last_message": 1000.0,
                "s3_key": "flights/2024/05/31/prev.json.gz",
            }
            mock_redis.get.return_value = json.dumps(pointer)

            # Gap of 20s exceeds the 10s ttl, so this should not stitch.
            new_first = datetime.fromtimestamp(1020.0, tz=timezone.utc)
            flight = _make_flight(_id="new-uuid", first_message=new_first)
            assert processor._try_stitch(flight) is None


# ---------------------------------------------------------------------------
# Dead-letter queue depth telemetry (both tables, see shared/fallback_queue.py)
# ---------------------------------------------------------------------------

class TestDeadLetterQueueDepthTelemetry:
    def test_both_dead_letter_topics_present_and_zero_by_default(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, _ = _make_processor(tmp_dir)
            mock_mqtt = MagicMock()
            processor._mqtt = mock_mqtt
            processor._mqtt_connected = True

            processor._publish_telemetry()

            calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
            assert calls["SkyFollower/archive/statistic/dead_letter_queue_depth"] == "0"
            assert calls["SkyFollower/archive/statistic/dead_letter_index_queue_depth"] == "0"

    def test_reflects_actual_dead_lettered_files(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, _ = _make_processor(tmp_dir)
            # Disable the retry-spacing cooldown so this test can force 5
            # real attempts back-to-back rather than waiting out real time.
            processor._fallback._min_retry_interval_seconds = 0
            processor._fallback.put("poison")
            for _ in range(5):
                processor._fallback.drain(
                    lambda _p: (_ for _ in ()).throw(RuntimeError("always fails"))
                )
            assert processor._fallback.dead_letter_depth() == 1

            mock_mqtt = MagicMock()
            processor._mqtt = mock_mqtt
            processor._mqtt_connected = True
            processor._publish_telemetry()

            calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
            assert calls["SkyFollower/archive/statistic/dead_letter_queue_depth"] == "1"
            assert calls["SkyFollower/archive/statistic/dead_letter_index_queue_depth"] == "0"


# ---------------------------------------------------------------------------
# Running version telemetry
# ---------------------------------------------------------------------------

class TestPublishTelemetryVersion:
    _TOPIC = "SkyFollower/archive/statistic/version"

    def test_reads_version_env_var(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.dict(os.environ, {"VERSION": "2026.08.01"}):
                processor, _ = _make_processor(tmp_dir)
            mock_mqtt = MagicMock()
            processor._mqtt = mock_mqtt
            processor._mqtt_connected = True

            processor._publish_telemetry()

            calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
            assert calls[self._TOPIC] == "2026.08.01"

    def test_falls_back_to_dev_when_unset(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop("VERSION", None)
                processor, _ = _make_processor(tmp_dir)
            mock_mqtt = MagicMock()
            processor._mqtt = mock_mqtt
            processor._mqtt_connected = True

            processor._publish_telemetry()

            calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
            assert calls[self._TOPIC] == "dev"


# ---------------------------------------------------------------------------
# HA autodiscovery
# ---------------------------------------------------------------------------

class TestHaAutodiscoveryStartedAt:
    def test_started_at_label_matches_receiver_vernacular(self):
        # "Start Time" -- not the old "Archive Started At" -- matching
        # receiver's and message-processor's label for the same field.
        with tempfile.TemporaryDirectory() as tmp_dir:
            processor, _ = _make_processor(tmp_dir)
            mock_mqtt = MagicMock()
            processor._mqtt = mock_mqtt
            processor._mqtt_connected = True

            processor._publish_ha_autodiscovery()

            configs = {
                c.args[0]: json.loads(c.args[1])
                for c in mock_mqtt.publish.call_args_list
                if c.args[0].startswith("homeassistant/")
            }
            cfg = configs["homeassistant/sensor/SkyFollower_archive_started_at/config"]
            assert cfg["name"] == "Start Time"
