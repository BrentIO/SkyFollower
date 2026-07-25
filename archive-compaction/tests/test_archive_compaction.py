"""
Tests for the archive-compaction daily Parquet index compaction job.

Covers:
- Target partition date math (day before yesterday, UTC)
- Per-flight vs. already-compacted file classification
- Compaction: merge, write, delete-only-what-was-included
- Unreadable / late-arriving files are left alone, not deleted
- Batch delete error handling
- MQTT completion stats
"""

from __future__ import annotations

import importlib.util
import io
import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ---------------------------------------------------------------------------
# Module import helper (archive-compaction/ contains a hyphen, so it can't
# be imported as a normal package -- same workaround used by every other
# hyphenated component's tests, e.g. data-runners/us-faa-registry).
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL_DIR = os.path.dirname(_HERE)  # archive-compaction/
_REPO_ROOT = os.path.abspath(os.path.join(_TOOL_DIR, ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "archive_compaction_main",
        os.path.join(_TOOL_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["archive_compaction_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

target_partition_prefix = _mod.target_partition_prefix
is_per_flight_file = _mod.is_per_flight_file
build_compacted_key = _mod.build_compacted_key
delete_keys = _mod.delete_keys
compact_partition = _mod.compact_partition
publish_completion_stats = _mod.publish_completion_stats
MQTT_ROOT = _mod.MQTT_ROOT
_PARQUET_INDEX_SCHEMA = _mod._PARQUET_INDEX_SCHEMA


# ---------------------------------------------------------------------------
# Fixtures / fakes
# ---------------------------------------------------------------------------

def _make_row(**overrides) -> dict:
    row = {
        "icao_hex": "A1B2C3",
        "registration": "N12345",
        "type_designator": "B738",
        "military": False,
        "operator_designator": "DAL",
        "ident": "DAL123",
        "first_message": datetime(2026, 7, 20, 12, 0, tzinfo=timezone.utc),
        "last_message": datetime(2026, 7, 20, 12, 5, tzinfo=timezone.utc),
        "s3_key": "flights/2026/07/20/A1B2C3_DAL123_uuid.json.gz",
    }
    row.update(overrides)
    return row


def _make_parquet_bytes(*rows: dict) -> bytes:
    table = pa.Table.from_pylist(list(rows), schema=_PARQUET_INDEX_SCHEMA)
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


class _FakeS3:
    """In-memory stand-in for the pieces of the boto3 S3 client this module
    uses: paginated list, get, put, and batch delete."""

    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.deleted: list[str] = []
        self.fail_get_for: set[str] = set()
        self.fail_delete_for: set[str] = set()

    def get_paginator(self, operation_name: str):
        assert operation_name == "list_objects_v2"
        return _FakePaginator(self)

    def get_object(self, Bucket, Key):
        if Key in self.fail_get_for:
            raise RuntimeError(f"simulated read failure for {Key}")
        if Key not in self.objects:
            raise KeyError(f"no such key: {Key}")
        body = MagicMock()
        body.read.return_value = self.objects[Key]
        return {"Body": body}

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.objects[Key] = Body

    def delete_objects(self, Bucket, Delete, **kwargs):
        errors = []
        for obj in Delete["Objects"]:
            key = obj["Key"]
            if key in self.fail_delete_for:
                errors.append({"Key": key, "Message": "simulated delete failure"})
                continue
            self.objects.pop(key, None)
            self.deleted.append(key)
        return {"Errors": errors}


class _FakePaginator:
    def __init__(self, fake_s3: _FakeS3) -> None:
        self._fake_s3 = fake_s3

    def paginate(self, Bucket, Prefix):
        matching = sorted(k for k in self._fake_s3.objects if k.startswith(Prefix))
        yield {"Contents": [{"Key": k} for k in matching]}


# ---------------------------------------------------------------------------
# target_partition_prefix
# ---------------------------------------------------------------------------

class TestTargetPartitionPrefix:
    def test_targets_day_before_yesterday(self):
        now = datetime(2026, 7, 25, 5, 0, tzinfo=timezone.utc)
        assert target_partition_prefix(now) == "index/year=2026/month=07/day=23/"

    def test_crosses_month_boundary(self):
        now = datetime(2026, 8, 1, 5, 0, tzinfo=timezone.utc)
        assert target_partition_prefix(now) == "index/year=2026/month=07/day=30/"

    def test_crosses_year_boundary(self):
        now = datetime(2026, 1, 1, 5, 0, tzinfo=timezone.utc)
        assert target_partition_prefix(now) == "index/year=2025/month=12/day=30/"


# ---------------------------------------------------------------------------
# is_per_flight_file
# ---------------------------------------------------------------------------

class TestIsPerFlightFile:
    def test_bare_uuid_is_per_flight(self):
        key = "index/year=2026/month=07/day=23/0198abcd-1234-7abc-8def-1234567890ab.parquet"
        assert is_per_flight_file(key) is True

    def test_compacted_prefix_is_not_per_flight(self):
        key = "index/year=2026/month=07/day=23/compacted-0198abcd-1234-7abc-8def-1234567890ab.parquet"
        assert is_per_flight_file(key) is False


# ---------------------------------------------------------------------------
# build_compacted_key
# ---------------------------------------------------------------------------

class TestBuildCompactedKey:
    def test_key_shape(self):
        prefix = "index/year=2026/month=07/day=23/"
        key = build_compacted_key(prefix)
        assert key.startswith(prefix + "compacted-")
        assert key.endswith(".parquet")


# ---------------------------------------------------------------------------
# delete_keys
# ---------------------------------------------------------------------------

class TestDeleteKeys:
    def test_all_succeed(self):
        s3 = _FakeS3()
        s3.objects = {"a": b"x", "b": b"y"}
        failed = delete_keys(s3, "bucket", ["a", "b"])
        assert failed == 0
        assert set(s3.deleted) == {"a", "b"}

    def test_reports_partial_failures(self):
        s3 = _FakeS3()
        s3.objects = {"a": b"x", "b": b"y"}
        s3.fail_delete_for = {"b"}
        failed = delete_keys(s3, "bucket", ["a", "b"])
        assert failed == 1
        assert s3.deleted == ["a"]
        assert "b" in s3.objects

    def test_whole_batch_call_raising_counts_all_as_failed(self):
        s3 = _FakeS3()

        def _raise(**kwargs):
            raise RuntimeError("boom")

        s3.delete_objects = _raise
        failed = delete_keys(s3, "bucket", ["a", "b", "c"])
        assert failed == 3


# ---------------------------------------------------------------------------
# compact_partition
# ---------------------------------------------------------------------------

class TestCompactPartition:
    _PREFIX = "index/year=2026/month=07/day=23/"

    def test_empty_partition_is_a_noop(self):
        s3 = _FakeS3()
        result = compact_partition(s3, "bucket", self._PREFIX)
        assert result == {"files_compacted": 0, "files_delete_failed": 0}
        assert s3.objects == {}

    def test_merges_and_deletes_sources(self):
        s3 = _FakeS3()
        keys = [f"{self._PREFIX}{i:08x}-0000-7000-8000-000000000000.parquet" for i in range(3)]
        for i, key in enumerate(keys):
            s3.objects[key] = _make_parquet_bytes(_make_row(icao_hex=f"AAAA0{i}"))

        result = compact_partition(s3, "bucket", self._PREFIX)

        assert result == {"files_compacted": 3, "files_delete_failed": 0}
        # Originals gone, exactly one compacted-* file remains.
        remaining = list(s3.objects.keys())
        assert len(remaining) == 1
        assert remaining[0].startswith(f"{self._PREFIX}compacted-")

        table = pq.read_table(io.BytesIO(s3.objects[remaining[0]]))
        assert table.num_rows == 3
        assert set(table.column("icao_hex").to_pylist()) == {"AAAA00", "AAAA01", "AAAA02"}

    def test_skips_already_compacted_file(self):
        s3 = _FakeS3()
        old_compacted_key = f"{self._PREFIX}compacted-existing.parquet"
        s3.objects[old_compacted_key] = _make_parquet_bytes(_make_row(icao_hex="OLD000"))
        new_flight_key = f"{self._PREFIX}0198abcd-0000-7000-8000-000000000000.parquet"
        s3.objects[new_flight_key] = _make_parquet_bytes(_make_row(icao_hex="NEW000"))

        result = compact_partition(s3, "bucket", self._PREFIX)

        assert result == {"files_compacted": 1, "files_delete_failed": 0}
        # The pre-existing compacted file was never read or deleted.
        assert old_compacted_key in s3.objects
        assert new_flight_key not in s3.objects

    def test_unreadable_file_is_not_deleted(self):
        s3 = _FakeS3()
        good_key = f"{self._PREFIX}good-0000-7000-8000-000000000000.parquet"
        bad_key = f"{self._PREFIX}bad-0000-7000-8000-000000000000.parquet"
        s3.objects[good_key] = _make_parquet_bytes(_make_row())
        s3.objects[bad_key] = b"not a real parquet file"
        s3.fail_get_for = {bad_key}

        result = compact_partition(s3, "bucket", self._PREFIX)

        assert result == {"files_compacted": 1, "files_delete_failed": 0}
        assert bad_key in s3.objects
        assert good_key not in s3.objects

    def test_delete_failure_is_reported_but_file_already_compacted(self):
        s3 = _FakeS3()
        key = f"{self._PREFIX}0198abcd-0000-7000-8000-000000000000.parquet"
        s3.objects[key] = _make_parquet_bytes(_make_row())
        s3.fail_delete_for = {key}

        result = compact_partition(s3, "bucket", self._PREFIX)

        assert result == {"files_compacted": 1, "files_delete_failed": 1}
        # Source lingers because the delete failed, but the compacted
        # output already contains its row -- reported, not silently lost.
        assert key in s3.objects


# ---------------------------------------------------------------------------
# publish_completion_stats
# ---------------------------------------------------------------------------

class TestPublishCompletionStats:
    _base_topic = f"{MQTT_ROOT}/statistic"

    def _setup_mock_client(self):
        mock_client = MagicMock()

        def fake_connect(host, port, keepalive):
            mock_client.on_connect(mock_client, None, None, 0, None)

        mock_client.connect.side_effect = fake_connect
        return mock_client

    def test_no_mqtt_config_skips(self):
        mc = self._setup_mock_client()
        with patch("archive_compaction_main.mqtt.Client", return_value=mc):
            publish_completion_stats({}, 0, 0, "success")
        mc.connect.assert_not_called()

    def test_publishes_all_stats(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("archive_compaction_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 5, 1, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list
                 if not c.args[0].startswith("homeassistant/")}
        assert calls[f"{self._base_topic}/files_compacted"] == "5"
        assert calls[f"{self._base_topic}/files_delete_failed"] == "1"
        assert calls[f"{self._base_topic}/last_run_status"] == "success"
        assert f"{self._base_topic}/last_run_at" in calls

    def test_stat_topics_retained(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("archive_compaction_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 5, 0, "success")
        stat_calls = [c for c in mc.publish.call_args_list
                      if c.args[0].startswith(self._base_topic)]
        assert len(stat_calls) == 4
        for call in stat_calls:
            assert call.kwargs.get("retain") is True

    def test_failure_status_published(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("archive_compaction_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list
                 if not c.args[0].startswith("homeassistant/")}
        assert calls[f"{self._base_topic}/last_run_status"] == "failure"
