"""
Tests for the archive-compaction daily Parquet index compaction job.

Covers:
- Target partition date math (day before yesterday, UTC)
- Per-flight vs. already-compacted file classification
- Compaction: merge, write, delete-only-what-was-included
- Unreadable / late-arriving files are left alone, not deleted
- Batch delete error handling
- Flight/index parity checking
- Watermark read/write
- The watermark-driven catch-up loop, including stopping on a mismatch
- MQTT completion stats
"""

from __future__ import annotations

import importlib.util
import io
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# ---------------------------------------------------------------------------
# Module import helper (archive-compaction/ contains a hyphen, so it can't
# be imported as a normal package -- same workaround used by every other
# hyphenated component's tests, e.g. runners/us-faa-registry).
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOL_DIR = os.path.dirname(_HERE)  # archive-compaction/
_REPO_ROOT = os.path.abspath(os.path.join(_TOOL_DIR, ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from shared.index_cache import local_index_path, write_local_index  # noqa: E402


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
index_prefix_for_date = _mod.index_prefix_for_date
flights_prefix_for_date = _mod.flights_prefix_for_date
is_per_flight_file = _mod.is_per_flight_file
build_compacted_key = _mod.build_compacted_key
delete_keys = _mod.delete_keys
compact_partition = _mod.compact_partition
read_parquet_table = _mod.read_parquet_table
check_date_parity = _mod.check_date_parity
read_watermark = _mod.read_watermark
write_watermark = _mod.write_watermark
run_compaction = _mod.run_compaction
publish_completion_stats = _mod.publish_completion_stats
_publish_ha_autodiscovery = _mod._publish_ha_autodiscovery
main = _mod.main
MQTT_ROOT = _mod.MQTT_ROOT
_PARQUET_INDEX_SCHEMA = _mod._PARQUET_INDEX_SCHEMA
_WATERMARK_KEY = _mod._WATERMARK_KEY


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
        self.get_calls: list[str] = []

    def get_paginator(self, operation_name: str):
        assert operation_name == "list_objects_v2"
        return _FakePaginator(self)

    def get_object(self, Bucket, Key):
        self.get_calls.append(Key)
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
        failed, deleted = delete_keys(s3, "bucket", ["a", "b"])
        assert failed == 0
        assert set(s3.deleted) == {"a", "b"}
        assert set(deleted) == {"a", "b"}

    def test_reports_partial_failures(self):
        s3 = _FakeS3()
        s3.objects = {"a": b"x", "b": b"y"}
        s3.fail_delete_for = {"b"}
        failed, deleted = delete_keys(s3, "bucket", ["a", "b"])
        assert failed == 1
        assert s3.deleted == ["a"]
        assert "b" in s3.objects
        assert deleted == ["a"]

    def test_whole_batch_call_raising_counts_all_as_failed(self):
        s3 = _FakeS3()

        def _raise(**kwargs):
            raise RuntimeError("boom")

        s3.delete_objects = _raise
        failed, deleted = delete_keys(s3, "bucket", ["a", "b", "c"])
        assert failed == 3
        assert deleted == []


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
# Local index cache: read_parquet_table / compact_partition local-first
# reads and post-compaction local cleanup
# ---------------------------------------------------------------------------

class TestLocalIndexCachePreferredOnRead:
    _PREFIX = "index/year=2026/month=07/day=23/"

    def test_reads_local_copy_without_calling_s3(self, tmp_path):
        s3 = _FakeS3()
        key = f"{self._PREFIX}0198abcd-0000-7000-8000-000000000000.parquet"
        payload = _make_parquet_bytes(_make_row(icao_hex="LOCAL0"))
        write_local_index(key, payload, base_dir=str(tmp_path))
        # Deliberately not in s3.objects -- if this reads from S3 at all,
        # get_object raises KeyError and the test fails loudly instead of
        # silently reading the wrong (empty) source.

        table = read_parquet_table(s3, "bucket", key, str(tmp_path))

        assert table.num_rows == 1
        assert table.column("icao_hex").to_pylist() == ["LOCAL0"]
        assert s3.get_calls == []

    def test_falls_back_to_s3_when_local_copy_missing(self, tmp_path):
        s3 = _FakeS3()
        key = f"{self._PREFIX}0198abcd-0000-7000-8000-000000000000.parquet"
        s3.objects[key] = _make_parquet_bytes(_make_row(icao_hex="FROMS3"))
        # No local file written under tmp_path for this key.

        table = read_parquet_table(s3, "bucket", key, str(tmp_path))

        assert table.column("icao_hex").to_pylist() == ["FROMS3"]
        assert s3.get_calls == [key]


class TestCompactPartitionLocalCache:
    _PREFIX = "index/year=2026/month=07/day=23/"

    def test_compacts_from_local_cache_with_no_s3_gets(self, tmp_path):
        s3 = _FakeS3()
        keys = [f"{self._PREFIX}{i:08x}-0000-7000-8000-000000000000.parquet" for i in range(3)]
        for i, key in enumerate(keys):
            payload = _make_parquet_bytes(_make_row(icao_hex=f"AAAA0{i}"))
            # Present in S3 (so listing finds it, matching reality) but
            # also cached locally -- get_calls staying empty below proves
            # the read path never issues a GetObject when the cache is warm.
            s3.objects[key] = payload
            write_local_index(key, payload, base_dir=str(tmp_path))

        result = compact_partition(s3, "bucket", self._PREFIX, str(tmp_path))

        assert result == {"files_compacted": 3, "files_delete_failed": 0}
        assert s3.get_calls == []

    def test_local_cleanup_after_successful_compaction(self, tmp_path):
        s3 = _FakeS3()
        key = f"{self._PREFIX}0198abcd-0000-7000-8000-000000000000.parquet"
        payload = _make_parquet_bytes(_make_row())
        s3.objects[key] = payload
        write_local_index(key, payload, base_dir=str(tmp_path))
        assert os.path.exists(local_index_path(key, str(tmp_path)))

        result = compact_partition(s3, "bucket", self._PREFIX, str(tmp_path))

        assert result == {"files_compacted": 1, "files_delete_failed": 0}
        # S3 source deleted, and its local cache copy cleaned up right
        # alongside it -- the shared volume must not grow unbounded.
        assert key not in s3.objects
        assert not os.path.exists(local_index_path(key, str(tmp_path)))

    def test_local_copy_kept_when_s3_delete_fails(self, tmp_path):
        """A post-inclusion S3 delete failure leaves the source object (and
        therefore a duplicate row) in place -- the local cache copy for
        that specific key should also survive, since only confirmed S3
        deletions trigger local cleanup."""
        s3 = _FakeS3()
        key = f"{self._PREFIX}0198abcd-0000-7000-8000-000000000000.parquet"
        payload = _make_parquet_bytes(_make_row())
        s3.objects[key] = payload
        s3.fail_delete_for = {key}
        write_local_index(key, payload, base_dir=str(tmp_path))

        result = compact_partition(s3, "bucket", self._PREFIX, str(tmp_path))

        assert result == {"files_compacted": 1, "files_delete_failed": 1}
        assert key in s3.objects
        assert os.path.exists(local_index_path(key, str(tmp_path)))

    def test_missing_local_copy_falls_back_and_still_compacts(self, tmp_path):
        """A row never cached locally (a partial/failed write on
        archive-processor's side, or one written before the cache existed)
        must not break compaction -- it just costs one S3 GetObject."""
        s3 = _FakeS3()
        key = f"{self._PREFIX}0198abcd-0000-7000-8000-000000000000.parquet"
        s3.objects[key] = _make_parquet_bytes(_make_row())
        # No local write for this key.

        result = compact_partition(s3, "bucket", self._PREFIX, str(tmp_path))

        assert result == {"files_compacted": 1, "files_delete_failed": 0}
        assert s3.get_calls == [key]
        assert key not in s3.objects


# ---------------------------------------------------------------------------
# check_date_parity
# ---------------------------------------------------------------------------

def _flight_key(d: date, uuid_str: str, icao_hex: str = "A1B2C3", ident: str = "DAL123") -> str:
    return f"{flights_prefix_for_date(d)}{icao_hex}_{ident}_{uuid_str}.json.gz"


def _index_key(d: date, uuid_str: str) -> str:
    return f"{index_prefix_for_date(d)}{uuid_str}.parquet"


class TestCheckDateParity:
    _DATE = date(2026, 7, 23)

    def test_clean_match_returns_empty_set(self):
        s3 = _FakeS3()
        s3.objects[_flight_key(self._DATE, "uuid-a")] = b"flight-json-gz"
        s3.objects[_index_key(self._DATE, "uuid-a")] = _make_parquet_bytes(_make_row())

        assert check_date_parity(s3, "bucket", self._DATE) == set()

    def test_missing_index_row_detected(self):
        s3 = _FakeS3()
        s3.objects[_flight_key(self._DATE, "uuid-a")] = b"flight-json-gz"
        s3.objects[_flight_key(self._DATE, "uuid-b")] = b"flight-json-gz"
        s3.objects[_index_key(self._DATE, "uuid-a")] = _make_parquet_bytes(_make_row())
        # uuid-b's index row never landed.

        assert check_date_parity(s3, "bucket", self._DATE) == {"uuid-b"}

    def test_orphaned_index_row_not_flagged(self):
        # An index row with no matching flight object isn't a reason to
        # block compaction -- only "flight exists, index doesn't" matters.
        s3 = _FakeS3()
        s3.objects[_index_key(self._DATE, "uuid-orphan")] = _make_parquet_bytes(_make_row())

        assert check_date_parity(s3, "bucket", self._DATE) == set()

    def test_already_compacted_file_ignored_on_index_side(self):
        s3 = _FakeS3()
        s3.objects[_flight_key(self._DATE, "uuid-a")] = b"flight-json-gz"
        s3.objects[f"{index_prefix_for_date(self._DATE)}compacted-existing.parquet"] = (
            _make_parquet_bytes(_make_row())
        )
        # uuid-a still has no per-flight index row of its own.

        assert check_date_parity(s3, "bucket", self._DATE) == {"uuid-a"}

    def test_no_flights_no_mismatch(self):
        s3 = _FakeS3()
        assert check_date_parity(s3, "bucket", self._DATE) == set()


# ---------------------------------------------------------------------------
# Watermark
# ---------------------------------------------------------------------------

class TestWatermark:
    def test_read_missing_watermark_returns_none(self):
        s3 = _FakeS3()
        assert read_watermark(s3, "bucket") is None

    def test_read_write_roundtrip(self):
        s3 = _FakeS3()
        write_watermark(s3, "bucket", date(2026, 7, 22))
        assert read_watermark(s3, "bucket") == date(2026, 7, 22)

    def test_write_uses_sibling_prefix_not_nested_in_flights_or_index(self):
        s3 = _FakeS3()
        write_watermark(s3, "bucket", date(2026, 7, 22))
        assert _WATERMARK_KEY in s3.objects
        assert not _WATERMARK_KEY.startswith("flights/")
        assert not _WATERMARK_KEY.startswith("index/")

    def test_read_corrupt_watermark_returns_none(self):
        s3 = _FakeS3()
        s3.objects[_WATERMARK_KEY] = b"not json"
        assert read_watermark(s3, "bucket") is None


# ---------------------------------------------------------------------------
# run_compaction (watermark-driven catch-up loop)
# ---------------------------------------------------------------------------

class TestRunCompaction:
    def _seed_clean_date(self, s3: _FakeS3, d: date, uuid_str: str = "uuid-a") -> None:
        s3.objects[_flight_key(d, uuid_str)] = b"flight-json-gz"
        s3.objects[_index_key(d, uuid_str)] = _make_parquet_bytes(_make_row())

    def test_first_run_with_no_watermark_compacts_only_the_cutoff_date(self):
        s3 = _FakeS3()
        now = datetime(2026, 7, 25, 5, 0, tzinfo=timezone.utc)
        cutoff = _mod._cutoff_date(now)  # 2026-07-23
        self._seed_clean_date(s3, cutoff)

        result = run_compaction(s3, "bucket", now)

        assert result["days_compacted"] == 1
        assert result["files_compacted"] == 1
        assert result["last_compacted_date"] == cutoff
        assert result["mismatch_date"] is None
        assert result["mismatch_uuids"] == set()
        assert read_watermark(s3, "bucket") == cutoff

    def test_catchup_compacts_every_backlogged_day_and_advances_watermark(self):
        s3 = _FakeS3()
        now = datetime(2026, 7, 25, 5, 0, tzinfo=timezone.utc)
        cutoff = _mod._cutoff_date(now)  # 2026-07-23
        write_watermark(s3, "bucket", cutoff - timedelta(days=3))
        for offset in (2, 1, 0):
            self._seed_clean_date(s3, cutoff - timedelta(days=offset), uuid_str=f"uuid-{offset}")

        result = run_compaction(s3, "bucket", now)

        assert result["days_compacted"] == 3
        assert result["files_compacted"] == 3
        assert result["last_compacted_date"] == cutoff
        assert read_watermark(s3, "bucket") == cutoff

    def test_already_caught_up_is_a_noop(self):
        s3 = _FakeS3()
        now = datetime(2026, 7, 25, 5, 0, tzinfo=timezone.utc)
        cutoff = _mod._cutoff_date(now)
        write_watermark(s3, "bucket", cutoff)

        result = run_compaction(s3, "bucket", now)

        assert result["days_compacted"] == 0
        assert result["files_compacted"] == 0
        assert result["last_compacted_date"] == cutoff
        assert result["mismatch_uuids"] == set()

    def test_multi_day_backlog_reads_and_cleans_up_local_cache_per_date(self, tmp_path):
        """A local cache organized by date (mirroring index/year=/month=/
        day=/) must serve every backlogged date a catch-up run touches, and
        each date's local copies are cleaned up as that date compacts --
        not deferred until the whole run finishes."""
        s3 = _FakeS3()
        now = datetime(2026, 7, 25, 5, 0, tzinfo=timezone.utc)
        cutoff = _mod._cutoff_date(now)  # 2026-07-23
        write_watermark(s3, "bucket", cutoff - timedelta(days=3))

        local_keys = []
        for offset in (2, 1, 0):
            d = cutoff - timedelta(days=offset)
            uuid_str = f"uuid-{offset}"
            self._seed_clean_date(s3, d, uuid_str=uuid_str)
            index_key = _index_key(d, uuid_str)
            local_keys.append(index_key)
            # Still present in S3 (parity checking and the post-compaction
            # delete both stay S3-based) but also cached locally, so the
            # read step below can be proven to prefer the local copy via
            # s3.get_calls staying empty.
            write_local_index(index_key, s3.objects[index_key], base_dir=str(tmp_path))

        result = run_compaction(s3, "bucket", now, str(tmp_path))

        assert result["days_compacted"] == 3
        assert result["files_compacted"] == 3
        # The only S3 GetObject calls made are for the watermark itself --
        # never for any of the per-flight index rows, which all came from
        # the local cache.
        assert set(s3.get_calls) <= {_WATERMARK_KEY}
        assert not any(k in s3.get_calls for k in local_keys)
        # Every backlogged date's local copy is gone once its S3 source is
        # confirmed deleted by the compaction that just ran.
        for index_key in local_keys:
            assert not os.path.exists(local_index_path(index_key, str(tmp_path)))

    def test_mismatch_stops_the_loop_and_leaves_watermark_behind(self):
        s3 = _FakeS3()
        now = datetime(2026, 7, 25, 5, 0, tzinfo=timezone.utc)
        cutoff = _mod._cutoff_date(now)  # 2026-07-23
        start_watermark = cutoff - timedelta(days=2)
        write_watermark(s3, "bucket", start_watermark)

        # Day 1 (watermark+1) is clean; day 2 (cutoff) has a flight with no
        # index row -- the loop should compact day 1, then stop at day 2
        # without touching the watermark any further.
        self._seed_clean_date(s3, start_watermark + timedelta(days=1), uuid_str="uuid-clean")
        s3.objects[_flight_key(cutoff, "uuid-missing")] = b"flight-json-gz"

        result = run_compaction(s3, "bucket", now)

        assert result["days_compacted"] == 1
        assert result["last_compacted_date"] == start_watermark + timedelta(days=1)
        assert result["mismatch_date"] == cutoff
        assert result["mismatch_uuids"] == {"uuid-missing"}
        # Watermark persisted in S3 matches the returned value -- the stuck
        # date and everything after it stays uncompacted for the next run.
        assert read_watermark(s3, "bucket") == start_watermark + timedelta(days=1)

    def test_mismatch_on_first_backlogged_day_compacts_nothing(self):
        s3 = _FakeS3()
        now = datetime(2026, 7, 25, 5, 0, tzinfo=timezone.utc)
        cutoff = _mod._cutoff_date(now)
        write_watermark(s3, "bucket", cutoff - timedelta(days=1))
        s3.objects[_flight_key(cutoff, "uuid-missing")] = b"flight-json-gz"

        result = run_compaction(s3, "bucket", now)

        assert result["days_compacted"] == 0
        assert result["files_compacted"] == 0
        assert result["mismatch_date"] == cutoff
        assert result["mismatch_uuids"] == {"uuid-missing"}
        assert read_watermark(s3, "bucket") == cutoff - timedelta(days=1)


# ---------------------------------------------------------------------------
# publish_completion_stats
# ---------------------------------------------------------------------------

class TestPublishCompletionStats:
    _base_topic = f"{MQTT_ROOT}/statistic"

    def _make_result(self, **overrides) -> dict:
        result = {
            "files_compacted": 0,
            "files_delete_failed": 0,
            "days_compacted": 0,
            "last_compacted_date": None,
            "mismatch_date": None,
            "mismatch_uuids": set(),
        }
        result.update(overrides)
        return result

    def _setup_mock_client(self):
        mock_client = MagicMock()

        def fake_connect(host, port, keepalive):
            mock_client.on_connect(mock_client, None, None, 0, None)

        mock_client.connect.side_effect = fake_connect
        return mock_client

    def test_no_mqtt_config_skips(self):
        mc = self._setup_mock_client()
        with patch("archive_compaction_main.mqtt.Client", return_value=mc):
            publish_completion_stats({}, self._make_result(), "success")
        mc.connect.assert_not_called()

    def test_publishes_all_stats(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        result = self._make_result(
            files_compacted=5,
            files_delete_failed=1,
            days_compacted=2,
            last_compacted_date=date(2026, 7, 23),
        )
        with patch("archive_compaction_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, result, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list
                 if not c.args[0].startswith("homeassistant/")}
        assert calls[f"{self._base_topic}/files_compacted"] == "5"
        assert calls[f"{self._base_topic}/files_delete_failed"] == "1"
        assert calls[f"{self._base_topic}/days_compacted"] == "2"
        assert calls[f"{self._base_topic}/last_compacted_date"] == "2026-07-23"
        assert calls[f"{self._base_topic}/mismatch_date"] == ""
        assert calls[f"{self._base_topic}/mismatch_uuids"] == ""
        assert calls[f"{self._base_topic}/last_run_status"] == "Success"
        assert f"{self._base_topic}/last_run_at" in calls

    def test_publishes_version(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch.dict(os.environ, {"VERSION": "2026.08.01"}):
            with patch("archive_compaction_main.mqtt.Client", return_value=mc):
                with patch("time.sleep"):
                    publish_completion_stats(cfg, self._make_result(), "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list
                 if not c.args[0].startswith("homeassistant/")}
        assert calls[f"{self._base_topic}/version"] == "2026.08.01"

    def test_publishes_version_dev_fallback_when_unset(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("VERSION", None)
            with patch("archive_compaction_main.mqtt.Client", return_value=mc):
                with patch("time.sleep"):
                    publish_completion_stats(cfg, self._make_result(), "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list
                 if not c.args[0].startswith("homeassistant/")}
        assert calls[f"{self._base_topic}/version"] == "dev"

    def test_publishes_mismatch_fields(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        result = self._make_result(
            days_compacted=1,
            last_compacted_date=date(2026, 7, 22),
            mismatch_date=date(2026, 7, 23),
            mismatch_uuids={"uuid-b", "uuid-a"},
        )
        with patch("archive_compaction_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, result, "mismatch")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list
                 if not c.args[0].startswith("homeassistant/")}
        assert calls[f"{self._base_topic}/mismatch_date"] == "2026-07-23"
        assert calls[f"{self._base_topic}/mismatch_uuids"] == "uuid-a,uuid-b"
        assert calls[f"{self._base_topic}/last_run_status"] == "Mismatch"

    def test_stat_topics_retained(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("archive_compaction_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, self._make_result(files_compacted=5), "success")
        stat_calls = [c for c in mc.publish.call_args_list
                      if c.args[0].startswith(self._base_topic)]
        assert len(stat_calls) == 9
        for call in stat_calls:
            assert call.kwargs.get("retain") is True

    def test_failure_status_published(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("archive_compaction_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, self._make_result(), "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list
                 if not c.args[0].startswith("homeassistant/")}
        assert calls[f"{self._base_topic}/last_run_status"] == "Failure"


# ---------------------------------------------------------------------------
# _publish_ha_autodiscovery
# ---------------------------------------------------------------------------

class TestPublishHaAutodiscovery:
    def test_sensor_count_and_version_entry(self):
        mc = MagicMock()
        _publish_ha_autodiscovery(mc)
        configs = {
            c.args[0]: json.loads(c.args[1])
            for c in mc.publish.call_args_list
            if c.args[0].startswith("homeassistant/sensor/")
        }
        assert len(configs) == 9
        version_topic = "homeassistant/sensor/SkyFollower_archive_compaction_version/config"
        assert version_topic in configs
        version_config = configs[version_topic]
        assert version_config["name"] == "Archive Compaction Version"
        assert version_config["state_topic"] == f"{MQTT_ROOT}/statistic/version"
        assert version_config["icon"] == "mdi:tag"
        assert version_config["unique_id"] == "SkyFollower_archive_compaction_version"
