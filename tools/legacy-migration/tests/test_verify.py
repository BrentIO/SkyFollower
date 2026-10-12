"""Tests for verify.py -- cache/resume, four-way reconciliation, accepted losses."""

from __future__ import annotations

import argparse
import io
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import verify
from shared.archive_index import PARQUET_INDEX_SCHEMA

DAY = "2024-05-31"


def _dt(day: str, hour: int = 12) -> datetime:
    y, m, d = map(int, day.split("-"))
    return datetime(y, m, d, hour, tzinfo=timezone.utc)


def _parquet_bytes(keys_and_days: list[tuple[str, str]], schema=PARQUET_INDEX_SCHEMA) -> bytes:
    rows = [
        {
            "icao_hex": "abc123", "registration": "", "type_designator": "", "military": False,
            "operator_designator": "", "ident": "", "first_message": _dt(day), "last_message": _dt(day),
            "s3_key": key,
        }
        for key, day in keys_and_days
    ]
    sink = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), sink)
    return sink.getvalue()


def _dest_key(doc_id: str, day: str = DAY) -> str:
    return "flights/" + day.replace("-", "/") + f"/{doc_id}.json.gz"


def _index_key(day: str = DAY) -> str:
    return "index/year=" + day[:4] + "/month=" + day[5:7] + "/day=" + day[8:] + "/legacy-migration.parquet"


class _FakeCollection:
    def __init__(self, docs: list[dict], fail_after: int | None = None):
        self.docs = docs
        self.find_calls = 0
        self.skips: list[int] = []
        self._fail_after = fail_after

    def find(self, query, projection=None, skip=0, no_cursor_timeout=False, batch_size=0):
        self.find_calls += 1
        self.skips.append(skip)
        return _FakeCursor(self.docs[skip:], self._fail_after)

    def estimated_document_count(self):
        return len(self.docs)


class _FakeCursor:
    def __init__(self, rows, fail_after):
        self._rows = rows
        self._fail_after = fail_after

    def __iter__(self):
        for index, row in enumerate(self._rows):
            if self._fail_after is not None and index >= self._fail_after:
                raise RuntimeError("simulated crash")
            yield row

    def close(self):
        pass


class _FakeBody:
    def __init__(self, data: bytes):
        self._data = data

    def read(self):
        return self._data


class _FakeS3:
    def __init__(self, source: dict, dest: dict, files: dict, fail_after_pages: int | None = None):
        # source/dest: key -> (etag, size); files: key -> bytes (Parquet bodies)
        self.buckets = {"src": source, "dst": dest}
        self.files = files
        self.list_calls: list[tuple] = []
        self.get_calls: list[str] = []
        self._fail_after = fail_after_pages

    def list_objects_v2(self, Bucket, MaxKeys, Prefix="", StartAfter=""):
        if self._fail_after is not None and len(self.list_calls) >= self._fail_after:
            raise RuntimeError("simulated crash")
        self.list_calls.append((Bucket, Prefix, StartAfter))
        keys = sorted(k for k in self.buckets[Bucket] if k.startswith(Prefix) and k > StartAfter)
        page = keys[:MaxKeys]
        return {
            "Contents": [
                {"Key": k, "ETag": self.buckets[Bucket][k][0], "Size": self.buckets[Bucket][k][1]} for k in page
            ],
            "IsTruncated": len(keys) > MaxKeys,
        }

    def get_object(self, Bucket, Key):
        self.get_calls.append(Key)
        return {"Body": _FakeBody(self.files[Key])}


def _doc(doc_id: str, day: str = DAY, migrated: bool = True, **extra) -> dict:
    doc = {
        "_id": doc_id,
        "first_message": _dt(day),
        "last_message": _dt(day, 13),
        "total_messages": 5,
        "aircraft": {"icao_hex": "abc123"},
    }
    if migrated:
        doc["migrated"] = _dt(day, 20)
    doc.update(extra)
    return doc


def _world(ids=("id1", "id2"), day: str = DAY):
    """A fully consistent migration of `ids` on `day`."""
    docs = [_doc(i, day) for i in ids]
    source = {f"{i}.gz": (f'"e-{i}"', 100) for i in ids}
    dest = {_dest_key(i, day): (f'"e-{i}"', 100) for i in ids}
    dest[_index_key(day)] = ('"idx"', 10)
    files = {_index_key(day): _parquet_bytes([(_dest_key(i, day), day) for i in ids])}
    return docs, source, dest, files


@pytest.fixture(autouse=True)
def _small_pages(monkeypatch):
    monkeypatch.setattr(verify, "PAGE_SIZE", 2)
    monkeypatch.setattr(verify, "MONGO_BATCH", 2)
    monkeypatch.setattr(verify, "MONGO_RESUME_MARGIN", 1)


def _args(tmp_path, **overrides) -> argparse.Namespace:
    values = dict(
        start_date=DAY, end_date=DAY, cache_dir=str(tmp_path), refresh=[], snapshot_only=False,
        accepted_loss_file=None, workers=2,
    )
    values.update(overrides)
    return argparse.Namespace(**values)


def _wire(monkeypatch, collection, s3):
    monkeypatch.setattr(verify, "connect_mongo", lambda cfg: collection)
    monkeypatch.setattr(verify, "build_s3_client", lambda: s3)
    monkeypatch.setattr(
        verify, "load_config",
        lambda *blocks: {
            "mongo": {},
            "legacy_migration_s3": {"source_bucket": "src", "dest_bucket": "dst"},
        },
    )


def _report(tmp_path) -> str:
    return (tmp_path / verify.REPORT_FILENAME).read_text()


class TestReconciliation:
    def test_clean_when_all_four_sets_agree(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        verify.run(_args(tmp_path))  # no raise

    def test_live_pipeline_objects_are_not_outliers(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        dest[_dest_key("live-1")] = ('"live"', 5)
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        verify.run(_args(tmp_path))  # no raise

    def test_missing_from_destination_is_named_by_id(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        del dest[_dest_key("id2")]
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path))
        assert "missing_from_destination\tid2" in _report(tmp_path)
        assert "parquet_not_in_destination" in _report(tmp_path)

    def test_missing_from_source(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        del source["id1.gz"]
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path))
        assert "missing_from_source\tid1" in _report(tmp_path)

    def test_missing_from_parquet(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        files[_index_key()] = _parquet_bytes([(_dest_key("id1"), DAY)])
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path))
        assert "missing_from_parquet\tid2" in _report(tmp_path)

    def test_parquet_row_without_mongo_flight(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        files[_index_key()] = _parquet_bytes([(_dest_key(i), DAY) for i in ("id1", "id2", "ghost")])
        dest[_dest_key("ghost")] = ('"g"', 1)
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path))
        assert "parquet_not_in_mongo" in _report(tmp_path)

    def test_orphan_source_object(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        source["stray.gz"] = ('"s"', 1)
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path))
        assert "source_not_in_mongo\tstray" in _report(tmp_path)

    def test_etag_mismatch(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        dest[_dest_key("id1")] = ('"different"', 100)
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path))
        assert "etag_or_size_mismatch" in _report(tmp_path)

    def test_size_mismatch_with_matching_etag(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        dest[_dest_key("id1")] = ('"e-id1"', 999)
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path))
        assert "etag_or_size_mismatch" in _report(tmp_path)

    def test_whole_day_missing_while_neighbours_are_fine(self, monkeypatch, tmp_path):
        days = ["2024-05-30", "2024-05-31", "2024-06-01"]
        docs, source, dest, files = [], {}, {}, {}
        for day in days:
            d, s, de, f = _world(ids=(f"id-{day}",), day=day)
            docs += d
            source.update(s)
            dest.update(de)
            files.update(f)
        for key in [k for k in dest if "2024/05/31" in k or "05/day=31" in k]:
            del dest[key]
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path, start_date="2024-05-30", end_date="2024-06-01"))
        report = _report(tmp_path)
        assert "day_missing_from_destination\t2024-05-31" in report
        assert "day_missing_index_file\t2024-05-31" in report

    def test_index_file_for_day_with_no_mongo_flights_is_an_orphan(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        other = "2024-05-30"
        dest[_index_key(other)] = ('"x"', 1)
        files[_index_key(other)] = _parquet_bytes([])
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path, start_date=other, end_date=DAY))
        assert "index_file_without_mongo_flights\t2024-05-30" in _report(tmp_path)

    def test_parquet_file_with_wrong_schema_or_day_is_invalid(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        files[_index_key()] = _parquet_bytes([(_dest_key(i), "2024-05-30") for i in ("id1", "id2")])
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path))
        assert "index_file_invalid" in _report(tmp_path)

    def test_unreadable_parquet_is_invalid(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        files[_index_key()] = b"not parquet"
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path))
        assert "index_file_invalid" in _report(tmp_path)


class TestMongoAccounting:
    def test_unmigrated_and_out_of_range_documents_fail_unless_accepted(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        docs.append(_doc("tail", migrated=False))
        docs.append(_doc("epoch", day="1970-01-01"))
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path))
        report = _report(tmp_path)
        assert "mongo_not_migrated\ttail" in report
        assert "mongo_migrated_out_of_range\tepoch" in report

    def test_accepted_losses_do_not_fail_the_run(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        docs.append(_doc("epoch", day="1970-01-01"))
        docs.append(_doc("nohex", aircraft={}))
        source["epoch.gz"] = ('"x"', 1)
        accepted = tmp_path / "accepted.txt"
        accepted.write_text("epoch  1970 first_message\nnohex missing icao_hex  # comment\n")
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        verify.run(_args(tmp_path, accepted_loss_file=str(accepted)))  # no raise

    def test_document_failing_a_guard_is_reported_with_its_reason(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        docs.append(_doc("zero", total_messages=0))
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path))
        assert "zero messages recorded" in _report(tmp_path)

    def test_accepted_loss_file_defaults_to_cache_dir(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        docs.append(_doc("nohex", aircraft={}))
        (tmp_path / verify.ACCEPTED_LOSS_FILENAME).write_text("nohex missing icao_hex\n")
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        verify.run(_args(tmp_path))  # no raise


class TestCache:
    def test_second_run_makes_no_remote_calls(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        collection, s3 = _FakeCollection(docs), _FakeS3(source, dest, files)
        _wire(monkeypatch, collection, s3)
        verify.run(_args(tmp_path))
        mongo_calls, list_calls, get_calls = collection.find_calls, len(s3.list_calls), len(s3.get_calls)

        verify.run(_args(tmp_path))
        assert collection.find_calls == mongo_calls
        assert len(s3.list_calls) == list_calls
        assert len(s3.get_calls) == get_calls

    def test_snapshot_only_builds_mongo_cache_and_stops(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        s3 = _FakeS3(source, dest, files)
        _wire(monkeypatch, _FakeCollection(docs), s3)
        verify.run(_args(tmp_path, snapshot_only=True))
        assert s3.list_calls == []

    def test_full_run_after_snapshot_does_not_reconnect_to_mongo(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        collection = _FakeCollection(docs)
        _wire(monkeypatch, collection, _FakeS3(source, dest, files))
        verify.run(_args(tmp_path, snapshot_only=True))
        calls = collection.find_calls

        def _no_mongo(cfg):
            raise AssertionError("Mongo must not be contacted once the snapshot is cached")

        monkeypatch.setattr(verify, "connect_mongo", _no_mongo)
        verify.run(_args(tmp_path))
        assert collection.find_calls == calls

    def test_killed_listing_resumes_without_relisting_finished_pages(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world(ids=("id1", "id2", "id3", "id4", "id5"))
        crashing = _FakeS3(source, dest, files, fail_after_pages=4)
        _wire(monkeypatch, _FakeCollection(docs), crashing)
        with pytest.raises(RuntimeError):
            verify.run(_args(tmp_path, workers=1))

        resumed = _FakeS3(source, dest, files)
        _wire(monkeypatch, _FakeCollection(docs), resumed)
        verify.run(_args(tmp_path, workers=1))  # no raise
        assert crashing.list_calls
        assert not set(crashing.list_calls) & set(resumed.list_calls)

    def test_killed_mongo_snapshot_resumes_near_where_it_stopped(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world(ids=("id1", "id2", "id3", "id4", "id5"))
        _wire(monkeypatch, _FakeCollection(docs, fail_after=4), _FakeS3(source, dest, files))
        with pytest.raises(RuntimeError):
            verify.run(_args(tmp_path))

        collection = _FakeCollection(docs)
        _wire(monkeypatch, collection, _FakeS3(source, dest, files))
        verify.run(_args(tmp_path))  # no raise
        assert collection.skips == [3]

    def test_snapshot_count_mismatch_fails(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        collection = _FakeCollection(docs)
        collection.estimated_document_count = lambda: len(docs) + 1
        _wire(monkeypatch, collection, _FakeS3(source, dest, files))
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path))

    def test_refresh_mongo_rescans_from_the_start(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        collection = _FakeCollection(docs)
        _wire(monkeypatch, collection, _FakeS3(source, dest, files))
        verify.run(_args(tmp_path))
        verify.run(_args(tmp_path, refresh=["mongo"]))
        assert collection.find_calls == 2 and collection.skips == [0, 0]

    def test_refresh_refetches_only_that_dataset(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        s3 = _FakeS3(source, dest, files)
        _wire(monkeypatch, _FakeCollection(docs), s3)
        verify.run(_args(tmp_path))
        before = list(s3.list_calls)

        verify.run(_args(tmp_path, refresh=["source"]))
        new_calls = s3.list_calls[len(before):]
        assert new_calls and all(c[0] == "src" for c in new_calls)

    def test_cache_for_another_bucket_is_refused(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        verify.run(_args(tmp_path))
        monkeypatch.setattr(
            verify, "load_config",
            lambda *blocks: {"mongo": {}, "legacy_migration_s3": {"source_bucket": "other", "dest_bucket": "dst"}},
        )
        with pytest.raises(SystemExit):
            verify.run(_args(tmp_path))


class TestDeferredIndexes:
    @staticmethod
    def _index_names(cache) -> set:
        return {r[0] for r in cache.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}

    def test_indexes_exist_after_a_full_run(self, monkeypatch, tmp_path):
        docs, source, dest, files = _world()
        _wire(monkeypatch, _FakeCollection(docs), _FakeS3(source, dest, files))
        verify.run(_args(tmp_path))
        cache = verify.Cache(str(tmp_path / verify.DB_FILENAME))
        assert {"dest_objects_id", "parquet_rows_id"} <= self._index_names(cache)

    def test_existing_index_is_dropped_before_listing_and_not_rebuilt_until_the_end(self, tmp_path):
        docs, source, dest, files = _world()
        cache = verify.Cache(str(tmp_path / "old.sqlite"))
        cache.execute("CREATE INDEX dest_objects_id ON dest_objects (id)")
        verify._list_dest(_FakeS3(source, dest, files), "dst", cache, 2, DAY, DAY)
        assert "dest_objects_id" not in self._index_names(cache)
        cache.build_indexes()
        assert "dest_objects_id" in self._index_names(cache)

    def test_finished_listing_keeps_its_index(self, tmp_path):
        docs, source, dest, files = _world()
        cache = verify.Cache(str(tmp_path / "done.sqlite"))
        verify._list_dest(_FakeS3(source, dest, files), "dst", cache, 2, DAY, DAY)
        cache.build_indexes()
        verify._list_dest(_FakeS3(source, dest, files), "dst", cache, 2, DAY, DAY)
        assert "dest_objects_id" in self._index_names(cache)
