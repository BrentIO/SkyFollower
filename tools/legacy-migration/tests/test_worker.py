"""Tests for worker.py's process_day -- guard/copy/DLQ/index-row wiring."""

from __future__ import annotations

import io
import json
from datetime import datetime, timezone

import pyarrow.parquet as pq
from botocore.exceptions import ClientError

import worker
from shared.archive_index import PARQUET_INDEX_SCHEMA


def _doc(doc_id, **overrides):
    doc = {
        "_id": doc_id,
        "first_message": datetime(2024, 5, 31, 10, 0, tzinfo=timezone.utc),
        "last_message": datetime(2024, 5, 31, 10, 5, tzinfo=timezone.utc),
        "total_messages": 10,
        "aircraft": {"icao_hex": "A8AE7F", "registration": "N659DL"},
        "ident": "DAL659",
        "migrated": datetime(2024, 6, 1, tzinfo=timezone.utc),
    }
    doc.update(overrides)
    return doc


class _FakeCollection:
    def __init__(self, docs):
        self._docs = docs

    def find(self, query):
        return list(self._docs)


class _FakeChannel:
    def __init__(self):
        self.dlq = []

    def basic_publish(self, exchange, routing_key, body, properties=None):
        payload = json.loads(body)
        self.dlq.append((routing_key, payload["_id"], payload["reason"]))


def _doc_id_from_key(key: str) -> str:
    return key.rsplit("/", 1)[-1].split(".")[0]


class _FakeS3:
    """head_object reports success with a matching ETag once an object has
    been "copied" (tracked in self._copied); copy_object records the move.
    """

    def __init__(self, missing_source_for=(), etag_mismatch_for=()):
        self._missing_source_for = set(missing_source_for)
        self._etag_mismatch_for = set(etag_mismatch_for)
        self._copied: set[str] = set()
        self.put_calls = []

    def head_object(self, Bucket, Key):
        doc_id = _doc_id_from_key(Key)
        if Bucket == "src":
            if doc_id in self._missing_source_for:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "HeadObject")
            return {"ETag": '"same"', "ContentLength": 5}
        if doc_id not in self._copied:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        if doc_id in self._etag_mismatch_for:
            return {"ETag": '"different"', "ContentLength": 5}
        return {"ETag": '"same"', "ContentLength": 5}

    def copy_object(self, Bucket, Key, CopySource):
        self._copied.add(_doc_id_from_key(Key))

    def put_object(self, Bucket, Key, Body):
        self.put_calls.append((Bucket, Key, Body))


class TestProcessDay:
    def test_all_flights_migrated_and_index_written(self):
        docs = [_doc("id1"), _doc("id2", ident="DAL2")]
        collection = _FakeCollection(docs)
        s3 = _FakeS3()
        channel = _FakeChannel()

        worker.process_day(collection, s3, "src", "dst", channel, "2024-05-31")

        assert channel.dlq == []
        assert len(s3.put_calls) == 1
        bucket, key, body = s3.put_calls[0]
        assert bucket == "dst"
        assert key == "index/year=2024/month=05/day=31/legacy-migration.parquet"
        table = pq.read_table(io.BytesIO(body))
        assert table.schema.names == PARQUET_INDEX_SCHEMA.names
        assert table.num_rows == 2

    def test_empty_day_is_a_no_op(self):
        collection = _FakeCollection([])
        s3 = _FakeS3()
        channel = _FakeChannel()

        worker.process_day(collection, s3, "src", "dst", channel, "2024-05-31")

        assert s3.put_calls == []
        assert channel.dlq == []

    def test_guard_failure_sends_to_dlq_and_is_excluded_from_index(self):
        docs = [_doc("id1", total_messages=0), _doc("id2")]
        collection = _FakeCollection(docs)
        s3 = _FakeS3()
        channel = _FakeChannel()

        worker.process_day(collection, s3, "src", "dst", channel, "2024-05-31")

        assert channel.dlq == [("legacy-migration-dlq", "id1", "zero messages recorded")]
        table = pq.read_table(io.BytesIO(s3.put_calls[0][2]))
        assert table.num_rows == 1

    def test_missing_source_object_sends_to_dlq(self):
        docs = [_doc("id1")]
        collection = _FakeCollection(docs)
        s3 = _FakeS3(missing_source_for={"id1"})
        channel = _FakeChannel()

        worker.process_day(collection, s3, "src", "dst", channel, "2024-05-31")

        assert channel.dlq == [("legacy-migration-dlq", "id1", "source object missing")]
        assert s3.put_calls == []

    def test_copy_verification_failure_sends_to_dlq(self):
        docs = [_doc("id1")]
        collection = _FakeCollection(docs)
        s3 = _FakeS3(etag_mismatch_for={"id1"})
        channel = _FakeChannel()

        worker.process_day(collection, s3, "src", "dst", channel, "2024-05-31")

        assert channel.dlq == [("legacy-migration-dlq", "id1", "copy verification failed")]
        assert s3.put_calls == []

    def test_already_copied_flight_skips_copy_but_still_indexed(self):
        docs = [_doc("id1")]
        collection = _FakeCollection(docs)
        s3 = _FakeS3()
        s3._copied.add("id1")  # pre-existing from an earlier/redelivered run
        channel = _FakeChannel()

        worker.process_day(collection, s3, "src", "dst", channel, "2024-05-31")

        assert channel.dlq == []
        assert len(s3.put_calls) == 1
