"""Tests for worker.py -- process_day's guard/copy/DLQ/index-row wiring,
on_message's connection-thread hand-off, the background worker loop's
ack/nack marshalling, and the reconnect loop."""

from __future__ import annotations

import argparse
import io
import json
import logging
import queue
import threading
import time
from datetime import datetime, timezone

import pika
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

import common
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
        self.acked = []
        self.nacked = []
        self.stop_consuming_called = False

    def basic_publish(self, exchange, routing_key, body, properties=None):
        payload = json.loads(body)
        self.dlq.append((routing_key, payload["_id"], payload["reason"]))

    def basic_ack(self, delivery_tag):
        self.acked.append(delivery_tag)

    def basic_nack(self, delivery_tag, requeue=False):
        self.nacked.append((delivery_tag, requeue))

    def stop_consuming(self):
        self.stop_consuming_called = True


class _FakeConnection:
    """add_callback_threadsafe runs the callback inline, standing in for
    pika's ioloop marshalling the call onto "the connection thread" --
    there's only one thread in these tests."""

    def __init__(self, closed: bool = False):
        self.is_closed = closed

    def add_callback_threadsafe(self, callback):
        callback()


class _FakeMethod:
    def __init__(self, delivery_tag=1):
        self.delivery_tag = delivery_tag


def _doc_id_from_key(key: str) -> str:
    return key.rsplit("/", 1)[-1].split(".")[0]


class _FakeS3:
    """head_object reports success with a matching ETag once an object has
    been "copied" (tracked in self._copied); copy_object records the move.
    """

    def __init__(self, missing_source_for=(), etag_mismatch_for=(), throttle_put_object_times=0):
        self._missing_source_for = set(missing_source_for)
        self._etag_mismatch_for = set(etag_mismatch_for)
        self._copied: set[str] = set()
        self.put_calls = []
        self._throttle_put_object_times = throttle_put_object_times

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
        if self._throttle_put_object_times > 0:
            self._throttle_put_object_times -= 1
            raise ClientError({"Error": {"Code": "SlowDown"}}, "PutObject")
        self.put_calls.append((Bucket, Key, Body))


class TestProcessDay:
    def test_all_flights_migrated_and_index_written(self):
        docs = [_doc("id1"), _doc("id2", ident="DAL2")]
        collection = _FakeCollection(docs)
        s3 = _FakeS3()

        dlq_entries = worker.process_day(collection, s3, "src", "dst", "2024-05-31")

        assert dlq_entries == []
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

        dlq_entries = worker.process_day(collection, s3, "src", "dst", "2024-05-31")

        assert s3.put_calls == []
        assert dlq_entries == []

    def test_guard_failure_sends_to_dlq_and_is_excluded_from_index(self):
        docs = [_doc("id1", total_messages=0), _doc("id2")]
        collection = _FakeCollection(docs)
        s3 = _FakeS3()

        dlq_entries = worker.process_day(collection, s3, "src", "dst", "2024-05-31")

        assert dlq_entries == [("id1", "zero messages recorded")]
        table = pq.read_table(io.BytesIO(s3.put_calls[0][2]))
        assert table.num_rows == 1

    def test_missing_source_object_sends_to_dlq(self):
        docs = [_doc("id1")]
        collection = _FakeCollection(docs)
        s3 = _FakeS3(missing_source_for={"id1"})

        dlq_entries = worker.process_day(collection, s3, "src", "dst", "2024-05-31")

        assert dlq_entries == [("id1", "source object missing")]
        assert s3.put_calls == []

    def test_copy_verification_failure_sends_to_dlq(self):
        docs = [_doc("id1")]
        collection = _FakeCollection(docs)
        s3 = _FakeS3(etag_mismatch_for={"id1"})

        dlq_entries = worker.process_day(collection, s3, "src", "dst", "2024-05-31")

        assert dlq_entries == [("id1", "copy verification failed")]
        assert s3.put_calls == []

    def test_already_copied_flight_skips_copy_but_still_indexed(self):
        docs = [_doc("id1")]
        collection = _FakeCollection(docs)
        s3 = _FakeS3()
        s3._copied.add("id1")  # pre-existing from an earlier/redelivered run

        dlq_entries = worker.process_day(collection, s3, "src", "dst", "2024-05-31")

        assert dlq_entries == []
        assert len(s3.put_calls) == 1

    def test_start_line_is_logged_before_any_s3_call(self, caplog):
        caplog.set_level(logging.INFO, logger=worker.logger.name)

        class _OrderCheckingS3(_FakeS3):
            """Fails the first time any S3 method is invoked without the
            start line already on the record -- a stronger proof than just
            checking presence somewhere in caplog after the fact."""

            def _assert_started(self):
                assert any(
                    r.getMessage() == "Day 2024-05-31: starting" for r in caplog.records
                ), "S3 call happened before the day-start line was logged"

            def head_object(self, Bucket, Key):
                self._assert_started()
                return super().head_object(Bucket, Key)

            def copy_object(self, Bucket, Key, CopySource):
                self._assert_started()
                return super().copy_object(Bucket, Key, CopySource)

            def put_object(self, Bucket, Key, Body):
                self._assert_started()
                return super().put_object(Bucket, Key, Body)

        docs = [_doc("id1")]
        collection = _FakeCollection(docs)
        s3 = _OrderCheckingS3()

        worker.process_day(collection, s3, "src", "dst", "2024-05-31")

        assert caplog.records[0].getMessage() == "Day 2024-05-31: starting"

    def test_throttled_index_put_object_is_retried_not_raised(self, monkeypatch):
        monkeypatch.setattr(common.time, "sleep", lambda *_: None)
        docs = [_doc("id1")]
        collection = _FakeCollection(docs)
        s3 = _FakeS3(throttle_put_object_times=2)

        dlq_entries = worker.process_day(collection, s3, "src", "dst", "2024-05-31")

        assert dlq_entries == []
        assert len(s3.put_calls) == 1


class TestOnMessage:
    """on_message now only parses and hands off -- it must never touch
    Mongo/S3 itself, so build_on_message doesn't even need them."""

    def test_valid_message_hands_off_without_doing_s3_work(self):
        handoff: queue.Queue = queue.Queue()
        channel = _FakeChannel()
        on_message = worker.build_on_message(handoff)

        on_message(channel, _FakeMethod(delivery_tag=7), None, json.dumps({"date": "2024-05-31"}).encode())

        assert channel.acked == []
        assert channel.nacked == []
        assert handoff.get_nowait() == ("2024-05-31", 7)

    def test_unparseable_json_is_dropped_not_requeued(self):
        handoff: queue.Queue = queue.Queue()
        channel = _FakeChannel()
        on_message = worker.build_on_message(handoff)

        on_message(channel, _FakeMethod(delivery_tag=3), None, b"not json")

        assert channel.acked == [3]
        assert channel.nacked == []
        assert handoff.empty()

    def test_missing_date_key_is_dropped_not_requeued(self):
        handoff: queue.Queue = queue.Queue()
        channel = _FakeChannel()
        on_message = worker.build_on_message(handoff)

        on_message(channel, _FakeMethod(delivery_tag=9), None, b"{}")

        assert channel.acked == [9]
        assert channel.nacked == []
        assert handoff.empty()


class TestWorkerLoop:
    """build_worker_loop is the background thread body: pulls one item off
    the hand-off queue, runs process_day, and marshals the ack/nack (plus
    any DLQ publishes) back through connection.add_callback_threadsafe --
    _FakeConnection runs those inline, standing in for "the connection
    thread"."""

    @staticmethod
    def _wait_until(predicate, timeout=2.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.01)
        raise AssertionError("condition never became true")

    def test_success_acks_and_publishes_collected_dlq_entries(self):
        docs = [_doc("id1", total_messages=0), _doc("id2")]
        collection = _FakeCollection(docs)
        s3 = _FakeS3()
        channel = _FakeChannel()
        connection = _FakeConnection()
        handoff: queue.Queue = queue.Queue()
        shutdown_event = threading.Event()
        handoff.put(("2024-05-31", 11))

        loop = worker.build_worker_loop(handoff, connection, channel, collection, s3, "src", "dst", shutdown_event)
        thread = threading.Thread(target=loop, daemon=True)
        thread.start()
        try:
            self._wait_until(lambda: channel.acked)
        finally:
            shutdown_event.set()
            thread.join(timeout=2)

        assert channel.dlq == [("legacy-migration-dlq", "id1", "zero messages recorded")]
        assert channel.acked == [11]
        assert channel.nacked == []

    def test_process_day_exception_nacks_with_requeue(self):
        class _ExplodingCollection:
            def find(self, query):
                raise RuntimeError("Mongo unavailable")

        channel = _FakeChannel()
        connection = _FakeConnection()
        handoff: queue.Queue = queue.Queue()
        shutdown_event = threading.Event()
        handoff.put(("2024-05-31", 5))

        loop = worker.build_worker_loop(
            handoff, connection, channel, _ExplodingCollection(), _FakeS3(), "src", "dst", shutdown_event
        )
        thread = threading.Thread(target=loop, daemon=True)
        thread.start()
        try:
            self._wait_until(lambda: channel.nacked)
        finally:
            shutdown_event.set()
            thread.join(timeout=2)

        assert channel.acked == []
        assert channel.nacked == [(5, True)]

    def test_stale_connection_after_reconnect_exits_without_touching_channel(self):
        """A worker thread left over from a dropped connection (run()'s
        loop has already moved on to a new connection/channel) must not
        keep polling forever -- see worker_loop's is_closed check."""
        channel = _FakeChannel()
        connection = _FakeConnection(closed=True)
        handoff: queue.Queue = queue.Queue()
        shutdown_event = threading.Event()

        loop = worker.build_worker_loop(handoff, connection, channel, _FakeCollection([]), _FakeS3(), "src", "dst", shutdown_event)
        thread = threading.Thread(target=loop, daemon=True)
        thread.start()
        thread.join(timeout=2)

        assert not thread.is_alive()
        assert channel.acked == []
        assert channel.nacked == []


class TestShutdownHandler:
    def test_stops_consuming_and_sets_shutdown_event(self):
        channel = _FakeChannel()
        shutdown_event = threading.Event()
        handler = worker.build_shutdown_handler(channel, shutdown_event)

        handler(None, None)

        assert channel.stop_consuming_called is True
        assert shutdown_event.is_set()

    def test_swallows_exception_from_stop_consuming(self):
        class _ExplodingChannel(_FakeChannel):
            def stop_consuming(self):
                raise RuntimeError("already stopped")

        handler = worker.build_shutdown_handler(_ExplodingChannel(), threading.Event())

        handler(None, None)  # must not raise


class TestRun:
    """run()'s connect/consume/reconnect loop, with Mongo/S3/RabbitMQ and
    the background worker thread all faked out -- this only exercises the
    reconnect-on-AMQPConnectionError-then-exit-on-shutdown control flow."""

    def test_reconnects_after_amqp_error_then_exits_on_shutdown(self, monkeypatch):
        attempts = {"count": 0}
        captured = {}

        class _RunFakeChannel:
            def queue_declare(self, **kwargs):
                pass

            def basic_qos(self, **kwargs):
                pass

            def basic_consume(self, **kwargs):
                pass

            def stop_consuming(self):
                pass

            def start_consuming(self):
                # Second connection: simulate stop_consuming() having been
                # called already (shutdown requested), so start_consuming()
                # returns normally instead of blocking.
                captured["shutdown_event"].set()

        class _RunFakeConnection:
            def channel(self):
                return _RunFakeChannel()

            def close(self):
                pass

        def fake_connect_rabbitmq(rabbitmq_cfg):
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise pika.exceptions.AMQPConnectionError("no route to host")
            return _RunFakeConnection()

        def fake_build_worker_loop(handoff, connection, channel, collection, s3_client, source_bucket, dest_bucket, shutdown_event):
            captured["shutdown_event"] = shutdown_event
            return lambda: None  # no real background work needed for this test

        monkeypatch.setattr(worker, "load_config", lambda *a, **k: {
            "rabbitmq": {}, "mongo": {}, "legacy_migration_s3": {"source_bucket": "src", "dest_bucket": "dst"},
        })
        monkeypatch.setattr(worker, "connect_mongo", lambda cfg: None)
        monkeypatch.setattr(worker, "build_s3_client", lambda: None)
        monkeypatch.setattr(worker, "declare_queues", lambda channel: None)
        monkeypatch.setattr(worker, "connect_rabbitmq", fake_connect_rabbitmq)
        monkeypatch.setattr(worker, "build_worker_loop", fake_build_worker_loop)
        monkeypatch.setattr(worker.signal, "signal", lambda *a, **k: None)
        monkeypatch.setattr(worker.time, "sleep", lambda *_: None)

        worker.run(argparse.Namespace())

        assert attempts["count"] == 2
