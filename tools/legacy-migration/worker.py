"""
Worker role: long-lived RabbitMQ consumer. Each message is one calendar
day (`{"date": "YYYY-MM-DD"}`); a worker holds exactly one day in flight
at a time (prefetch_count=1). Scale by running more worker containers
(`docker compose ... up --scale worker=N`), not by adding concurrency
inside one process.

Per day: query Mongo for that day's flights, run per-flight guards, copy
anything not already present at the computed destination key, and upload
one compacted Parquet index file covering every flight in the day. See
common.py's guard_reason()/copy_and_verify() for the per-flight rules.

process_day() can run minutes long for a busy day and must never run on
the pika connection thread -- a BlockingConnection can only send AMQP
heartbeats while control is inside pika's own loop, so a long callback
starves heartbeats and the broker drops the connection. on_message()
instead only parses the delivery and hands it to a background thread
(build_worker_loop), which marshals the ack/nack and any DLQ publishes
back onto the connection thread via connection.add_callback_threadsafe(),
since a pika channel is not thread-safe.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import queue
import signal
import threading
import time

import pika
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError

from common import (
    DLQ_NAME,
    MIGRATED_EXISTS_FILTER,
    WORK_QUEUE_NAME,
    build_completed_flight,
    build_s3_client,
    compacted_index_key,
    connect_mongo,
    connect_rabbitmq,
    day_bounds_utc,
    declare_queues,
    dest_object_exists,
    copy_and_verify,
    guard_reason,
    publish_dlq,
    s3_retry,
    source_key,
)

from shared.archive_index import PARQUET_INDEX_SCHEMA, build_s3_key, flight_index_row
from shared.config import load_config
from shared.timing import RECONNECT_BACKOFF_SECONDS

logger = logging.getLogger("legacy-migration.worker")


def add_arguments(parser: argparse.ArgumentParser) -> None:
    pass  # no CLI flags -- the worker is a long-lived consumer, fully config-driven


def process_day(collection, s3_client, source_bucket: str, dest_bucket: str, date_str: str) -> list[tuple[str, str]]:
    """
    Runs entirely on the background worker thread, never on the pika
    connection thread. Returns collected DLQ entries as `(doc_id, reason)`
    pairs -- publishing them, like the eventual ack/nack, is the
    connection thread's job, so this function never touches the channel.
    """
    logger.info("Day %s: starting", date_str)

    start, end = day_bounds_utc(date_str)
    query = {**MIGRATED_EXISTS_FILTER, "first_message": {"$gte": start, "$lt": end}}

    dlq_entries: list[tuple[str, str]] = []
    rows: list[dict] = []
    for doc in collection.find(query):
        doc_id = doc.get("_id")

        reason = guard_reason(doc)
        if reason:
            dlq_entries.append((doc_id, reason))
            continue

        try:
            flight = build_completed_flight(doc)
        except ValidationError as exc:
            # A single bad document, not a reason to fail the whole day.
            dlq_entries.append((doc_id, f"unexpected document shape: {exc}"))
            continue

        dest_key = build_s3_key(flight)

        if not dest_object_exists(s3_client, dest_bucket, dest_key):
            try:
                copy_and_verify(s3_client, source_bucket, source_key(doc_id), dest_bucket, dest_key)
            except FileNotFoundError:
                dlq_entries.append((doc_id, "source object missing"))
                continue
            except ValueError:
                dlq_entries.append((doc_id, "copy verification failed"))
                continue
            logger.debug("Migrated %s -> %s", doc_id, dest_key)

        rows.append(flight_index_row(flight, dest_key))

    if not rows:
        logger.info("Day %s: no flights, no-op", date_str)
        return dlq_entries

    table = pa.Table.from_pylist(rows, schema=PARQUET_INDEX_SCHEMA)
    sink = io.BytesIO()
    pq.write_table(table, sink)

    index_key = compacted_index_key(date_str)
    s3_retry(s3_client.put_object, Bucket=dest_bucket, Key=index_key, Body=sink.getvalue())
    logger.info("Day %s: migrated/verified %d flight(s), wrote %s", date_str, len(rows), index_key)
    return dlq_entries


def build_on_message(handoff: "queue.Queue"):
    """Runs on the pika connection thread: parses just enough to route
    the message, then hands off to the background worker thread and
    returns immediately -- process_day's work must never block this thread."""

    def on_message(ch, method, _properties, body):
        try:
            payload = json.loads(body)
            date_str = payload["date"]
        except (json.JSONDecodeError, KeyError) as exc:
            # A malformed message can never succeed on redelivery -- ack
            # (drop) rather than nack, to avoid an infinite redelivery loop
            # that also wedges the queue behind it (prefetch_count=1).
            logger.error("Malformed queue message, dropping: %s (body=%r)", exc, body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        handoff.put((date_str, method.delivery_tag))

    return on_message


def _schedule(connection, callback) -> None:
    """Marshal a callback onto the pika connection thread. If the
    connection was already replaced by a reconnect, this fails harmlessly
    and the day is picked up again on redelivery (S3 copies are idempotent
    via dest_object_exists(), and the index write is a plain overwrite)."""
    try:
        connection.add_callback_threadsafe(callback)
    except Exception as exc:
        logger.warning("Could not schedule callback on connection thread: %s", exc)


def build_worker_loop(handoff, connection, channel, collection, s3_client, source_bucket, dest_bucket, shutdown_event):
    """Background thread body: pulls one `(date_str, delivery_tag)` off
    `handoff`, runs process_day, and marshals the outcome (DLQ publishes
    plus ack/nack) back onto the connection thread via `_schedule`."""

    def _ack_success(dlq_entries: list[tuple[str, str]], delivery_tag: int) -> None:
        for doc_id, reason in dlq_entries:
            publish_dlq(channel, doc_id, reason)
        channel.basic_ack(delivery_tag=delivery_tag)

    def _nack_failure(delivery_tag: int) -> None:
        channel.basic_nack(delivery_tag=delivery_tag, requeue=True)

    def worker_loop() -> None:
        while not shutdown_event.is_set():
            if connection.is_closed:
                # Connection was replaced by a reconnect elsewhere -- nothing left to do.
                return
            try:
                date_str, delivery_tag = handoff.get(timeout=1.0)
            except queue.Empty:
                continue

            try:
                dlq_entries = process_day(collection, s3_client, source_bucket, dest_bucket, date_str)
            except Exception:
                # Systemic (Mongo/S3/RabbitMQ) failure, not a per-flight data
                # problem -- those are collected into dlq_entries and never raise.
                logger.exception("Unrecoverable error processing day %s; requeueing", date_str)
                _schedule(connection, lambda tag=delivery_tag: _nack_failure(tag))
                continue

            _schedule(
                connection,
                lambda entries=dlq_entries, tag=delivery_tag: _ack_success(entries, tag),
            )

    return worker_loop


def build_shutdown_handler(channel, shutdown_event: threading.Event):
    """Factory rather than a closure inlined in run() so the handler is
    unit-testable in isolation from signal.signal()/a real connection."""

    def _handle_signal(sig, frame):
        # Signal handlers run on the main thread even while it's blocked
        # inside start_consuming()'s C call, so stop_consuming() is safe to
        # call directly here. A day still in flight is deliberately not
        # waited on: RabbitMQ will requeue it once this connection closes,
        # and S3 progress made on it so far is idempotent on redelivery.
        logger.info("Shutdown requested, stopping consumer...")
        shutdown_event.set()
        try:
            channel.stop_consuming()
        except Exception:
            pass

    return _handle_signal


def run(args: argparse.Namespace) -> None:
    cfg = load_config("rabbitmq", "mongo", "legacy_migration_s3")
    collection = connect_mongo(cfg["mongo"])
    s3_client = build_s3_client()
    source_bucket = cfg["legacy_migration_s3"]["source_bucket"]
    dest_bucket = cfg["legacy_migration_s3"]["dest_bucket"]

    shutdown_event = threading.Event()

    while not shutdown_event.is_set():
        connection = None
        try:
            connection = connect_rabbitmq(cfg["rabbitmq"])
            channel = connection.channel()
            declare_queues(channel)
            # A day in flight at a time -- see module docstring.
            channel.basic_qos(prefetch_count=1)

            handoff: "queue.Queue" = queue.Queue()
            channel.basic_consume(queue=WORK_QUEUE_NAME, on_message_callback=build_on_message(handoff))

            # One thread per connection: if this reconnects, an old thread
            # still finishing a day against the closed connection just logs
            # a harmless warning from _schedule() instead of acking.
            threading.Thread(
                target=build_worker_loop(
                    handoff, connection, channel, collection, s3_client, source_bucket, dest_bucket, shutdown_event
                ),
                daemon=True,
                name="legacy-migration-day-worker",
            ).start()

            signal.signal(signal.SIGTERM, build_shutdown_handler(channel, shutdown_event))
            signal.signal(signal.SIGINT, build_shutdown_handler(channel, shutdown_event))

            logger.info("Worker ready, consuming from %s (DLQ: %s)", WORK_QUEUE_NAME, DLQ_NAME)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as exc:
            if shutdown_event.is_set():
                break
            logger.warning("RabbitMQ connection lost: %s. Reconnecting in %ss...", exc, RECONNECT_BACKOFF_SECONDS)
            time.sleep(RECONNECT_BACKOFF_SECONDS)
        except Exception as exc:
            if shutdown_event.is_set():
                break
            logger.error("RabbitMQ error: %s. Reconnecting in %ss...", exc, RECONNECT_BACKOFF_SECONDS)
            time.sleep(RECONNECT_BACKOFF_SECONDS)
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass
