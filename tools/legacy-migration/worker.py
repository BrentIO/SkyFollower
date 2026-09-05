"""
Worker role: long-lived RabbitMQ consumer. Each message is one calendar
day (`{"date": "YYYY-MM-DD"}`); a worker holds exactly one day in flight
at a time (prefetch_count=1) since a day is a substantial unit of work,
not a quick pop-and-done.

Per day: query Mongo for that day's flights, run per-flight guards, copy
anything not already present at the computed destination key, and upload
one compacted Parquet index file covering every flight in the day. See
common.py's guard_reason()/copy_and_verify() for the per-flight rules this
implements.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import signal

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

logger = logging.getLogger("legacy-migration.worker")


def add_arguments(parser: argparse.ArgumentParser) -> None:
    pass  # no CLI flags -- the worker is a long-lived consumer, fully config-driven


def process_day(collection, s3_client, source_bucket: str, dest_bucket: str, channel, date_str: str) -> None:
    start, end = day_bounds_utc(date_str)
    query = {**MIGRATED_EXISTS_FILTER, "first_message": {"$gte": start, "$lt": end}}

    rows: list[dict] = []
    for doc in collection.find(query):
        doc_id = doc.get("_id")

        reason = guard_reason(doc)
        if reason:
            publish_dlq(channel, doc_id, reason)
            continue

        try:
            flight = build_completed_flight(doc)
        except ValidationError as exc:
            # Not one of guard_reason()'s known cardinality-4/2 cases --
            # an unexpected shape problem. Still a single bad document,
            # not a reason to fail the whole day.
            publish_dlq(channel, doc_id, f"unexpected document shape: {exc}")
            continue

        dest_key = build_s3_key(flight)

        if not dest_object_exists(s3_client, dest_bucket, dest_key):
            try:
                copy_and_verify(s3_client, source_bucket, source_key(doc_id), dest_bucket, dest_key)
            except FileNotFoundError:
                publish_dlq(channel, doc_id, "source object missing")
                continue
            except ValueError:
                publish_dlq(channel, doc_id, "copy verification failed")
                continue
            logger.debug("Migrated %s -> %s", doc_id, dest_key)

        rows.append(flight_index_row(flight, dest_key))

    if not rows:
        logger.info("Day %s: no flights, no-op", date_str)
        return

    table = pa.Table.from_pylist(rows, schema=PARQUET_INDEX_SCHEMA)
    sink = io.BytesIO()
    pq.write_table(table, sink)

    index_key = compacted_index_key(date_str)
    s3_retry(s3_client.put_object, Bucket=dest_bucket, Key=index_key, Body=sink.getvalue())
    logger.info("Day %s: migrated/verified %d flight(s), wrote %s", date_str, len(rows), index_key)


def build_on_message(collection, s3_client, source_bucket: str, dest_bucket: str):
    """Factory rather than a closure inlined in run() so the callback is
    unit-testable without a real Mongo/S3/RabbitMQ connection."""

    def on_message(ch, method, _properties, body):
        try:
            payload = json.loads(body)
            date_str = payload["date"]
        except (json.JSONDecodeError, KeyError) as exc:
            # Can never succeed on redelivery -- acking (dropping) it,
            # rather than nacking, is what prevents this from becoming an
            # infinite redelivery loop that also wedges the queue behind
            # it (prefetch_count=1).
            logger.error("Malformed queue message, dropping: %s (body=%r)", exc, body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        try:
            process_day(collection, s3_client, source_bucket, dest_bucket, ch, date_str)
        except Exception:
            # Systemic (Mongo/S3/RabbitMQ) failure, not a per-flight data
            # problem -- those are handled inside process_day and never
            # reach here. Give up on this day and let RabbitMQ redeliver
            # it, rather than waiting on a timeout.
            logger.exception("Unrecoverable error processing day %s; requeueing", date_str)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
            return
        ch.basic_ack(delivery_tag=method.delivery_tag)

    return on_message


def build_shutdown_handler(channel):
    """Factory rather than a closure inlined in run() so the handler is
    unit-testable in isolation from signal.signal()/a real connection."""

    def _handle_signal(sig, frame):
        # Signal handlers run on the main thread even when delivered while
        # it's blocked inside start_consuming()'s C call, so calling
        # stop_consuming() directly here is safe -- same as
        # archive-processor's shutdown(). Without a handler, SIGTERM (from
        # docker compose down/stop or a scale-down) exits immediately with
        # no clean stop_consuming()/connection.close(), sitting out the
        # full stop-grace period before SIGKILL.
        logger.info("Shutdown requested, stopping consumer...")
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

    connection = connect_rabbitmq(cfg["rabbitmq"])
    channel = connection.channel()
    declare_queues(channel)
    # A day in flight at a time -- see module docstring.
    channel.basic_qos(prefetch_count=1)

    on_message = build_on_message(collection, s3_client, source_bucket, dest_bucket)
    channel.basic_consume(queue=WORK_QUEUE_NAME, on_message_callback=on_message)
    logger.info("Worker ready, consuming from %s (DLQ: %s)", WORK_QUEUE_NAME, DLQ_NAME)

    signal.signal(signal.SIGTERM, build_shutdown_handler(channel))
    signal.signal(signal.SIGINT, build_shutdown_handler(channel))

    try:
        channel.start_consuming()
    finally:
        connection.close()
