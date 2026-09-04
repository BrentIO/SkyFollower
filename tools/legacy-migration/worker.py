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

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import ValidationError

from common import (
    DLQ_NAME,
    MIGRATED_EXISTS_FILTER,
    WORK_QUEUE_NAME,
    build_completed_flight,
    build_s3_client,
    connect_mongo,
    connect_rabbitmq,
    day_bounds_utc,
    declare_queues,
    dest_object_exists,
    copy_and_verify,
    guard_reason,
    publish_dlq,
    source_key,
)

from shared.archive_index import PARQUET_INDEX_SCHEMA, build_s3_key, flight_index_row
from shared.config import load_config

logger = logging.getLogger("legacy-migration.worker")

# Deliberately a fixed name, not a per-run UUID: this tool never deletes
# (see the issue's IAM policy -- no s3:DeleteObject anywhere), so a re-run
# of a day must overwrite this same object via PutObject rather than leave
# an orphaned duplicate behind under the same partition. archive-processor's
# own per-flight index files (build_index_s3_key) DO need a UUID name
# since many are written per day; this tool writes exactly one file per
# day, so a name derived from the date alone is sufficient.
_COMPACTED_INDEX_FILENAME = "legacy-migration.parquet"


def add_arguments(parser: argparse.ArgumentParser) -> None:
    pass  # no CLI flags -- the worker is a long-lived consumer, fully config-driven


def _compacted_index_key(date_str: str) -> str:
    yyyy, mm, dd = date_str.split("-")
    return f"index/year={yyyy}/month={mm}/day={dd}/{_COMPACTED_INDEX_FILENAME}"


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

    index_key = _compacted_index_key(date_str)
    s3_client.put_object(Bucket=dest_bucket, Key=index_key, Body=sink.getvalue())
    logger.info("Day %s: migrated/verified %d flight(s), wrote %s", date_str, len(rows), index_key)


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

    def on_message(ch, method, _properties, body):
        payload = json.loads(body)
        date_str = payload["date"]
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

    channel.basic_consume(queue=WORK_QUEUE_NAME, on_message_callback=on_message)
    logger.info("Worker ready, consuming from %s (DLQ: %s)", WORK_QUEUE_NAME, DLQ_NAME)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()
