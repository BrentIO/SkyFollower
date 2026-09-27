"""
Shared helpers for tools/legacy-migration's three roles (producer, worker,
verify).

Every legacy Mongo flight document is a stub (CompletedFlight minus
positions[]/velocities[], plus a `migrated` timestamp) whose field shape
matches shared.models.CompletedFlight exactly, so it parses directly into
that model rather than through a second, hand-rolled schema.
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional

import pika
from botocore.exceptions import ClientError

from shared.models import CompletedFlight
from shared.timing import RABBITMQ_BLOCKED_CONNECTION_TIMEOUT_SECONDS, RECONNECT_BACKOFF_SECONDS

logger = logging.getLogger("legacy-migration")

# Plain durable work queue (competing consumers), not the consistent-hash
# exchange live message processors use -- no per-aircraft state here needs
# to stay co-located, so plain round-robin distribution is simpler.
WORK_QUEUE_NAME = "legacy-migration"
DLQ_NAME = "legacy-migration-dlq"

# Earliest recorded flight; the producer's --start-date default.
EARLIEST_FLIGHT_DATE = "2022-07-11"

# Source objects are flat `{_id}.gz` keys at the legacy bucket root.
SOURCE_KEY_SUFFIX = ".gz"

_THROTTLE_ERROR_CODES = {
    "SlowDown",
    "RequestLimitExceeded",
    "Throttling",
    "ThrottlingException",
    "TooManyRequests",
}
_NOT_FOUND_ERROR_CODES = {"404", "NoSuchKey", "NotFound"}
_MAX_S3_ATTEMPTS = 5


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------

def today_utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def day_bounds_utc(date_str: str) -> tuple[datetime, datetime]:
    """[start, end) UTC bounds for one calendar day, e.g. "2024-05-31"."""
    start = parse_date(date_str)
    return start, start + timedelta(days=1)


def iter_dates(start_date: str, end_date: str) -> Iterator[str]:
    """Every calendar day from start_date to end_date, both inclusive."""
    current = parse_date(start_date)
    end = parse_date(end_date)
    if end < current:
        raise ValueError(f"--end-date {end_date} is before --start-date {start_date}")
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def source_key(doc_id: str) -> str:
    return f"{doc_id}{SOURCE_KEY_SUFFIX}"


# Fixed name, not a per-run UUID: this tool never deletes, so a re-run of a
# day must overwrite this same object via PutObject rather than leave an
# orphaned duplicate. Shared by worker.py (writes it) and verify.py (HeadObjects it).
COMPACTED_INDEX_FILENAME = "legacy-migration.parquet"


def compacted_index_key(date_str: str) -> str:
    yyyy, mm, dd = date_str.split("-")
    return f"index/year={yyyy}/month={mm}/day={dd}/{COMPACTED_INDEX_FILENAME}"


# ---------------------------------------------------------------------------
# Mongo
# ---------------------------------------------------------------------------

def connect_mongo(mongo_cfg: dict):
    """Returns the flights collection, read-only. `tz_aware=True` is load-
    bearing: without it, pymongo returns naive datetimes, which
    build_s3_key()'s `.astimezone(timezone.utc)` would silently treat as
    local time instead of UTC, corrupting the destination date."""
    from pymongo import MongoClient

    client = MongoClient(mongo_cfg["uri"], tz_aware=True)
    return client[mongo_cfg["database"]][mongo_cfg["collection"]]


# Every Mongo query in this tool must be scoped by this filter -- it matches
# the `first_message_migrated_partial` index. Any other predicate shape
# degrades to a full collection scan over ~8.75M documents.
MIGRATED_EXISTS_FILTER = {"migrated": {"$exists": True}}


# ---------------------------------------------------------------------------
# RabbitMQ
# ---------------------------------------------------------------------------

def connect_rabbitmq(rabbitmq_cfg: dict) -> "pika.BlockingConnection":
    credentials = pika.PlainCredentials(rabbitmq_cfg["username"], rabbitmq_cfg["password"])
    params = pika.ConnectionParameters(
        host=rabbitmq_cfg["host"],
        port=rabbitmq_cfg["port"],
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=RABBITMQ_BLOCKED_CONNECTION_TIMEOUT_SECONDS,
    )
    return pika.BlockingConnection(params)


def declare_queues(channel) -> None:
    channel.queue_declare(queue=WORK_QUEUE_NAME, durable=True)
    channel.queue_declare(queue=DLQ_NAME, durable=True)


def publish_dlq(channel, doc_id: str, reason: str) -> None:
    """Publish-and-forget: the DLQ is a dead end for human review, not a
    transient failure with retry/redelivery semantics."""
    channel.basic_publish(
        exchange="",
        routing_key=DLQ_NAME,
        body=json.dumps({"_id": doc_id, "reason": reason}).encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    logger.error("DLQ %s: %s", doc_id, reason)


def publish_day(channel, date_str: str) -> None:
    channel.basic_publish(
        exchange="",
        routing_key=WORK_QUEUE_NAME,
        body=json.dumps({"date": date_str}).encode("utf-8"),
        properties=pika.BasicProperties(delivery_mode=2),
    )


# ---------------------------------------------------------------------------
# Data-quality guards (worker-level, per-flight)
# ---------------------------------------------------------------------------

def guard_reason(doc: dict) -> Optional[str]:
    """
    Returns a DLQ reason if `doc` fails a per-flight guard, else None.
    Checked before the HeadObject/CopyObject step. Does not cover "source
    missing" or "copy verification failed" -- those are only discoverable
    during the copy itself (see copy_and_verify).
    """
    total_messages = doc.get("total_messages")
    if not (isinstance(total_messages, (int, float)) and total_messages > 0):
        return "zero messages recorded"

    first_message = doc.get("first_message")
    last_message = doc.get("last_message")
    if first_message is not None and last_message is not None and last_message < first_message:
        return "last_message before first_message"

    if not (doc.get("aircraft") or {}).get("icao_hex"):
        return "missing aircraft.icao_hex"

    return None


def build_completed_flight(doc: dict) -> CompletedFlight:
    """
    Parses a legacy Mongo flight stub into the same CompletedFlight model
    live flights use. Only call after guard_reason(doc) is None --
    CompletedFlight enforces shape, not the DLQ guards above (it accepts
    total_messages == 0).
    """
    return CompletedFlight.model_validate(doc)


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

def is_not_found(exc: ClientError) -> bool:
    return exc.response.get("Error", {}).get("Code", "") in _NOT_FOUND_ERROR_CODES


def s3_retry(fn, *args, **kwargs):
    """
    Retries a boto3 S3 call with exponential backoff on throttling only (a
    404 is never retried -- see is_not_found). Backoff base delay reuses
    RECONNECT_BACKOFF_SECONDS.
    """
    delay = RECONNECT_BACKOFF_SECONDS
    for attempt in range(1, _MAX_S3_ATTEMPTS + 1):
        try:
            return fn(*args, **kwargs)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code not in _THROTTLE_ERROR_CODES or attempt == _MAX_S3_ATTEMPTS:
                raise
            logger.warning(
                "S3 throttled (%s), retrying in %ss (attempt %s/%s)",
                code, delay, attempt, _MAX_S3_ATTEMPTS,
            )
            time.sleep(delay)
            delay *= 2


def dest_object_exists(s3_client, dest_bucket: str, dest_key: str) -> bool:
    """Per-flight idempotency check: skip a flight already copied by an
    earlier or redelivered run of this same day."""
    try:
        s3_retry(s3_client.head_object, Bucket=dest_bucket, Key=dest_key)
        return True
    except ClientError as exc:
        if is_not_found(exc):
            return False
        raise


def copy_and_verify(s3_client, source_bucket: str, source_key_: str, dest_bucket: str, dest_key: str) -> None:
    """Cross-bucket copy-only move: CopyObject, then an ETag/size
    integrity check. Raises FileNotFoundError if the source is missing, or
    ValueError if the check fails -- both immediate DLQ candidates."""
    try:
        source_head = s3_retry(s3_client.head_object, Bucket=source_bucket, Key=source_key_)
    except ClientError as exc:
        if is_not_found(exc):
            raise FileNotFoundError(source_key_) from exc
        raise

    s3_retry(
        s3_client.copy_object,
        Bucket=dest_bucket,
        Key=dest_key,
        CopySource={"Bucket": source_bucket, "Key": source_key_},
    )

    dest_head = s3_retry(s3_client.head_object, Bucket=dest_bucket, Key=dest_key)
    if (
        dest_head["ETag"] != source_head["ETag"]
        or dest_head["ContentLength"] != source_head["ContentLength"]
    ):
        raise ValueError("copy verification failed")


def build_s3_client():
    import boto3
    from botocore.config import Config as BotoConfig

    # max_attempts=0: s3_retry() above is the single retry loop, so
    # throttling stays visible to it instead of being silently absorbed
    # inside botocore.
    return boto3.client("s3", config=BotoConfig(retries={"max_attempts": 0}))
