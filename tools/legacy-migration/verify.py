"""
Verify role: run by the operator before deleting the legacy S3 bucket by
hand. Two checks, both read-only (no writes, no deletes, needs no IAM
beyond what the migration already has):

1. Per-day count reconciliation: Mongo's migrated-flight count for the day
   vs. the destination bucket's object count under that day's prefix.
   `s3_count < mongo_count` is flagged for attention (reconcile against
   the DLQ -- flights sent there are deliberately not copied); for days
   the live pipeline has also started writing to, s3_count naturally
   exceeds mongo_count and that alone is not a problem.

2. Byte-exactness: every copied object's ETag (== MD5 for a single-part
   object under SSE-S3, true for both buckets here -- see the issue's
   "Verification" section) compared source vs. destination. A mismatch is
   an anomaly to investigate, not something this tool fixes automatically.
"""

from __future__ import annotations

import argparse
import logging

from common import build_s3_client, connect_mongo, iter_dates, source_key, MIGRATED_EXISTS_FILTER, day_bounds_utc

from shared.config import load_config

logger = logging.getLogger("legacy-migration.verify")


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD, inclusive")


def _dest_prefix(date_str: str) -> str:
    yyyy, mm, dd = date_str.split("-")
    return f"flights/{yyyy}/{mm}/{dd}/"


def _list_dest_objects(s3_client, dest_bucket: str, date_str: str) -> list[dict]:
    """Every object (Key, ETag) under this day's destination prefix."""
    objects: list[dict] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=dest_bucket, Prefix=_dest_prefix(date_str)):
        objects.extend(page.get("Contents", []))
    return objects


def _doc_id_from_dest_key(key: str) -> str:
    # flights/{YYYY}/{MM}/{DD}/{uuid}.json.gz -> {uuid}
    filename = key.rsplit("/", 1)[-1]
    return filename.removesuffix(".json.gz")


def run(args: argparse.Namespace) -> None:
    cfg = load_config("mongo", "legacy_migration_s3")
    collection = connect_mongo(cfg["mongo"])
    s3_client = build_s3_client()
    source_bucket = cfg["legacy_migration_s3"]["source_bucket"]
    dest_bucket = cfg["legacy_migration_s3"]["dest_bucket"]

    attention_days = 0
    mismatch_count = 0
    objects_checked = 0

    for date_str in iter_dates(args.start_date, args.end_date):
        start, end = day_bounds_utc(date_str)
        mongo_count = collection.count_documents(
            {**MIGRATED_EXISTS_FILTER, "first_message": {"$gte": start, "$lt": end}}
        )
        dest_objects = _list_dest_objects(s3_client, dest_bucket, date_str)
        s3_count = len(dest_objects)

        if s3_count < mongo_count:
            attention_days += 1
            logger.warning(
                "%s: ATTENTION mongo=%d s3=%d (short by %d -- reconcile against the DLQ)",
                date_str, mongo_count, s3_count, mongo_count - s3_count,
            )
        else:
            logger.info("%s: OK mongo=%d s3=%d", date_str, mongo_count, s3_count)

        for obj in dest_objects:
            doc_id = _doc_id_from_dest_key(obj["Key"])
            try:
                source_head = s3_client.head_object(Bucket=source_bucket, Key=source_key(doc_id))
            except Exception:
                logger.warning("%s: could not HeadObject source for %s (%s)", date_str, doc_id, obj["Key"])
                mismatch_count += 1
                continue
            objects_checked += 1
            if source_head["ETag"] != obj["ETag"]:
                mismatch_count += 1
                logger.warning(
                    "%s: ETag mismatch for %s -- source=%s dest=%s (key %s)",
                    date_str, doc_id, source_head["ETag"], obj["ETag"], obj["Key"],
                )

    clean = attention_days == 0 and mismatch_count == 0
    logger.info(
        "Verify complete: %d day(s) needing attention, %d object(s) checked, %d ETag mismatch(es) -- %s",
        attention_days, objects_checked, mismatch_count, "CLEAN" if clean else "NOT CLEAN",
    )
    if not clean:
        raise SystemExit(1)
