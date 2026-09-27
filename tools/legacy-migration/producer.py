"""
Producer role: runs once per pass, publishes one message per calendar day
to the `legacy-migration` work queue, and runs a one-time catch-all sweep
for documents whose `first_message` falls outside the requested range
before the day-walk begins.

Re-running over an overlapping range is safe: the day-walk has no memory of
prior runs, and worker.py's per-flight HeadObject check makes re-copying idempotent.
"""

from __future__ import annotations

import argparse
import logging

from common import (
    EARLIEST_FLIGHT_DATE,
    MIGRATED_EXISTS_FILTER,
    connect_mongo,
    connect_rabbitmq,
    day_bounds_utc,
    declare_queues,
    iter_dates,
    publish_day,
    publish_dlq,
    today_utc_date,
)

from shared.config import load_config

logger = logging.getLogger("legacy-migration.producer")


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start-date", default=EARLIEST_FLIGHT_DATE, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD, inclusive (default: today, UTC)")
    parser.add_argument(
        "--sweep",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Force the catch-all sweep on/off. Default: auto -- on only when "
            "[--start-date, --end-date] covers the full recorded history."
        ),
    )


def _run_catch_all_sweep(collection, channel, start_date: str, end_date: str) -> int:
    """A document whose first_message falls outside every day the walk
    below will generate would never reach a worker; this sweep catches
    those. Naturally idempotent -- re-running just duplicates a DLQ message."""
    start_dt, _ = day_bounds_utc(start_date)
    _, end_dt_exclusive = day_bounds_utc(end_date)

    query = {
        **MIGRATED_EXISTS_FILTER,
        "$or": [
            {"first_message": {"$lt": start_dt}},
            {"first_message": {"$gte": end_dt_exclusive}},
        ],
    }
    count = 0
    for doc in collection.find(query, {"_id": 1}):
        publish_dlq(channel, doc["_id"], "first_message outside requested range")
        count += 1
    return count


def _should_sweep(start_date: str, end_date: str) -> bool:
    """The sweep's first_message predicates only mean "outside recorded
    history" when the requested range covers the full history -- a
    narrower range (a tail re-run, a windowed test run) would instead
    match millions of already-migrated documents and flood the DLQ."""
    return start_date <= EARLIEST_FLIGHT_DATE and end_date >= today_utc_date()


def run(args: argparse.Namespace) -> None:
    start_date = args.start_date
    end_date = args.end_date or today_utc_date()
    should_sweep = args.sweep if args.sweep is not None else _should_sweep(start_date, end_date)

    cfg = load_config("rabbitmq", "mongo")
    collection = connect_mongo(cfg["mongo"])
    connection = connect_rabbitmq(cfg["rabbitmq"])
    try:
        channel = connection.channel()
        declare_queues(channel)

        if should_sweep:
            logger.info("Running catch-all sweep for first_message outside [%s, %s]", start_date, end_date)
            swept = _run_catch_all_sweep(collection, channel, start_date, end_date)
            logger.info("Catch-all sweep complete: %d document(s) sent to the DLQ", swept)
        else:
            logger.info(
                "Windowed run -- skipping catch-all sweep "
                "(run a full-range pass to sweep for out-of-range documents)"
            )

        published = 0
        for date_str in iter_dates(start_date, end_date):
            publish_day(channel, date_str)
            published += 1
            if published % 100 == 0:
                logger.info("Published %d day(s), most recent %s", published, date_str)

        logger.info("Producer finished: %d day(s) published across [%s, %s]", published, start_date, end_date)
    finally:
        connection.close()
