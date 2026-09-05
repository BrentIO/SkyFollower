"""
Producer role: runs once per pass, publishes one message per calendar day
to the `legacy-migration` work queue, and runs a one-time catch-all sweep
for documents whose `first_message` falls outside the requested range
before the day-walk begins.

Both passes (the bulk history, then the ~90-day tail after the operator
drives the remainder to `migrated` using the legacy system's own offload
tool) are this same script with different --start-date/--end-date bounds.
Re-running over an overlapping range is safe -- see worker.py's per-flight
HeadObject idempotency check and this producer's day-walk, which is itself
just "publish a message for this date" with no memory of prior runs.
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
    """
    A document whose first_message falls outside every day the walk below
    will ever generate -- missing entirely from the walk's range, or
    outside it -- would never match any day's range query and never reach
    a worker. Both branches below are pure first_message range predicates
    scoped by MIGRATED_EXISTS_FILTER, so first_message_migrated_partial
    covers them directly; this must never become a collection-wide scan
    (see the issue's "Measured baseline" section for why that matters at
    ~8.75M documents).

    Naturally idempotent: a document already sent to the DLQ on a prior
    run still matches the same query here, so re-running just means a
    duplicate DLQ message -- harmless for a human-reviewed dead end.
    """
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
    """The catch-all sweep's first_message predicates only mean "outside
    recorded history" when the requested range covers the full history --
    for any narrower range (a pass-2 tail re-run, or a windowed test run)
    both predicates instead match millions of already-migrated documents
    and flood the DLQ. Auto-enable only for a full-history range; an
    operator can still force either way with --sweep/--no-sweep."""
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
