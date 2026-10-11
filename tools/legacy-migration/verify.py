"""
Verify role: run by the operator before deleting the legacy S3 bucket and
MongoDB by hand. Read-only against both.

Every remote fetch is persisted to a SQLite cache (VERIFY_CACHE_DIR) as it
arrives, so a run that dies partway resumes at the last completed page and a
re-run never repeats a fetch it already has:

1. Mongo snapshot: one pass over the whole collection (_id, first_message
   day, migrated?, guard reason). After this, Mongo is no longer needed.
2. Source and destination bucket listings (ETag + size come with the
   listing, so there is no per-object HEAD), the index/ listing, and each
   day's compacted Parquet file.
3. Reconciliation of every flight _id across Mongo, source bucket,
   destination bucket and Parquet; any outlier in any direction is reported
   unless the operator listed it in the accepted-loss file.

Matching ETag + size is trusted as byte-exactness; object contents are never
downloaded or compared.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import re
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Callable, Iterable, Iterator, Optional

import pyarrow.parquet as pq

from common import (
    COMPACTED_INDEX_FILENAME,
    SOURCE_KEY_SUFFIX,
    build_s3_client,
    connect_mongo,
    guard_reason,
    iter_dates,
    s3_retry,
)

from shared.archive_index import PARQUET_INDEX_SCHEMA
from shared.config import load_config

logger = logging.getLogger("legacy-migration.verify")

PAGE_SIZE = 1000
MONGO_BATCH = 50_000
DEFAULT_CACHE_DIR = "/app/verify-cache"
DB_FILENAME = "verify-cache.sqlite"
ACCEPTED_LOSS_FILENAME = "accepted-loss.txt"
REPORT_FILENAME = "verify-report.tsv"
REFRESH_TARGETS = ("mongo", "source", "dest", "index", "parquet")
EXAMPLES_PER_CATEGORY = 10

_DEST_KEY_RE = re.compile(r"^flights/(\d{4})/(\d{2})/(\d{2})/([^/]+)\.json\.gz$")
_INDEX_KEY_RE = re.compile(r"^index/year=(\d{4})/month=(\d{2})/day=(\d{2})/" + re.escape(COMPACTED_INDEX_FILENAME) + r"$")
_MAX_CHAR = "\U0010ffff"
_HEX_BOUNDS = list("0123456789abcdef")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS progress (name TEXT PRIMARY KEY, last_key TEXT, done INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS mongo_docs (id TEXT PRIMARY KEY, day TEXT, migrated INTEGER NOT NULL, guard TEXT) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS mongo_docs_day ON mongo_docs (day);
CREATE TABLE IF NOT EXISTS source_objects (id TEXT PRIMARY KEY, etag TEXT, size INTEGER) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS dest_objects (key TEXT PRIMARY KEY, id TEXT, day TEXT, etag TEXT, size INTEGER) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS dest_objects_id ON dest_objects (id);
CREATE TABLE IF NOT EXISTS index_files (day TEXT PRIMARY KEY, key TEXT) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS parquet_days (day TEXT PRIMARY KEY, rows INTEGER NOT NULL, problems TEXT) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS parquet_rows (s3_key TEXT PRIMARY KEY, id TEXT, day TEXT) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS parquet_rows_id ON parquet_rows (id);
"""

_REFRESH_TABLES = {
    "mongo": ["mongo_docs"],
    "source": ["source_objects"],
    "dest": ["dest_objects"],
    "index": ["index_files", "parquet_days", "parquet_rows"],
    "parquet": ["parquet_days", "parquet_rows"],
}
_REFRESH_PROGRESS = {
    "mongo": "mongo",
    "source": "source:%",
    "dest": "dest:%",
    "index": "index",
    "parquet": None,
}


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD, inclusive")
    parser.add_argument(
        "--cache-dir",
        default=None,
        help=f"Where fetched data is persisted (default: $VERIFY_CACHE_DIR or {DEFAULT_CACHE_DIR})",
    )
    parser.add_argument(
        "--refresh",
        action="append",
        choices=REFRESH_TARGETS + ("all",),
        default=[],
        help="Discard and re-fetch a cached dataset; repeatable",
    )
    parser.add_argument(
        "--snapshot-only",
        action="store_true",
        help="Build the Mongo snapshot, then exit; Mongo can be shut down afterwards",
    )
    parser.add_argument(
        "--accepted-loss-file",
        default=None,
        help=f"One '<_id> [reason]' per line (default: <cache-dir>/{ACCEPTED_LOSS_FILENAME} if present)",
    )
    parser.add_argument("--workers", type=int, default=16, help="Concurrent S3 listing/GET threads")


class Cache:
    """SQLite-backed store; every page is committed together with its
    progress marker, so a crash never leaves progress ahead of the data."""

    def __init__(self, path: str):
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._lock = threading.Lock()
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def execute(self, sql: str, params: Iterable = ()) -> list:
        with self._lock:
            return self._conn.execute(sql, tuple(params)).fetchall()

    def meta(self, key: str) -> Optional[str]:
        rows = self.execute("SELECT value FROM meta WHERE key = ?", (key,))
        return rows[0][0] if rows else None

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute("INSERT OR REPLACE INTO meta VALUES (?, ?)", (key, value))
            self._conn.commit()

    def progress(self, name: str) -> Optional[tuple[Optional[str], bool]]:
        rows = self.execute("SELECT last_key, done FROM progress WHERE name = ?", (name,))
        return (rows[0][0], bool(rows[0][1])) if rows else None

    def save_page(self, name: str, insert_sql: str, rows: list, last_key: Optional[str], done: bool) -> None:
        with self._lock:
            if rows:
                self._conn.executemany(insert_sql, rows)
            self._conn.execute("INSERT OR REPLACE INTO progress VALUES (?, ?, ?)", (name, last_key, int(done)))
            self._conn.commit()

    def save_parquet_day(self, day: str, row_count: int, problems: Optional[str], rows: list) -> None:
        with self._lock:
            self._conn.executemany("INSERT OR REPLACE INTO parquet_rows VALUES (?, ?, ?)", rows)
            self._conn.execute("INSERT OR REPLACE INTO parquet_days VALUES (?, ?, ?)", (day, row_count, problems))
            self._conn.commit()

    def set_accepted(self, ids: Iterable[str]) -> None:
        with self._lock:
            self._conn.execute("CREATE TEMP TABLE IF NOT EXISTS accepted (id TEXT PRIMARY KEY)")
            self._conn.execute("DELETE FROM accepted")
            self._conn.executemany("INSERT OR IGNORE INTO accepted VALUES (?)", [(i,) for i in ids])
            self._conn.commit()

    def iter_query(self, sql: str, params: dict) -> Iterator[tuple]:
        with self._lock:
            yield from self._conn.execute(sql, params)

    def refresh(self, targets: list[str]) -> None:
        selected = REFRESH_TARGETS if "all" in targets else targets
        with self._lock:
            for target in selected:
                for table in _REFRESH_TABLES[target]:
                    self._conn.execute(f"DELETE FROM {table}")
                pattern = _REFRESH_PROGRESS[target]
                if pattern:
                    self._conn.execute("DELETE FROM progress WHERE name LIKE ?", (pattern,))
                logger.info("Discarded cached %s data", target)
            self._conn.commit()


def _bind_buckets(cache: Cache, source_bucket: str, dest_bucket: str) -> None:
    for key, value in (("source_bucket", source_bucket), ("dest_bucket", dest_bucket)):
        stored = cache.meta(key)
        if stored is None:
            cache.set_meta(key, value)
        elif stored != value:
            raise SystemExit(
                f"Cache was built for {key}={stored!r} but this run uses {value!r}; "
                "use a different --cache-dir or pass --refresh all"
            )


def _load_accepted(path: Optional[str]) -> dict[str, str]:
    if not path or not os.path.exists(path):
        return {}
    accepted: dict[str, str] = {}
    with open(path) as fh:
        for line in fh:
            line = line.split("#", 1)[0].strip()
            if not line:
                continue
            parts = line.split(None, 1)
            accepted[parts[0]] = parts[1] if len(parts) > 1 else "unspecified"
    return accepted


def _day_of(value) -> Optional[str]:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Mongo snapshot
# ---------------------------------------------------------------------------

_MONGO_PROJECTION = {
    "first_message": 1,
    "last_message": 1,
    "total_messages": 1,
    "aircraft.icao_hex": 1,
    "migrated": 1,
}


def _snapshot_mongo(cache: Cache) -> None:
    state = cache.progress("mongo")
    if state and state[1]:
        logger.info("Mongo snapshot: reusing cached copy")
        return

    collection = connect_mongo(load_config("mongo")["mongo"])
    last = state[0] if state else None
    total = 0
    while True:
        query = {"_id": {"$gt": last}} if last is not None else {}
        docs = list(collection.find(query, _MONGO_PROJECTION).sort("_id", 1).limit(MONGO_BATCH))
        if not docs:
            cache.save_page("mongo", "", [], last, True)
            break
        rows = [
            (str(doc["_id"]), _day_of(doc.get("first_message")), int("migrated" in doc), guard_reason(doc))
            for doc in docs
        ]
        last = str(docs[-1]["_id"])
        total += len(rows)
        cache.save_page("mongo", "INSERT OR REPLACE INTO mongo_docs VALUES (?, ?, ?, ?)", rows, last, False)
        logger.info("Mongo snapshot: %d document(s) this run", total)


# ---------------------------------------------------------------------------
# S3 listings
# ---------------------------------------------------------------------------

def _list_segment(
    s3_client,
    bucket: str,
    cache: Cache,
    name: str,
    insert_sql: str,
    row_fn: Callable[[dict], Optional[tuple]],
    prefix: str = "",
    start_after: str = "",
    stop_before: Optional[str] = None,
) -> None:
    state = cache.progress(name)
    if state and state[1]:
        return
    last = state[0] if state and state[0] else start_after

    while True:
        kwargs = {"Bucket": bucket, "MaxKeys": PAGE_SIZE}
        if prefix:
            kwargs["Prefix"] = prefix
        if last:
            kwargs["StartAfter"] = last
        response = s3_retry(s3_client.list_objects_v2, **kwargs)

        finished = not response.get("IsTruncated")
        rows = []
        for obj in response.get("Contents", []):
            if stop_before is not None and obj["Key"] >= stop_before:
                finished = True
                break
            last = obj["Key"]
            row = row_fn(obj)
            if row is not None:
                rows.append(row)
        cache.save_page(name, insert_sql, rows, last, finished)
        if finished:
            return


def _source_row(obj: dict) -> tuple:
    key = obj["Key"]
    doc_id = key[: -len(SOURCE_KEY_SUFFIX)] if key.endswith(SOURCE_KEY_SUFFIX) else key
    return (doc_id, obj["ETag"], obj["Size"])


def _dest_row(obj: dict) -> tuple:
    key = obj["Key"]
    match = _DEST_KEY_RE.match(key)
    if match:
        yyyy, mm, dd, doc_id = match.groups()
        return (key, doc_id, f"{yyyy}-{mm}-{dd}", obj["ETag"], obj["Size"])
    return (key, key.rsplit("/", 1)[-1], None, obj["ETag"], obj["Size"])


def _index_row(obj: dict) -> Optional[tuple]:
    match = _INDEX_KEY_RE.match(obj["Key"])
    if not match:
        return None
    yyyy, mm, dd = match.groups()
    return (f"{yyyy}-{mm}-{dd}", obj["Key"])


def _run_parallel(tasks: list[Callable[[], None]], workers: int) -> None:
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        for future in [pool.submit(task) for task in tasks]:
            future.result()


def _list_source(s3_client, bucket: str, cache: Cache, workers: int) -> None:
    # Ranges, not prefixes, so keys outside 0-f are still covered exactly once.
    starts = [""] + [b + _MAX_CHAR for b in _HEX_BOUNDS[:-1]]
    stops: list[Optional[str]] = list(_HEX_BOUNDS[1:]) + [None]
    sql = "INSERT OR REPLACE INTO source_objects VALUES (?, ?, ?)"
    tasks = [
        (lambda i=i: _list_segment(
            s3_client, bucket, cache, f"source:{i:02d}", sql, _source_row,
            start_after=starts[i], stop_before=stops[i],
        ))
        for i in range(len(starts))
    ]
    logger.info("Listing source bucket (%d range(s))", len(tasks))
    _run_parallel(tasks, workers)


def _months(start_date: str, end_date: str) -> list[str]:
    seen: list[str] = []
    for day in iter_dates(start_date, end_date):
        month = day[:7]
        if not seen or seen[-1] != month:
            seen.append(month)
    return seen


def _list_dest(s3_client, bucket: str, cache: Cache, workers: int, start_date: str, end_date: str) -> None:
    sql = "INSERT OR REPLACE INTO dest_objects VALUES (?, ?, ?, ?, ?)"
    tasks = [
        (lambda month=month: _list_segment(
            s3_client, bucket, cache, f"dest:{month}", sql, _dest_row,
            prefix=f"flights/{month.replace('-', '/')}/",
        ))
        for month in _months(start_date, end_date)
    ]
    logger.info("Listing destination bucket (%d month(s))", len(tasks))
    _run_parallel(tasks, workers)


def _list_index(s3_client, bucket: str, cache: Cache) -> None:
    _list_segment(
        s3_client, bucket, cache, "index", "INSERT OR REPLACE INTO index_files VALUES (?, ?)",
        _index_row, prefix="index/",
    )


def _fetch_parquet_day(s3_client, bucket: str, cache: Cache, day: str, key: str) -> None:
    body = s3_retry(s3_client.get_object, Bucket=bucket, Key=key)["Body"].read()
    problems: list[str] = []
    rows: list[tuple] = []
    row_count = 0
    try:
        table = pq.read_table(io.BytesIO(body))
    except Exception as exc:
        problems.append(f"unreadable: {exc}")
    else:
        row_count = table.num_rows
        if not table.schema.equals(PARQUET_INDEX_SCHEMA):
            problems.append("schema does not match PARQUET_INDEX_SCHEMA")
        if "s3_key" in table.schema.names and "first_message" in table.schema.names:
            bad_dates = 0
            for s3_key, first_message in zip(
                table.column("s3_key").to_pylist(), table.column("first_message").to_pylist()
            ):
                if _day_of(first_message) != day:
                    bad_dates += 1
                match = _DEST_KEY_RE.match(s3_key or "")
                if not match or "-".join(match.groups()[:3]) != day:
                    problems.append(f"s3_key outside the day: {s3_key}")
                    continue
                rows.append((s3_key, match.group(4), day))
            if bad_dates:
                problems.append(f"{bad_dates} row(s) with first_message outside the day")
            if len({r[0] for r in rows}) != len(rows):
                problems.append("duplicate s3_key rows")
        else:
            problems.append("missing s3_key or first_message column")
    cache.save_parquet_day(day, row_count, "; ".join(problems[:5]) or None, rows)


def _fetch_parquet(s3_client, bucket: str, cache: Cache, workers: int, start_date: str, end_date: str) -> None:
    done = {r[0] for r in cache.execute("SELECT day FROM parquet_days")}
    pending = [
        (day, key)
        for day, key in cache.execute(
            "SELECT day, key FROM index_files WHERE day BETWEEN ? AND ? ORDER BY day", (start_date, end_date)
        )
        if day not in done
    ]
    logger.info("Reading %d Parquet index file(s) (%d already cached)", len(pending), len(done))
    _run_parallel(
        [(lambda d=d, k=k: _fetch_parquet_day(s3_client, bucket, cache, d, k)) for d, k in pending],
        workers,
    )


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

_IN_RANGE = "m.migrated = 1 AND m.day BETWEEN :start AND :end"
_NOT_ACCEPTED = "NOT IN (SELECT id FROM accepted)"

_CHECKS: list[tuple[str, str]] = [
    ("mongo_not_migrated", """
        SELECT m.id, 'day=' || COALESCE(m.day, 'null') || ' guard=' || COALESCE(m.guard, 'none')
        FROM mongo_docs m WHERE m.migrated = 0 AND m.id """ + _NOT_ACCEPTED),
    ("mongo_migrated_out_of_range", """
        SELECT m.id, 'day=' || COALESCE(m.day, 'null') || ' guard=' || COALESCE(m.guard, 'none')
        FROM mongo_docs m
        WHERE m.migrated = 1 AND (m.day IS NULL OR m.day NOT BETWEEN :start AND :end) AND m.id """ + _NOT_ACCEPTED),
    ("missing_from_source", f"""
        SELECT m.id, 'day=' || m.day FROM mongo_docs m
        WHERE {_IN_RANGE} AND m.id {_NOT_ACCEPTED}
          AND NOT EXISTS (SELECT 1 FROM source_objects s WHERE s.id = m.id)"""),
    ("missing_from_destination", f"""
        SELECT m.id, 'day=' || m.day || ' guard=' || COALESCE(m.guard, 'none') FROM mongo_docs m
        WHERE {_IN_RANGE} AND m.id {_NOT_ACCEPTED}
          AND NOT EXISTS (SELECT 1 FROM dest_objects d WHERE d.id = m.id)"""),
    ("missing_from_parquet", f"""
        SELECT m.id, 'day=' || m.day FROM mongo_docs m
        WHERE {_IN_RANGE} AND m.id {_NOT_ACCEPTED}
          AND NOT EXISTS (SELECT 1 FROM parquet_rows p WHERE p.id = m.id)"""),
    ("parquet_not_in_mongo", f"""
        SELECT p.s3_key, 'day=' || p.day FROM parquet_rows p
        WHERE p.id {_NOT_ACCEPTED}
          AND NOT EXISTS (SELECT 1 FROM mongo_docs m WHERE m.id = p.id AND {_IN_RANGE})"""),
    ("parquet_not_in_destination", """
        SELECT p.s3_key, 'day=' || p.day FROM parquet_rows p
        WHERE NOT EXISTS (SELECT 1 FROM dest_objects d WHERE d.key = p.s3_key)"""),
    ("destination_day_mismatch", f"""
        SELECT d.key, 'destination day=' || COALESCE(d.day, 'null') || ' mongo day=' || m.day
        FROM dest_objects d JOIN mongo_docs m ON m.id = d.id
        WHERE {_IN_RANGE} AND d.day IS NOT m.day"""),
    ("parquet_day_mismatch", f"""
        SELECT p.s3_key, 'parquet day=' || p.day || ' mongo day=' || m.day
        FROM parquet_rows p JOIN mongo_docs m ON m.id = p.id
        WHERE {_IN_RANGE} AND p.day IS NOT m.day"""),
    ("source_not_in_mongo", """
        SELECT s.id, 'size=' || s.size FROM source_objects s
        WHERE s.id """ + _NOT_ACCEPTED + """
          AND NOT EXISTS (SELECT 1 FROM mongo_docs m WHERE m.id = s.id)"""),
    ("etag_or_size_mismatch", """
        SELECT d.key, 'source etag=' || s.etag || ' size=' || s.size || ' destination etag=' || d.etag || ' size=' || d.size
        FROM dest_objects d JOIN source_objects s ON s.id = d.id
        WHERE d.etag != s.etag OR d.size != s.size"""),
    ("day_missing_from_destination", f"""
        SELECT m.day, 'mongo flights=' || COUNT(*) FROM mongo_docs m
        WHERE {_IN_RANGE} AND m.id {_NOT_ACCEPTED}
        GROUP BY m.day
        HAVING SUM(EXISTS (SELECT 1 FROM dest_objects d WHERE d.id = m.id)) = 0"""),
    ("day_missing_index_file", f"""
        SELECT m.day, 'mongo flights=' || COUNT(*) FROM mongo_docs m
        WHERE {_IN_RANGE} AND m.id {_NOT_ACCEPTED}
          AND m.day NOT IN (SELECT day FROM index_files)
        GROUP BY m.day"""),
    ("index_file_without_mongo_flights", """
        SELECT i.day, i.key FROM index_files i
        WHERE i.day BETWEEN :start AND :end
          AND NOT EXISTS (SELECT 1 FROM mongo_docs m WHERE m.day = i.day AND m.migrated = 1)"""),
    ("index_file_invalid", """
        SELECT pd.day, 'rows=' || pd.rows || ' ' || COALESCE(pd.problems, 'empty file')
        FROM parquet_days pd
        WHERE pd.day BETWEEN :start AND :end AND (pd.problems IS NOT NULL OR pd.rows = 0)"""),
]


def _reconcile(cache: Cache, start_date: str, end_date: str, accepted: dict[str, str], report_path: str) -> bool:
    cache.set_accepted(accepted)

    params = {"start": start_date, "end": end_date}
    totals = cache.execute(
        "SELECT COUNT(*), SUM(migrated = 1 AND day BETWEEN ? AND ?), SUM(migrated = 0) FROM mongo_docs",
        (start_date, end_date),
    )[0]
    logger.info(
        "Mongo documents: %d total, %d migrated in range, %d not migrated",
        totals[0], totals[1] or 0, totals[2] or 0,
    )

    problem_count = 0
    with open(report_path, "w") as report:
        report.write("category\tkey\tdetail\n")
        for category, sql in _CHECKS:
            count = 0
            for key, detail in cache.iter_query(sql, params):
                report.write(f"{category}\t{key}\t{detail}\n")
                if count < EXAMPLES_PER_CATEGORY:
                    logger.warning("%s: %s (%s)", category, key, detail)
                count += 1
            if count:
                problem_count += count
                logger.warning("%s: %d outlier(s) (all listed in %s)", category, count, report_path)

    missing_accepted = [
        i for i in accepted
        if not cache.execute("SELECT 1 FROM dest_objects WHERE id = ?", (i,))
    ]
    reasons: dict[str, int] = {}
    for doc_id in missing_accepted:
        reasons[accepted[doc_id]] = reasons.get(accepted[doc_id], 0) + 1
    logger.info(
        "Accepted losses: %d listed, %d absent from the destination%s",
        len(accepted), len(missing_accepted),
        "".join(f"; {n} x {reason}" for reason, n in sorted(reasons.items())),
    )
    live = cache.execute(
        "SELECT COUNT(*) FROM dest_objects d WHERE NOT EXISTS (SELECT 1 FROM mongo_docs m WHERE m.id = d.id)"
    )[0][0]
    logger.info("Destination objects not belonging to the migration (live pipeline): %d", live)
    return problem_count == 0


def run(args: argparse.Namespace) -> None:
    cfg = load_config("legacy_migration_s3")["legacy_migration_s3"]
    cache_dir = args.cache_dir or os.environ.get("VERIFY_CACHE_DIR", DEFAULT_CACHE_DIR)
    os.makedirs(cache_dir, exist_ok=True)
    cache = Cache(os.path.join(cache_dir, DB_FILENAME))

    if args.refresh:
        cache.refresh(args.refresh)
    _bind_buckets(cache, cfg["source_bucket"], cfg["dest_bucket"])

    accepted_path = args.accepted_loss_file or os.path.join(cache_dir, ACCEPTED_LOSS_FILENAME)
    accepted = _load_accepted(accepted_path)
    logger.info("Accepted-loss file: %d id(s) from %s", len(accepted), accepted_path if accepted else "none")

    _snapshot_mongo(cache)
    if args.snapshot_only:
        logger.info("Mongo snapshot complete; MongoDB is no longer needed")
        return

    s3_client = build_s3_client()
    _list_source(s3_client, cfg["source_bucket"], cache, args.workers)
    _list_dest(s3_client, cfg["dest_bucket"], cache, args.workers, args.start_date, args.end_date)
    _list_index(s3_client, cfg["dest_bucket"], cache)
    _fetch_parquet(s3_client, cfg["dest_bucket"], cache, args.workers, args.start_date, args.end_date)

    report_path = os.path.join(cache_dir, REPORT_FILENAME)
    clean = _reconcile(cache, args.start_date, args.end_date, accepted, report_path)
    logger.info("Verify complete -- %s", "CLEAN" if clean else "NOT CLEAN")
    if not clean:
        raise SystemExit(1)
