"""
Shared SQLite-backed fallback/retry queue with poison-message dead-lettering.

Every component that talks to an external dependency (RabbitMQ, S3) queues
locally when that dependency is unreachable, and drains oldest-first once it
recovers. Before this module, `receiver`, `message-processor`, and
`archive-processor` each hand-rolled a near-identical class with no way to
tell "still down, keep retrying" apart from "this exact item will never
succeed" -- a single permanently-failing item blocked everything queued
behind it forever, since drain() stops the whole pass on the first
exception and always re-selects the same oldest (still-failing) row next.

`FallbackQueue` adds a per-row retry count: below `retry_threshold`, a
failure behaves exactly as before (stop the pass, retry from the top next
time). At the threshold, the row is dead-lettered -- written out as a
standalone JSON file under `{dirname(db_path)}/dead_letters/{table_name}/`
for an operator to inspect/discard out-of-band -- and the drain pass
continues to whatever's queued behind it, instead of stopping.

A raw attempt count alone isn't enough: a caller can retry a row far more
often than once per `retry_threshold`-worth-of-outage-time if its own
retry trigger fires in rapid bursts (e.g. a flapping connection
reconnecting every few seconds, each reconnect immediately re-attempting
the head-of-queue row) -- which would dead-letter a perfectly recoverable
row within seconds of real instability, not genuine unrecoverability.
`min_retry_interval_seconds` bounds the *rate* of attempts against a
single row, independent of how often the caller invokes drain(): the
oldest row is skipped (the whole pass stops, to preserve strict
oldest-first ordering) until at least that long has passed since its own
last attempt.

Some failures aren't poison and aren't a recoverable outage either: the
dependency is simply not present in this environment on purpose (e.g. a
message processor publishing completed flights with `mandatory=True`
against an `archive` queue that no operator ever declared because this
deployment runs no archiver). Retrying such a row forever is correct --
dead-lettering it throws away legitimate primary data. A caller passes
the exception type(s) that mean "environmental, not poison" via
`non_poison_exceptions`; a row failing only with those types behaves like
a permanent below-threshold failure and is never written to
`dead_letters/`. To keep that from growing the retryable table without
bound, a caller can also opt into `retryable_max_bytes`: a ring-buffer
cap on the plain `queue` table itself (same oldest-first eviction the
dead-letter directory already has), so an environment that never stands
up the dependency keeps roughly its most recent cap's worth of rows and
drains them all automatically once the dependency finally appears.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Optional

from shared.timing import FALLBACK_RETRY_BACKOFF_SECONDS

logger = logging.getLogger(__name__)

DEFAULT_RETRY_THRESHOLD = 5
DEFAULT_DEAD_LETTER_MAX_BYTES = 100 * 1024 * 1024

# drain_one() outcomes.
DRAIN_EMPTY = "empty"        # nothing queued
DRAIN_PROGRESSED = "progressed"  # one row published (or dead-lettered) and removed
DRAIN_STOP = "stop"         # oldest row failed or is in its retry cooldown


class FallbackQueue:
    def __init__(
        self,
        db_path: str,
        table_name: str = "queue",
        retry_threshold: int = DEFAULT_RETRY_THRESHOLD,
        dead_letter_max_bytes: int = DEFAULT_DEAD_LETTER_MAX_BYTES,
        min_retry_interval_seconds: float = FALLBACK_RETRY_BACKOFF_SECONDS,
        non_poison_exceptions: tuple[type[BaseException], ...] = (),
        retryable_max_bytes: Optional[int] = None,
    ) -> None:
        self._table = table_name
        self._retry_threshold = retry_threshold
        self._dead_letter_max_bytes = dead_letter_max_bytes
        self._min_retry_interval_seconds = min_retry_interval_seconds
        # Exception types that mean "this dependency isn't present in this
        # environment on purpose" -- kept broker-agnostic: the caller
        # supplies the concrete types (e.g. pika.exceptions.UnroutableError).
        # A row failing only with these is retried forever, never dead-lettered.
        self._non_poison_exceptions = non_poison_exceptions
        # Opt-in ring-buffer cap on the plain retryable table (bytes of
        # payload text). Only meaningful for a caller whose queue can
        # legitimately grow unbounded because a non-poison failure keeps
        # rows retrying forever. None = no cap (the historical behaviour).
        self._retryable_max_bytes = retryable_max_bytes
        self._dead_letter_dir = os.path.join(
            os.path.dirname(os.path.abspath(db_path)), "dead_letters", table_name
        )

        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self._table} "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT, "
            " queued_at REAL, retry_count INTEGER DEFAULT 0, "
            " last_attempted_at REAL)"
        )
        existing = {row[1] for row in self._conn.execute(f"PRAGMA table_info({self._table})")}
        if "retry_count" not in existing:
            self._conn.execute(f"ALTER TABLE {self._table} ADD COLUMN retry_count INTEGER DEFAULT 0")
        if "last_attempted_at" not in existing:
            self._conn.execute(f"ALTER TABLE {self._table} ADD COLUMN last_attempted_at REAL")
        self._conn.commit()

        self._lock = threading.Lock()
        # Single-flight guard: drain() only locks around each individual
        # SELECT/DELETE/UPDATE, not the whole fetch-process-delete cycle for
        # a row, so two overlapping drain() calls (e.g. a reconnect trigger
        # and a periodic telemetry-tick trigger firing close together)
        # could each SELECT the same oldest row before either removes it.
        # This lock ensures at most one drain runs at a time for *this*
        # queue instance.
        self._drain_lock = threading.Lock()

    def put(self, payload: str) -> None:
        with self._lock:
            if self._retryable_max_bytes is not None:
                self._evict_retryable_over_cap_locked(len(payload))
            self._conn.execute(
                f"INSERT INTO {self._table} (payload, queued_at, retry_count) VALUES (?, ?, 0)",
                (payload, time.time()),
            )
            self._conn.commit()

    def _evict_retryable_over_cap_locked(self, incoming_bytes: int) -> None:
        """Ring-buffer eviction for the plain retryable table, mirroring
        `_evict_oldest_if_over_cap()` for the dead-letter directory. Caller
        must hold `self._lock`; the commit happens with the subsequent
        INSERT in put(). Evicts oldest rows until the table plus the
        incoming payload fits under the cap. This is a capacity eviction of
        legitimate queued data, not a poison classification -- logged as
        such, and never routed through `dead_letters/`."""
        total = self._conn.execute(
            f"SELECT COALESCE(SUM(LENGTH(payload)), 0) FROM {self._table}"
        ).fetchone()[0]
        while total + incoming_bytes > self._retryable_max_bytes:
            row = self._conn.execute(
                f"SELECT id, LENGTH(payload) FROM {self._table} ORDER BY id ASC LIMIT 1"
            ).fetchone()
            if row is None:
                return
            self._conn.execute(f"DELETE FROM {self._table} WHERE id=?", (row[0],))
            total -= row[1] or 0
            logger.warning(
                "Retryable queue %s over capacity cap (%d bytes); evicted oldest "
                "row id=%s to make room -- capacity eviction of queued data, not a "
                "poison classification",
                self._table, self._retryable_max_bytes, row[0],
            )

    def drain(self, process_fn: Callable[[str], None]) -> bool:
        """Drain queued items oldest-first via process_fn(payload).

        Returns True if the queue was empty when this returned -- False in
        every other case, including a row still sitting in its retry
        cooldown, since the queue isn't actually empty either way. Callers
        that gate other state on "the backlog is fully clear" need this
        distinction, not just to call this and move on.

        A row that reaches `retry_threshold` is dead-lettered and skipped;
        the pass continues to whatever's queued behind it rather than
        stopping, since that failure has already been judged permanent
        rather than a dependency that might still recover.

        A row whose failure is an instance of `non_poison_exceptions` is
        never dead-lettered no matter how many times it's attempted -- that
        failure means the dependency isn't deployed in this environment,
        which is a permanent config choice, not a property of the row. It
        keeps behaving like a below-threshold failure (stop the pass, retry
        next time); disk growth for this case is bounded by
        `retryable_max_bytes` instead.

        A row attempted less than `min_retry_interval_seconds` ago is left
        alone this pass -- this caps how fast a row can accumulate retries
        regardless of how often the caller invokes drain(), so a caller
        whose own retry trigger fires in rapid bursts (e.g. a flapping
        connection reconnecting every few seconds, each reconnect
        immediately re-draining) can't burn through retry_threshold in
        well under a real recovery window and dead-letter a row that was
        never actually poison. The whole pass stops rather than skipping
        ahead to a newer row, to preserve strict oldest-first ordering.
        """
        while True:
            step = self.drain_one(process_fn)
            if step == DRAIN_EMPTY:
                return True
            if step == DRAIN_STOP:
                return False
            # DRAIN_PROGRESSED -- keep going to whatever's queued behind it.

    def drain_one(self, process_fn: Callable[[str], None]) -> str:
        """Process at most one row -- the oldest -- and return one of:

        * ``DRAIN_EMPTY``      -- the queue is empty, nothing was attempted.
        * ``DRAIN_PROGRESSED`` -- the oldest row was published (or judged
          poison and dead-lettered) and removed; there may be more behind it.
        * ``DRAIN_STOP``       -- the oldest row failed a below-threshold
          attempt, hit a non-poison failure, or is still inside its
          retry cooldown; it stays queued and the caller should back off
          before retrying.

        Same per-row semantics as ``drain()`` -- retry counting,
        dead-lettering, the non-poison carve-out, strict oldest-first
        ordering -- exposed one row at a time so a caller can interleave
        higher-priority work between rows instead of running the whole
        backlog in one uninterruptible pass. ``drain()`` is this in a loop.
        """
        with self._lock:
            cur = self._conn.execute(
                f"SELECT id, payload, retry_count, last_attempted_at "
                f"FROM {self._table} ORDER BY id ASC LIMIT 1"
            )
            row = cur.fetchone()
            if row is None:
                return DRAIN_EMPTY
            row_id, payload, retry_count, last_attempted_at = row

        if (
            last_attempted_at is not None
            and (time.time() - last_attempted_at) < self._min_retry_interval_seconds
        ):
            return DRAIN_STOP

        try:
            process_fn(payload)
            with self._lock:
                self._conn.execute(f"DELETE FROM {self._table} WHERE id=?", (row_id,))
                self._conn.commit()
            return DRAIN_PROGRESSED
        except Exception as exc:
            new_count = retry_count + 1
            if isinstance(exc, self._non_poison_exceptions):
                # Environmental, not poison: the dependency simply isn't
                # present in this deployment. Behave exactly like a
                # below-threshold failure forever -- stop the pass, retry
                # from the top next drain, respect the cooldown -- and
                # never dead-letter. retry_count/last_attempted_at still
                # advance, purely so an operator can see how long the row
                # has been stuck.
                with self._lock:
                    self._conn.execute(
                        f"UPDATE {self._table} SET retry_count=?, last_attempted_at=? WHERE id=?",
                        (new_count, time.time(), row_id),
                    )
                    self._conn.commit()
                return DRAIN_STOP
            if new_count >= self._retry_threshold:
                self._dead_letter(row_id, payload, new_count, exc)
                return DRAIN_PROGRESSED
            with self._lock:
                self._conn.execute(
                    f"UPDATE {self._table} SET retry_count=?, last_attempted_at=? WHERE id=?",
                    (new_count, time.time(), row_id),
                )
                self._conn.commit()
            return DRAIN_STOP

    def drain_in_background(
        self, process_fn: Callable[[str], None], on_done: Optional[Callable[[], None]] = None
    ) -> None:
        """Spawn a background thread to drain(), unless a drain is already
        in progress for this queue -- in which case this is a cheap no-op
        rather than a second overlapping drain. Never blocks the caller.
        on_done(), if given, runs after the drain completes."""
        if not self._drain_lock.acquire(blocking=False):
            logger.debug("Drain already in progress; skipping this trigger.")
            return

        def _run() -> None:
            try:
                self.drain(process_fn)
            finally:
                self._drain_lock.release()
            if on_done:
                on_done()

        threading.Thread(target=_run, daemon=True, name="fallback-drain").start()

    def depth(self) -> int:
        with self._lock:
            cur = self._conn.execute(f"SELECT COUNT(*) FROM {self._table}")
            return cur.fetchone()[0]

    def dead_letter_depth(self) -> int:
        """Live count of files in the dead-letter directory -- not a
        separately-tracked number that could drift. An operator manually
        deleting a file has the same effect as the code deleting one: the
        next call just recounts."""
        if not os.path.isdir(self._dead_letter_dir):
            return 0
        return sum(
            1 for name in os.listdir(self._dead_letter_dir)
            if os.path.isfile(os.path.join(self._dead_letter_dir, name))
        )

    def _dead_letter(self, row_id: int, payload: str, retry_count: int, exc: Exception) -> None:
        with self._lock:
            self._conn.execute(f"DELETE FROM {self._table} WHERE id=?", (row_id,))
            self._conn.commit()

        os.makedirs(self._dead_letter_dir, exist_ok=True)
        self._evict_oldest_if_over_cap()

        try:
            parsed_payload = json.loads(payload)
        except (json.JSONDecodeError, TypeError):
            parsed_payload = payload

        record = {
            "payload": parsed_payload,
            "retry_count": retry_count,
            "error": str(exc),
            "dead_lettered_at": datetime.now(timezone.utc).isoformat(),
        }
        # Filename sorts oldest-first lexically (fixed-width epoch seconds)
        # and is collision-free even for two dead-letters in the same
        # queue instance at the same microsecond, since row_id is unique.
        filename = f"{time.time():016.6f}_{row_id}.json"
        path = os.path.join(self._dead_letter_dir, filename)
        with open(path, "w") as f:
            json.dump(record, f)

        logger.error(
            "Dead-lettered poison message in %s (id=%s, retry_count=%d): %s -- wrote %s",
            self._table, row_id, retry_count, exc, path,
        )

    def _evict_oldest_if_over_cap(self) -> None:
        """Approximate ring-buffer eviction: if the directory is already at
        or over the cap, delete the single oldest file before this write.
        Not a precise pre-check against the incoming file's exact size --
        individual dead-letter files are small JSON payloads, so a
        one-out-one-in swap is sufficient in the normal case and
        self-corrects on the next write if it isn't."""
        try:
            names = os.listdir(self._dead_letter_dir)
        except OSError:
            return

        total = 0
        for name in names:
            try:
                total += os.path.getsize(os.path.join(self._dead_letter_dir, name))
            except OSError:
                continue

        if total < self._dead_letter_max_bytes or not names:
            return

        oldest = sorted(names)[0]
        oldest_path = os.path.join(self._dead_letter_dir, oldest)
        try:
            os.remove(oldest_path)
            logger.warning(
                "Dead-letter directory %s at capacity; evicted oldest file %s",
                self._dead_letter_dir, oldest,
            )
        except OSError as remove_exc:
            logger.warning("Failed to evict oldest dead-letter file %s: %s", oldest_path, remove_exc)
