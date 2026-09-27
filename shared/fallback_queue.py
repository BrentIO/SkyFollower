"""
Shared SQLite-backed fallback/retry queue with poison-message
dead-lettering, used by the receiver, message processor, and archive
processor when RabbitMQ/S3 is unreachable.

Each row tracks a retry_count. Below `retry_threshold`, a failure just
stops the pass so the same row is retried first next time. At the
threshold, the row is dead-lettered -- written to
`{dirname(db_path)}/dead_letters/{table_name}/` for out-of-band inspection
-- and the pass continues past it, since it's been judged permanently
unrecoverable rather than a dependency still down.

`min_retry_interval_seconds` bounds how often a single row can be
re-attempted, independent of how often the caller invokes drain() -- so a
rapidly-retriggering caller (e.g. a flapping reconnect) can't burn through
`retry_threshold` and dead-letter a row that was never actually poison.

`non_poison_exceptions` marks failures caused by a dependency that is
deliberately absent from this deployment, not a poison payload: those rows
retry forever and are never dead-lettered. `retryable_max_bytes` caps the
resulting unbounded growth with ring-buffer eviction on the plain queue
table.
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
        # Failures meaning "dependency deliberately absent here" -- retried
        # forever, never dead-lettered.
        self._non_poison_exceptions = non_poison_exceptions
        # Opt-in ring-buffer cap (bytes of payload text) on the plain
        # retryable table. None = no cap.
        self._retryable_max_bytes = retryable_max_bytes
        self._dead_letter_dir = os.path.join(
            os.path.dirname(os.path.abspath(db_path)), "dead_letters", table_name
        )

        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        # WAL + NORMAL: commits no longer fsync every call, only at a
        # checkpoint. A process crash still loses nothing (WAL replays on
        # reopen); only a host power loss or kernel panic can drop the
        # last few committed rows -- a trade worth it since these writes
        # land on hot paths during an outage.
        self._conn.execute("PRAGMA synchronous=NORMAL")
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
        # Single-flight guard: `_lock` only covers each individual
        # SELECT/DELETE/UPDATE, so two overlapping drain() calls could each
        # select the same oldest row before either removes it. This lock
        # ensures at most one drain runs at a time for this queue instance.
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

    def put_many(self, payloads: list[str]) -> None:
        """Batch form of put(): one executemany plus a single commit for the
        whole list, so a burst of queued writes costs one commit instead of
        one per message. Rows keep their list order (AUTOINCREMENT id), so a
        later oldest-first drain sees them in the order they were handed in.
        An empty list is a no-op."""
        if not payloads:
            return
        now = time.time()
        with self._lock:
            if self._retryable_max_bytes is not None:
                for payload in payloads:
                    self._evict_retryable_over_cap_locked(len(payload))
            self._conn.executemany(
                f"INSERT INTO {self._table} (payload, queued_at, retry_count) VALUES (?, ?, 0)",
                [(payload, now) for payload in payloads],
            )
            self._conn.commit()

    def _evict_retryable_over_cap_locked(self, incoming_bytes: int) -> None:
        """Ring-buffer eviction for the plain retryable table, mirroring
        `_evict_oldest_if_over_cap()` for the dead-letter directory.
        Caller must hold `self._lock`. This is capacity eviction of
        legitimate queued data, never routed through `dead_letters/`."""
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

        Returns True only if the queue was empty when this returned --
        False for every other case, including a row still in its retry
        cooldown, since the queue isn't actually empty either way.

        A row is left in place (pass stops) on a below-threshold failure,
        a non-poison failure, or an active retry cooldown; it's removed
        (pass continues) on success or once it's dead-lettered at
        `retry_threshold`. See module docstring for the non-poison and
        cooldown rationale.
        """
        while True:
            step = self.drain_one(process_fn)
            if step == DRAIN_EMPTY:
                return True
            if step == DRAIN_STOP:
                return False
            # DRAIN_PROGRESSED -- keep going to whatever's queued behind it.

    def drain_one(self, process_fn: Callable[[str], None]) -> str:
        """Process at most one row -- the oldest -- and return
        ``DRAIN_EMPTY``, ``DRAIN_PROGRESSED``, or ``DRAIN_STOP`` (see
        module-level constants). Same per-row semantics as ``drain()``,
        exposed one row at a time so a caller can interleave
        higher-priority work between rows. ``drain()`` is this in a loop.
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
            return self._record_failure(row_id, payload, retry_count, exc)

    def drain_batch(self, process_fn: Callable[[str], None], max_batch: int) -> str:
        """Batched form of ``drain_one()``: process up to ``max_batch``
        oldest rows in one pass (one commit for all successes), with the
        same outcome codes and per-row semantics -- strict oldest-first,
        stopping selection at the first row still in its retry cooldown,
        and applying normal retry/dead-letter handling to the first row
        that raises.
        """
        if max_batch < 1:
            raise ValueError("max_batch must be >= 1")

        with self._lock:
            rows = self._conn.execute(
                f"SELECT id, payload, retry_count, last_attempted_at "
                f"FROM {self._table} ORDER BY id ASC LIMIT ?",
                (max_batch,),
            ).fetchall()
        if not rows:
            return DRAIN_EMPTY

        now = time.time()
        succeeded: list[int] = []
        failed: Optional[tuple[int, str, int, Exception]] = None
        cooled_down = False
        for row_id, payload, retry_count, last_attempted_at in rows:
            if (
                last_attempted_at is not None
                and (now - last_attempted_at) < self._min_retry_interval_seconds
            ):
                cooled_down = True
                break
            try:
                process_fn(payload)
                succeeded.append(row_id)
            except Exception as exc:
                failed = (row_id, payload, retry_count, exc)
                break

        if succeeded:
            placeholders = ",".join("?" * len(succeeded))
            with self._lock:
                self._conn.execute(
                    f"DELETE FROM {self._table} WHERE id IN ({placeholders})",
                    succeeded,
                )
                self._conn.commit()

        if failed is not None:
            outcome = self._record_failure(*failed)
            return DRAIN_PROGRESSED if (succeeded or outcome == DRAIN_PROGRESSED) else DRAIN_STOP
        if cooled_down:
            return DRAIN_PROGRESSED if succeeded else DRAIN_STOP
        return DRAIN_PROGRESSED

    def _record_failure(
        self, row_id: int, payload: str, retry_count: int, exc: Exception
    ) -> str:
        """Apply the per-row outcome of a failed ``process_fn`` -- advance
        retry_count/last_attempted_at, or dead-letter once the threshold is
        reached. Returns ``DRAIN_PROGRESSED`` if the row was dead-lettered
        (and so removed), ``DRAIN_STOP`` otherwise (row stays queued).
        Shared verbatim by ``drain_one()`` and ``drain_batch()``."""
        new_count = retry_count + 1
        if isinstance(exc, self._non_poison_exceptions):
            # Environmental, not poison -- never dead-letter; counters
            # still advance so an operator can see how long it's stuck.
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
        """Live count of files in the dead-letter directory rather than a
        separately-tracked number, so it can't drift from reality."""
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
        # Fixed-width epoch seconds sort oldest-first lexically; row_id
        # keeps two same-microsecond dead-letters collision-free.
        filename = f"{time.time():016.6f}_{row_id}.json"
        path = os.path.join(self._dead_letter_dir, filename)
        with open(path, "w") as f:
            json.dump(record, f)

        logger.error(
            "Dead-lettered poison message in %s (id=%s, retry_count=%d): %s -- wrote %s",
            self._table, row_id, retry_count, exc, path,
        )

    def _evict_oldest_if_over_cap(self) -> None:
        """Approximate ring-buffer eviction: if the directory is already
        at or over the cap, delete the single oldest file before this
        write. Not a precise pre-check against the incoming file's exact
        size -- a one-out-one-in swap is sufficient given small JSON
        payloads, and self-corrects on the next write otherwise."""
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
