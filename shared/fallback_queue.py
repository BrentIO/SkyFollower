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

logger = logging.getLogger(__name__)

DEFAULT_RETRY_THRESHOLD = 5
DEFAULT_DEAD_LETTER_MAX_BYTES = 100 * 1024 * 1024


class FallbackQueue:
    def __init__(
        self,
        db_path: str,
        table_name: str = "queue",
        retry_threshold: int = DEFAULT_RETRY_THRESHOLD,
        dead_letter_max_bytes: int = DEFAULT_DEAD_LETTER_MAX_BYTES,
    ) -> None:
        self._table = table_name
        self._retry_threshold = retry_threshold
        self._dead_letter_max_bytes = dead_letter_max_bytes
        self._dead_letter_dir = os.path.join(
            os.path.dirname(os.path.abspath(db_path)), "dead_letters", table_name
        )

        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self._table} "
            "(id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT, "
            " queued_at REAL, retry_count INTEGER DEFAULT 0)"
        )
        existing = {row[1] for row in self._conn.execute(f"PRAGMA table_info({self._table})")}
        if "retry_count" not in existing:
            self._conn.execute(f"ALTER TABLE {self._table} ADD COLUMN retry_count INTEGER DEFAULT 0")
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
            self._conn.execute(
                f"INSERT INTO {self._table} (payload, queued_at, retry_count) VALUES (?, ?, 0)",
                (payload, time.time()),
            )
            self._conn.commit()

    def drain(self, process_fn: Callable[[str], None]) -> bool:
        """Drain queued items oldest-first via process_fn(payload).

        Returns True if the queue was fully drained (empty when this
        returned). Returns False if it stopped early because process_fn
        raised and the failing row's retry count is still below
        `retry_threshold` -- callers that gate other state on "the backlog
        is fully clear" need to tell these two outcomes apart, not just
        call this and move on.

        A row that reaches `retry_threshold` is dead-lettered and skipped;
        the pass continues to whatever's queued behind it rather than
        stopping, since that failure has already been judged permanent
        rather than a dependency that might still recover.
        """
        while True:
            with self._lock:
                cur = self._conn.execute(
                    f"SELECT id, payload, retry_count FROM {self._table} ORDER BY id ASC LIMIT 1"
                )
                row = cur.fetchone()
                if row is None:
                    return True
                row_id, payload, retry_count = row

            try:
                process_fn(payload)
                with self._lock:
                    self._conn.execute(f"DELETE FROM {self._table} WHERE id=?", (row_id,))
                    self._conn.commit()
            except Exception as exc:
                new_count = retry_count + 1
                if new_count >= self._retry_threshold:
                    self._dead_letter(row_id, payload, new_count, exc)
                    continue
                with self._lock:
                    self._conn.execute(
                        f"UPDATE {self._table} SET retry_count=? WHERE id=?", (new_count, row_id)
                    )
                    self._conn.commit()
                return False

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
