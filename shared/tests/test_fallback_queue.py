import json
import os
import sqlite3
import tempfile
import time
from unittest.mock import patch

from shared.fallback_queue import (
    DRAIN_EMPTY,
    DRAIN_PROGRESSED,
    DRAIN_STOP,
    FallbackQueue,
)
from shared.timing import FALLBACK_RETRY_BACKOFF_SECONDS


def _make_queue(tmp_dir: str, **kwargs) -> FallbackQueue:
    return FallbackQueue(os.path.join(tmp_dir, "queue.db"), **kwargs)


class TestPutAndDepth:
    def test_put_increases_depth(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td)
            assert q.depth() == 0
            q.put("first")
            q.put("second")
            assert q.depth() == 2

    def test_survives_reopen(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "queue.db")
            q = FallbackQueue(path)
            q.put("persistent")
            del q
            q2 = FallbackQueue(path)
            assert q2.depth() == 1

    def test_wal_mode_enabled(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "queue.db")
            FallbackQueue(path)
            conn = sqlite3.connect(path)
            row = conn.execute("PRAGMA journal_mode").fetchone()
            conn.close()
            assert row[0] == "wal"

    def test_schema_columns(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "queue.db")
            FallbackQueue(path)
            conn = sqlite3.connect(path)
            info = conn.execute("PRAGMA table_info(queue)").fetchall()
            conn.close()
            col_names = {row[1] for row in info}
            assert {"id", "payload", "queued_at", "retry_count", "last_attempted_at"}.issubset(col_names)

    def test_custom_table_name(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "s3.db")
            q1 = FallbackQueue(path, table_name="queue")
            q2 = FallbackQueue(path, table_name="index_queue")
            q1.put("a")
            q2.put("b")
            q2.put("c")
            assert q1.depth() == 1
            assert q2.depth() == 2

    def test_migrates_pre_existing_table_missing_retry_count(self):
        """A queue.db created before retry_count existed has no such
        column -- CREATE TABLE IF NOT EXISTS alone won't add it to an
        existing file, so __init__ must ALTER TABLE it in, same pattern as
        message-processor's _migrate_schema."""
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "queue.db")
            conn = sqlite3.connect(path)
            conn.execute(
                "CREATE TABLE queue (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "payload TEXT, queued_at REAL)"
            )
            conn.execute("INSERT INTO queue (payload, queued_at) VALUES ('old', 1.0)")
            conn.commit()
            conn.close()

            q = FallbackQueue(path)
            assert q.depth() == 1
            # The pre-existing row's retry_count backfills to the column
            # default (0), not an error or NULL that breaks comparisons.
            conn = sqlite3.connect(path)
            row = conn.execute("SELECT retry_count FROM queue").fetchone()
            conn.close()
            assert row[0] == 0

    def test_migrates_pre_existing_table_missing_last_attempted_at(self):
        """A queue.db created with retry_count but before last_attempted_at
        existed (e.g. an earlier build of this same PR) needs the same
        ALTER TABLE treatment, independently of the retry_count migration."""
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "queue.db")
            conn = sqlite3.connect(path)
            conn.execute(
                "CREATE TABLE queue (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "payload TEXT, queued_at REAL, retry_count INTEGER DEFAULT 0)"
            )
            conn.execute(
                "INSERT INTO queue (payload, queued_at, retry_count) VALUES ('old', 1.0, 2)"
            )
            conn.commit()
            conn.close()

            q = FallbackQueue(path, retry_threshold=5)
            assert q.depth() == 1
            # NULL last_attempted_at (never attempted since migrating) means
            # the row is immediately eligible -- not permanently cooled down.
            result = q.drain(lambda p: (_ for _ in ()).throw(RuntimeError("still broken")))
            assert result is False
            conn = sqlite3.connect(path)
            row = conn.execute("SELECT retry_count FROM queue").fetchone()
            conn.close()
            assert row[0] == 3  # pre-existing retry_count (2) preserved and incremented


class TestDrainOrderingAndBackoff:
    def test_drain_calls_process_fn_in_order(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td)
            q.put("first")
            q.put("second")
            q.put("third")

            drained: list[str] = []
            result = q.drain(drained.append)

            assert drained == ["first", "second", "third"]
            assert q.depth() == 0
            assert result is True

    def test_drain_on_empty_queue_is_noop(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td)
            called = []
            result = q.drain(called.append)
            assert called == []
            assert result is True

    def test_drain_stops_on_failure_below_threshold_and_increments_retry_count(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=5)
            q.put("first")
            q.put("second")

            def fail(_payload):
                raise RuntimeError("dependency down")

            result = q.drain(fail)

            assert result is False
            assert q.depth() == 2  # nothing removed, nothing dead-lettered
            assert q.dead_letter_depth() == 0

            conn = sqlite3.connect(os.path.join(td, "queue.db"))
            row = conn.execute("SELECT retry_count FROM queue ORDER BY id ASC LIMIT 1").fetchone()
            conn.close()
            assert row[0] == 1

    def test_repeated_failures_of_same_row_accumulate_retry_count(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=5, min_retry_interval_seconds=0)
            q.put("poison")

            for expected in (1, 2, 3, 4):
                result = q.drain(lambda _p: (_ for _ in ()).throw(RuntimeError("still broken")))
                assert result is False
                conn = sqlite3.connect(os.path.join(td, "queue.db"))
                row = conn.execute("SELECT retry_count FROM queue").fetchone()
                conn.close()
                assert row[0] == expected


class TestDrainOne:
    """drain_one() exposes drain()'s per-row step so a caller can
    interleave higher-priority work between rows. Same retry / dead-letter
    / cooldown / oldest-first semantics, one row at a time."""

    def test_empty_queue_returns_drain_empty(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td)
            assert q.drain_one(lambda _p: None) == DRAIN_EMPTY

    def test_one_call_processes_exactly_one_row_oldest_first(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td)
            q.put("first")
            q.put("second")

            seen = []
            assert q.drain_one(seen.append) == DRAIN_PROGRESSED
            assert seen == ["first"]
            assert q.depth() == 1

            assert q.drain_one(seen.append) == DRAIN_PROGRESSED
            assert seen == ["first", "second"]
            assert q.drain_one(seen.append) == DRAIN_EMPTY

    def test_failure_below_threshold_returns_stop_and_keeps_row(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=5)
            q.put("x")
            assert q.drain_one(lambda _p: (_ for _ in ()).throw(RuntimeError())) == DRAIN_STOP
            assert q.depth() == 1

    def test_row_in_retry_cooldown_returns_stop(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=5, min_retry_interval_seconds=999)
            q.put("x")
            q.drain_one(lambda _p: (_ for _ in ()).throw(RuntimeError()))
            # Second call, still inside the cooldown -- process_fn must not
            # even be invoked.
            calls = []
            assert q.drain_one(calls.append) == DRAIN_STOP
            assert calls == []

    def test_poison_row_is_dead_lettered_and_returns_progressed(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=1, min_retry_interval_seconds=0)
            q.put("poison")
            assert q.drain_one(lambda _p: (_ for _ in ()).throw(RuntimeError())) == DRAIN_PROGRESSED
            assert q.depth() == 0
            assert q.dead_letter_depth() == 1


class TestMinRetryInterval:
    """Covers a false-positive dead-lettering risk: a caller whose
    own retry trigger fires in rapid bursts (e.g. a flapping RabbitMQ
    connection reconnecting every few seconds, each reconnect immediately
    re-draining) could otherwise burn through retry_threshold in well
    under a real recovery window, dead-lettering a row that was never
    actually poison -- just unlucky timing during a brief instability."""

    def test_default_min_retry_interval_is_thirty_seconds(self):
        assert FALLBACK_RETRY_BACKOFF_SECONDS == 30

    def test_first_attempt_is_never_blocked_by_cooldown(self):
        """A freshly queued row has no last_attempted_at yet, so it's
        eligible immediately regardless of min_retry_interval_seconds."""
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, min_retry_interval_seconds=9999)
            q.put("payload")
            attempted = []
            q.drain(attempted.append)
            assert attempted == ["payload"]

    def test_immediate_retry_after_a_failure_is_blocked(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=5, min_retry_interval_seconds=9999)
            q.put("poison")

            attempts = []

            def fail(payload):
                attempts.append(payload)
                raise RuntimeError("still broken")

            first = q.drain(fail)
            second = q.drain(fail)  # immediately after -- should be skipped

            assert first is False
            assert second is False
            assert len(attempts) == 1  # the second drain() never actually called fail again

            conn = sqlite3.connect(os.path.join(td, "queue.db"))
            row = conn.execute("SELECT retry_count FROM queue").fetchone()
            conn.close()
            assert row[0] == 1  # not incremented by the blocked second call

    def test_retry_allowed_again_once_the_interval_elapses(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=5, min_retry_interval_seconds=0.05)
            q.put("poison")

            attempts = []

            def fail(payload):
                attempts.append(payload)
                raise RuntimeError("still broken")

            q.drain(fail)
            time.sleep(0.1)
            q.drain(fail)

            assert len(attempts) == 2
            conn = sqlite3.connect(os.path.join(td, "queue.db"))
            row = conn.execute("SELECT retry_count FROM queue").fetchone()
            conn.close()
            assert row[0] == 2

    def test_flapping_bursts_cannot_reach_threshold_faster_than_the_interval_allows(self):
        """Simulates a flapping connection retrying every 10ms (far faster
        than min_retry_interval_seconds) -- the row should still only
        accumulate roughly one retry per interval, not one per call.

        Uses a fake clock instead of real time.sleep(): a real-sleep
        version of this test was flaky under CPU contention from parallel
        pytest-xdist workers, where scheduling delays let more actual
        wall-clock time elapse per loop iteration than the nominal 0.01s
        sleep implied, pushing more calls through the 0.1s cooldown gate
        than expected. A fake clock advanced by a fixed amount per
        iteration is deterministic regardless of real scheduling."""
        with tempfile.TemporaryDirectory() as td:
            # retry_threshold set high enough that reaching it would require
            # ~1s of simulated elapsed time at one attempt per 0.1s interval
            # -- the 20-call burst below spans well under that (~0.2s
            # simulated time), so if the cooldown weren't working it would
            # dead-letter almost immediately (call 1 already reaches a low
            # threshold).
            q = _make_queue(td, retry_threshold=10, min_retry_interval_seconds=0.1)

            def fail(_payload):
                raise RuntimeError("still broken")

            fake_now = [1_000_000.0]
            with patch("shared.fallback_queue.time.time", lambda: fake_now[0]):
                q.put("poison")
                for _ in range(20):  # far more calls than retry_threshold
                    q.drain(fail)
                    fake_now[0] += 0.01  # much shorter than the 0.1s interval

            # 20 calls * 0.01s advance = ~0.2s of simulated elapsed time,
            # which only allows a couple of real attempts through the
            # cooldown gate -- nowhere near 20 calls' worth, and nowhere
            # near retry_threshold.
            assert q.dead_letter_depth() == 0
            conn = sqlite3.connect(os.path.join(td, "queue.db"))
            row = conn.execute("SELECT retry_count FROM queue").fetchone()
            conn.close()
            assert row[0] < 5

    def test_cooldown_stops_the_whole_pass_preserving_order(self):
        """A cooling-down oldest row must not be skipped in favor of a
        newer row behind it -- strict oldest-first ordering matters (e.g.
        archive-processor's split-flight stitching depends on it)."""
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=5, min_retry_interval_seconds=9999)
            q.put("first")
            q.put("second")

            def fail(_payload):
                raise RuntimeError("still broken")

            q.drain(fail)  # "first" fails once, now cooling down
            attempted = []
            q.drain(attempted.append)  # should not reach "second"

            assert attempted == []
            assert q.depth() == 2


class TestDeadLettering:
    def test_row_dead_lettered_at_threshold_and_removed_from_queue(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=2, min_retry_interval_seconds=0)
            q.put('{"flight_id": "abc"}')

            q.drain(lambda _p: (_ for _ in ()).throw(RuntimeError("attempt 1")))  # retry_count -> 1
            result = q.drain(lambda _p: (_ for _ in ()).throw(ValueError("attempt 2")))  # -> 2, dead-lettered

            assert result is True  # queue is now empty -- nothing left to stop on
            assert q.depth() == 0
            assert q.dead_letter_depth() == 1

    def test_drain_continues_past_dead_lettered_row_to_next_item(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=1)
            q.put("poison")
            q.put("healthy")

            processed = []

            def process(payload):
                if payload == "poison":
                    raise RuntimeError("always fails")
                processed.append(payload)

            result = q.drain(process)

            assert result is True
            assert processed == ["healthy"]
            assert q.depth() == 0
            assert q.dead_letter_depth() == 1

    def test_dead_letter_file_contents(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=1)
            q.put('{"_id": "flight-123"}')

            q.drain(lambda _p: (_ for _ in ()).throw(RuntimeError("boom")))

            dl_dir = os.path.join(td, "dead_letters", "queue")
            files = os.listdir(dl_dir)
            assert len(files) == 1
            with open(os.path.join(dl_dir, files[0])) as f:
                record = json.load(f)

            assert record["payload"] == {"_id": "flight-123"}  # parsed, not a string-in-a-string
            assert record["retry_count"] == 1
            assert "boom" in record["error"]
            assert "dead_lettered_at" in record

    def test_dead_letter_handles_non_json_payload(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=1)
            q.put("not-json")

            q.drain(lambda _p: (_ for _ in ()).throw(RuntimeError("boom")))

            dl_dir = os.path.join(td, "dead_letters", "queue")
            with open(os.path.join(dl_dir, os.listdir(dl_dir)[0])) as f:
                record = json.load(f)
            assert record["payload"] == "not-json"

    def test_dead_letter_depth_zero_when_directory_never_created(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td)
            assert q.dead_letter_depth() == 0

    def test_default_retry_threshold_is_five(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, min_retry_interval_seconds=0)
            q.put("poison")

            for _ in range(4):
                result = q.drain(lambda _p: (_ for _ in ()).throw(RuntimeError("x")))
                assert result is False
                assert q.dead_letter_depth() == 0

            result = q.drain(lambda _p: (_ for _ in ()).throw(RuntimeError("x")))
            assert result is True
            assert q.dead_letter_depth() == 1


class TestDeadLetterEviction:
    def test_eviction_when_directory_at_cap(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=1, dead_letter_max_bytes=1)
            q.put("first")
            q.drain(lambda _p: (_ for _ in ()).throw(RuntimeError("x")))
            assert q.dead_letter_depth() == 1

            q.put("second")
            q.drain(lambda _p: (_ for _ in ()).throw(RuntimeError("x")))
            # cap is 1 byte -- always over, so the first file is evicted
            # before the second is written, leaving exactly one on disk.
            assert q.dead_letter_depth() == 1

    def test_eviction_removes_oldest_file(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=1, dead_letter_max_bytes=1)
            q.put("first")
            q.drain(lambda _p: (_ for _ in ()).throw(RuntimeError("x")))
            dl_dir = os.path.join(td, "dead_letters", "queue")
            first_files = set(os.listdir(dl_dir))

            time.sleep(0.01)
            q.put("second")
            q.drain(lambda _p: (_ for _ in ()).throw(RuntimeError("x")))
            second_files = set(os.listdir(dl_dir))

            assert first_files.isdisjoint(second_files)

    def test_no_eviction_when_under_cap(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=1, dead_letter_max_bytes=100 * 1024 * 1024)
            for i in range(3):
                q.put(f"item-{i}")
                q.drain(lambda _p: (_ for _ in ()).throw(RuntimeError("x")))
            assert q.dead_letter_depth() == 3


class _EnvAbsent(RuntimeError):
    """Stands in for pika.exceptions.UnroutableError in these tests --
    FallbackQueue is broker-agnostic and takes the type from the caller."""


class TestNonPoisonExceptions:
    def test_non_poison_exception_never_dead_letters_regardless_of_attempts(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(
                td,
                retry_threshold=3,
                min_retry_interval_seconds=0,
                non_poison_exceptions=(_EnvAbsent,),
            )
            q.put('{"_id": "flight-1"}')

            for _ in range(25):  # far past retry_threshold
                result = q.drain(lambda _p: (_ for _ in ()).throw(_EnvAbsent("no archive queue")))
                assert result is False

            assert q.dead_letter_depth() == 0
            assert q.depth() == 1  # still retryable, nothing lost

    def test_non_poison_row_still_advances_bookkeeping(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(
                td, retry_threshold=3, min_retry_interval_seconds=0,
                non_poison_exceptions=(_EnvAbsent,),
            )
            q.put("payload")
            for expected in (1, 2, 3, 4, 5):
                q.drain(lambda _p: (_ for _ in ()).throw(_EnvAbsent("x")))
                conn = sqlite3.connect(os.path.join(td, "queue.db"))
                row = conn.execute("SELECT retry_count FROM queue").fetchone()
                conn.close()
                assert row[0] == expected

    def test_non_poison_row_drains_once_dependency_recovers(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(
                td, retry_threshold=2, min_retry_interval_seconds=0,
                non_poison_exceptions=(_EnvAbsent,),
            )
            q.put("a")
            q.put("b")
            for _ in range(10):
                q.drain(lambda _p: (_ for _ in ()).throw(_EnvAbsent("x")))
            assert q.depth() == 2
            assert q.dead_letter_depth() == 0

            drained = []
            result = q.drain(drained.append)  # dependency now present
            assert result is True
            assert drained == ["a", "b"]
            assert q.depth() == 0

    def test_a_non_listed_exception_still_dead_letters_at_threshold(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(
                td, retry_threshold=3, min_retry_interval_seconds=0,
                non_poison_exceptions=(_EnvAbsent,),
            )
            q.put('{"_id": "poison"}')

            for _ in range(2):
                result = q.drain(lambda _p: (_ for _ in ()).throw(ValueError("bad payload")))
                assert result is False
                assert q.dead_letter_depth() == 0

            result = q.drain(lambda _p: (_ for _ in ()).throw(ValueError("bad payload")))
            assert result is True
            assert q.dead_letter_depth() == 1
            assert q.depth() == 0


class TestRetryableTableSizeCap:
    def test_oldest_row_evicted_first_when_over_cap(self):
        with tempfile.TemporaryDirectory() as td:
            # cap fits ~2 of these 10-byte payloads
            q = _make_queue(td, retryable_max_bytes=25)
            q.put("payload-01")  # 10 bytes
            q.put("payload-02")  # 20 bytes total
            q.put("payload-03")  # would be 30 -> evict payload-01 first

            drained = []
            q.drain(drained.append)
            assert drained == ["payload-02", "payload-03"]

    def test_cap_never_exceeded_across_many_puts(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retryable_max_bytes=100)
            for i in range(50):
                q.put(f"{i:020d}")  # 20 bytes each

            conn = sqlite3.connect(os.path.join(td, "queue.db"))
            total = conn.execute("SELECT COALESCE(SUM(LENGTH(payload)), 0) FROM queue").fetchone()[0]
            conn.close()
            assert total <= 100

    def test_no_cap_by_default_leaves_table_unbounded(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td)  # no retryable_max_bytes
            for i in range(200):
                q.put(f"{i:020d}")
            assert q.depth() == 200

    def test_cap_eviction_is_not_dead_lettering(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retryable_max_bytes=25)
            for i in range(10):
                q.put(f"payload-{i:02d}")
            assert q.dead_letter_depth() == 0


class TestDrainInBackground:
    def test_noop_while_a_drain_is_already_in_progress(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td)
            q.put("payload")
            q._drain_lock.acquire()
            try:
                calls = []
                q.drain_in_background(calls.append)
                time.sleep(0.05)
                assert calls == []
                assert q.depth() == 1
            finally:
                q._drain_lock.release()

    def test_runs_and_releases_the_guard(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td)
            q.put("payload")
            calls = []

            q.drain_in_background(calls.append)

            deadline = time.monotonic() + 2
            while q._drain_lock.locked() and time.monotonic() < deadline:
                time.sleep(0.01)

            assert calls == ["payload"]
            assert q.depth() == 0
            assert not q._drain_lock.locked()

    def test_on_done_called_after_drain_completes(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td)
            q.put("payload")
            done = []

            with patch("shared.fallback_queue.threading.Thread") as MockThread:
                class _ImmediateThread:
                    def __init__(self, target=None, daemon=None, name=None):
                        self._target = target

                    def start(self):
                        self._target()

                MockThread.side_effect = _ImmediateThread
                q.drain_in_background(lambda p: None, on_done=lambda: done.append(True))

            assert done == [True]


class TestIndependentDrainLocksPerTable:
    def test_two_tables_in_same_db_have_independent_drain_locks(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "s3.db")
            q1 = FallbackQueue(path, table_name="queue")
            q2 = FallbackQueue(path, table_name="index_queue")
            q1.put("a")
            q2.put("b")

            q1._drain_lock.acquire()
            try:
                calls = []
                q2.drain_in_background(calls.append)
                deadline = time.monotonic() + 2
                while q2._drain_lock.locked() and time.monotonic() < deadline:
                    time.sleep(0.01)
                assert calls == ["b"]
            finally:
                q1._drain_lock.release()
