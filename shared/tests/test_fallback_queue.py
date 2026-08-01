import json
import os
import sqlite3
import tempfile
import time
from unittest.mock import patch

from shared.fallback_queue import FallbackQueue


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
            assert {"id", "payload", "queued_at", "retry_count"}.issubset(col_names)

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
            q = _make_queue(td, retry_threshold=5)
            q.put("poison")

            for expected in (1, 2, 3, 4):
                result = q.drain(lambda _p: (_ for _ in ()).throw(RuntimeError("still broken")))
                assert result is False
                conn = sqlite3.connect(os.path.join(td, "queue.db"))
                row = conn.execute("SELECT retry_count FROM queue").fetchone()
                conn.close()
                assert row[0] == expected


class TestDeadLettering:
    def test_row_dead_lettered_at_threshold_and_removed_from_queue(self):
        with tempfile.TemporaryDirectory() as td:
            q = _make_queue(td, retry_threshold=2)
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
            q = _make_queue(td)
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
