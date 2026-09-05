"""Tests for producer.py -- the catch-all sweep and day-walk publishing."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

import producer


class _FakeCollection:
    def __init__(self, docs):
        self._docs = docs
        self.last_query = None

    def find(self, query, projection=None):
        self.last_query = query
        return list(self._docs)


class _FakeChannel:
    def __init__(self):
        self.dlq = []
        self.published_dates = []

    def queue_declare(self, queue, durable=True):
        pass

    def basic_publish(self, exchange, routing_key, body, properties=None):
        payload = json.loads(body)
        if routing_key == "legacy-migration-dlq":
            self.dlq.append(payload["_id"])
        else:
            self.published_dates.append(payload["date"])


class _FakeConnection:
    def __init__(self, channel):
        self._channel = channel
        self.closed = False

    def channel(self):
        return self._channel

    def close(self):
        self.closed = True


class TestCatchAllSweep:
    def test_sweeps_documents_outside_the_requested_range(self):
        collection = _FakeCollection([{"_id": "out-of-range-1"}, {"_id": "out-of-range-2"}])
        channel = _FakeChannel()
        count = producer._run_catch_all_sweep(collection, channel, "2024-05-01", "2024-05-31")
        assert count == 2
        assert channel.dlq == ["out-of-range-1", "out-of-range-2"]

    def test_query_is_scoped_by_migrated_exists_and_first_message_range(self):
        collection = _FakeCollection([])
        channel = _FakeChannel()
        producer._run_catch_all_sweep(collection, channel, "2024-05-01", "2024-05-31")
        query = collection.last_query
        assert query["migrated"] == {"$exists": True}
        assert query["$or"][0]["first_message"]["$lt"] == datetime(2024, 5, 1, tzinfo=timezone.utc)
        assert query["$or"][1]["first_message"]["$gte"] == datetime(2024, 6, 1, tzinfo=timezone.utc)


class TestShouldSweep:
    def test_true_for_full_history_range(self, monkeypatch):
        monkeypatch.setattr(producer, "today_utc_date", lambda: "2026-09-05")
        assert producer._should_sweep(producer.EARLIEST_FLIGHT_DATE, "2026-09-05") is True
        # Wider than strictly necessary is still a full-history range.
        assert producer._should_sweep("2020-01-01", "2027-01-01") is True

    def test_false_for_a_later_start_date(self, monkeypatch):
        monkeypatch.setattr(producer, "today_utc_date", lambda: "2026-09-05")
        assert producer._should_sweep("2026-08-01", "2026-09-05") is False

    def test_false_for_an_earlier_end_date(self, monkeypatch):
        monkeypatch.setattr(producer, "today_utc_date", lambda: "2026-09-05")
        assert producer._should_sweep(producer.EARLIEST_FLIGHT_DATE, "2022-07-13") is False


class TestRun:
    def _run(self, monkeypatch, collection, channel, connection, **args_kwargs):
        monkeypatch.setattr(producer, "connect_mongo", lambda cfg: collection)
        monkeypatch.setattr(producer, "connect_rabbitmq", lambda cfg: connection)
        monkeypatch.setattr(
            producer, "load_config",
            lambda *blocks: {"rabbitmq": {}, "mongo": {}},
        )
        args_kwargs.setdefault("sweep", None)
        producer.run(argparse.Namespace(**args_kwargs))

    def test_publishes_one_day_per_date(self, monkeypatch):
        collection = _FakeCollection([])
        channel = _FakeChannel()
        connection = _FakeConnection(channel)

        self._run(monkeypatch, collection, channel, connection, start_date="2024-05-30", end_date="2024-06-01")

        assert channel.published_dates == ["2024-05-30", "2024-05-31", "2024-06-01"]
        assert connection.closed is True

    def test_end_date_defaults_to_today_utc(self, monkeypatch):
        collection = _FakeCollection([])
        channel = _FakeChannel()
        connection = _FakeConnection(channel)
        monkeypatch.setattr(producer, "today_utc_date", lambda: "2024-05-31")

        self._run(monkeypatch, collection, channel, connection, start_date="2024-05-31", end_date=None)

        assert channel.published_dates == ["2024-05-31"]

    def test_windowed_run_publishes_zero_sweep_dlq_messages(self, monkeypatch):
        # A pass-2-shaped tail run: start_date well after EARLIEST_FLIGHT_DATE.
        # Every already-migrated document in bulk history would match the
        # sweep's own first_message < start_date branch if the sweep ran.
        collection = _FakeCollection([{"_id": "bulk-history-doc"}])
        channel = _FakeChannel()
        connection = _FakeConnection(channel)
        monkeypatch.setattr(producer, "today_utc_date", lambda: "2026-09-05")

        self._run(monkeypatch, collection, channel, connection, start_date="2026-08-01", end_date="2026-09-05")

        assert channel.dlq == []
        assert collection.last_query is None

    def test_full_range_run_still_sweeps(self, monkeypatch):
        collection = _FakeCollection([{"_id": "genuinely-out-of-range"}])
        channel = _FakeChannel()
        connection = _FakeConnection(channel)
        monkeypatch.setattr(producer, "today_utc_date", lambda: "2026-09-05")

        self._run(
            monkeypatch, collection, channel, connection,
            start_date=producer.EARLIEST_FLIGHT_DATE, end_date="2026-09-05",
        )

        assert channel.dlq == ["genuinely-out-of-range"]

    def test_sweep_flag_forces_it_on_for_a_windowed_range(self, monkeypatch):
        collection = _FakeCollection([{"_id": "forced-sweep-doc"}])
        channel = _FakeChannel()
        connection = _FakeConnection(channel)
        monkeypatch.setattr(producer, "today_utc_date", lambda: "2026-09-05")

        self._run(
            monkeypatch, collection, channel, connection,
            start_date="2026-08-01", end_date="2026-09-05", sweep=True,
        )

        assert channel.dlq == ["forced-sweep-doc"]

    def test_no_sweep_flag_forces_it_off_for_a_full_range(self, monkeypatch):
        collection = _FakeCollection([{"_id": "would-have-swept"}])
        channel = _FakeChannel()
        connection = _FakeConnection(channel)
        monkeypatch.setattr(producer, "today_utc_date", lambda: "2026-09-05")

        self._run(
            monkeypatch, collection, channel, connection,
            start_date=producer.EARLIEST_FLIGHT_DATE, end_date="2026-09-05", sweep=False,
        )

        assert channel.dlq == []
        assert collection.last_query is None
