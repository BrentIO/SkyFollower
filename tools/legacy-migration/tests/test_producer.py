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


class TestRun:
    def test_publishes_one_day_per_date_and_runs_sweep_first(self, monkeypatch):
        collection = _FakeCollection([])
        channel = _FakeChannel()
        connection = _FakeConnection(channel)

        monkeypatch.setattr(producer, "connect_mongo", lambda cfg: collection)
        monkeypatch.setattr(producer, "connect_rabbitmq", lambda cfg: connection)
        monkeypatch.setattr(
            producer, "load_config",
            lambda *blocks: {"rabbitmq": {}, "mongo": {}},
        )

        args = argparse.Namespace(start_date="2024-05-30", end_date="2024-06-01")
        producer.run(args)

        assert channel.published_dates == ["2024-05-30", "2024-05-31", "2024-06-01"]
        assert connection.closed is True

    def test_end_date_defaults_to_today_utc(self, monkeypatch):
        collection = _FakeCollection([])
        channel = _FakeChannel()
        connection = _FakeConnection(channel)

        monkeypatch.setattr(producer, "connect_mongo", lambda cfg: collection)
        monkeypatch.setattr(producer, "connect_rabbitmq", lambda cfg: connection)
        monkeypatch.setattr(producer, "load_config", lambda *blocks: {"rabbitmq": {}, "mongo": {}})
        monkeypatch.setattr(producer, "today_utc_date", lambda: "2024-05-31")

        args = argparse.Namespace(start_date="2024-05-31", end_date=None)
        producer.run(args)

        assert channel.published_dates == ["2024-05-31"]
