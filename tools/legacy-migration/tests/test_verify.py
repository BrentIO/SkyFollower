"""Tests for verify.py -- count reconciliation and ETag byte-exactness."""

from __future__ import annotations

import argparse
import re

import pytest
from botocore.exceptions import ClientError

import verify


def _date_from_index_key(key: str) -> str:
    # index/year={YYYY}/month={MM}/day={DD}/legacy-migration.parquet -> YYYY-MM-DD
    match = re.match(r"index/year=(\d{4})/month=(\d{2})/day=(\d{2})/", key)
    return "-".join(match.groups())


class _FakeCollection:
    def __init__(self, counts: dict[str, int]):
        self._counts = counts

    def count_documents(self, query):
        start = query["first_message"]["$gte"]
        return self._counts.get(start.strftime("%Y-%m-%d"), 0)


class _FakePaginator:
    def __init__(self, pages_by_prefix: dict):
        self._pages_by_prefix = pages_by_prefix

    def paginate(self, Bucket, Prefix):
        return self._pages_by_prefix.get(Prefix, [])


class _FakeS3:
    def __init__(self, pages_by_prefix: dict, source_etags: dict, index_exists=True):
        self._paginator = _FakePaginator(pages_by_prefix)
        self._source_etags = source_etags
        # True (default): every compacted-index HeadObject succeeds, so
        # tests that don't care about the index-presence check are
        # unaffected by it. A set of "YYYY-MM-DD" strings: only those days'
        # index files exist. False: no index file exists for any day.
        self._index_exists = index_exists

    def get_paginator(self, name):
        return self._paginator

    def head_object(self, Bucket, Key):
        if Key.startswith("index/"):
            exists = (
                self._index_exists is True
                or (self._index_exists and _date_from_index_key(Key) in self._index_exists)
            )
            if not exists:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "HeadObject")
            return {}
        doc_id = Key[: -len(".gz")]
        return {"ETag": self._source_etags[doc_id]}


def _wire(monkeypatch, collection, s3):
    monkeypatch.setattr(verify, "connect_mongo", lambda cfg: collection)
    monkeypatch.setattr(verify, "build_s3_client", lambda: s3)
    monkeypatch.setattr(
        verify, "load_config",
        lambda *blocks: {
            "mongo": {},
            "legacy_migration_s3": {"source_bucket": "src", "dest_bucket": "dst"},
        },
    )


class TestRun:
    def test_clean_run_when_counts_and_etags_match(self, monkeypatch):
        prefix = "flights/2024/05/31/"
        pages = {prefix: [{"Contents": [{"Key": prefix + "id1.json.gz", "ETag": '"same"'}]}]}
        _wire(monkeypatch, _FakeCollection({"2024-05-31": 1}), _FakeS3(pages, {"id1": '"same"'}))

        verify.run(argparse.Namespace(start_date="2024-05-31", end_date="2024-05-31"))  # no raise

    def test_s3_short_of_mongo_count_raises_systemexit(self, monkeypatch):
        _wire(monkeypatch, _FakeCollection({"2024-05-31": 5}), _FakeS3({}, {}))
        with pytest.raises(SystemExit):
            verify.run(argparse.Namespace(start_date="2024-05-31", end_date="2024-05-31"))

    def test_s3_ahead_of_mongo_count_is_clean(self, monkeypatch):
        """Days after live-pipeline cutover naturally have more S3 objects
        than Mongo-tracked legacy flights -- a lower bound, not equality."""
        prefix = "flights/2024/05/31/"
        pages = {
            prefix: [{"Contents": [
                {"Key": prefix + "id1.json.gz", "ETag": '"same"'},
                {"Key": prefix + "id2.json.gz", "ETag": '"same2"'},
            ]}]
        }
        _wire(
            monkeypatch,
            _FakeCollection({"2024-05-31": 1}),
            _FakeS3(pages, {"id1": '"same"', "id2": '"same2"'}),
        )
        verify.run(argparse.Namespace(start_date="2024-05-31", end_date="2024-05-31"))  # no raise

    def test_etag_mismatch_raises_systemexit(self, monkeypatch):
        prefix = "flights/2024/05/31/"
        pages = {prefix: [{"Contents": [{"Key": prefix + "id1.json.gz", "ETag": '"dest"'}]}]}
        _wire(monkeypatch, _FakeCollection({"2024-05-31": 1}), _FakeS3(pages, {"id1": '"source"'}))
        with pytest.raises(SystemExit):
            verify.run(argparse.Namespace(start_date="2024-05-31", end_date="2024-05-31"))

    def test_missing_compacted_index_raises_systemexit(self, monkeypatch):
        prefix = "flights/2024/05/31/"
        pages = {prefix: [{"Contents": [{"Key": prefix + "id1.json.gz", "ETag": '"same"'}]}]}
        _wire(
            monkeypatch,
            _FakeCollection({"2024-05-31": 1}),
            _FakeS3(pages, {"id1": '"same"'}, index_exists=set()),
        )
        with pytest.raises(SystemExit):
            verify.run(argparse.Namespace(start_date="2024-05-31", end_date="2024-05-31"))

    def test_present_compacted_index_is_clean(self, monkeypatch):
        prefix = "flights/2024/05/31/"
        pages = {prefix: [{"Contents": [{"Key": prefix + "id1.json.gz", "ETag": '"same"'}]}]}
        _wire(
            monkeypatch,
            _FakeCollection({"2024-05-31": 1}),
            _FakeS3(pages, {"id1": '"same"'}, index_exists={"2024-05-31"}),
        )
        verify.run(argparse.Namespace(start_date="2024-05-31", end_date="2024-05-31"))  # no raise

    def test_zero_mongo_count_day_does_not_require_an_index_file(self, monkeypatch):
        """An empty day is a no-op for the worker too (see worker.process_day) --
        it never writes an index file, so verify must not flag it."""
        _wire(
            monkeypatch,
            _FakeCollection({"2024-05-31": 0}),
            _FakeS3({}, {}, index_exists=set()),
        )
        verify.run(argparse.Namespace(start_date="2024-05-31", end_date="2024-05-31"))  # no raise
