"""Tests for common.py -- the shared date/guard/S3-retry helpers."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError

import common


def _make_doc(**overrides) -> dict:
    doc = {
        "_id": "3c17c3b0-a82c-46db-831e-b5482a53a82e",
        "first_message": datetime(2024, 5, 31, 12, 0, 0, tzinfo=timezone.utc),
        "last_message": datetime(2024, 5, 31, 12, 5, 0, tzinfo=timezone.utc),
        "total_messages": 42,
        "aircraft": {"icao_hex": "A8AE7F", "registration": "N659DL"},
        "ident": "DAL659",
        "migrated": datetime(2024, 6, 1, tzinfo=timezone.utc),
    }
    doc.update(overrides)
    return doc


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code}}, "HeadObject")


class TestIterDates:
    def test_single_day(self):
        assert list(common.iter_dates("2024-05-31", "2024-05-31")) == ["2024-05-31"]

    def test_multi_day_range_inclusive(self):
        assert list(common.iter_dates("2024-05-30", "2024-06-01")) == [
            "2024-05-30", "2024-05-31", "2024-06-01",
        ]

    def test_end_before_start_raises(self):
        with pytest.raises(ValueError):
            list(common.iter_dates("2024-06-01", "2024-05-30"))


class TestDayBounds:
    def test_bounds_are_utc_midnight_to_midnight(self):
        start, end = common.day_bounds_utc("2024-05-31")
        assert start == datetime(2024, 5, 31, tzinfo=timezone.utc)
        assert end == datetime(2024, 6, 1, tzinfo=timezone.utc)


class TestSourceKey:
    def test_flat_gz_key(self):
        assert common.source_key("abc-123") == "abc-123.gz"


class TestGuardReason:
    def test_valid_flight_has_no_guard_reason(self):
        assert common.guard_reason(_make_doc()) is None

    def test_zero_total_messages(self):
        assert common.guard_reason(_make_doc(total_messages=0)) == "zero messages recorded"

    def test_missing_total_messages(self):
        doc = _make_doc()
        del doc["total_messages"]
        assert common.guard_reason(doc) == "zero messages recorded"

    def test_last_before_first(self):
        doc = _make_doc(
            first_message=datetime(2024, 5, 31, 12, 5, tzinfo=timezone.utc),
            last_message=datetime(2024, 5, 31, 12, 0, tzinfo=timezone.utc),
        )
        assert common.guard_reason(doc) == "last_message before first_message"

    def test_missing_icao_hex(self):
        assert common.guard_reason(_make_doc(aircraft={})) == "missing aircraft.icao_hex"

    def test_zero_messages_takes_priority_over_missing_icao_hex(self):
        # Real legacy documents matching this shape (the 4 known
        # aircraft-metadata stubs) trip both guards; only the first
        # matters for the DLQ reason.
        assert common.guard_reason(
            _make_doc(total_messages=0, aircraft={})
        ) == "zero messages recorded"


class TestBuildCompletedFlight:
    def test_round_trips_legacy_stub_fields(self):
        flight = common.build_completed_flight(_make_doc())
        assert flight.id == "3c17c3b0-a82c-46db-831e-b5482a53a82e"
        assert flight.aircraft["icao_hex"] == "A8AE7F"
        assert flight.ident == "DAL659"
        assert flight.positions == []
        assert flight.receiver_sources == []

    def test_extra_legacy_only_fields_are_ignored(self):
        doc = _make_doc(category="Large", adsb_version=2)
        flight = common.build_completed_flight(doc)
        assert flight.aircraft["icao_hex"] == "A8AE7F"


class TestIsNotFound:
    @pytest.mark.parametrize("code", ["404", "NoSuchKey", "NotFound"])
    def test_recognizes_not_found_codes(self, code):
        assert common.is_not_found(_client_error(code)) is True

    def test_other_codes_are_not_not_found(self):
        assert common.is_not_found(_client_error("SlowDown")) is False


class TestS3Retry:
    def test_succeeds_first_try(self):
        assert common.s3_retry(lambda: 42) == 42

    def test_retries_on_throttle_then_succeeds(self, monkeypatch):
        monkeypatch.setattr(common.time, "sleep", lambda *_: None)
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] < 3:
                raise _client_error("SlowDown")
            return "ok"

        assert common.s3_retry(flaky) == "ok"
        assert calls["n"] == 3

    def test_non_throttle_error_raises_immediately(self):
        def boom():
            raise _client_error("AccessDenied")

        with pytest.raises(ClientError):
            common.s3_retry(boom)

    def test_exhausts_attempts_and_raises(self, monkeypatch):
        monkeypatch.setattr(common.time, "sleep", lambda *_: None)

        def always_throttled():
            raise _client_error("Throttling")

        with pytest.raises(ClientError):
            common.s3_retry(always_throttled)


class _FakeS3:
    """Fakes exactly the boto3 calls copy_and_verify()/dest_object_exists()
    make. head_object's *first* call always represents the source-side
    check; the second (only made by copy_and_verify, after copy_object)
    represents the destination-side check."""

    def __init__(self, head_exc=None, source_head=None, dest_head=None, copy_exc=None):
        self._head_exc = head_exc
        self._source_head = source_head or {"ETag": '"abc123"', "ContentLength": 10}
        self._dest_head = dest_head or dict(self._source_head)
        self._copy_exc = copy_exc
        self._head_calls = 0
        self.copy_calls = []

    def head_object(self, Bucket, Key):
        self._head_calls += 1
        if self._head_exc is not None and self._head_calls == 1:
            raise self._head_exc
        return self._source_head if self._head_calls == 1 else self._dest_head

    def copy_object(self, Bucket, Key, CopySource):
        if self._copy_exc is not None:
            raise self._copy_exc
        self.copy_calls.append((Bucket, Key, CopySource))


class TestDestObjectExists:
    def test_true_when_head_object_succeeds(self):
        assert common.dest_object_exists(_FakeS3(), "bucket", "key") is True

    def test_false_when_not_found(self):
        client = _FakeS3(head_exc=_client_error("404"))
        assert common.dest_object_exists(client, "bucket", "key") is False

    def test_reraises_other_errors(self):
        client = _FakeS3(head_exc=_client_error("AccessDenied"))
        with pytest.raises(ClientError):
            common.dest_object_exists(client, "bucket", "key")


class TestCopyAndVerify:
    def test_source_missing_raises_file_not_found(self):
        client = _FakeS3(head_exc=_client_error("NoSuchKey"))
        with pytest.raises(FileNotFoundError):
            common.copy_and_verify(client, "src", "id.gz", "dst", "flights/2024/05/31/id.json.gz")

    def test_successful_copy_verifies_and_returns(self):
        client = _FakeS3()
        common.copy_and_verify(client, "src", "id.gz", "dst", "flights/2024/05/31/id.json.gz")
        assert client.copy_calls == [
            ("dst", "flights/2024/05/31/id.json.gz", {"Bucket": "src", "Key": "id.gz"})
        ]

    def test_etag_mismatch_raises_value_error(self):
        client = _FakeS3(dest_head={"ETag": '"different"', "ContentLength": 10})
        with pytest.raises(ValueError):
            common.copy_and_verify(client, "src", "id.gz", "dst", "flights/2024/05/31/id.json.gz")

    def test_content_length_mismatch_raises_value_error(self):
        client = _FakeS3(dest_head={"ETag": '"abc123"', "ContentLength": 999})
        with pytest.raises(ValueError):
            common.copy_and_verify(client, "src", "id.gz", "dst", "flights/2024/05/31/id.json.gz")
