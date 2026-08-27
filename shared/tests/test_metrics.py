"""
Tests for shared/metrics.py's next_period_boundary() -- the absolute UTC
boundary computation that drives shared/lua/incr_period_counter.lua's
EXPIREAT argument for message-processor's and archive-processor's period
counters.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from shared.metrics import next_period_boundary


class TestNextPeriodBoundaryHour:
    def test_mid_hour_rolls_to_top_of_next_hour(self):
        now = datetime(2026, 8, 23, 14, 37, 12, tzinfo=timezone.utc)
        expected = datetime(2026, 8, 23, 15, 0, 0, tzinfo=timezone.utc)
        assert next_period_boundary("hour", now) == int(expected.timestamp())

    def test_exact_top_of_hour_still_rolls_to_the_next_one(self):
        # At the instant the previous boundary lands, the new key created
        # by that increment must expire at the *next* boundary, not the one
        # that just passed -- otherwise it would expire immediately.
        now = datetime(2026, 8, 23, 15, 0, 0, tzinfo=timezone.utc)
        expected = datetime(2026, 8, 23, 16, 0, 0, tzinfo=timezone.utc)
        assert next_period_boundary("hour", now) == int(expected.timestamp())

    def test_last_second_of_hour_rolls_to_next_hour(self):
        now = datetime(2026, 8, 23, 14, 59, 59, tzinfo=timezone.utc)
        expected = datetime(2026, 8, 23, 15, 0, 0, tzinfo=timezone.utc)
        assert next_period_boundary("hour", now) == int(expected.timestamp())

    def test_rolls_across_a_day_boundary(self):
        now = datetime(2026, 8, 23, 23, 45, 0, tzinfo=timezone.utc)
        expected = datetime(2026, 8, 24, 0, 0, 0, tzinfo=timezone.utc)
        assert next_period_boundary("hour", now) == int(expected.timestamp())

    def test_ignores_microseconds(self):
        now = datetime(2026, 8, 23, 14, 0, 0, 999999, tzinfo=timezone.utc)
        expected = datetime(2026, 8, 23, 15, 0, 0, tzinfo=timezone.utc)
        assert next_period_boundary("hour", now) == int(expected.timestamp())


class TestNextPeriodBoundaryToday:
    def test_mid_day_rolls_to_next_midnight_utc(self):
        now = datetime(2026, 8, 23, 14, 37, 12, tzinfo=timezone.utc)
        expected = datetime(2026, 8, 24, 0, 0, 0, tzinfo=timezone.utc)
        assert next_period_boundary("today", now) == int(expected.timestamp())

    def test_exact_midnight_still_rolls_to_the_next_one(self):
        now = datetime(2026, 8, 24, 0, 0, 0, tzinfo=timezone.utc)
        expected = datetime(2026, 8, 25, 0, 0, 0, tzinfo=timezone.utc)
        assert next_period_boundary("today", now) == int(expected.timestamp())

    def test_rolls_across_a_month_boundary(self):
        now = datetime(2026, 8, 31, 23, 59, 59, tzinfo=timezone.utc)
        expected = datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert next_period_boundary("today", now) == int(expected.timestamp())


class TestNextPeriodBoundaryDefaultsAndValidation:
    def test_omitted_now_uses_current_utc_time(self):
        before = datetime.now(timezone.utc)
        result = next_period_boundary("hour")
        after = datetime.now(timezone.utc)
        # The returned boundary must be strictly after "now", and no more
        # than one hour past the latest possible "now" sampled around the call.
        assert result > int(before.timestamp())
        assert result <= int(after.timestamp()) + 3600

    def test_invalid_period_raises(self):
        with pytest.raises(ValueError, match="period"):
            next_period_boundary("lifetime")

    def test_garbage_period_raises(self):
        with pytest.raises(ValueError, match="period"):
            next_period_boundary("week")
