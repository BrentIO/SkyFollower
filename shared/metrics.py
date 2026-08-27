"""
Shared helper for the Redis-backed period-counter mechanism used by
message-processor and archive-processor (see shared/lua/incr_period_counter.lua).

A period counter (an "hour" or "today" bucket) resets itself at a real UTC
clock boundary via Redis's own EXPIREAT rather than any scheduled reset job.
next_period_boundary() computes that absolute boundary instant so the
caller can hand it straight to incr_period_counter.lua's EXPIREAT argument,
with no drift between when this is computed and when Redis executes the
expire.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

_VALID_PERIODS = frozenset({"hour", "today"})


def next_period_boundary(period: str, now: Optional[datetime] = None) -> int:
    """Absolute Unix timestamp (UTC) of the next top-of-hour ("hour") or
    next midnight UTC ("today") boundary strictly after `now` (defaults to
    the current UTC time if omitted). "lifetime" periods have no boundary —
    they never expire and aren't accepted here; callers reset a lifetime
    key explicitly at process startup instead (see each component's
    start()).
    """
    if period not in _VALID_PERIODS:
        raise ValueError(f"period must be one of {_VALID_PERIODS}, got: {period!r}")

    if now is None:
        now = datetime.now(timezone.utc)

    if period == "hour":
        boundary = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:  # "today"
        boundary = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    return int(boundary.timestamp())
