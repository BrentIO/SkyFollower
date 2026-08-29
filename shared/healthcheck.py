#!/usr/bin/env python3
"""Docker HEALTHCHECK entrypoint for the long-running services (receiver,
message processor, archive processor).

Each of those writes /app/health/heartbeat every HEALTHCHECK_INTERVAL_SECONDS
seconds, and only while it is genuinely connected to its upstreams -- so a
stale file means the process is wedged or disconnected, not merely idle.

The staleness threshold (HEALTHCHECK_MAX_AGE_SECONDS) is a shade under three
write intervals: one missed write is normal jitter (a slow tick, a paused
container), two in a row is not. Compose polls this every 15s with
retries: 3, so a real outage is reported within roughly a minute while a
single hiccup never flips the container to unhealthy.

Deliberately dependency-free and stdlib-only -- it runs inside every one of
those images, and a healthcheck that can fail on an import is worse than no
healthcheck at all. shared/timing.py, imported below for the threshold, is
itself stdlib-only for exactly this reason.
"""

import os
import sys
import time

# /app is on PYTHONPATH in every image; this keeps the import working when
# the script is invoked directly (its own directory, not /app, is sys.path[0]).
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from shared.timing import HEALTHCHECK_MAX_AGE_SECONDS

HEARTBEAT_PATH = "/app/health/heartbeat"


def main() -> int:
    try:
        age = time.time() - os.path.getmtime(HEARTBEAT_PATH)
    except OSError:
        return 1
    return 0 if age < HEALTHCHECK_MAX_AGE_SECONDS else 1


if __name__ == "__main__":
    sys.exit(main())
