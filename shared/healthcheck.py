#!/usr/bin/env python3
"""Docker HEALTHCHECK entrypoint for the long-running services (receiver,
message processor, archive processor).

Each writes /app/health/heartbeat every HEALTHCHECK_INTERVAL_SECONDS
seconds, only while genuinely connected to its upstreams, so a stale file
means wedged or disconnected, not merely idle.

Deliberately dependency-free and stdlib-only: a healthcheck that can fail
on an import is worse than no healthcheck at all, which is also why
shared/timing.py (imported below) is itself stdlib-only.
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
