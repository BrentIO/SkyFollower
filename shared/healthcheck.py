#!/usr/bin/env python3
"""Docker HEALTHCHECK entrypoint for the long-running services (receiver,
message processor, archive processor).

Each of those writes /app/health/heartbeat every 15 seconds, and only while
it is genuinely connected to its upstreams -- so a stale file means the
process is wedged or disconnected, not merely idle.

The 40-second staleness threshold is a shade under three write intervals:
one missed write is normal jitter (a slow tick, a paused container), two in
a row is not. Compose polls this every 15s with retries: 3, so a real
outage is reported within roughly a minute while a single hiccup never
flips the container to unhealthy.

Deliberately dependency-free and stdlib-only -- it runs inside every one of
those images, and a healthcheck that can fail on an import is worse than no
healthcheck at all.
"""

import os
import sys
import time

HEARTBEAT_PATH = "/app/health/heartbeat"
MAX_AGE_SECONDS = 40


def main() -> int:
    try:
        age = time.time() - os.path.getmtime(HEARTBEAT_PATH)
    except OSError:
        return 1
    return 0 if age < MAX_AGE_SECONDS else 1


if __name__ == "__main__":
    sys.exit(main())
