#!/usr/bin/env python3
"""
SkyFollower TCP Traffic Replayer

Reads a captured NDJSON file (from tools/traffic-recorder) and serves its
1090 MHz messages over a raw TCP *listen* socket in readsb's wire format
(``*<hex>;``), so an unmodified receiver can be pointed at this tool via its
normal ``sources`` config instead of a real readsb instance. Where
tools/traffic-replayer stands in for RabbitMQ (downstream of the receiver),
this tool stands in for readsb (upstream of it), exercising the receiver's
own TCP read loop, frame parsing, routing, publish, and fallback-queue code.

Two replay modes, matching tools/traffic-replayer:

  relative  - preserves the original inter-message timing from received_at
  stress    - sends back-to-back with no artificial pacing, letting TCP flow
              control (a blocking sendall stalling on a full send buffer) be
              the only backpressure

Usage:
    python main.py --input capture.ndjson    --mode relative --port 30002
    python main.py --input capture.ndjson.gz --mode stress   --port 30002
"""

from __future__ import annotations

import argparse
import gzip
import json
import signal
import socket
import sys
import threading
import time

# readsb sends each Mode S frame as ``*<hex>;`` followed by a newline; the
# receiver's parser (shared/adsb_1090.py's parse_tcp_stream) collects the hex
# characters between ``*`` and ``;``. Only source=="1090" rows from a capture
# are served - this tool is deliberately single-listener/single-source (see
# README).
SOURCE_1090 = "1090"

# How often the running "sent" counter is printed during a replay, matching
# tools/traffic-replayer's progress cadence.
PROGRESS_INTERVAL_SECONDS = 5


# ---------------------------------------------------------------------------
# Capture loading (pure - no socket, unit-testable)
# ---------------------------------------------------------------------------

def load_messages(path: str) -> tuple[list[dict], int]:
    """Load an NDJSON capture, keep only ``source == "1090"`` rows, and sort
    them by ``received_at``.

    ``path`` ending in ``.gz`` is read through ``gzip.open(path, "rt")``;
    anything else is read as plain text. Malformed JSON lines print a warning
    to stderr and are skipped rather than aborting the load.

    Returns ``(kept_messages, discarded_count)`` where ``discarded_count`` is
    the number of non-1090 rows filtered out (reported at startup so the drop
    is never silent).
    """
    opener = gzip.open if path.endswith(".gz") else open
    kept: list[dict] = []
    discarded = 0
    with opener(path, "rt") as handle:
        for lineno, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"Warning: skipping line {lineno}: {exc}", file=sys.stderr)
                continue
            if message.get("source") == SOURCE_1090:
                kept.append(message)
            else:
                discarded += 1

    kept.sort(key=lambda message: message["received_at"])
    return kept, discarded


# ---------------------------------------------------------------------------
# Wire format (pure - unit-testable)
# ---------------------------------------------------------------------------

def format_frame(raw: str) -> bytes:
    """Encode one raw Mode S hex string as a readsb wire frame: ``*<hex>;``
    plus a trailing newline. Round-trips through
    ``shared.adsb_1090.parse_tcp_stream``."""
    return b"*" + raw.encode("ascii") + b";\n"


# ---------------------------------------------------------------------------
# Replay (timing + formatting logic; the socket sits behind a plain
#         ``.sendall(bytes)`` sink so this is unit-testable with a fake sink
#         and a fake clock, exactly as tools/traffic-replayer's replay() is)
# ---------------------------------------------------------------------------

class ReplayOutcome:
    """Result of one replay pass over the capture."""

    __slots__ = ("sent", "total", "reason")

    # reason values:
    #   "complete"          - every message was handed to sendall()
    #   "connection-closed" - the socket failed before the capture finished
    #                         (real message loss - must be reported plainly)
    #   "stopped"           - stop_event was set (Ctrl+C / SIGTERM)
    def __init__(self, sent: int, total: int, reason: str):
        self.sent = sent
        self.total = total
        self.reason = reason

    @property
    def complete(self) -> bool:
        return self.reason == "complete"


def replay(
    messages: list[dict],
    sink,
    mode: str,
    stop_event: threading.Event,
) -> ReplayOutcome:
    """Serve ``messages`` to ``sink`` (anything with ``.sendall(bytes)``).

    In ``relative`` mode each message waits until its original offset from
    the first message's ``received_at`` has elapsed; the tool never bursts to
    catch up if it falls behind, and never waits longer than necessary once
    it has. In ``stress`` mode there is no pacing at all - the only thing
    that can slow a send is a blocking ``sendall`` stalling on a full kernel
    send buffer, which is correct TCP backpressure, not something to engineer
    around.
    """
    total = len(messages)
    if total == 0:
        return ReplayOutcome(0, 0, "complete")

    first_ts = messages[0]["received_at"]
    last_ts = messages[-1]["received_at"]
    replay_start = time.monotonic()
    last_progress = replay_start
    sent = 0

    for message in messages:
        if stop_event.is_set():
            return ReplayOutcome(sent, total, "stopped")

        if mode == "relative":
            target = replay_start + (message["received_at"] - first_ts)
            now = time.monotonic()
            if target > now:
                time.sleep(target - now)

        try:
            sink.sendall(format_frame(message["raw"]))
        except OSError:
            return ReplayOutcome(sent, total, "connection-closed")
        sent += 1

        now = time.monotonic()
        if now - last_progress >= PROGRESS_INTERVAL_SECONDS:
            _print_progress(sent, total, replay_start, now, mode, message["received_at"],
                            first_ts, last_ts)
            last_progress = now

    return ReplayOutcome(sent, total, "complete")


def _print_progress(sent, total, replay_start, now, mode, current_ts, first_ts, last_ts):
    elapsed = now - replay_start
    rate = sent / elapsed if elapsed > 0 else 0
    if mode == "relative":
        remaining = (last_ts - first_ts) - (current_ts - first_ts)
        print(
            f"  {sent:,}/{total:,} sent  {rate:,.0f} msg/s  ~{remaining:.0f}s remaining",
            flush=True,
        )
    else:
        print(f"  {sent:,}/{total:,} sent  {rate:,.0f} msg/s", flush=True)


# ---------------------------------------------------------------------------
# Socket glue
# ---------------------------------------------------------------------------

def _wait_for_disconnect(conn: socket.socket, stop_event: threading.Event) -> None:
    """Block after a completed replay until the client goes away (or the tool
    is stopped), so a still-connected receiver isn't forced into a reconnect
    loop by the tool closing the socket the instant the capture ends."""
    print(
        "Full capture sent; holding the connection open. Restart the receiver "
        "to replay from the start, or Ctrl+C to exit.",
        flush=True,
    )
    conn.settimeout(1.0)
    while not stop_event.is_set():
        try:
            data = conn.recv(4096)
        except socket.timeout:
            continue
        except OSError:
            return
        if not data:
            print("Receiver disconnected.", flush=True)
            return


def _serve(
    listen_sock: socket.socket,
    messages: list[dict],
    mode: str,
    stop_event: threading.Event,
) -> None:
    """Accept one connection at a time; each fresh connection replays the
    capture from the start (this tool has no resume-from-offset concept - a
    repeatable from-the-top replay is the point)."""
    total = len(messages)

    while not stop_event.is_set():
        try:
            conn, addr = listen_sock.accept()
        except socket.timeout:
            continue
        except OSError:
            break

        print(
            f"Receiver connected from {addr[0]}:{addr[1]} - replaying "
            f"{total:,} messages from the start ({mode} mode)",
            flush=True,
        )
        conn.settimeout(None)
        start = time.monotonic()
        try:
            outcome = replay(messages, conn, mode, stop_event)
        except OSError:
            outcome = ReplayOutcome(0, total, "connection-closed")
        elapsed = time.monotonic() - start
        rate = outcome.sent / elapsed if elapsed > 0 else 0

        if outcome.reason == "complete":
            print(
                f"\nDone: {outcome.sent:,}/{total:,} messages sent in "
                f"{elapsed:.1f}s ({rate:,.0f} msg/s average)",
                flush=True,
            )
            _wait_for_disconnect(conn, stop_event)
        elif outcome.reason == "connection-closed":
            print(
                f"\nReplay interrupted: {outcome.sent:,} / {total:,} sent before "
                f"the connection closed ({elapsed:.1f}s, {rate:,.0f} msg/s average)",
                flush=True,
            )
        else:  # stopped
            print(
                f"\nReplay stopped: {outcome.sent:,} / {total:,} sent "
                f"({elapsed:.1f}s, {rate:,.0f} msg/s average)",
                flush=True,
            )

        try:
            conn.close()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="SkyFollower TCP Traffic Replayer")
    parser.add_argument(
        "--input",
        required=True,
        help="Input NDJSON capture file (.ndjson, or .ndjson.gz for a gzip-compressed capture)",
    )
    parser.add_argument(
        "--mode",
        choices=["relative", "stress"],
        required=True,
        help="relative: preserve original timing; stress: send as fast as the socket accepts",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=30002,
        help="TCP port to listen on (default: 30002, readsb's raw 1090 port)",
    )
    parser.add_argument(
        "--bind",
        default="0.0.0.0",
        help="Address to bind the listen socket to (default: 0.0.0.0)",
    )
    args = parser.parse_args()

    print(f"Loading {args.input} ...", flush=True)
    try:
        messages, discarded = load_messages(args.input)
    except OSError as exc:
        print(f"Failed to read {args.input}: {exc}", file=sys.stderr)
        sys.exit(1)

    total = len(messages)
    if total == 0:
        print("No source=1090 messages found in capture file.", file=sys.stderr)
        sys.exit(1)

    print(
        f"Loaded {total + discarded:,} messages "
        f"({total:,} source=1090, {discarded:,} discarded).",
        flush=True,
    )

    stop_event = threading.Event()

    def _shutdown(_signum, _frame):
        stop_event.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        listen_sock.bind((args.bind, args.port))
    except OSError as exc:
        print(f"Failed to bind {args.bind}:{args.port}: {exc}", file=sys.stderr)
        sys.exit(1)
    listen_sock.listen(1)
    listen_sock.settimeout(1.0)

    print(
        f"Listening on {args.bind}:{args.port} - point the receiver's sources "
        f"config here with source tag 1090. Ctrl+C to exit.",
        flush=True,
    )

    try:
        _serve(listen_sock, messages, args.mode, stop_event)
    finally:
        listen_sock.close()

    print("Exiting.", flush=True)


if __name__ == "__main__":
    main()
