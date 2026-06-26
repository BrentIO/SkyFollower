#!/usr/bin/env python3
"""
SkyFollower Traffic Recorder

Connects to one or more readsb TCP sources, captures raw ADS-B messages, and
writes them to an NDJSON file for later replay by the traffic-replayer tool.

Usage:
    python main.py --output capture.ndjson --duration 7200 \
        --sources "localhost:30002:1090" "localhost:30978:978"
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sys
import threading
import time

import pyModeS as pms

# ---------------------------------------------------------------------------
# TCP stream parser (same protocol as receiver)
# ---------------------------------------------------------------------------

_STAR = 0x2A
_SEMI = 0x3B
_HEX_BYTES = frozenset(
    list(range(0x30, 0x3A))
    + list(range(0x41, 0x47))
    + list(range(0x61, 0x67))
)


def parse_tcp_stream(data: bytes, buf: bytearray) -> list[str]:
    messages: list[str] = []
    for byte in data:
        if byte == _STAR:
            buf.clear()
        elif byte == _SEMI:
            if buf:
                messages.append(buf.decode("ascii").upper())
                buf.clear()
        elif byte in _HEX_BYTES:
            buf.extend([byte])
        else:
            buf.clear()
    return messages


# ---------------------------------------------------------------------------
# Per-source capture thread
# ---------------------------------------------------------------------------

class SourceCapture(threading.Thread):
    def __init__(
        self,
        host: str,
        port: int,
        source_tag: str,
        output_lock: threading.Lock,
        output_file,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(daemon=True, name=f"capture-{host}:{port}")
        self.host = host
        self.port = port
        self.source_tag = source_tag
        self._output_lock = output_lock
        self._output_file = output_file
        self._stop = stop_event
        self.count = 0
        self.error: Exception | None = None
        self.connected_event = threading.Event()

    def run(self) -> None:
        try:
            self._connect_and_capture()
        except Exception as exc:
            if not self._stop.is_set():
                self.error = exc
                print(
                    f"Error on {self.host}:{self.port} (source={self.source_tag}): {exc}",
                    flush=True,
                )
                self._stop.set()
        finally:
            self.connected_event.set()

    def _connect_and_capture(self) -> None:
        with socket.create_connection((self.host, self.port), timeout=10) as sock:
            print(f"Connected to {self.host}:{self.port} (source={self.source_tag})", flush=True)
            self.connected_event.set()
            sock.settimeout(1.0)
            buf = bytearray()
            while not self._stop.is_set():
                try:
                    data = sock.recv(4096)
                except socket.timeout:
                    continue
                if not data:
                    break
                for raw_hex in parse_tcp_stream(data, buf):
                    try:
                        decoded = pms.decode(raw_hex)
                        icao_hex = decoded.get("icao") if decoded else None
                    except Exception:
                        continue
                    if not icao_hex or len(icao_hex) != 6:
                        continue
                    record = {
                        "raw": raw_hex,
                        "icao_hex": icao_hex.upper(),
                        "received_at": time.time(),
                        "source": self.source_tag,
                    }
                    line = json.dumps(record, separators=(",", ":"))
                    with self._output_lock:
                        self._output_file.write(line + "\n")
                        self._output_file.flush()
                    self.count += 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _parse_source(s: str) -> tuple[str, int, str]:
    parts = s.split(":")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            f"Source must be host:port:source_tag, got: {s!r}"
        )
    host, port_str, tag = parts
    try:
        port = int(port_str)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid port in source {s!r}: {port_str!r}")
    return host, port, tag


def main() -> None:
    parser = argparse.ArgumentParser(description="SkyFollower Traffic Recorder")
    parser.add_argument("--output", required=True, help="Output NDJSON file path")
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Capture duration in seconds (default: unlimited)",
    )
    parser.add_argument(
        "--sources",
        nargs="+",
        required=True,
        metavar="HOST:PORT:SOURCE_TAG",
        help='One or more readsb sources, e.g. "localhost:30002:1090"',
    )
    args = parser.parse_args()

    sources = []
    for s in args.sources:
        try:
            sources.append(_parse_source(s))
        except argparse.ArgumentTypeError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    stop_event = threading.Event()

    def _shutdown(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)

    with open(args.output, "a") as output_file:
        output_lock = threading.Lock()
        threads = []
        for host, port, tag in sources:
            print(f"Connecting to {host}:{port} (source={tag}) ...")
            t = SourceCapture(host, port, tag, output_lock, output_file, stop_event)
            t.start()
            threads.append(t)

        for t in threads:
            t.connected_event.wait(timeout=15)

        if stop_event.is_set():
            for t in threads:
                t.join(timeout=5)
            sys.exit(1)

        start = time.monotonic()
        deadline = (start + args.duration) if args.duration else None
        print("Receiving data...")

        try:
            while not stop_event.is_set():
                stop_event.wait(30)
                if stop_event.is_set():
                    break

                elapsed = time.monotonic() - start
                if deadline and elapsed >= args.duration:
                    print(f"\nDuration reached ({args.duration}s). Stopping.")
                    stop_event.set()
                    break

                counts = ", ".join(
                    f"{t.source_tag}={t.count}" for t in threads
                )
                total = sum(t.count for t in threads)
                print(
                    f"[{int(elapsed):5d}s] {total} messages captured  ({counts})",
                    flush=True,
                )
        except KeyboardInterrupt:
            stop_event.set()

        for t in threads:
            t.join(timeout=5)

        total = sum(t.count for t in threads)
        elapsed = time.monotonic() - start
        failed = [t for t in threads if t.error]
        if failed:
            print(f"\nCapture stopped due to connection error. {total} messages in {elapsed:.1f}s → {args.output}")
            sys.exit(1)
        print(f"\nCapture complete: {total} messages in {elapsed:.1f}s → {args.output}")


if __name__ == "__main__":
    main()
