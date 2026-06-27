#!/usr/bin/env python3
"""
SkyFollower Traffic Recorder

Connects to one or more readsb/dump978-fa TCP sources, captures raw ADS-B and
UAT messages, and writes them to an NDJSON file for later replay by the
traffic-replayer tool.

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
# 1090 TCP stream parser  (*hex;\n  — readsb raw format)
# ---------------------------------------------------------------------------

_STAR = 0x2A
_SEMI = 0x3B
_HEX_BYTES = frozenset(
    list(range(0x30, 0x3A))
    + list(range(0x41, 0x47))
    + list(range(0x61, 0x67))
)


def parse_1090_stream(data: bytes, buf: bytearray) -> list[str]:
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
# 978 line parser  (-hex;rs=N;rssi=N;t=N.NNN;  — dump978-fa format)
# ---------------------------------------------------------------------------

def parse_978_line(line: str) -> tuple[str, str, float, float | None, int | None] | None:
    """
    Parse a dump978-fa output line.

    Returns (raw_hex, icao_hex, received_at, rssi, rs) or None if the line
    should be skipped (!-preambles, blank lines, unrecognised format).

    raw_hex preserves the leading - (downlink) or + (uplink) symbol so
    downstream processors can distinguish frame direction.

    ICAO address is at bytes 1-3 of the UAT payload. In raw_hex that is
    chars [3:9] — one char for the symbol, two chars for byte 0, then the
    three ICAO bytes.

    rssi: received signal strength (dBFS float).
    rs:   Reed-Solomon errors corrected (int; absent on clean frames).
    Timestamp is taken from the t= field in the metadata.
    """
    line = line.strip()
    if not line or line.startswith("!"):
        return None
    if line[0] not in ("-", "+"):
        return None

    symbol = line[0]
    parts = line[1:].split(";")
    hex_payload = parts[0].upper()
    if len(hex_payload) < 8:
        return None
    if not all(c in "0123456789ABCDEF" for c in hex_payload):
        return None

    raw_hex = symbol + hex_payload
    icao_hex = hex_payload[2:8]

    received_at: float = time.time()
    rssi: float | None = None
    rs: int | None = None

    for field in parts[1:]:
        if field.startswith("t="):
            try:
                received_at = float(field[2:])
            except ValueError:
                pass
        elif field.startswith("rssi="):
            try:
                rssi = float(field[5:])
            except ValueError:
                pass
        elif field.startswith("rs="):
            try:
                rs = int(field[3:])
            except ValueError:
                pass

    return raw_hex, icao_hex, received_at, rssi, rs


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
            if self.source_tag == "978":
                self._capture_978(sock)
            else:
                self._capture_1090(sock)

    def _capture_1090(self, sock: socket.socket) -> None:
        buf = bytearray()
        while not self._stop.is_set():
            try:
                data = sock.recv(4096)
            except socket.timeout:
                continue
            if not data:
                break
            for raw_hex in parse_1090_stream(data, buf):
                try:
                    decoded = pms.decode(raw_hex)
                    icao_hex = decoded.get("icao") if decoded else None
                except Exception:
                    continue
                if not icao_hex or len(icao_hex) != 6:
                    continue
                self._write(raw_hex, icao_hex.upper(), time.time())

    def _capture_978(self, sock: socket.socket) -> None:
        line_buf = b""
        while not self._stop.is_set():
            try:
                data = sock.recv(4096)
            except socket.timeout:
                continue
            if not data:
                break
            line_buf += data
            while b"\n" in line_buf:
                raw_line, line_buf = line_buf.split(b"\n", 1)
                result = parse_978_line(raw_line.decode("ascii", errors="ignore"))
                if result:
                    raw_hex, icao_hex, received_at, rssi, rs = result
                    self._write(raw_hex, icao_hex, received_at, rssi=rssi, rs=rs)

    def _write(
        self,
        raw_hex: str,
        icao_hex: str,
        received_at: float,
        rssi: float | None = None,
        rs: int | None = None,
    ) -> None:
        record = {
            "raw": raw_hex,
            "icao_hex": icao_hex,
            "received_at": received_at,
            "source": self.source_tag,
        }
        if rssi is not None:
            record["rssi"] = rssi
        if rs is not None:
            record["rs"] = rs
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
        help='One or more sources, e.g. "localhost:30002:1090" "localhost:30978:978"',
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

                counts = ", ".join(f"{t.source_tag}={t.count}" for t in threads)
                total = sum(t.count for t in threads)
                print(f"[{int(elapsed):5d}s] {total} messages captured  ({counts})", flush=True)
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
