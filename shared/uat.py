"""
Shared UAT (978 MHz) line-protocol parsing for SkyFollower.

The receiver and the traffic-recorder tool both need to parse dump978-fa's
line-based output format; this is the single implementation both import,
so the two never drift out of sync.
"""

from __future__ import annotations

import time


def parse_978_line(line: str) -> tuple[str, str, float] | None:
    """
    Parse a dump978-fa output line.

    Returns (raw_hex, icao_hex, received_at) or None if the line should be
    skipped (!-preambles, blank lines, unrecognised format).

    raw_hex preserves the leading - (downlink) or + (uplink) symbol so
    downstream processors can distinguish frame direction.

    ICAO address is at bytes 1-3 of the UAT payload. In raw_hex that is
    chars [3:9] — one char for the symbol, two chars for byte 0, then the
    three ICAO bytes.

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
    for field in parts[1:]:
        if field.startswith("t="):
            try:
                received_at = float(field[2:])
            except ValueError:
                pass
            break

    return raw_hex, icao_hex, received_at
