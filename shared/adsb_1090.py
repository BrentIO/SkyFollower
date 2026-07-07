"""
Shared readsb raw-format (*hex;) TCP stream parsing for SkyFollower.

The receiver and the traffic-recorder tool both need to parse readsb's raw
Mode S output format (used for 1090 MHz ADS-B and MLAT); this is the single
implementation both import, so the two never drift out of sync.
"""

from __future__ import annotations

# readsb sends messages as:   *{hex_chars};\n
# We collect hex chars between '*' (0x2A) and ';' (0x3B).
_STAR = 0x2A   # '*'
_SEMI = 0x3B   # ';'

_HEX_BYTES = frozenset(
    list(range(0x30, 0x3A))   # 0-9
    + list(range(0x41, 0x47)) # A-F
    + list(range(0x61, 0x67)) # a-f
)


def parse_tcp_stream(data: bytes, buf: bytearray) -> list[str]:
    """
    Parse a chunk of raw TCP data from a readsb stream.

    *buf* is a mutable bytearray that carries state between calls (partial
    message accumulator).  Returns a list of complete uppercase hex strings
    (without the surrounding ``*`` / ``;``).

    The buffer starts in "waiting for star" mode when empty.  It accumulates
    hex bytes after a ``*`` and emits a message when ``;`` is encountered.
    Non-hex bytes between ``*`` and ``;`` cause the current partial to be
    discarded and collection to restart.
    """
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
            # Invalid byte inside message — discard partial
            buf.clear()

    return messages
