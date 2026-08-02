"""
ISO 3166-1 alpha-2 country code to flag emoji.

A country's flag emoji is simply its two-letter code re-encoded as a pair of
Unicode Regional Indicator Symbols, so no lookup table is needed -- any valid
alpha-2 code works, including ones not yet used by any runner today.
"""

from __future__ import annotations

_REGIONAL_INDICATOR_OFFSET = ord("\U0001F1E6") - ord("A")


def country_flag(iso_code: str) -> str:
    """Return the flag emoji for a 2-letter ISO 3166-1 alpha-2 country code."""
    code = iso_code.upper()
    if len(code) != 2 or not code.isalpha():
        raise ValueError(f"Not a 2-letter ISO 3166-1 alpha-2 code: {iso_code!r}")
    return "".join(chr(ord(c) + _REGIONAL_INDICATOR_OFFSET) for c in code)
