"""
Aircraft/registrant/operator enrichment helpers shared by the message
processor (enrich once per flight) and the archive processor (backfill
empty fields at write time), so the two can't drift apart.
"""

from __future__ import annotations

import copy
import json
import re
from typing import Optional

# US registration (tail number) idents skip the operator lookup.
US_REG_RE = re.compile(
    r"^[LT]?N[CXR]?[1-9]((\d{0,4})|(\d{0,3}[A-HJ-NP-Z])|(\d{0,2}[A-HJ-NP-Z]{2}))$"
)


def ident_matches_registration(ident: str, aircraft: dict) -> bool:
    """True when the broadcast ident is just the aircraft's own tail number
    (registration) rather than a route-bearing flight identifier/callsign —
    dash-insensitive, matching SkyFollower-legacy's setIdent()/_getOperator()
    precedent. Shared by operator enrichment and route-leg resolution, since
    both need the same "is this really a flight number" judgment call."""
    registration = aircraft.get("registration", "")
    return bool(registration) and registration.replace("-", "") == ident.replace("-", "")


def operator_lookup_prefix(ident: Optional[str], aircraft: dict) -> Optional[str]:
    """ICAO airline prefix to look up as operator:{prefix}, or None when the
    ident is a US tail number, equals the registration, or has no 2+ letter
    prefix."""
    if not ident or US_REG_RE.match(ident) or ident_matches_registration(ident, aircraft):
        return None
    prefix = re.split(r"[^a-zA-Z]", ident)[0]
    return prefix if len(prefix) >= 2 else None


def split_merged_aircraft(raw: str) -> tuple[dict, dict]:
    """Post-process merge_aircraft.lua's JSON result into (aircraft, registrant)."""
    aircraft = json.loads(raw)
    # wake_turbulence_category is receiver-decode-only; registry data must never seed it.
    aircraft.pop("wake_turbulence_category", None)
    # registrant is the legal owner, a sibling of operator rather than an airframe property.
    registrant = aircraft.pop("registrant", None) or {}
    return aircraft, registrant


def is_empty(value) -> bool:
    """Missing/null/""/{}/[] are empty; 0 and False are data."""
    return value is None or value == "" or (isinstance(value, (dict, list)) and not value)


def _leaf_count(value) -> int:
    if isinstance(value, dict):
        return sum(_leaf_count(v) for v in value.values() if not is_empty(v))
    return 0 if is_empty(value) else 1


def fill_empty_leaves(stored: dict, fresh: dict, exclude: frozenset = frozenset()) -> int:
    """Copy each non-empty leaf of `fresh` into `stored` where `stored` is
    empty, recursing into sub-objects; never overwrites data. Mutates
    `stored`; returns the number of leaves filled."""
    filled = 0
    for key, fresh_value in fresh.items():
        if key in exclude or is_empty(fresh_value):
            continue
        stored_value = stored.get(key)
        if is_empty(stored_value):
            stored[key] = copy.deepcopy(fresh_value)
            filled += _leaf_count(fresh_value)
        elif isinstance(stored_value, dict) and isinstance(fresh_value, dict):
            filled += fill_empty_leaves(stored_value, fresh_value)
    return filled
