"""
Shared make/model -> ICAO type-designator consensus inference.

Direct-hex civil registries (US FAA, Transport Canada, CZ CAA, IM ARDIS,
NO CAA, ...) publish a raw manufacturer/model string but never an ICAO type
designator. Mictronics independently has a type designator for a large
share of the *same hexes* -- those hexes double as free training labels:
group a registry's own tails by normalized (manufacturer, model), and for
each group, apply the majority Mictronics designator (among the tails in
that group whose Mictronics designator is known) to that group's other,
unlabelled tails.

See issue #1888 for the precision/coverage measurements behind the
>= 3 examples / >= 90% agreement threshold below -- loosening either bar
was measured to cost meaningfully more precision than the coverage it
buys, so both are kept as fixed constants rather than per-registry tuning
knobs.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import Iterable, Optional

from shared.redis_keys import aircraft_mictronics_key, aircraft_type_key

MIN_LABELLED_EXAMPLES = 3
MIN_AGREEMENT = 0.90

_NON_ALNUM = re.compile(r"[^A-Z0-9]+")


def normalize_make_model_key(manufacturer: Optional[str], model: Optional[str]) -> Optional[str]:
    """Normalize (manufacturer, model) into a single consensus-grouping key:
    uppercased, with non-alphanumeric characters stripped from each half.
    Returns None if either half is empty after normalization -- an empty
    half can't be a meaningful grouping key."""

    def _clean(value: Optional[str]) -> str:
        return _NON_ALNUM.sub("", (value or "").upper())

    mfr = _clean(manufacturer)
    mdl = _clean(model)
    if not mfr or not mdl:
        return None
    return f"{mfr}|{mdl}"


def build_consensus_table(
    labelled_examples: Iterable[tuple[Optional[str], Optional[str], Optional[str]]],
    *,
    min_examples: int = MIN_LABELLED_EXAMPLES,
    min_agreement: float = MIN_AGREEMENT,
) -> dict[str, str]:
    """Build a normalized-(manufacturer, model)-key -> majority type
    designator table from labelled (manufacturer, model, type_designator)
    examples -- each one a tail whose registry make/model is known and
    whose ICAO type designator is independently known (from Mictronics).

    A group is included only when it has >= min_examples labelled examples
    AND its majority designator holds >= min_agreement share of that
    group's votes. Every other group -- including a (manufacturer, model)
    with zero labelled examples -- is simply absent from the returned
    table, which is this function's abstain signal.
    """
    groups: dict[str, Counter] = defaultdict(Counter)
    for manufacturer, model, type_designator in labelled_examples:
        if not type_designator:
            continue
        key = normalize_make_model_key(manufacturer, model)
        if key is None:
            continue
        groups[key][type_designator] += 1

    table: dict[str, str] = {}
    for key, counts in groups.items():
        total = sum(counts.values())
        if total < min_examples:
            continue
        designator, top_count = counts.most_common(1)[0]
        if (top_count / total) >= min_agreement:
            table[key] = designator
    return table


def infer_type_designator(
    consensus_table: dict[str, str],
    manufacturer: Optional[str],
    model: Optional[str],
) -> Optional[str]:
    """Look up a deduced type designator for (manufacturer, model) in a
    consensus table built by build_consensus_table(). Returns None
    (abstains) when there is no qualifying key."""
    key = normalize_make_model_key(manufacturer, model)
    if key is None:
        return None
    return consensus_table.get(key)


def fetch_mictronics_type_designators(r, icao_hexes: Iterable[str], batch_size: int = 5000) -> dict[str, str]:
    """Batch-fetch aircraft.type_designator from aircraft:mictronics:{hex}
    for the given hexes via Redis pipelining. Returns a hex -> designator
    map containing only hexes that resolved to a non-empty designator --
    everything else (no Mictronics doc at all, or a doc with no
    type_designator) is simply absent, which is exactly the "unlabelled"
    set the consensus vote is meant to fill in for."""
    hexes = list(icao_hexes)
    result: dict[str, str] = {}
    for start in range(0, len(hexes), batch_size):
        chunk = hexes[start:start + batch_size]
        pipe = r.pipeline()
        for icao_hex in chunk:
            pipe.json().get(aircraft_mictronics_key(icao_hex))
        docs = pipe.execute()
        for icao_hex, doc in zip(chunk, docs):
            if not doc:
                continue
            designator = (doc.get("aircraft") or {}).get("type_designator")
            if designator:
                result[icao_hex] = designator
    return result


def resolve_description_code(r, type_designator: str) -> Optional[str]:
    """Look up aircraft:type:{designator} and return its description_code,
    or None if the type isn't known or the field is empty. A lookup
    failure is treated the same as "not found" -- the caller already has a
    valid deduced type_designator, and a missing/failed description_code
    lookup shouldn't discard that."""
    try:
        type_doc = r.json().get(aircraft_type_key(type_designator))
    except Exception:
        return None
    if not type_doc:
        return None
    description_code = (type_doc.get("description_code") or "").strip()
    return description_code or None
