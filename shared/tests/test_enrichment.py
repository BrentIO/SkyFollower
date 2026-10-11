import json

import pytest

from shared.enrichment import (
    fill_empty_leaves,
    ident_matches_registration,
    is_empty,
    operator_lookup_prefix,
    split_merged_aircraft,
)


class TestSplitMergedAircraft:
    def test_pops_wake_turbulence_and_splits_registrant(self):
        raw = json.dumps({
            "icao_hex": "A8AE7F", "registration": "N659DL",
            "wake_turbulence_category": "heavy", "registrant": {"names": ["Delta"]},
        })
        aircraft, registrant = split_merged_aircraft(raw)
        assert aircraft == {"icao_hex": "A8AE7F", "registration": "N659DL"}
        assert registrant == {"names": ["Delta"]}

    def test_missing_registrant_is_empty_dict(self):
        aircraft, registrant = split_merged_aircraft(json.dumps({"icao_hex": "A8AE7F"}))
        assert registrant == {}


class TestOperatorLookupPrefix:
    @pytest.mark.parametrize("ident, aircraft, expected", [
        ("DAL659", {}, "DAL"),
        ("N12345", {}, None),
        ("CGABC", {"registration": "C-GABC"}, None),
        ("X1234", {}, None),
        ("", {}, None),
        (None, {}, None),
    ])
    def test_skip_rules(self, ident, aircraft, expected):
        assert operator_lookup_prefix(ident, aircraft) == expected

    def test_ident_matches_registration_dash_insensitive(self):
        assert ident_matches_registration("CGABC", {"registration": "C-GABC"})
        assert not ident_matches_registration("CGABC", {})


class TestFillEmptyLeaves:
    @pytest.mark.parametrize("value, empty", [
        (None, True), ("", True), ({}, True), ([], True),
        (0, False), (False, False), ("x", False), ([1], False),
    ])
    def test_is_empty(self, value, empty):
        assert is_empty(value) is empty

    def test_fills_missing_and_empty_but_never_overwrites(self):
        stored = {"a": "keep", "b": None, "c": "", "d": 0, "e": False}
        fresh = {"a": "new", "b": "B", "c": "C", "d": 5, "e": True, "f": "F"}
        assert fill_empty_leaves(stored, fresh) == 3
        assert stored == {"a": "keep", "b": "B", "c": "C", "d": 0, "e": False, "f": "F"}

    def test_recurses_into_sub_objects(self):
        stored = {"powerplant": {"type": "jet", "model": None}}
        fresh = {"powerplant": {"type": "turboprop", "model": "PW127", "count": 2}}
        assert fill_empty_leaves(stored, fresh) == 2
        assert stored == {"powerplant": {"type": "jet", "model": "PW127", "count": 2}}

    def test_counts_leaves_of_filled_sub_object(self):
        stored = {}
        assert fill_empty_leaves(stored, {"powerplant": {"type": "jet", "model": ""}}) == 1

    def test_exclude_and_empty_fresh_values_skipped(self):
        stored = {}
        assert fill_empty_leaves(stored, {"x": "1", "y": "", "z": None}, frozenset({"x"})) == 0
        assert stored == {}

    def test_filled_values_are_copies(self):
        fresh = {"sub": {"k": "v"}}
        stored = {}
        fill_empty_leaves(stored, fresh)
        stored["sub"]["k"] = "changed"
        assert fresh["sub"]["k"] == "v"
