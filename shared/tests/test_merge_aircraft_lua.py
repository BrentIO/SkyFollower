"""
Integration tests for shared/lua/merge_aircraft.lua, run against a live
Redis (RedisJSON + Lua scripting required -- a redis-stack instance).

Exercises the actual Lua script via EVALSHA rather than mocking the merge
behavior, since Lua semantics (e.g. cjson.null handling) can't be verified
by testing Python code alone.

Requires a reachable Redis at REDIS_TEST_HOST:REDIS_TEST_PORT (defaults to
localhost:6379); skipped, not failed, when none is reachable.
"""

from __future__ import annotations

import os
import pathlib
import uuid

import pytest

redis = pytest.importorskip("redis")

# Keeps this module's tests on one pytest-xdist worker: a module-scoped
# fixture is per-worker-process, so splitting across workers would let
# them race each other against the same live Redis keys (icao_hex only
# draws from a 256-value space, so a collision is a real risk).
pytestmark = pytest.mark.xdist_group(name="merge_aircraft_lua")

_LUA_PATH = pathlib.Path(__file__).parent.parent / "lua" / "merge_aircraft.lua"
_REDIS_HOST = os.environ.get("REDIS_TEST_HOST", "localhost")
_REDIS_PORT = int(os.environ.get("REDIS_TEST_PORT", "6379"))


@pytest.fixture(scope="module")
def redis_client():
    client = redis.Redis(
        host=_REDIS_HOST, port=_REDIS_PORT, decode_responses=True, socket_connect_timeout=2,
    )
    try:
        client.ping()
    except (redis.exceptions.RedisError, OSError):
        pytest.skip(f"No Redis reachable at {_REDIS_HOST}:{_REDIS_PORT} for live Lua script testing")
    yield client
    client.close()


@pytest.fixture(scope="module")
def merge_sha(redis_client):
    return redis_client.script_load(_LUA_PATH.read_text())


@pytest.fixture
def icao_hex(redis_client):
    """A fresh, collision-free test hex per test, cleaned up afterward."""
    hex_ = "FFFE" + uuid.uuid4().hex[:2].upper()
    yield hex_
    redis_client.delete(
        f"aircraft:mictronics:{hex_}", f"aircraft:registry:{hex_}", f"aircraft:livery:{hex_}",
    )


def _merge(redis_client, merge_sha, hex_):
    raw = redis_client.evalsha(merge_sha, 0, hex_)
    if raw is None:
        return None
    import json

    return json.loads(raw)


_CODE_BLOCKS_KEY = "lookup:icao-code-blocks"
_COUNTRIES_KEY = "lookup:icao-countries"


@pytest.fixture
def code_blocks_and_countries(redis_client):
    """Seeds the two global hex-range lookup keys used by
    apply_country_resolution(), then deletes them afterward. The seeded
    code-blocks table matches the icao_hex fixture's entire "FFFE00"-
    "FFFEFF" range at a lower significant_bitmask than the narrower
    0xFFFE10-only block, so tests can pick which one matches by choosing
    a hex inside or outside the narrow block.
    """
    redis_client.json().set(_CODE_BLOCKS_KEY, "$", [
        # Narrower block: higher significant_bitmask, wins for FFFE10-FFFE1F.
        {"bitmask": 0xFFFE10, "significant_bitmask": 0xFFFFF0, "country_code": "NN"},
        {"bitmask": 0xFFFE00, "significant_bitmask": 0xFFFF00, "country_code": "BB"},
    ])
    redis_client.json().set(_COUNTRIES_KEY, "$", {
        "AA": "Test Country A",
        "BB": "Test Country B",
        "NN": "Test Country Narrow",
        # No entry for "CC": covers a country_code with no resolvable name.
    })
    yield
    redis_client.delete(_CODE_BLOCKS_KEY, _COUNTRIES_KEY)


@pytest.fixture
def broad_block_hex(redis_client):
    """A fixed hex ("FFFE20") inside code_blocks_and_countries' broad
    FFFE00-FFFEFF block (country_code BB) but outside its narrower
    FFFE10-FFFE1F sub-block, for tests that need to assert on BB
    specifically rather than "some hex-range match"."""
    hex_ = "FFFE20"
    yield hex_
    redis_client.delete(
        f"aircraft:mictronics:{hex_}", f"aircraft:registry:{hex_}", f"aircraft:livery:{hex_}",
    )


class TestManufacturerModelFallback:
    def test_manufacturer_and_model_present_composes_fallback(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$",
            {"aircraft": {"manufacturer": "GULFSTREAM AEROSPACE", "model": "GV-SP (G550)"}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["manufacturer_model"] == "GULFSTREAM AEROSPACE GV-SP (G550)"

    def test_manufacturer_only_composes_fallback(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"aircraft": {"manufacturer": "DJI"}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["manufacturer_model"] == "DJI"

    def test_model_only_composes_fallback(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"aircraft": {"model": "AGRAS T30"}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["manufacturer_model"] == "AGRAS T30"

    def test_neither_present_leaves_manufacturer_model_unset(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(f"aircraft:registry:{icao_hex}", "$", {"military": False})
        result = _merge(redis_client, merge_sha, icao_hex)
        assert "manufacturer_model" not in result

    def test_no_aircraft_object_does_not_crash_or_create_one(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(f"aircraft:registry:{icao_hex}", "$", {"military": False})
        result = _merge(redis_client, merge_sha, icao_hex)
        assert "aircraft" not in result

    def test_existing_mictronics_manufacturer_model_not_overwritten(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$",
            {"aircraft": {"manufacturer_model": "BOEING 757-200", "type_designator": "B752"}},
        )
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$",
            {"aircraft": {"manufacturer": "BOEING", "model": "757-2Q8"}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["manufacturer_model"] == "BOEING 757-200"

    def test_explicit_null_manufacturer_model_triggers_fallback(self, redis_client, merge_sha, icao_hex):
        """cjson.decode turns JSON null into cjson.null, not Lua nil — the
        fallback must still fire, not silently skip this case."""
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$",
            {"aircraft": {"manufacturer_model": None, "manufacturer": "PIPER", "model": "J3C-65"}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["manufacturer_model"] == "PIPER J3C-65"

    def test_values_are_trimmed_and_joined_with_single_space(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$",
            {"aircraft": {"manufacturer": "  PIPER  ", "model": "  J3C-65  "}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["manufacturer_model"] == "PIPER J3C-65"

    def test_empty_string_manufacturer_treated_as_absent(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$",
            {"aircraft": {"manufacturer": "", "model": "J3C-65"}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["manufacturer_model"] == "J3C-65"

    def test_both_keys_absent_still_returns_none(self, redis_client, merge_sha, icao_hex):
        assert _merge(redis_client, merge_sha, icao_hex) is None


class TestDescriptionCodePromotion:
    """Guards the generic aircraft.* top-level promotion for
    description_code specifically -- no dedicated Lua logic exists for it,
    unlike manufacturer_model's fallback."""

    def test_mictronics_description_code_promoted_to_top_level(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$",
            {"aircraft": {"manufacturer_model": "BOEING 767-332ER", "description_code": "L2J"}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["description_code"] == "L2J"


class TestLiveryLayer:
    """Covers the third merge tier -- aircraft:livery:{icao_hex}, written
    by the airportwebcams-special-liveries runner -- on top of the
    existing mictronics/registry two-key merge."""

    def test_livery_absent_merge_unaffected(self, redis_client, merge_sha, icao_hex):
        """No special livery (the common case): must merge with no
        special_livery key at all."""
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$",
            {"aircraft": {"manufacturer_model": "BOEING 757-200"}},
        )
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"registration": "N659DL", "military": False},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["registration"] == "N659DL"
        assert "special_livery" not in result

    def test_livery_only_key_present(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:livery:{icao_hex}", "$", {"special_livery": "America250"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["special_livery"] == "America250"

    def test_livery_layered_on_top_of_mictronics_and_registry(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$",
            {"aircraft": {"manufacturer_model": "AIRBUS A320"}},
        )
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"registration": "N775JB", "military": False},
        )
        redis_client.json().set(
            f"aircraft:livery:{icao_hex}", "$", {"special_livery": "America250"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["manufacturer_model"] == "AIRBUS A320"
        assert result["registration"] == "N775JB"
        assert result["military"] is False
        assert result["special_livery"] == "America250"

    def test_livery_wins_on_field_overlap_with_registry(self, redis_client, merge_sha, icao_hex):
        """Livery is deep-merged last, so it must win over registry on any
        overlapping field."""
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"registration": "REGISTRY-VALUE"},
        )
        redis_client.json().set(
            f"aircraft:livery:{icao_hex}", "$", {"registration": "LIVERY-VALUE"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["registration"] == "LIVERY-VALUE"

    def test_all_three_keys_absent_still_returns_none(self, redis_client, merge_sha, icao_hex):
        assert _merge(redis_client, merge_sha, icao_hex) is None


class TestFlattening:
    """Covers promoting the nested aircraft.aircraft sub-object to the
    top level -- runners write several fields one level deeper than
    icao_hex/registration/military/registrant, but AircraftRecord's
    documented shape is flat."""

    def test_nested_fields_promoted_to_top_level(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$",
            {
                "icao_hex": icao_hex,
                "registration": "N659DL",
                "aircraft": {
                    "type": "Airplane",
                    "category": "Land",
                    "manufacturer": "BOEING",
                    "model": "757-232",
                    "seats": 199,
                    "powerplant": {"type": "Turbo-fan", "count": 2},
                    "serial_number": "12345",
                    "manufactured_date": "2005-01-01",
                },
            },
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert "aircraft" not in result
        assert result["type"] == "Airplane"
        assert result["category"] == "Land"
        assert result["manufacturer"] == "BOEING"
        assert result["model"] == "757-232"
        assert result["seats"] == 199
        assert result["powerplant"] == {"type": "Turbo-fan", "count": 2}
        assert result["serial_number"] == "12345"
        assert result["manufactured_date"] == "2005-01-01"
        assert result["icao_hex"] == icao_hex  # already top-level, unaffected
        assert result["registration"] == "N659DL"

    def test_no_nested_aircraft_key_is_a_no_op(self, redis_client, merge_sha, icao_hex):
        """A record with no nested aircraft sub-object at all must merge
        unchanged -- no synthetic `aircraft` key created."""
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"icao_hex": icao_hex, "registration": "N659DL"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert "aircraft" not in result
        assert result["registration"] == "N659DL"

    def test_existing_top_level_key_not_overwritten_by_nested_copy(self, redis_client, merge_sha, icao_hex):
        """If a top-level key and the nested aircraft sub-object's key of
        the same name both exist, the top-level value wins."""
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$",
            {"category": "TOP-LEVEL-VALUE", "aircraft": {"category": "NESTED-VALUE"}},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["category"] == "TOP-LEVEL-VALUE"


class TestDataSources:
    """Covers merge_aircraft.lua's data_sources aggregation: every
    present key's own scalar `source` collected into an array, instead of
    only the last-written key's `source` surviving."""

    def test_no_source_fields_present_no_data_sources_key(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(f"aircraft:registry:{icao_hex}", "$", {"registration": "N659DL"})
        result = _merge(redis_client, merge_sha, icao_hex)
        assert "data_sources" not in result
        assert "source" not in result

    def test_single_source_mictronics_only(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$", {"source": "mictronics"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["data_sources"] == ["mictronics"]

    def test_two_sources_mictronics_and_registry_order_preserved(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$", {"source": "mictronics"},
        )
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"source": "us-faa-registry", "registration": "N659DL"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["data_sources"] == ["mictronics", "us-faa-registry"]

    def test_three_sources_all_present_mictronics_registry_livery_order(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$", {"source": "mictronics"},
        )
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"source": "us-faa-registry"},
        )
        redis_client.json().set(
            f"aircraft:livery:{icao_hex}", "$", {"source": "airportwebcams-special-liveries", "special_livery": "America250"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["data_sources"] == ["mictronics", "us-faa-registry", "airportwebcams-special-liveries"]

    def test_mictronics_absent_registry_and_livery_still_both_collected(self, redis_client, merge_sha, icao_hex):
        """Regression guard: ipairs() over {mictronics_doc, registry_doc,
        livery_doc} would silently stop at the first nil element; with
        mictronics absent this must not also skip registry/livery."""
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"source": "us-faa-registry"},
        )
        redis_client.json().set(
            f"aircraft:livery:{icao_hex}", "$", {"source": "airportwebcams-special-liveries"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["data_sources"] == ["us-faa-registry", "airportwebcams-special-liveries"]

    def test_bare_source_scalar_never_leaks_through(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$", {"source": "mictronics"},
        )
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"source": "us-faa-registry"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert "source" not in result
        assert result["data_sources"] == ["mictronics", "us-faa-registry"]

    def test_key_present_without_source_field_does_not_crash_or_gap_the_array(self, redis_client, merge_sha, icao_hex):
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$", {"aircraft": {"manufacturer_model": "BOEING 757-200"}},
        )
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"source": "us-faa-registry"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["data_sources"] == ["us-faa-registry"]


class TestCountryResolution:
    """Covers #1848 -- country/country_code resolution: a registry-sourced
    country_code wins outright when present; otherwise icao_hex is
    matched against lookup:icao-code-blocks (descending-significant_bitmask
    longest-prefix match), then lookup:icao-countries resolves the ISO2
    code to an English name. See code_blocks_and_countries above for the
    seeded table shape."""

    def test_hex_range_fallback_resolves_when_no_registry_country_code(
        self, redis_client, merge_sha, broad_block_hex, code_blocks_and_countries,
    ):
        """No registry record at all: the hex-range table alone must
        resolve country/country_code, from the broad block (BB)."""
        redis_client.json().set(f"aircraft:mictronics:{broad_block_hex}", "$", {"source": "mictronics"})
        result = _merge(redis_client, merge_sha, broad_block_hex)
        assert result["country_code"] == "BB"
        assert result["country"] == "Test Country B"

    def test_hex_range_match_takes_first_hit_in_descending_significance_order(
        self, redis_client, merge_sha, code_blocks_and_countries,
    ):
        """A hex inside the narrower FFFE10-FFFE1F block must resolve to
        that block's country (NN), not the broader block (BB) that also
        contains it -- proving the scan takes the highest-significance
        match, not just any match."""
        hex_ = "FFFE15"
        redis_client.delete(f"aircraft:mictronics:{hex_}", f"aircraft:registry:{hex_}", f"aircraft:livery:{hex_}")
        redis_client.json().set(f"aircraft:mictronics:{hex_}", "$", {"source": "mictronics"})
        try:
            result = _merge(redis_client, merge_sha, hex_)
            assert result["country_code"] == "NN"
            assert result["country"] == "Test Country Narrow"
        finally:
            redis_client.delete(f"aircraft:mictronics:{hex_}")

    def test_registry_country_code_wins_over_hex_range(
        self, redis_client, merge_sha, icao_hex, code_blocks_and_countries,
    ):
        """A registry-sourced country_code must be kept even though the
        hex also matches a hex-range block for a different country."""
        redis_client.json().set(f"aircraft:mictronics:{icao_hex}", "$", {"source": "mictronics"})
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"source": "us-faa-registry", "country_code": "AA"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["country_code"] == "AA"
        assert result["country"] == "Test Country A"

    def test_no_match_when_hex_range_table_absent(self, redis_client, merge_sha, icao_hex):
        """No lookup:icao-code-blocks key at all must not crash and must
        leave country/country_code entirely unset."""
        redis_client.delete(_CODE_BLOCKS_KEY, _COUNTRIES_KEY)
        redis_client.json().set(f"aircraft:mictronics:{icao_hex}", "$", {"source": "mictronics"})
        result = _merge(redis_client, merge_sha, icao_hex)
        assert "country_code" not in result
        assert "country" not in result

    def test_no_match_when_hex_range_table_present_but_hex_unallocated(
        self, redis_client, merge_sha, icao_hex,
    ):
        """A populated table with no row covering this hex must resolve
        to no country, not raise or fall through to a wrong one."""
        redis_client.json().set(_CODE_BLOCKS_KEY, "$", [
            {"bitmask": 0x000000, "significant_bitmask": 0xFFFFFF, "country_code": "ZZ"},
        ])
        redis_client.json().set(_COUNTRIES_KEY, "$", {"ZZ": "Unknown or unassigned country"})
        try:
            redis_client.json().set(f"aircraft:mictronics:{icao_hex}", "$", {"source": "mictronics"})
            result = _merge(redis_client, merge_sha, icao_hex)
            assert "country_code" not in result
            assert "country" not in result
        finally:
            redis_client.delete(_CODE_BLOCKS_KEY, _COUNTRIES_KEY)

    def test_registry_country_code_with_no_countries_entry_leaves_country_unresolved(
        self, redis_client, merge_sha, icao_hex, code_blocks_and_countries,
    ):
        """A country_code with no matching row in lookup:icao-countries
        must still keep country_code, just leave country unset."""
        redis_client.json().set(f"aircraft:mictronics:{icao_hex}", "$", {"source": "mictronics"})
        redis_client.json().set(
            f"aircraft:registry:{icao_hex}", "$", {"source": "some-registry", "country_code": "CC"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["country_code"] == "CC"
        assert "country" not in result

    def test_registry_country_null_treated_as_absent_and_falls_back_to_hex_range(
        self, redis_client, merge_sha, broad_block_hex, code_blocks_and_countries,
    ):
        """cjson.decode turns JSON null into cjson.null, not Lua nil: an
        explicit country_code: null must still fall through to the
        hex-range table, not block the fallback."""
        redis_client.json().set(f"aircraft:mictronics:{broad_block_hex}", "$", {"source": "mictronics"})
        redis_client.json().set(
            f"aircraft:registry:{broad_block_hex}", "$", {"source": "us-faa-registry", "country_code": None},
        )
        result = _merge(redis_client, merge_sha, broad_block_hex)
        assert result["country_code"] == "BB"
        assert result["country"] == "Test Country B"

    def test_military_field_untouched_by_code_blocks_is_military(
        self, redis_client, merge_sha, broad_block_hex, code_blocks_and_countries,
    ):
        """AircraftRecord.military must stay sourced only from
        Mictronics, never from code-blocks.csv's IsMilitary flag."""
        redis_client.json().set(
            f"aircraft:mictronics:{broad_block_hex}", "$", {"source": "mictronics", "military": False},
        )
        result = _merge(redis_client, merge_sha, broad_block_hex)
        assert result["military"] is False
        assert result["country_code"] == "BB"


class TestHexRangeOnlyRecord:
    """Covers #1889 -- when mictronics/registry/livery are all absent, a
    hex that resolves in the hex-range table must yield a minimal
    synthesized record instead of nil, so /api/aircraft returns 200 with a
    country guess rather than a 404."""

    def test_hex_range_match_with_all_three_keys_absent_returns_minimal_record(
        self, redis_client, merge_sha, broad_block_hex, code_blocks_and_countries,
    ):
        result = _merge(redis_client, merge_sha, broad_block_hex)
        assert result == {
            "icao_hex": broad_block_hex,
            "country_code": "BB",
            "country": "Test Country B",
            "data_sources": ["icao-hex-range"],
        }

    def test_no_hex_range_match_with_all_three_keys_absent_still_returns_none(
        self, redis_client, merge_sha, icao_hex,
    ):
        """No lookup:icao-code-blocks key at all must still return nil,
        not a synthesized record with no country in it."""
        redis_client.delete(_CODE_BLOCKS_KEY, _COUNTRIES_KEY)
        assert _merge(redis_client, merge_sha, icao_hex) is None

    def test_hex_range_table_present_but_hex_unallocated_with_all_three_keys_absent_returns_none(
        self, redis_client, merge_sha, icao_hex,
    ):
        """The code-block table's own no-match case, combined with all
        three keys absent, must still be nil, not a record with a missing
        country_code."""
        redis_client.json().set(_CODE_BLOCKS_KEY, "$", [
            {"bitmask": 0x000000, "significant_bitmask": 0xFFFFFF, "country_code": "ZZ"},
        ])
        redis_client.json().set(_COUNTRIES_KEY, "$", {"ZZ": "Unknown or unassigned country"})
        try:
            assert _merge(redis_client, merge_sha, icao_hex) is None
        finally:
            redis_client.delete(_CODE_BLOCKS_KEY, _COUNTRIES_KEY)

    def test_hex_range_match_with_no_countries_entry_still_returns_record_without_country_name(
        self, redis_client, merge_sha, icao_hex,
    ):
        """A hex-range match whose country_code has no row in
        lookup:icao-countries must still synthesize the record, just
        without a `country` name."""
        redis_client.json().set(_CODE_BLOCKS_KEY, "$", [
            {"bitmask": 0xFFFE00, "significant_bitmask": 0xFFFF00, "country_code": "CC"},
        ])
        redis_client.json().set(_COUNTRIES_KEY, "$", {"AA": "Test Country A"})
        try:
            result = _merge(redis_client, merge_sha, icao_hex)
            assert result == {
                "icao_hex": icao_hex,
                "country_code": "CC",
                "data_sources": ["icao-hex-range"],
            }
        finally:
            redis_client.delete(_CODE_BLOCKS_KEY, _COUNTRIES_KEY)

    def test_any_real_data_present_is_unaffected_even_with_no_hex_range_match(
        self, redis_client, merge_sha, icao_hex,
    ):
        """A hex with real Mictronics data and no hex-range match must
        merge exactly as before."""
        redis_client.json().set(
            f"aircraft:mictronics:{icao_hex}", "$", {"source": "mictronics", "registration": "N659DL"},
        )
        result = _merge(redis_client, merge_sha, icao_hex)
        assert result["registration"] == "N659DL"
        assert result["data_sources"] == ["mictronics"]
        assert "country_code" not in result
