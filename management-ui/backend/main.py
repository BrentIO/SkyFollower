#!/usr/bin/env python3
"""
SkyFollower Management UI Backend

FastAPI service that is the sole write path for the rules and areas
configuration read by every message processor (config:rules / config:areas
in Redis, polled every 30 seconds). No authentication — single-instance,
trusted-network deployment.

Named "management" to leave room for a future, separate UI for viewing live
aircraft movement, distinct from this configuration-focused one.

Runs on port 8000, bound to 127.0.0.1 only inside the container. The
Dockerfile is a multi-stage build: a node stage produces the static React
frontend bundle, and the final stage runs both uvicorn and nginx --
nginx serves the built frontend at / and proxies /api/* to this process.
"""

from __future__ import annotations

import calendar
import gzip
import hashlib
import json
import logging
import os
import pathlib
import re
import sys
import tempfile
import threading
import time
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Annotated, Any, Literal, Optional, Union

import boto3
import redis as redis_lib
import sqlglot
from cryptography.fernet import Fernet, InvalidToken
from fastapi import FastAPI, HTTPException, Response
from fastapi import Query as FastAPIQuery
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, Field, field_validator
from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query as RedisSearchQuery
from sqlglot import exp
from uuid_extensions import uuid7

# Add the repo root to sys.path so shared/ is importable when this module is
# run outside Docker (e.g. tests, local `uvicorn main:app`, OpenAPI export).
# In the Docker image PYTHONPATH=/app already covers this and _REPO_ROOT
# below resolves to "/", which is simply never used.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_HERE))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from shared.config import RECEIVER_SOURCE_TAGS, load_config  # noqa: E402
from shared.redis_client import build_redis_client  # noqa: E402
from shared.logging_setup import configure_logging  # noqa: E402
from shared.flight_path import build_flight_path  # noqa: E402
from shared.models import AircraftRecord, AirportRecord, OperatorRecord  # noqa: E402
from shared.glue_projection import YEAR_RANGE as _GLUE_YEAR_RANGE  # noqa: E402
from shared.redis_keys import (  # noqa: E402
    AIRCRAFT_MICTRONICS_SEARCH_INDEX,
    AIRCRAFT_REGISTRY_SEARCH_INDEX,
    AIRPORT_SEARCH_INDEX,
    airport_key,
    archive_search_index_key,
    archive_search_key,
    config_areas_key,
    config_areas_version_key,
    config_rules_key,
    config_rules_version_key,
    normalize_flight_ident,
    operator_key,
    rule_trigger_day_key,
    rule_trigger_lifetime_key,
)

try:
    from message_processor.rules_engine import RulesEngine
except ModuleNotFoundError as exc:
    if exc.name != "message_processor":
        # Some other module rules_engine.py imports (e.g. shapely) is
        # missing -- that's a real dependency problem, not the local-dev
        # package-name workaround below, so don't mask it.
        raise

    # message-processor/ can't be imported as a normal package here -- the
    # hyphen in the directory name isn't a valid Python identifier -- so
    # register it under the dotted name 'message_processor' via importlib,
    # the same workaround message-processor/tests/*.py use. In the Docker
    # image the directory is copied to message_processor/ (underscore), so
    # the plain import above already succeeds there and this never runs.
    import importlib.util

    _mp_dir = os.path.join(_REPO_ROOT, "message-processor")
    _spec = importlib.util.spec_from_file_location(
        "message_processor",
        os.path.join(_mp_dir, "__init__.py"),
        submodule_search_locations=[_mp_dir],
    )
    _pkg = importlib.util.module_from_spec(_spec)
    _pkg.__path__ = [_mp_dir]
    _pkg.__package__ = "message_processor"
    sys.modules["message_processor"] = _pkg
    _spec.loader.exec_module(_pkg)

    from message_processor.rules_engine import RulesEngine  # noqa: E402

logger = logging.getLogger("management-ui-backend")


# ---------------------------------------------------------------------------
# Schema models -- matching SkyFollower-legacy's rules.example.json /
# areas.example.geojson shape (condition values are strings even for
# numeric fields, e.g. altitude "10000", military "true"; only
# matched_rules is a real array). These ARE the actual route parameter
# types for create/update (see create_rule/update_rule/create_area/
# update_area below) -- FastAPI/Pydantic validates a request body against
# them at the ingress boundary, returning a structured 422 for a bad shape
# (missing field, wrong type, an operator not valid for a condition's
# type, etc.) before the route function ever runs.
#
# RulesEngine (message-processor/rules_engine.py) remains a second,
# independent enforcement layer underneath this one -- not made redundant
# by it. It's the only validation applied to config:rules/config:areas
# written some other way than through this API (a hand-edited Redis
# value, a restored backup, a future integration writing directly to
# Redis), and it enforces things a single condition's fields can't express
# on their own (e.g. an `area` condition's value must name an area that
# actually exists in config:areas).
#
# One known, deliberate gap versus RulesEngine's own leniency: RulesEngine
# tolerates a disabled placeholder rule with only {"enabled": false} and
# nothing else (no identifier, no conditions), silently skipping it rather
# than validating it. Rule below requires identifier/conditions
# unconditionally, so that placeholder shape can no longer be created or
# updated through this API -- only by writing directly to Redis. No
# current caller (UI or tests) relies on submitting that shape through the
# API, so this is treated as an acceptable narrowing, not a regression.
# ---------------------------------------------------------------------------

_IDENTIFIER_PATTERN = r"^\S+$"  # non-empty, no whitespace anywhere

# Named example payloads, keyed for Swagger UI's example picker (the
# dropdown legacy's swagger.yml used via components/examples). Reused below
# both as each model's JSON-Schema-level `examples` (schema.examples --
# Swagger UI doesn't turn this into a picker on its own, it's just visible
# in the schema/model view) and, in _custom_openapi(), as real OpenAPI
# Example Objects at the request/response content level (content.
# application/json.examples -- this is what actually drives the picker).
#
# First three rule examples adapted from SkyFollower-legacy's
# rules.example.json (the third example's "callsign" condition type is
# renamed "ident", matching this repo's Conditions table -- legacy predates
# that rename). Fourth demonstrates force_archive and a datetime-range date
# condition (YYYY-MM-DDTHH:MMZ, not just YYYY-MM-DD). The area example is
# the "LI" polygon from legacy's areas.example.geojson, referenced by the
# "Grandma's Flight Home" rule example.
_RULE_EXAMPLES: dict[str, dict] = {
    "All aircraft below 10,000": {
        "name": "All aircraft below 10,000",
        "description": "Any aircraft with an altitude at or below 10,000ft",
        "identifier": "acft_10k_and_below",
        "enabled": True,
        "conditions": [
            {"type": "altitude", "operator": "maximum", "value": "10000"},
        ],
    },
    "UAL B757-200": {
        "name": "UAL B757-200",
        "description": "United Airlines Boeing 757-200's between 12,000 and "
        "15,000ft heading north after takeoff",
        "identifier": "Northbound_United_B75s_12k-15k",
        "enabled": True,
        "conditions": [
            {"type": "altitude", "operator": "maximum", "value": "15000"},
            {"type": "altitude", "operator": "minimum", "value": "12000"},
            {"type": "operator_airline_designator", "operator": "equals", "value": "UAL"},
            {"type": "aircraft_type_designator", "operator": "equals", "value": "B752"},
            {"type": "heading", "operator": "equals", "value": "340,020"},
            {"type": "vertical_speed", "operator": "minimum", "value": "500"},
        ],
    },
    "Grandma's Flight Home": {
        "name": "Grandma's Flight Home",
        "description": "Grandma's Flight Home Arriving on Christmas Eve",
        "identifier": "grandma",
        "enabled": True,
        "conditions": [
            {"type": "ident", "operator": "equals", "value": "DAL2"},
            {"type": "date", "operator": "minimum", "value": "2022-12-24"},
            {"type": "date", "operator": "maximum", "value": "2022-12-24"},
            {"type": "area", "operator": "equals", "value": "LI"},
        ],
    },
    "B-52 Force Persist Window": {
        "name": "B-52 Force Persist Window",
        "description": "Any B-52 (aircraft_type_designator B52) seen between "
        "2026-01-15 11:31 PM EST and 2027-06-27 2:50 PM EDT (entered in US "
        "Eastern time, stored as UTC -- note the date rolls back a day for "
        "the first one), force-archived even if the flight would otherwise "
        "be skipped for being external-only",
        "identifier": "b52_force_persist_2026_2027",
        "enabled": True,
        "force_archive": True,
        "conditions": [
            {"type": "aircraft_type_designator", "operator": "equals", "value": "B52"},
            {"type": "date", "operator": "minimum", "value": "2026-01-16T04:31Z"},
            {"type": "date", "operator": "maximum", "value": "2027-06-27T18:50Z"},
        ],
    },
    "Air Force One overhead, verified via 1090": {
        "name": "Air Force One overhead, verified via 1090",
        "description": "ADFDF8 (Air Force One) force-archived only when actually received "
        "via 1090MHz ADS-B, not merely via an EXTERNAL-tagged source -- an EXTERNAL source's "
        "provenance isn't guaranteed the way a direct receive path's is, so seeing it there "
        "alone doesn't confirm it's actually nearby. If it isn't showing up on 1090, skip the "
        "force-archive and let the normal external-only archive skip apply.",
        "identifier": "af1_1090_verified",
        "enabled": True,
        "force_archive": True,
        "conditions": [
            {"type": "aircraft_icao_hex", "operator": "equals", "value": "ADFDF8"},
            {"type": "receiver_source", "operator": "equals", "value": ["1090"]},
        ],
    },
}

_AREA_EXAMPLES: dict[str, dict] = {
    "Long Island (LI)": {
        "identifier": "LI",
        "name": "Long Island",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [-73.8006591796875, 40.82835864973048],
                [-73.97369384765625, 40.734770989672406],
                [-74.03961181640625, 40.54720023441049],
                [-73.7677001953125, 40.538851525354666],
                [-73.245849609375, 40.58684239087908],
                [-72.70751953125, 40.73685214795608],
                [-71.8560791015625, 40.97160353279909],
                [-71.80938720703125, 41.044145364313174],
                [-71.89727783203125, 41.14970617453726],
                [-72.11700439453125, 41.21998578493921],
                [-72.36145019531249, 41.17038447781618],
                [-72.70477294921874, 41.03585891144301],
                [-72.83935546875, 41.04621681452063],
                [-73.1964111328125, 40.994410999439516],
                [-73.553466796875, 40.94671366508002],
                [-73.8006591796875, 40.82835864973048],
            ]],
        },
    },
    "ILS 17L approach corridor": {
        "identifier": "ILS_17L",
        "name": "ILS 17L",
        "geometry": {
            "type": "LineString",
            "coordinates": [
                [-81.16, 28.77],
                [-81.29, 28.7],
                [-81.2845, 28.628056],
                [-81.2825847, 28.4436808],
            ],
        },
    },
    "SHREK2 waypoint": {
        "identifier": "SHREK2",
        "name": "SHREK2",
        "geometry": {
            "type": "Point",
            "coordinates": [-81.9198611, 28.9936389],
        },
    },
}


def _validate_int_range(value: str, minimum: int, maximum: int, label: str) -> str:
    """Shared by the numeric-range condition types below -- `value` stays a
    plain `str` on the wire (matching SkyFollower-legacy's convention), so
    the bound check parses it rather than using a `Field(ge=..., le=...)`
    constraint, which only applies to actual numeric field types."""
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ValueError(f"{label} value must be an integer") from None
    if not (minimum <= parsed <= maximum):
        raise ValueError(f"{label} value must be between {minimum} and {maximum}")
    return value


class _ConditionBase(BaseModel):
    """
    Shared base for the per-type condition models below. Each subclass
    fixes its own `type` literal and restricts `operator` to the set that
    type actually supports -- see CLAUDE.md's Conditions table and
    message-processor/rules_engine.py's per-type `_validate_*` methods,
    which every subclass's `operator` here must keep matching.

    `value` is a plain `str` on every subclass (or `list[str]` for
    `matched_rules`) -- matching SkyFollower-legacy's convention of a
    string even for numeric fields (altitude "10000", heading "340,020"
    for min,max wrap-around, military "true"/"false"). Where RulesEngine
    enforces a bound or charset on `value`, the matching subclass below
    mirrors it (numeric range via a `field_validator`, charset via
    `Field(pattern=...)`) so Swagger documents the same constraint and a
    bad request gets a `422` at ingress instead of only a `400` from
    RulesEngine two calls deep. Every other type's `value` stays
    unconstrained beyond `str`/`list[str]` -- this project doesn't
    duplicate every RulesEngine check here, only the ones the frontend
    also fast-fails on (see `management-ui/frontend/src/components/
    RuleForm.tsx`'s `validateCondition`).
    """

    model_config = {"extra": "forbid"}


class AltitudeCondition(_ConditionBase):
    type: Literal["altitude"]
    operator: Literal["minimum", "maximum"]
    # Numeric range on a string field can't be a JSON Schema minimum/maximum
    # keyword (those only apply to type: integer/number), so the 0-65000
    # bound is expressed as a regex instead: single digit, 2-4 digits with
    # no leading zero (10-9999), 10000-59999, 60000-64999, or exactly 65000.
    # field_validator below is a redundant safety net against a regex bug,
    # not the primary enforcement.
    value: str = Field(
        pattern=r"^([0-9]|[1-9][0-9]{1,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65000)$",
        description="Altitude in feet, as a string. Must be an integer 0-65000.",
    )

    @field_validator("value")
    @classmethod
    def _check_range(cls, v: str) -> str:
        return _validate_int_range(v, 0, 65000, "altitude")


class VelocityCondition(_ConditionBase):
    type: Literal["velocity"]
    operator: Literal["minimum", "maximum"]
    # 0-1334: single digit, 2-3 digits with no leading zero (10-999),
    # 1000-1299, 1300-1329, or 1330-1334.
    value: str = Field(
        pattern=r"^([0-9]|[1-9][0-9]|[1-9][0-9]{2}|1[0-2][0-9]{2}|13[0-2][0-9]|133[0-4])$",
        description="Velocity in knots, as a string. Must be an integer 0-1334.",
    )

    @field_validator("value")
    @classmethod
    def _check_range(cls, v: str) -> str:
        return _validate_int_range(v, 0, 1334, "velocity")


class VerticalSpeedCondition(_ConditionBase):
    type: Literal["vertical_speed"]
    operator: Literal["minimum", "maximum"]
    # -10000-10000: optional leading '-', then single digit, 2-4 digits
    # with no leading zero (10-9999), or exactly 10000.
    value: str = Field(
        pattern=r"^-?([0-9]|[1-9][0-9]{1,3}|10000)$",
        description="Vertical speed in ft/min, as a string (negative = descending). "
        "Must be an integer -10000-10000.",
    )

    @field_validator("value")
    @classmethod
    def _check_range(cls, v: str) -> str:
        return _validate_int_range(v, -10000, 10000, "vertical_speed")


class HeadingCondition(_ConditionBase):
    type: Literal["heading"]
    operator: Literal["equals"]
    value: str


class DateCondition(_ConditionBase):
    type: Literal["date"]
    operator: Literal["minimum", "maximum"]
    value: str


class IdentCondition(_ConditionBase):
    type: Literal["ident"]
    operator: Literal["equals"]
    value: str


class SquawkCondition(_ConditionBase):
    type: Literal["squawk"]
    operator: Literal["equals"]
    # 4-digit octal -- a real transponder never sends 8/9 in any position,
    # matching rules_engine.py's _validate_squawk.
    value: str = Field(pattern=r"^[0-7]{4}$")


class MilitaryCondition(_ConditionBase):
    type: Literal["military"]
    operator: Literal["equals"]
    value: str


class ReceiverSourceCondition(_ConditionBase):
    type: Literal["receiver_source"]
    operator: Literal["equals"]
    # 1-2 of "1090"/"978"/"EXTERNAL", no duplicates -- all 3 would be
    # equivalent to no filter at all (every flight has at least one), so
    # RulesEngine rejects that as dead weight rather than a real filter.
    value: list[Literal[RECEIVER_SOURCE_TAGS]] = Field(min_length=1, max_length=2)

    @field_validator("value")
    @classmethod
    def _check_no_duplicates(cls, v: list[str]) -> list[str]:
        if len(set(v)) != len(v):
            raise ValueError("receiver_source list must not contain duplicates")
        return v


class OperatorAirlineDesignatorCondition(_ConditionBase):
    type: Literal["operator_airline_designator"]
    operator: Literal["equals"]
    value: str


class AircraftTypeDesignatorCondition(_ConditionBase):
    type: Literal["aircraft_type_designator"]
    operator: Literal["equals"]
    value: str


class AircraftRegistrationCondition(_ConditionBase):
    type: Literal["aircraft_registration"]
    operator: Literal["equals"]
    # Letters/numbers/hyphens only, no leading/trailing hyphen -- first/last
    # char anchored to [0-9A-Za-z] inherently requires 2+ characters and
    # rules out a leading/trailing hyphen (interior hyphens like "RA-12345"
    # are fine). Case-insensitive since rules_engine.py's
    # _validate_aircraft_registration uppercases before matching, so
    # lowercase input is equally valid there.
    value: str = Field(pattern=r"^[0-9A-Za-z][0-9A-Za-z-]*[0-9A-Za-z]$")


class AircraftIcaoHexCondition(_ConditionBase):
    type: Literal["aircraft_icao_hex"]
    operator: Literal["equals"]
    # Exactly 6 hex characters, case-insensitive (rules_engine.py's
    # _validate_aircraft_icao_hex uppercases before matching).
    value: str = Field(pattern=r"^[0-9A-Fa-f]{6}$")


class AircraftPowerplantCountCondition(_ConditionBase):
    type: Literal["aircraft_powerplant_count"]
    operator: Literal["equals", "minimum", "maximum"]
    # 0-99: single digit, or two digits with no leading zero (10-99).
    value: str = Field(
        pattern=r"^([0-9]|[1-9][0-9])$",
        description="Number of engines, as a string. Must be an integer 0-99.",
    )

    @field_validator("value")
    @classmethod
    def _check_range(cls, v: str) -> str:
        return _validate_int_range(v, 0, 99, "aircraft_powerplant_count")


class WakeTurbulenceCategoryCondition(_ConditionBase):
    type: Literal["wake_turbulence_category"]
    operator: Literal["equals"]
    value: str


class MatchedRulesCondition(_ConditionBase):
    type: Literal["matched_rules"]
    operator: Literal["in_list", "not_in_list"]
    value: list[str] = Field(min_length=1)


class AreaCondition(_ConditionBase):
    type: Literal["area"]
    operator: Literal["equals"]
    value: str


# Discriminated union keyed by `type` -- Swagger renders this as a `oneOf`
# with each variant's own accurate `operator` enum, instead of the single
# flat model this replaces, which allowed all 5 operators on every type
# regardless of whether RulesEngine would ever actually accept them.
Condition = Annotated[
    Union[
        AltitudeCondition, VelocityCondition, VerticalSpeedCondition, HeadingCondition,
        DateCondition, IdentCondition, SquawkCondition, MilitaryCondition,
        ReceiverSourceCondition,
        OperatorAirlineDesignatorCondition, AircraftTypeDesignatorCondition,
        AircraftRegistrationCondition, AircraftIcaoHexCondition,
        AircraftPowerplantCountCondition, WakeTurbulenceCategoryCondition,
        MatchedRulesCondition, AreaCondition,
    ],
    Field(discriminator="type"),
]


class Rule(BaseModel):
    """
    A notification rule. Fires at most once per flight per `identifier`.
    `identifier` is the routing key used in /api/rules/{identifier} and must
    not contain spaces; `name` is a free-text display label and may.
    Documents the shape of a normal (enabled) rule -- RulesEngine
    additionally tolerates a disabled placeholder rule with only
    `enabled: false` and nothing else, silently skipping it rather than
    validating it; that leniency is a narrow exception this model doesn't
    allow, so that shape can no longer be created/updated through this API
    (only by writing directly to Redis) -- see the "Schema models" comment
    above.
    """

    model_config = {"json_schema_extra": {"examples": list(_RULE_EXAMPLES.values())}}

    name: str = Field(default="", max_length=64)
    description: str = Field(default="", max_length=2000)
    identifier: str = Field(pattern=_IDENTIFIER_PATTERN, max_length=64)
    enabled: bool
    force_archive: bool = False
    conditions: list[Condition] = Field(min_length=1)


class RuleWithTriggerCounts(Rule):
    """`Rule` plus its trigger-count read-outs. Response-only: these two
    fields are computed fresh from Redis on every GET (see
    `_rule_trigger_counts`), never accepted on POST/PUT -- the create/update
    endpoints keep the plain `Rule` body model."""

    triggered_lifetime: int = Field(
        default=0, description="Times this rule has fired since it was created."
    )
    triggered_last_30_days: int = Field(
        default=0, description="Times this rule has fired in the trailing 30 days (true rolling window)."
    )


class PolygonGeometry(BaseModel):
    type: Literal["Polygon"]
    coordinates: list[list[list[float]]]


class LineStringGeometry(BaseModel):
    type: Literal["LineString"]
    coordinates: list[list[float]]


class PointGeometry(BaseModel):
    type: Literal["Point"]
    coordinates: list[float]


# Discriminated union keyed by `type`, same pattern as Condition above.
# message-processor/rules_engine.py's `area` condition only ever evaluates
# Polygon areas (`_load_areas` skips anything else with a debug log) -- that
# does not change here. LineString/Point areas are valid to draw, name,
# save, and display in the areas editor, but are simply not selectable as
# an `area` condition's value.
AreaGeometry = Annotated[
    Union[PolygonGeometry, LineStringGeometry, PointGeometry],
    Field(discriminator="type"),
]


class Area(BaseModel):
    """
    A named GeoJSON area (Polygon, LineString, or Point). Only a Polygon
    area is usable as a rules' `area` condition's value (matched against
    `identifier`, not `name`) -- message-processor/rules_engine.py skips
    any other geometry type there. `identifier` is the routing key used in
    /api/areas/{identifier} and must not contain spaces; `name` is a
    free-text display label and may.
    """

    model_config = {
        "populate_by_name": True,
        "json_schema_extra": {"examples": list(_AREA_EXAMPLES.values())},
    }

    identifier: str = Field(pattern=_IDENTIFIER_PATTERN)
    name: str = ""
    geometry: AreaGeometry
    # Prevents the shape from being dragged/vertex-edited on the map while
    # true; does not restrict name edits or deletion. Toggling this saves
    # immediately (see AreasView.tsx's toggleLock) rather than going
    # through the dirty/Save flow, since it's a direct state flip like
    # delete, not an in-progress geometry edit.
    locked: bool = False
    # simplestyle-spec (https://github.com/mapbox/simplestyle-spec)
    # properties, matching legacy SkyFollower's areas.geojson convention.
    # All optional -- an area with none of them set falls back to
    # AreasView.tsx's default color scheme (its per-feature Terra Draw
    # styling callbacks coalesce to the same default Terra Draw itself
    # already uses, #3f97e0, when a field is absent). fill/marker-size/
    # marker-symbol aren't cross-validated against geometry type here,
    # matching simplestyle itself -- a Point with a stray `fill` set is
    # simply never read by anything, not rejected.
    fill: Optional[str] = None
    fill_opacity: Optional[float] = Field(default=None, alias="fill-opacity")
    stroke: Optional[str] = None
    stroke_width: Optional[float] = Field(default=None, alias="stroke-width")
    stroke_opacity: Optional[float] = Field(default=None, alias="stroke-opacity")
    marker_color: Optional[str] = Field(default=None, alias="marker-color")
    marker_size: Optional[Literal["small", "medium", "large"]] = Field(default=None, alias="marker-size")
    marker_symbol: Optional[str] = Field(default=None, alias="marker-symbol")


class ErrorDetail(BaseModel):
    """Shape of every 4xx/5xx response -- FastAPI's HTTPException(detail=...) serializes to this."""

    detail: str


# Named 400 examples, same picker mechanism as _RULE_EXAMPLES/_AREA_EXAMPLES
# above -- these are exact detail messages RulesEngine actually raises (see
# message-processor/rules_engine.py's _parse_rule/_validate_area), not
# invented text, so they show real failure modes.
_RULE_ERROR_EXAMPLES: dict[str, dict] = {
    "Rule has no conditions": {
        "detail": "Rule #0 invalid: rule 'bad-rule' has no conditions",
    },
    "Identifier contains spaces": {
        "detail": "Rule #0 invalid: identifier 'my rule' must be non-empty and contain no spaces",
    },
}

_AREA_ERROR_EXAMPLES: dict[str, dict] = {
    "Identifier contains spaces or invalid geometry": {
        "detail": "Area 'Long Island' failed validation (check identifier has no "
        "spaces and geometry is a valid Polygon)",
    },
}


_redis: Optional[redis_lib.Redis] = None
_engine: Optional[RulesEngine] = None
# SHA-1 digests of shared/lua/merge_aircraft.lua and route_airports.lua,
# loaded once at startup (see lifespan() below) so every lookup call is a
# single EVALSHA round trip -- same pattern message-processor/main.py uses.
_merge_aircraft_sha: Optional[str] = None
_route_airports_sha: Optional[str] = None

# Archive search -- Athena/Glue query layer over the archive's Parquet
# index. _fernet is generated fresh at every process startup, held only in
# memory, never written to the environment or disk -- see "Flight fetch" in
# _encrypt_s3_key/_decrypt_token below for why.
_s3_client: Optional[object] = None
_athena_client: Optional[object] = None
_s3_bucket: str = ""
_athena_cfg: dict = {}
_fernet: Optional[Fernet] = None


# ---------------------------------------------------------------------------
# config:rules/config:areas are the only two Redis keys in the whole schema
# holding user-authored state with no automatic regeneration path (every
# other key is either repopulated by a runner or transient operational
# state -- see CLAUDE.md's Redis Key Schema). Redis's own AOF is the only
# persistence for them today; these two functions add a second, independent
# copy on a host-mounted volume, so a lost/corrupted Redis volume doesn't
# mean losing every rule and area a user has authored. Read at call time
# (not cached at import time) so DATA_DIR can be overridden per-test.
# ---------------------------------------------------------------------------

def _data_dir() -> str:
    return os.environ.get("DATA_DIR", "/app/data")


def _rules_backup_path() -> str:
    return os.path.join(_data_dir(), "rules-backup.json")


def _areas_backup_path() -> str:
    return os.path.join(_data_dir(), "areas-backup.json")


def _write_backup_file(path: str, body: str) -> None:
    """Atomically write `body` to `path` (temp file + os.replace) so a crash
    mid-write can't leave a truncated backup behind. Best-effort: a failure
    here is logged, not raised -- the Redis write this follows already
    succeeded, so a backup problem shouldn't turn a successful save into a
    user-facing error."""
    directory = os.path.dirname(path)
    try:
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=directory, prefix=".tmp-")
        try:
            with os.fdopen(fd, "w") as f:
                f.write(body)
            os.replace(tmp_path, path)
        except BaseException:
            os.unlink(tmp_path)
            raise
    except OSError as exc:
        logger.error("Failed to write backup file %s: %s", path, exc)


def _reconcile_backup_with_redis(key: str, version_key: str, backup_path: str, label: str) -> None:
    """Reconciles config:rules/config:areas between Redis and its on-disk
    backup file at startup, in whichever single direction fills a gap.
    Redis is always authoritative when it has data:

    - Redis has `key`, backup file exists: nothing to do -- except ensure
      `version_key` exists (see below).
    - Redis has `key`, backup file is missing -- an existing deployment
      upgrading to this feature has real data in Redis but has never
      written a backup file (only _save_rules_array/_save_areas_array do
      that, on save): seed the file from Redis's current value so it
      doesn't stay empty until the next edit. Never overwrites a backup
      file that already exists.
    - Redis has `key` but not `version_key` -- a deployment from before the
      version key existed, a partially-restored volume, or a manual seed:
      compute sha256(body) and set it. Without this a message processor
      polls forever with `redis.get(version_key) == self._rules_version ==
      None` and never loads the rules that are sitting in `key`.
    - Redis is missing `key`, backup file exists: restore Redis from the
      file (and its `:version` hash, so RulesEngine's poll-based reload
      picks it up) -- a lost/corrupted Redis volume, or a fresh one.
    - Both missing: nothing to do -- same empty-array behavior as today.

    The body and its version hash are always written together in one
    transaction (see _redis_set_config_pair for why).
    """
    existing = _redis.get(key)
    if existing is not None:
        if not os.path.exists(backup_path):
            _write_backup_file(backup_path, existing)
            logger.info("Seeded %s backup file %s from existing Redis data.", label, backup_path)
        if _redis.get(version_key) is None:
            version = hashlib.sha256(existing.encode()).hexdigest()
            _redis.set(version_key, version)
            logger.info(
                "Set missing %s -- message processors would not have reloaded %s without it.",
                version_key, label,
            )
        return

    if not os.path.exists(backup_path):
        return

    try:
        with open(backup_path) as f:
            body = f.read()
        json.loads(body)  # corrupt/truncated backup shouldn't crash startup
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("%s backup file %s is unreadable or corrupt: %s", label, backup_path, exc)
        return

    version = hashlib.sha256(body.encode()).hexdigest()
    pipe = _redis.pipeline(transaction=True)
    pipe.set(key, body)
    pipe.set(version_key, version)
    pipe.execute()
    logger.info("Restored %s from backup file %s (Redis key was missing).", label, backup_path)


# In the Docker image, shared/ is copied flat alongside this file (WORKDIR
# /app has both main.py and shared/ directly under it -- see
# management-ui/Dockerfile), so _HERE/shared/lua is correct there; _REPO_ROOT
# resolves to "/" in that image (see the comment above _REPO_ROOT) and is
# only useful outside Docker, where shared/ is two directories up from this
# file's actual location instead.
_LUA_DIR = pathlib.Path(_HERE) / "shared" / "lua"
if not _LUA_DIR.is_dir():
    _LUA_DIR = pathlib.Path(_REPO_ROOT) / "shared" / "lua"


# Every RediSearch index management-ui queries (via _search_one() below),
# plus enough of its schema to create it empty. Each index is otherwise
# only created lazily by the data runner that owns it, the first time that
# runner actually runs (e.g. runners/mictronics/main.py's own
# _ensure_search_index) -- on a fresh install, or if that runner just
# hasn't had a scheduled run yet, the index simply doesn't exist and
# querying it raises a raw "No such index" Redis error. lifespan() below
# creates all three unconditionally at startup instead, so a query against
# an unpopulated index returns a normal empty result instead of an error.
# Field/prefix values must stay in sync with each owning runner's schema
# (runners/mictronics/main.py, runners/us-faa-registry/main.py and its
# per-country siblings, runners/ourairports/main.py).
_SEARCH_INDEX_SCHEMAS: list[tuple[str, str, list[tuple[str, str]]]] = [
    (
        AIRCRAFT_MICTRONICS_SEARCH_INDEX,
        "aircraft:mictronics:",
        [("$.icao_hex", "icao_hex"), ("$.registration", "registration")],
    ),
    (
        AIRCRAFT_REGISTRY_SEARCH_INDEX,
        "aircraft:registry:",
        [("$.icao_hex", "icao_hex"), ("$.registration", "registration")],
    ),
    (AIRPORT_SEARCH_INDEX, "airport:", [("$.icao_code", "icao_code"), ("$.iata_code", "iata_code")]),
]


def _ensure_search_index(r: redis_lib.Redis, index: str, prefix: str, tag_fields: list[tuple[str, str]]) -> None:
    """Create `index` (empty, if unpopulated) if it doesn't already exist --
    the same lazy-creation pattern each owning data-runner performs on its
    own first run, just performed unconditionally here so management-ui
    never has to wait on that runner having executed first."""
    try:
        r.ft(index).info()
    except Exception:
        r.ft(index).create_index(
            fields=[TagField(path, as_name=as_name) for path, as_name in tag_fields],
            definition=IndexDefinition(prefix=[prefix], index_type=IndexType.JSON),
        )
        logger.info("Created search index %r.", index)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _redis, _engine, _merge_aircraft_sha, _route_airports_sha
    global _s3_client, _athena_client, _s3_bucket, _athena_cfg, _fernet
    config = load_config("redis", "s3", "athena")
    configure_logging(config.get("log_level"))

    redis_config = config.get("redis", {})
    _redis = build_redis_client(redis_config)
    for index, prefix, tag_fields in _SEARCH_INDEX_SCHEMAS:
        _ensure_search_index(_redis, index, prefix, tag_fields)
    _engine = RulesEngine(_redis)
    _merge_aircraft_sha = _redis.script_load((_LUA_DIR / "merge_aircraft.lua").read_text())
    _route_airports_sha = _redis.script_load((_LUA_DIR / "route_airports.lua").read_text())

    _reconcile_backup_with_redis(config_rules_key(), config_rules_version_key(), _rules_backup_path(), "rules")
    _reconcile_backup_with_redis(config_areas_key(), config_areas_version_key(), _areas_backup_path(), "areas")
    _engine.reload_if_changed()

    _s3_bucket = config.get("s3", {}).get("bucket", "")
    _athena_cfg = config.get("athena", {})
    # No credential arguments: boto3 reads AWS_ACCESS_KEY_ID,
    # AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION from its own default
    # credential chain, which an instance role can also satisfy.
    session = boto3.Session()
    _s3_client = session.client("s3")
    _athena_client = session.client("athena")
    _fernet = Fernet(Fernet.generate_key())

    _reconcile_stuck_archive_searches()

    logger.info("Management UI backend started.")
    yield
    logger.info("Management UI backend shutting down.")


app = FastAPI(
    title="SkyFollower Management",
    description="Rules/areas configuration API (writes config:rules/"
    "config:areas in Redis, read by every message processor), read-only "
    "reference-data lookups (aircraft/operator/airport/route) over the same "
    "enrichment Redis already holds for rule evaluation, and an archive "
    "search API (Athena/Glue query layer over the S3 archive's Parquet "
    "index).",
    version="9999.99.99",
    lifespan=lifespan,
)


_RULE_BODY_PATHS = [("/api/rules", "post"), ("/api/rules/{identifier}", "put")]
_AREA_BODY_PATHS = [("/api/areas", "post"), ("/api/areas/{identifier}", "put")]


def _named_examples(examples: dict[str, dict]) -> dict[str, dict]:
    """Convert {name: value} into OpenAPI Example Objects: {name: {"value": value}}."""
    return {name: {"value": value} for name, value in examples.items()}


# (path, method, response status code) for every route whose single-item
# response body is a Rule/Area -- these get the same named example picker
# as the matching request body, so "try it out" and the response preview
# both offer the same choices.
_RULE_RESPONSE_LOCATIONS = [
    ("/api/rules/{identifier}", "get", "200"),
    ("/api/rules", "post", "201"),
    ("/api/rules/{identifier}", "put", "200"),
]
_AREA_RESPONSE_LOCATIONS = [
    ("/api/areas/{identifier}", "get", "200"),
    ("/api/areas", "post", "201"),
    ("/api/areas/{identifier}", "put", "200"),
]


def _custom_openapi() -> dict:
    """
    Replace the auto-generated request/response body schema on every
    POST/PUT/single-item-GET route with a clean $ref to Rule/Area, and add
    named OpenAPI Example Objects so Swagger UI shows a picker (its "try it
    out" panel only offers a picker from content.application/json.examples
    -- an Example Object map at the request/response body level -- not from
    a schema's own JSON-Schema-level `examples` array, which Rule/Area also
    carry for other tooling but which Swagger UI doesn't turn into a
    selector on its own). Also adds the same kind of named-example picker to
    each route's 400 response (real RulesEngine failure messages, not
    invented text), and fills in field descriptions on FastAPI's own
    built-in ValidationError model (used for every route's 422), which
    ships with no descriptions of its own.

    FastAPI already infers the correct $ref for each request body from the
    route's actual parameter type (Rule/Area -- see the Schema models
    comment above), so the `schema=` overwrite below is a no-op replace
    with the same value FastAPI would already generate; it's kept so this
    loop's other job -- injecting the named `examples` Swagger UI's "try it
    out" picker actually reads from (content.application/json.examples,
    not a schema's own JSON-Schema-level `examples`) -- has a single place
    to overwrite both at once via a plain dict assignment.
    """
    if app.openapi_schema:
        return app.openapi_schema

    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    for path, method in _RULE_BODY_PATHS:
        schema["paths"][path][method]["requestBody"]["content"]["application/json"].update(
            schema={"$ref": "#/components/schemas/Rule"},
            examples=_named_examples(_RULE_EXAMPLES),
        )
    for path, method in _AREA_BODY_PATHS:
        schema["paths"][path][method]["requestBody"]["content"]["application/json"].update(
            schema={"$ref": "#/components/schemas/Area"},
            examples=_named_examples(_AREA_EXAMPLES),
        )
    for path, method, status in _RULE_RESPONSE_LOCATIONS:
        schema["paths"][path][method]["responses"][status]["content"]["application/json"]["examples"] = (
            _named_examples(_RULE_EXAMPLES)
        )
    for path, method, status in _AREA_RESPONSE_LOCATIONS:
        schema["paths"][path][method]["responses"][status]["content"]["application/json"]["examples"] = (
            _named_examples(_AREA_EXAMPLES)
        )
    for path, method in _RULE_BODY_PATHS:
        schema["paths"][path][method]["responses"]["400"]["content"]["application/json"]["examples"] = (
            _named_examples(_RULE_ERROR_EXAMPLES)
        )
    for path, method in _AREA_BODY_PATHS:
        schema["paths"][path][method]["responses"]["400"]["content"]["application/json"]["examples"] = (
            _named_examples(_AREA_ERROR_EXAMPLES)
        )

    # FastAPI's own built-in ValidationError model (used for the 422s every
    # route gets automatically) ships with no field descriptions -- add
    # them so the schema explains what loc/msg/type/input/ctx actually mean
    # instead of just their bare types.
    validation_error_props = schema["components"]["schemas"]["ValidationError"]["properties"]
    validation_error_props["loc"]["description"] = (
        "Path to the invalid field, e.g. [\"body\", \"identifier\"] or [\"path\", \"identifier\"]"
    )
    validation_error_props["msg"]["description"] = "Human-readable description of the error"
    validation_error_props["type"]["description"] = (
        "Machine-readable error type, e.g. \"missing\" or \"string_type\""
    )
    validation_error_props["input"]["description"] = "The value that was actually submitted"
    validation_error_props["ctx"]["description"] = "Extra context about the error, if any (varies by type)"

    app.openapi_schema = schema
    return app.openapi_schema


app.openapi = _custom_openapi


def _redis_get(key: str) -> Optional[str]:
    try:
        return _redis.get(key)
    except redis_lib.RedisError as exc:
        raise HTTPException(status_code=500, detail=f"Redis error: {exc}") from exc


def _redis_set(key: str, value: str, **kwargs) -> None:
    try:
        _redis.set(key, value, **kwargs)
    except redis_lib.RedisError as exc:
        raise HTTPException(status_code=500, detail=f"Redis error: {exc}") from exc


def _redis_set_config_pair(body_key: str, body: str, version_key: str, version: str) -> None:
    """Write a config body (config:rules / config:areas) and its
    `:version` hash in one MULTI/EXEC transaction, so a failure can never
    leave the two permanently skewed -- a message processor trusts the
    version key as a fast-path change signal and would not notice a stale
    body under a matching hash (see message-processor/rules_engine.py's
    _reload_config)."""
    try:
        pipe = _redis.pipeline(transaction=True)
        pipe.set(body_key, body)
        pipe.set(version_key, version)
        pipe.execute()
    except redis_lib.RedisError as exc:
        raise HTTPException(status_code=500, detail=f"Redis error: {exc}") from exc


def _redis_json_get(key: str) -> Optional[dict]:
    """operator:{designator}/airport:{code} are real RedisJSON documents
    (written via shared/redis_json.py's set_json(), same as every
    aircraft:*/{icao_hex} key) -- a plain GET against one raises WRONGTYPE
    (verified against a live Redis Stack instance), unlike a plain Redis
    string. JSON.GET is required; redis-py's .json().get() returns the
    already-decoded dict directly, no json.loads() needed."""
    try:
        return _redis.json().get(key)
    except redis_lib.RedisError as exc:
        raise HTTPException(status_code=500, detail=f"Redis error: {exc}") from exc


_NOT_FOUND = {404: {"description": "Not found", "model": ErrorDetail}}
_CONFLICT = {409: {"description": "Identifier already exists", "model": ErrorDetail}}
# Distinct from _CONFLICT: raised by delete_area when a rule's `area`
# condition still references the area being deleted, rather than a
# duplicate-identifier clash -- see delete_area's referential-integrity
# check below.
_AREA_IN_USE = {409: {"description": "Area is referenced by one or more rules", "model": ErrorDetail}}
_REDIS_ERROR = {500: {"description": "Redis error", "model": ErrorDetail}}
_VALIDATION_ERROR = {400: {"description": "Validation error", "model": ErrorDetail}}
# Distinct from _REDIS_ERROR: only raised by _search_one() when the specific
# failure is "index does not exist yet" (see its docstring) rather than a
# genuine connectivity/auth failure -- routes that call _search_one() add
# this alongside _REDIS_ERROR, not instead of it.
_SEARCH_INDEX_UNAVAILABLE = {
    503: {"description": "Search index not ready yet -- data hasn't been loaded", "model": ErrorDetail}
}


# ---------------------------------------------------------------------------
# Reference-data lookup helpers (aircraft/operator/airport/route) -- read-only
# queries against enrichment Redis already holds; no separate write path.
# ---------------------------------------------------------------------------

def _redis_evalsha(sha: str, *args: str) -> Optional[str]:
    try:
        return _redis.evalsha(sha, 0, *args)
    except redis_lib.RedisError as exc:
        raise HTTPException(status_code=500, detail=f"Redis error: {exc}") from exc


# Same character set every data runner's own _escape_tag applies before a
# RediSearch TagField query -- there's no shared helper for this today, so
# this duplicates that logic rather than reaching into a runner module.
_TAG_SPECIAL_CHARS = ",.<>{}[]\"':;!@#$%^&*()-+=~"


def _escape_tag(value: str) -> str:
    """Escape special characters for use in a RediSearch TagField query."""
    return "".join(f"\\{ch}" if ch in _TAG_SPECIAL_CHARS else ch for ch in value)


def _search_one(index: str, field: str, value: str) -> Optional[str]:
    """Single-tag exact-match FT.SEARCH against `index`; returns the first
    matching document's key (e.g. "aircraft:mictronics:A8AE7F"), or None."""
    try:
        result = _redis.ft(index).search(RedisSearchQuery(f"@{field}:{{{_escape_tag(value)}}}").paging(0, 1))
    except redis_lib.RedisError as exc:
        # lifespan() proactively creates all three search indices at
        # startup, so this should be rare -- a safety net for an index
        # created after that point (e.g. concurrently, or by a future
        # .ft(...) call site lifespan() doesn't cover). redis-py/RediSearch
        # expose no dedicated exception type for "index doesn't exist" --
        # it surfaces as a generic ResponseError/RedisError whose message
        # is literally "No such index <name>" (the same string Redis
        # itself returns), so this checks message text rather than
        # exception type. That's inherently a little fragile against
        # future Redis/RediSearch wording changes.
        if "no such index" in str(exc).lower():
            raise HTTPException(
                status_code=503,
                detail=(
                    f"Search index {index!r} does not exist yet -- the data runner "
                    "that populates it may not have run yet. See the initial "
                    "data-runner bulk load step in the getting-started docs."
                ),
            ) from exc
        raise HTTPException(status_code=500, detail=f"Redis error: {exc}") from exc
    return result.docs[0].id if result.docs else None


def _flatten_aircraft_doc(doc: dict) -> dict:
    """merge_aircraft.lua's output nests type/manufacturer/powerplant fields
    under an `aircraft` sub-object, mirroring how the
    mictronics/country-registry runners store them (see their own
    build_aircraft_record functions) -- AircraftRecord's shape is flat,
    matching the legacy AROI /registration/icao_hex/{hex} response, so
    promote them to the top level before parsing. setdefault() so a field
    already present at the top level (there aren't any today, but a future
    runner change shouldn't silently reorder precedence) is never
    overwritten by the nested copy."""
    nested = doc.pop("aircraft", None)
    if isinstance(nested, dict):
        for key, value in nested.items():
            doc.setdefault(key, value)
    return doc


class RouteLookup(BaseModel):
    """
    Resolved route for a flight ident. `ident` echoes the path parameter
    used to resolve it, so the frontend doesn't have to separately remember
    what it searched for. `origin`/`destination` are the first/last airport
    in the resolved sequence (a quick-glance header); `stops` is the full
    sequence in order, duplicates preserved (e.g. a round trip returns the
    same airport at both ends). `operator` is resolved from the ident's
    ICAO airline-designator prefix (same logic as message-processor's
    `_enrich_operator`) and is best-effort -- a route still resolves
    without one. When the route itself is unknown but the operator
    resolves (e.g. a part-135 operator with no scheduled-service route
    data), `origin`/`destination`/`stops` are omitted rather than the
    endpoint 404ing.
    """

    ident: str
    origin: Optional[AirportRecord] = None
    destination: Optional[AirportRecord] = None
    stops: list[AirportRecord] = []
    operator: Optional[OperatorRecord] = None


# ---------------------------------------------------------------------------
# Rules storage helpers -- config:rules stores the full array as one JSON
# blob (that's what message processors poll and hot-reload), so every
# per-item operation below is read-full-array, splice, validate, write-back.
# ---------------------------------------------------------------------------

def _load_rules_array() -> list[dict]:
    raw = _redis_get(config_rules_key())
    if not raw:
        return []
    return json.loads(raw)


_RULE_TRIGGER_WINDOW_DAYS = 30


def _rule_trigger_counts(identifiers: list[str]) -> dict[str, tuple[int, int]]:
    """`(triggered_lifetime, triggered_last_30_days)` for each rule
    identifier, computed fresh: two MGETs total regardless of rule count --
    one for every rule's lifetime key, one for every rule's 30 most recent
    daily keys (today back through 29 days ago). A missing key counts as 0.
    The 30-day figure is a true trailing window (summed daily keys), not a
    fixed-boundary reset. Fails soft -- a Redis error yields (0, 0) for
    every rule rather than failing the whole rule list, since this is a
    display-only figure alongside the real rule data."""
    if not identifiers:
        return {}
    today = datetime.now(timezone.utc).date()
    days = [(today - timedelta(days=i)).isoformat() for i in range(_RULE_TRIGGER_WINDOW_DAYS)]
    lifetime_keys = [rule_trigger_lifetime_key(i) for i in identifiers]
    day_keys = [rule_trigger_day_key(i, d) for i in identifiers for d in days]
    try:
        lifetime_vals = _redis.mget(lifetime_keys)
        day_vals = _redis.mget(day_keys) if day_keys else []
    except redis_lib.RedisError:
        return {ident: (0, 0) for ident in identifiers}

    out: dict[str, tuple[int, int]] = {}
    for idx, ident in enumerate(identifiers):
        lv = lifetime_vals[idx]
        lifetime = int(lv) if lv is not None else 0
        chunk = day_vals[idx * _RULE_TRIGGER_WINDOW_DAYS:(idx + 1) * _RULE_TRIGGER_WINDOW_DAYS]
        last_30 = sum(int(v) for v in chunk if v is not None)
        out[ident] = (lifetime, last_30)
    return out


def _with_trigger_counts(rule: dict, counts: dict[str, tuple[int, int]]) -> dict:
    lifetime, last_30 = counts.get(rule.get("identifier", ""), (0, 0))
    return {**rule, "triggered_lifetime": lifetime, "triggered_last_30_days": last_30}


def _delete_rule_trigger_keys(identifier: str) -> None:
    """Remove a deleted rule's trigger-count keys, so a later rule created
    with the same identifier starts at zero rather than inheriting history.
    The lifetime key plus every possible daily key over the 31-day TTL
    window (today back 31 days); DEL on a nonexistent key is a safe no-op.
    Fails soft -- a leftover key just expires on its own TTL."""
    today = datetime.now(timezone.utc).date()
    keys = [rule_trigger_lifetime_key(identifier)]
    keys += [
        rule_trigger_day_key(identifier, (today - timedelta(days=i)).isoformat())
        for i in range(32)
    ]
    try:
        _redis.delete(*keys)
    except redis_lib.RedisError:
        pass


def _save_rules_array(rules: list[dict]) -> None:
    body = json.dumps(rules)
    if not _engine.load_rules_json(body):
        raise HTTPException(status_code=400, detail=_engine.last_error or "Invalid rules")

    version = hashlib.sha256(body.encode()).hexdigest()
    _redis_set_config_pair(config_rules_key(), body, config_rules_version_key(), version)
    _write_backup_file(_rules_backup_path(), body)


@app.get(
    "/api/rules",
    tags=["rules"],
    response_model=list[RuleWithTriggerCounts],
    responses={**_REDIS_ERROR},
)
def list_rules():
    rules = _load_rules_array()
    counts = _rule_trigger_counts([r.get("identifier", "") for r in rules])
    return JSONResponse(content=[_with_trigger_counts(r, counts) for r in rules])


@app.get(
    "/api/rules/{identifier}",
    tags=["rules"],
    response_model=RuleWithTriggerCounts,
    responses={**_NOT_FOUND, **_REDIS_ERROR},
)
def get_rule(identifier: str):
    for rule in _load_rules_array():
        if rule.get("identifier") == identifier:
            counts = _rule_trigger_counts([identifier])
            return JSONResponse(content=_with_trigger_counts(rule, counts))
    raise HTTPException(status_code=404, detail=f"Rule '{identifier}' not found")


@app.post(
    "/api/rules",
    tags=["rules"],
    status_code=201,
    response_model=RuleWithTriggerCounts,
    responses={**_CONFLICT, **_VALIDATION_ERROR, **_REDIS_ERROR},
)
def create_rule(rule: Rule):
    rules = _load_rules_array()
    identifier = rule.identifier
    if any(r.get("identifier") == identifier for r in rules):
        raise HTTPException(status_code=409, detail=f"Rule '{identifier}' already exists")

    rule_dict = rule.model_dump()
    rules.append(rule_dict)
    _save_rules_array(rules)
    # A fresh identifier has no counters yet -- these come back 0/0, so the
    # editor's post-save re-seed shows a consistent shape.
    return JSONResponse(
        status_code=201,
        content=_with_trigger_counts(rule_dict, _rule_trigger_counts([identifier])),
    )


@app.put(
    "/api/rules/{identifier}",
    tags=["rules"],
    response_model=RuleWithTriggerCounts,
    responses={**_NOT_FOUND, **_VALIDATION_ERROR, **_REDIS_ERROR},
)
def update_rule(identifier: str, rule: Rule):
    rules = _load_rules_array()
    idx = next((i for i, r in enumerate(rules) if r.get("identifier") == identifier), None)
    if idx is None:
        raise HTTPException(status_code=404, detail=f"Rule '{identifier}' not found")

    if rule.identifier != identifier:
        raise HTTPException(
            status_code=400,
            detail=f"Body identifier '{rule.identifier}' does not match path identifier '{identifier}'",
        )

    rules[idx] = rule.model_dump()
    _save_rules_array(rules)
    return JSONResponse(
        content=_with_trigger_counts(rules[idx], _rule_trigger_counts([identifier])),
    )


@app.delete(
    "/api/rules/{identifier}",
    tags=["rules"],
    status_code=204,
    responses={**_NOT_FOUND, **_VALIDATION_ERROR, **_REDIS_ERROR},
)
def delete_rule(identifier: str):
    rules = _load_rules_array()
    remaining = [r for r in rules if r.get("identifier") != identifier]
    if len(remaining) == len(rules):
        raise HTTPException(status_code=404, detail=f"Rule '{identifier}' not found")

    _save_rules_array(remaining)
    _delete_rule_trigger_keys(identifier)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Areas storage helpers -- config:areas stores a GeoJSON FeatureCollection
# (what RulesEngine.load_areas_json and the message processor expect); the
# API exposes a flattened [{identifier, name, geometry}, ...] array instead,
# translated to/from that FeatureCollection at this boundary.
# ---------------------------------------------------------------------------

# simplestyle-spec property names (https://github.com/mapbox/simplestyle-spec)
# -- the exact keys Area's style fields alias to, and also the keys they
# live under in the persisted GeoJSON Feature's `properties`, so a feature
# round-tripped through _area_to_feature/_feature_to_area stays valid
# simplestyle GeoJSON the whole way, not just a SkyFollower-internal shape.
_AREA_STYLE_KEYS = (
    "fill", "fill-opacity", "stroke", "stroke-width", "stroke-opacity",
    "marker-color", "marker-size", "marker-symbol",
)


def _feature_to_area(feature: dict) -> dict:
    props = feature.get("properties", {})
    area = {
        "identifier": props.get("identifier", ""),
        "name": props.get("name", ""),
        "geometry": feature.get("geometry", {}),
        "locked": bool(props.get("locked", False)),
    }
    # Omitted (not None) when absent, matching simplestyle-spec convention
    # and area.model_dump(exclude_none=True)'s shape -- an area that never
    # set a style property looks identical whether it just got created or
    # round-tripped through storage.
    for key in _AREA_STYLE_KEYS:
        if key in props:
            area[key] = props[key]
    return area


def _area_to_feature(area: dict) -> dict:
    properties = {
        "identifier": area.get("identifier", ""),
        "name": area.get("name", ""),
        "locked": area.get("locked", False),
    }
    for key in _AREA_STYLE_KEYS:
        if area.get(key) is not None:
            properties[key] = area[key]
    return {
        "type": "Feature",
        "properties": properties,
        "geometry": area.get("geometry", {}),
    }


def _load_areas_array() -> list[dict]:
    raw = _redis_get(config_areas_key())
    if not raw:
        return []
    collection = json.loads(raw)
    return [_feature_to_area(f) for f in collection.get("features", [])]


def _save_areas_array(areas: list[dict], expect_identifier: Optional[str] = None) -> None:
    collection = {
        "type": "FeatureCollection",
        "features": [_area_to_feature(a) for a in areas],
    }
    body = json.dumps(collection)
    if not _engine.load_areas_json(body):
        raise HTTPException(status_code=400, detail=_engine.last_error or "Invalid areas")

    # _load_areas() is deliberately lenient at the per-feature level (a bad
    # individual feature is silently dropped, not a hard failure -- see
    # message-processor/rules_engine.py), so a successful reload doesn't
    # guarantee the item this call cares about actually survived it. But
    # RulesEngine only ever stages Polygon areas at all -- a LineString/
    # Point area is *never* present in _engine._areas by design, not
    # because anything went wrong, so this safety net only means something
    # for a Polygon area. Pydantic's own Area/AreaGeometry validation
    # (already run before this function is ever called) is the only and
    # authoritative check for LineString/Point areas.
    if expect_identifier is not None:
        saved = next((a for a in areas if a.get("identifier") == expect_identifier), None)
        is_polygon = saved is not None and saved.get("geometry", {}).get("type") == "Polygon"
        if is_polygon and not any(a["identifier"] == expect_identifier for a in _engine._areas):
            raise HTTPException(
                status_code=400,
                detail=f"Area '{expect_identifier}' failed validation "
                "(check identifier has no spaces and geometry is a valid Polygon)",
            )

    version = hashlib.sha256(body.encode()).hexdigest()
    _redis_set_config_pair(config_areas_key(), body, config_areas_version_key(), version)
    _write_backup_file(_areas_backup_path(), body)


@app.get("/api/areas", tags=["areas"], response_model=list[Area], responses={**_REDIS_ERROR})
def list_areas():
    return JSONResponse(content=_load_areas_array())


@app.get(
    "/api/areas/{identifier}",
    tags=["areas"],
    response_model=Area,
    responses={**_NOT_FOUND, **_REDIS_ERROR},
)
def get_area(identifier: str):
    for area in _load_areas_array():
        if area.get("identifier") == identifier:
            return JSONResponse(content=area)
    raise HTTPException(status_code=404, detail=f"Area '{identifier}' not found")


@app.post(
    "/api/areas",
    tags=["areas"],
    status_code=201,
    response_model=Area,
    responses={**_CONFLICT, **_VALIDATION_ERROR, **_REDIS_ERROR},
)
def create_area(area: Area):
    areas = _load_areas_array()
    identifier = area.identifier
    if any(a.get("identifier") == identifier for a in areas):
        raise HTTPException(status_code=409, detail=f"Area '{identifier}' already exists")

    area_dict = area.model_dump(by_alias=True, exclude_none=True)
    areas.append(area_dict)
    _save_areas_array(areas, expect_identifier=identifier)
    return JSONResponse(status_code=201, content=area_dict)


@app.put(
    "/api/areas/{identifier}",
    tags=["areas"],
    response_model=Area,
    responses={**_NOT_FOUND, **_VALIDATION_ERROR, **_REDIS_ERROR},
)
def update_area(identifier: str, area: Area):
    areas = _load_areas_array()
    idx = next((i for i, a in enumerate(areas) if a.get("identifier") == identifier), None)
    if idx is None:
        raise HTTPException(status_code=404, detail=f"Area '{identifier}' not found")

    if area.identifier != identifier:
        raise HTTPException(
            status_code=400,
            detail=f"Body identifier '{area.identifier}' does not match path identifier '{identifier}'",
        )

    areas[idx] = area.model_dump(by_alias=True, exclude_none=True)
    _save_areas_array(areas, expect_identifier=identifier)
    return JSONResponse(content=areas[idx])


@app.delete(
    "/api/areas/{identifier}",
    tags=["areas"],
    status_code=204,
    responses={**_NOT_FOUND, **_AREA_IN_USE, **_VALIDATION_ERROR, **_REDIS_ERROR},
)
def delete_area(identifier: str):
    areas = _load_areas_array()
    remaining = [a for a in areas if a.get("identifier") != identifier]
    if len(remaining) == len(areas):
        raise HTTPException(status_code=404, detail=f"Area '{identifier}' not found")

    # Referential integrity: an area condition pointing at a deleted area
    # would otherwise be silently invalid, and message-processor's
    # RulesEngine._load_rules treats any invalid rule as fatal to the whole
    # reload -- reject the delete rather than let that happen downstream.
    referencing_rules = [
        rule.get("identifier")
        for rule in _load_rules_array()
        if any(
            condition.get("type") == "area" and condition.get("value") == identifier
            for condition in rule.get("conditions", [])
        )
    ]
    if referencing_rules:
        raise HTTPException(
            status_code=409,
            detail=f"Area '{identifier}' is referenced by rule(s): "
            f"{', '.join(referencing_rules)}",
        )

    _save_areas_array(remaining)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Reference-data lookup (aircraft/operator/airport/route) -- static enrichment
# already held in Redis for rule evaluation, exposed read-only for browsing.
# Distinct from a future live-aircraft-position UI (see this module's
# docstring) and from the Athena archive search below (historical flights,
# not current Redis state).
# ---------------------------------------------------------------------------

@app.get(
    "/api/aircraft",
    tags=["reference-data"],
    response_model=AircraftRecord,
    responses={**_NOT_FOUND, **_VALIDATION_ERROR, **_REDIS_ERROR, **_SEARCH_INDEX_UNAVAILABLE},
)
def get_aircraft(
    icao_hex: Optional[str] = FastAPIQuery(default=None, title="ICAO Hex", description="6-character ICAO hex, e.g. A8AE7F"),
    registration: Optional[str] = FastAPIQuery(default=None, description="Aircraft registration, e.g. N659DL"),
):
    if icao_hex and registration:
        raise HTTPException(status_code=422, detail="Specify either icao_hex or registration, not both")
    if not icao_hex and not registration:
        raise HTTPException(status_code=422, detail="Specify either icao_hex or registration")

    if icao_hex:
        raw = _redis_evalsha(_merge_aircraft_sha, icao_hex.upper())
        if not raw:
            raise HTTPException(status_code=404, detail=f"No aircraft data found for '{icao_hex}'")
        return JSONResponse(content=_flatten_aircraft_doc(json.loads(raw)))

    # Mictronics first (broader coverage), then the country-registry index --
    # a registration can exist in either, or both.
    doc_id = _search_one(AIRCRAFT_MICTRONICS_SEARCH_INDEX, "registration", registration)
    if doc_id is None:
        doc_id = _search_one(AIRCRAFT_REGISTRY_SEARCH_INDEX, "registration", registration)
    if doc_id is None:
        raise HTTPException(
            status_code=404, detail=f"No aircraft data found for registration '{registration}'"
        )

    resolved_hex = doc_id.rsplit(":", 1)[-1]
    raw = _redis_evalsha(_merge_aircraft_sha, resolved_hex)
    if not raw:
        raise HTTPException(
            status_code=404, detail=f"No aircraft data found for registration '{registration}'"
        )
    return JSONResponse(content=_flatten_aircraft_doc(json.loads(raw)))


@app.get(
    "/api/operators/{designator}",
    tags=["reference-data"],
    response_model=OperatorRecord,
    responses={**_NOT_FOUND, **_REDIS_ERROR},
)
def get_operator(designator: str):
    doc = _redis_json_get(operator_key(designator))
    if not doc:
        raise HTTPException(status_code=404, detail=f"No operator data found for '{designator}'")
    return JSONResponse(content=doc)


@app.get(
    "/api/airports/{code}",
    tags=["reference-data"],
    response_model=AirportRecord,
    responses={**_NOT_FOUND, **_REDIS_ERROR, **_SEARCH_INDEX_UNAVAILABLE},
)
def get_airport(code: str):
    normalized = code.strip().upper()
    doc: Optional[dict] = None
    if len(normalized) == 4:
        doc = _redis_json_get(airport_key(normalized))
    elif len(normalized) == 3:
        doc_id = _search_one(AIRPORT_SEARCH_INDEX, "iata_code", normalized)
        if doc_id is not None:
            doc = _redis_json_get(doc_id)
    # Any other length can't match either key shape -- falls through to the
    # same 404 a genuine miss gets, rather than a separate 400: Redis can't
    # distinguish "malformed code" from "well-formed but unknown" any better
    # than it can the misses described in this issue's Miss semantics.
    if not doc:
        raise HTTPException(status_code=404, detail=f"No airport data found for '{code}'")
    return JSONResponse(content=doc)


# No-hyphen registration prefixes -- mirrors the frontend's
# lookupClassifier.ts NO_HYPHEN_REGISTRATION_PREFIX. Combined with "contains
# a digit" (checked separately, since some of these prefixes are also valid
# airline-designator leads), this tells get_route() a bare registration like
# "N659DL" isn't a flight ident worth an operator-prefix lookup.
_NO_HYPHEN_REGISTRATION_PREFIX = re.compile(r"^(N|HL|JA)", re.IGNORECASE)


@app.get(
    "/api/routes/{ident}",
    tags=["reference-data"],
    response_model=RouteLookup,
    responses={**_NOT_FOUND, **_REDIS_ERROR},
)
def get_route(ident: str):
    raw = _redis_evalsha(_route_airports_sha, normalize_flight_ident(ident.upper()))
    airports = json.loads(raw) if raw else []

    # Best-effort operator enrichment -- same ICAO airline-designator
    # extraction message-processor's _enrich_operator uses (letters before
    # the first digit). A too-short/missing prefix or no matching
    # operator:{designator} record just omits `operator`; it never fails
    # the route lookup itself. Resolved unconditionally (even when the
    # route itself is unknown) so a part-135 operator's flight numbers --
    # which never get scheduled-service route data -- still resolve to
    # their operator instead of a flat 404.
    #
    # Skipped for a bare no-hyphen registration shape (e.g. "N659DL",
    # "HL7404"): those aren't flight idents, and their letters-before-digits
    # prefix isn't an airline designator. Mirrors the frontend's
    # lookupClassifier.ts isRegistration() no-hyphen branch.
    operator = None
    if not (_NO_HYPHEN_REGISTRATION_PREFIX.match(ident) and re.search(r"\d", ident)):
        prefix = re.split(r"[^a-zA-Z]", ident)[0]
        if len(prefix) >= 2:
            operator = _redis_json_get(operator_key(prefix))

    if not airports:
        if operator is None:
            raise HTTPException(status_code=404, detail=f"No route data found for '{ident}'")
        return JSONResponse(content={"ident": ident.upper(), "operator": operator})

    return JSONResponse(content={
        "ident": ident.upper(),
        "origin": airports[0],
        "destination": airports[-1],
        "stops": airports,
        "operator": operator,
    })


# ---------------------------------------------------------------------------
# Archive search -- Athena/Glue query layer over the archive's Parquet
# index (see archive-processor's Parquet Index section and
# specs/data-dictionary.yaml's archive_parquet_index record for the 9
# underlying columns). A search record lives at archive_search:{uuid} in
# Redis for a fixed 7 days from creation (never refreshed on access).
#
# Two independent read paths over the same search, deliberately shaped
# differently:
#  - The PAGED VIEW (get_archive_search_results) is served from a bounded,
#    in-process LRU (_result_cache) holding at most _RESULT_ROW_CAP rows per
#    search -- one get_query_results call ever, no S3 read at all. See
#    _fetch_and_cache_results.
#  - DOWNLOAD (download_archive_search) always goes straight to S3 via a
#    presigned URL, for every result size, via a second, separate query
#    that never selects s3_key -- see _build_download_query and
#    _run_or_get_download_query_execution. The backend never reads those
#    result bytes.
# ---------------------------------------------------------------------------

# ARCHIVE_SEARCH_TTL_SECONDS must expire a search's Redis record before the
# Athena results file it points at is aged out of the results bucket by that
# bucket's own lifecycle policy -- otherwise a still-listed search would
# resolve to a deleted S3 object. Keep this comfortably under that lifecycle.
ARCHIVE_SEARCH_TTL_SECONDS = 7 * 86400
_PAGE_SIZE = 100
_PAGE_SIZE_MIN = 25
_PAGE_SIZE_MAX = 500
ATHENA_POLL_BACKOFF_SECONDS = [1, 2, 4, 8, 16]
ATHENA_POLL_DEADLINE_SECONDS = 120
_RESULT_CACHE_MAX_ENTRIES = 10
# The paged view's whole memory budget is these two numbers multiplied
# together (10 x 500 rows =~ 4MB worst case) -- raise one only after
# reconsidering the other, or the unbounded-memory failure this pair exists
# to prevent comes back. Requesting _RESULT_ROW_CAP + 1 rows from Athena
# makes "does a 501st row exist" answerable from a single get_query_results
# call: getting back more than _RESULT_ROW_CAP data rows means the true
# match count is larger, without ever running a separate COUNT query.
_RESULT_ROW_CAP = 500
_DOWNLOAD_PRESIGN_TTL_SECONDS = 15 * 60

# Column order here is exactly what the Athena SELECT below returns, so
# _row_from_athena_result_row can map each result row positionally without
# needing to consult the header row Athena also returns as row 0. s3_key IS
# selected (needed server-side to mint each row's fetch token and derive its
# flight UUID -- see _row_from_athena_result_row) but is never included in
# the dict a response actually returns to the browser.
_SEARCH_SELECT_COLUMNS = [
    "icao_hex", "registration", "type_designator", "military",
    "operator_designator", "ident", "first_message", "last_message", "s3_key",
]

# Columns the download endpoint's own sanitized query selects -- everything
# _SEARCH_SELECT_COLUMNS has except s3_key, which must never be selected
# there at all (see _build_download_query): the object S3 hands the browser
# is what this list produces, so the storage layout can only leak here by
# being added back to this list.
_DOWNLOAD_SELECT_COLUMNS = [
    "icao_hex", "registration", "type_designator", "military",
    "operator_designator", "ident", "first_message", "last_message",
]

# Columns a results-page request may sort by -- every field
# ArchiveSearchResultRow exposes to the browser except uuid/token, which are
# server-derived rather than a real Athena column a user would sort on.
_SORTABLE_COLUMNS = (
    "icao_hex", "registration", "type_designator", "military",
    "operator_designator", "ident", "first_message", "last_message",
)

# Cheap early rejection before ever calling Athena -- not a real security
# boundary (the querying IAM identity is already read-only on just this one
# table), purely so a mistake produces an instant, clear 400 instead of a
# slower, more opaque Athena AccessDenied. Word-boundary so a legitimate
# value that happens to contain one of these words (e.g. ident = 'INSERT1')
# doesn't false-positive.
_FORBIDDEN_WHERE_CLAUSE_RE = re.compile(
    r"\b(DROP|CREATE|ALTER|INSERT|DELETE|UPDATE|GRANT)\b", re.IGNORECASE
)

# Athena (Presto/Trino) follows strict ANSI SQL: double quotes denote an
# identifier (column/table reference), single quotes denote a string
# literal. A clause like operator_designator = "DAL" is parsed as a
# comparison against a column named DAL, not the string 'DAL' -- an easy
# mistake since most languages treat both quote styles as equivalent for
# strings. Flag any double-quoted, identifier-shaped token that isn't
# actually one of the known searchable columns, since it's almost
# certainly a mistaken string literal rather than a legitimate quoted
# column reference.
_DOUBLE_QUOTED_RE = re.compile(r'"([A-Za-z_][A-Za-z0-9_]*)"')

_AWS_ERROR = {502: {"description": "AWS (Athena/S3) error", "model": ErrorDetail}}


class ArchiveSearchCreate(BaseModel):
    name: str = Field(..., min_length=1)
    where_clause: str = Field(..., min_length=1)
    # Both optional -- an omitted bound defaults to the full archive range
    # (_ARCHIVE_EPOCH .. tomorrow UTC) at creation time. UTC calendar dates,
    # matching the year/month/day partition columns they narrow.
    start_date: Optional[date] = None
    end_date: Optional[date] = None


class ArchiveSearchSummary(BaseModel):
    uuid: str
    name: str
    status: Literal["RUNNING", "COMPLETE", "FAILED", "ABORTED"]
    submitted_at: str
    expires_at: str
    # Only ever set for FAILED (Athena's own StateChangeReason) or ABORTED
    # (this backend's own deadline/restart message) -- absent otherwise.
    error: Optional[str] = None


class ArchiveSearchDetail(ArchiveSearchSummary):
    where_clause: str
    # The RESOLVED range actually queried (explicit input intersected with
    # whatever _derive_bounds could prove from where_clause, clamped to
    # _ARCHIVE_EPOCH..tomorrow UTC) -- not the raw optional request fields.
    # Optional here only so a record written before this field existed still
    # deserializes (see get_archive_search's .get() reads).
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    # What the operator actually typed, captured before the
    # _ARCHIVE_EPOCH/tomorrow substitution -- None means the field was left
    # blank. Optional here for the same backward-compatibility reason as
    # start_date/end_date above.
    requested_start_date: Optional[date] = None
    requested_end_date: Optional[date] = None


class ArchiveSearchResultsPage(BaseModel):
    rows: list[ArchiveSearchResultRow]
    # The cached match count, not just len(rows) -- rows is only this page's
    # slice. Exact whenever `truncated` is False; when True, this is the
    # _RESULT_ROW_CAP cache size, not the real (unknown, unread) match count.
    total_rows: int
    # True when more than _RESULT_ROW_CAP rows actually matched -- the exact
    # count beyond the cap is deliberately never computed (that would mean
    # reading the whole result just to count it). See Download for the full
    # set in this case.
    truncated: bool = False


class ArchiveSearchResultRow(BaseModel):
    """One archive_parquet_index row, minus s3_key (never sent to the
    browser -- see "Flight fetch" below) plus the flight's own uuid (parsed
    server-side from s3_key, not a column in the index itself) and an
    encrypted, opaque token in s3_key's place."""

    uuid: str
    icao_hex: str = Field(title="ICAO Hex")
    registration: str
    type_designator: str
    military: bool
    operator_designator: str
    ident: str
    first_message: str
    last_message: str
    token: str


def _validate_where_clause(where_clause: str) -> None:
    if ";" in where_clause:
        raise HTTPException(status_code=400, detail="where_clause must not contain ';'")
    match = _FORBIDDEN_WHERE_CLAUSE_RE.search(where_clause)
    if match:
        raise HTTPException(
            status_code=400,
            detail=f"where_clause contains a forbidden keyword: '{match.group(1)}'",
        )
    for quoted in _DOUBLE_QUOTED_RE.finditer(where_clause):
        name = quoted.group(1)
        if name not in _SEARCH_SELECT_COLUMNS:
            raise HTTPException(
                status_code=400,
                detail=(
                    f'"{name}" is double-quoted, which Athena treats as a column '
                    f"reference, not a string literal -- did you mean '{name}' "
                    "(single quotes)?"
                ),
            )


# Lower bound of the archive. Built from shared.glue_projection.YEAR_RANGE
# (the same source specs/aws/cloudformation.yaml's Glue table and
# shared/tests/test_cloudformation_template.py are checked against) rather
# than a hand-copied literal, so this can never silently drift out of sync
# with projection.year.range -- a range wider than the projection can't
# match anything, so widening one always means widening both.
#
# Deliberately NOT tightened to the real earliest flight (2022-07-11, per the
# S3 migration open item). Being earlier than the data costs a handful of
# extra empty partition LISTs; being LATER than the data silently drops rows
# with no error. Keep this aligned with the projection's lower bound, not
# with when the archive actually starts, so the two stay coupled to each
# other and to nothing else.
_ARCHIVE_EPOCH = date(_GLUE_YEAR_RANGE[0], 1, 1)

# Columns _derive_bounds will read a bound from. first_message gives both a
# lower and upper bound -- it's a single instant, so any comparison against
# it constrains both sides of the range from that one predicate.
# last_message only ever gives an upper bound: last_message <= T implies
# first_message <= T (the flight can't end after it starts... after T), but
# last_message >= T says nothing about how early the flight could have
# started. Do not add a lower-bound use of last_message.
_LOWER_BOUND_COLUMNS = ("first_message",)
_UPPER_BOUND_COLUMNS = ("first_message", "last_message")


def _partition_predicate(start: date, end: date) -> str:
    """OR-joined clauses on the year/month/day partition columns covering
    `[start, end]` inclusive, using the coarsest clause that exactly covers
    each span (a whole year, then whole months within a year, then a day
    range within a month) so a wide range doesn't degenerate into 1,000+
    single-day ORs. year is unpadded 4-digit; month/day are zero-padded to
    2 digits, matching projection.month.digits/projection.day.digits in
    specs/aws/cloudformation.yaml -- an unpadded 'month=9' matches no
    partition Athena actually generates.

    "Whole month" is judged against the real last day of that month
    (calendar.monthrange), not the 31st -- partition projection generates
    day=01..31 unconditionally regardless of the month's real length, so a
    surplus day prefix (e.g. day=30 in February) just LISTs an empty
    location rather than causing a mismatch.
    """
    clauses = []
    cur = start
    while cur <= end:
        year_end = date(cur.year, 12, 31)
        if cur.month == 1 and cur.day == 1 and year_end <= end:
            clauses.append(f"(year='{cur.year}')")
            cur = year_end + timedelta(days=1)
            continue

        month_last_day = calendar.monthrange(cur.year, cur.month)[1]
        month_end = date(cur.year, cur.month, month_last_day)
        if cur.day == 1 and month_end <= end:
            # Extend across any further contiguous whole months, still
            # within this calendar year (a year boundary always gets its
            # own clause via the whole-year case above on the next lap).
            run_end_month = cur.month
            probe = month_end
            while probe < date(cur.year, 12, 31):
                next_month_first = probe + timedelta(days=1)
                if next_month_first.year != cur.year:
                    break
                next_last_day = calendar.monthrange(next_month_first.year, next_month_first.month)[1]
                next_month_end = date(next_month_first.year, next_month_first.month, next_last_day)
                if next_month_end > end:
                    break
                run_end_month = next_month_first.month
                probe = next_month_end
            if run_end_month == cur.month:
                clauses.append(f"(year='{cur.year}' AND month='{cur.month:02d}')")
            else:
                clauses.append(
                    f"(year='{cur.year}' AND month BETWEEN '{cur.month:02d}' AND '{run_end_month:02d}')"
                )
            cur = probe + timedelta(days=1)
            continue

        month_last = date(cur.year, cur.month, month_last_day)
        day_end = min(end, month_last)
        clauses.append(
            f"(year='{cur.year}' AND month='{cur.month:02d}' "
            f"AND day BETWEEN '{cur.day:02d}' AND '{day_end.day:02d}')"
        )
        cur = day_end + timedelta(days=1)

    return " OR ".join(clauses)


def _literal_date(node: exp.Expression) -> Optional[date]:
    """First string literal under `node`, read as a leading YYYY-MM-DD --
    tolerant of a full timestamp literal ('2026-09-01 00:00:00') since that's
    the only literal shape this UI's WHERE clauses actually use."""
    for lit in node.find_all(exp.Literal):
        if lit.is_string:
            try:
                return date.fromisoformat(lit.this[:10])
            except ValueError:
                return None
    return None


def _comparison_sides(node: exp.Binary) -> tuple[Optional[str], Optional[date], bool]:
    """(column_name, literal_date, flipped) for a binary comparison node --
    `flipped` is True when the literal appears on the left (e.g.
    ""timestamp '...' <= first_message""), so the caller can invert which
    side of the comparison the column is really on."""
    left, right = node.this, node.expression
    if isinstance(left, exp.Column):
        return left.name, _literal_date(right), False
    if isinstance(right, exp.Column):
        return right.name, _literal_date(left), True
    return None, None, False


def _derive_bounds(where_clause: str) -> tuple[Optional[date], Optional[date]]:
    """The widest date range that can contain every row `where_clause` could
    possibly match, read off its own first_message/last_message predicates
    -- or (None, None) if nothing could be proven, meaning the caller must
    fall back to the full archive range. This is an optimisation layered on
    top of a WHERE clause that is already fully evaluated by Athena; getting
    it wrong must never drop a row that where_clause itself would have
    matched, so every bail-out below is deliberately conservative.

    Only sound inside a pure AND conjunction, where every conjunct is a
    necessary condition on a matching row. An OR or a NOT breaks that --
    `first_message > X OR icao_hex = 'ABC'` can match rows outside the
    range implied by the first_message predicate alone -- so either one
    anywhere in the clause bails out to (None, None) rather than risk
    narrowing past a row Athena would have returned.

    Uses sqlglot rather than a regex specifically so a column reference is
    never confused with the same text inside a string literal --
    e.g. ident = 'first_message > 2020-01-01' has zero real column
    predicates on it, and a regex scanning the raw text would get that
    wrong silently (fewer rows, no error) rather than just not narrowing.
    """
    try:
        tree = sqlglot.parse_one(where_clause, dialect="trino")
    except Exception:
        return None, None
    if tree is None or tree.find(exp.Or) or tree.find(exp.Not):
        return None, None

    lo: Optional[date] = None
    hi: Optional[date] = None

    for node in tree.find_all(exp.Between):
        if isinstance(node.this, exp.Column):
            column = node.this.name
            if column in _LOWER_BOUND_COLUMNS:
                low = _literal_date(node.args["low"])
                if low is not None:
                    lo = low if lo is None else max(lo, low)
            if column in _UPPER_BOUND_COLUMNS:
                high = _literal_date(node.args["high"])
                if high is not None:
                    hi = high if hi is None else min(hi, high)

    # >=/> give a lower bound; <=/< give an upper bound -- unless the
    # column turns out to be on the literal's side of the operator
    # (`flipped`), which inverts which bound the comparison actually
    # establishes (""timestamp '...' <= first_message"" is a LOWER bound
    # on first_message, even though <= normally reads as an upper one).
    for comparison_cls, implies in ((exp.GTE, "lo"), (exp.GT, "lo"), (exp.LTE, "hi"), (exp.LT, "hi")):
        for node in tree.find_all(comparison_cls):
            column, literal, flipped = _comparison_sides(node)
            if not column or literal is None:
                continue
            bound = implies if not flipped else ("hi" if implies == "lo" else "lo")
            if bound == "lo" and column in _LOWER_BOUND_COLUMNS:
                lo = literal if lo is None else max(lo, literal)
            elif bound == "hi" and column in _UPPER_BOUND_COLUMNS:
                hi = literal if hi is None else min(hi, literal)

    for node in tree.find_all(exp.EQ):
        column, literal, _flipped = _comparison_sides(node)
        if literal is None:
            continue
        if column in _LOWER_BOUND_COLUMNS:
            lo = literal if lo is None else max(lo, literal)
        if column in _UPPER_BOUND_COLUMNS:
            hi = literal if hi is None else min(hi, literal)

    return lo, hi


def _resolve_search_range(
    where_clause: str, explicit_start: Optional[date], explicit_end: Optional[date]
) -> tuple[Optional[date], Optional[date]]:
    """Intersects three independent constraints on the query's date range --
    the archive's own bounds, what where_clause's own predicates can prove
    (widened by a day each side as boundary/timezone insurance), and
    whatever the operator explicitly set -- and returns the tightest result.
    (None, None) signals an empty intersection (e.g. an explicit range that
    doesn't overlap what where_clause could ever match): a real, zero-row
    answer, distinguished by the caller from explicit_start > explicit_end,
    which is a 400 on the operator's own input rather than a derived
    emptiness.
    """
    derived_lo, derived_hi = _derive_bounds(where_clause)
    today = datetime.now(timezone.utc).date()

    lower_bounds = [_ARCHIVE_EPOCH]
    if derived_lo is not None:
        lower_bounds.append(derived_lo - timedelta(days=1))
    if explicit_start is not None:
        lower_bounds.append(explicit_start)

    upper_bounds = [today + timedelta(days=1)]
    if derived_hi is not None:
        upper_bounds.append(derived_hi + timedelta(days=1))
    if explicit_end is not None:
        upper_bounds.append(explicit_end)

    start = max(lower_bounds)
    end = min(upper_bounds)
    if start > end:
        return None, None
    return start, end


def _build_search_query(partition_predicate: str, where_clause: str) -> str:
    """The SELECT list and FROM table are always backend-controlled, never
    influenced by user input -- where_clause only ever fills the second
    WHERE fragment, parenthesized so it can't prematurely close the clause
    and inject a sibling SQL construct. partition_predicate is backend-
    generated too (see _partition_predicate) -- its only purpose is
    pruning Athena's partition scan; it must always be a superset of what
    where_clause alone would match, never a narrower filter."""
    columns = ", ".join(_SEARCH_SELECT_COLUMNS)
    table = f'{_athena_cfg["database"]}.{_athena_cfg["table"]}'
    return f"SELECT {columns} FROM {table} WHERE ({partition_predicate}) AND ({where_clause})"


# Anchors on the UUID immediately preceding ".json.gz", so it matches both
# the legacy `{icao_hex}_{ident}_{uuid}.json.gz` key shape and the current,
# simplified `{uuid}.json.gz` shape -- the Python-side _UUID_FROM_S3_KEY_RE
# below uses the identical pattern so the two derivations can never disagree.
_UUID_FROM_S3_KEY_PATTERN = r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.json\.gz$"


def _build_download_query(partition_predicate: str, where_clause: str) -> str:
    """Same partition-predicate/where_clause contract as _build_search_query
    (see its docstring) -- the difference is entirely in the SELECT list.
    s3_key is never selected here, so the archive's storage layout (bucket,
    date-folder prefix, filename) can never reach the browser via this
    query's result object; the flight uuid is instead derived from s3_key
    in SQL via regexp_extract, using the same pattern
    _UUID_FROM_S3_KEY_PATTERN names for the Python-side equivalent."""
    columns = ", ".join(_DOWNLOAD_SELECT_COLUMNS)
    table = f'{_athena_cfg["database"]}.{_athena_cfg["table"]}'
    uuid_expr = f"regexp_extract(s3_key, '{_UUID_FROM_S3_KEY_PATTERN}', 1)"
    return (
        f"SELECT {uuid_expr} AS uuid, {columns} FROM {table} "
        f"WHERE ({partition_predicate}) AND ({where_clause})"
    )


def _expires_at(submitted_at_iso: str) -> str:
    submitted = datetime.fromisoformat(submitted_at_iso)
    return (submitted + timedelta(seconds=ARCHIVE_SEARCH_TTL_SECONDS)).isoformat()


def _search_summary(uuid: str, record: dict) -> dict:
    return {
        "uuid": uuid,
        "name": record["name"],
        "status": record["status"],
        "submitted_at": record["submitted_at"],
        "expires_at": _expires_at(record["submitted_at"]),
        "error": record.get("error"),
    }


def _iter_active_searches() -> list[tuple[str, dict]]:
    """SMEMBERS archive_search:index + a GET per uuid -- O(active
    searches), not the O(entire keyspace) SCAN MATCH archive_search:*
    this replaced (see archive_search_index_key's own docstring for why).
    Self-heals the index: a uuid whose backing archive_search:{uuid} key
    has already expired (7-day TTL) is SREMed from the index right here,
    since a plain Redis SET has no way to be notified when TTL expiry
    removes a member's backing key out from under it."""
    index_key = archive_search_index_key()
    try:
        uuids = _redis.smembers(index_key)
    except redis_lib.RedisError as exc:
        raise HTTPException(status_code=500, detail=f"Redis error: {exc}") from exc

    results = []
    for uuid in uuids:
        raw = _redis_get(archive_search_key(uuid))
        if raw is None:
            try:
                _redis.srem(index_key, uuid)
            except redis_lib.RedisError as exc:
                logger.warning("Failed to prune stale archive search index entry %s: %s", uuid, exc)
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError:
            continue
        results.append((uuid, record))
    return results


def _get_search_record(uuid: str) -> dict:
    raw = _redis_get(archive_search_key(uuid))
    if raw is None:
        raise HTTPException(status_code=404, detail=f"Search '{uuid}' not found")
    return json.loads(raw)


def _update_search_record(uuid: str, **fields) -> None:
    """Conditional SET ... XX KEEPTTL -- only writes if the key still
    exists (a no-op otherwise), and never resets/extends the fixed 7-day
    TTL set at creation. Guards against the background polling thread
    resurrecting a record the user already deleted: every write it makes
    goes through this same function, so a delete that lands between this
    thread's last GET and its next write is never undone by that write."""
    key = archive_search_key(uuid)
    try:
        raw = _redis.get(key)
        if raw is None:
            return
        record = json.loads(raw)
        record.update(fields)
        _redis.set(key, json.dumps(record), xx=True, keepttl=True)
    except redis_lib.RedisError as exc:
        logger.warning("Failed to update archive search %s: %s", uuid, exc)


def _reconcile_stuck_archive_searches() -> None:
    """On startup, any archive_search:* record still RUNNING had its
    polling thread die with the previous process -- nothing is left alive
    to ever finish that job, so mark it ABORTED rather than leaving it
    stuck RUNNING forever."""
    for uuid, record in _iter_active_searches():
        if record.get("status") == "RUNNING":
            _update_search_record(
                uuid, status="ABORTED", error="Process restarted while this search was running"
            )
            logger.info("Marked stuck archive search %s ABORTED on startup.", uuid)


def _poll_search_execution(uuid: str, query_execution_id: str) -> None:
    """One thread per in-flight search. Exponential backoff (1s, 2s, 4s,
    8s, 16s, then capped at 30s) for up to 2 minutes wall-clock total --
    if the deadline is hit without reaching a terminal state, this gives
    up (ABORTED) independent of whether Athena itself might still be
    running."""
    deadline = time.monotonic() + ATHENA_POLL_DEADLINE_SECONDS
    attempt = 0
    while time.monotonic() < deadline:
        delay = (
            ATHENA_POLL_BACKOFF_SECONDS[attempt]
            if attempt < len(ATHENA_POLL_BACKOFF_SECONDS)
            else ATHENA_POLL_BACKOFF_SECONDS[-1] * 2  # 30s cap, per design
        )
        attempt += 1
        time.sleep(min(delay, 30))

        try:
            resp = _athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        except Exception as exc:
            logger.warning("get_query_execution failed for search %s: %s", uuid, exc)
            continue

        state = resp["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            _update_search_record(uuid, status="COMPLETE")
            return
        if state in ("FAILED", "CANCELLED"):
            reason = resp["QueryExecution"]["Status"].get("StateChangeReason", "")
            _update_search_record(uuid, status="FAILED", error=reason)
            return
        # QUEUED / RUNNING -- keep polling

    try:
        _athena_client.stop_query_execution(QueryExecutionId=query_execution_id)
    except Exception as exc:
        logger.warning("Best-effort stop_query_execution failed for search %s: %s", uuid, exc)
    _update_search_record(uuid, status="ABORTED", error="Deadline exceeded (2 minutes)")


class _BoundedResultCache:
    """Hand-rolled LRU (OrderedDict, move-to-end on access, pop oldest when
    over the cap) rather than a new dependency -- simple enough to
    implement correctly without one. Lives only in process memory, wiped
    on restart: a page request for a search that was mid-viewing when the
    container restarted is just a cache miss, not an error."""

    def __init__(self, max_entries: int) -> None:
        self._max_entries = max_entries
        self._data: OrderedDict[str, Any] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            if key not in self._data:
                return None
            self._data.move_to_end(key)
            return self._data[key]

    def put(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = value
            self._data.move_to_end(key)
            while len(self._data) > self._max_entries:
                self._data.popitem(last=False)

    def discard(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)


_result_cache = _BoundedResultCache(_RESULT_CACHE_MAX_ENTRIES)


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    without_scheme = uri.removeprefix("s3://")
    bucket, _, key = without_scheme.partition("/")
    return bucket, key


def _result_output_location(query_execution_id: str) -> str:
    resp = _athena_client.get_query_execution(QueryExecutionId=query_execution_id)
    return resp["QueryExecution"]["ResultConfiguration"]["OutputLocation"]


_UUID_FROM_S3_KEY_RE = re.compile(_UUID_FROM_S3_KEY_PATTERN)


def _uuid_from_s3_key(s3_key: str) -> str:
    """Extract the flight UUID from an S3 flight object key -- anchored on
    the UUID immediately preceding ".json.gz" (see
    _UUID_FROM_S3_KEY_PATTERN's own comment), so this works against both the
    legacy `flights/{YYYY}/{MM}/{DD}/{icao_hex}_{ident}_{uuid}.json.gz` key
    shape and the current, simplified
    `flights/{YYYY}/{MM}/{DD}/{uuid}.json.gz` shape."""
    match = _UUID_FROM_S3_KEY_RE.search(s3_key)
    return match.group(1) if match else ""


def _encrypt_s3_key(s3_key: str) -> str:
    return _fernet.encrypt(s3_key.encode()).decode()


def _decrypt_token(token: str) -> str:
    try:
        return _fernet.decrypt(token.encode()).decode()
    except InvalidToken as exc:
        raise HTTPException(status_code=400, detail="Invalid or expired flight token") from exc


def _fetch_and_cache_results(uuid: str, query_execution_id: str) -> tuple[list[dict], bool]:
    """Returns (rows, truncated), where `rows` never exceeds
    _RESULT_ROW_CAP. A single get_query_results call, bounded at
    _RESULT_ROW_CAP + 1 rows, both builds the cached page window and answers
    "did more than _RESULT_ROW_CAP rows match" without a separate count
    query -- see _RESULT_ROW_CAP's own comment. No S3 read of any kind
    happens on this path; the full result set is never downloaded to the
    backend."""
    cached = _result_cache.get(uuid)
    if cached is not None:
        return cached["rows"], cached["truncated"]

    try:
        resp = _athena_client.get_query_results(
            QueryExecutionId=query_execution_id, MaxResults=_RESULT_ROW_CAP + 1,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch search results: {exc}") from exc

    # get_query_results returns the column header as its first row -- skip it.
    data_rows = resp["ResultSet"]["Rows"][1:]
    truncated = len(data_rows) > _RESULT_ROW_CAP
    rows = [_row_from_athena_result_row(row["Data"]) for row in data_rows[:_RESULT_ROW_CAP]]
    _result_cache.put(uuid, {"rows": rows, "truncated": truncated})
    return rows, truncated


def _row_from_athena_result_row(data: list[dict]) -> dict:
    fields = [cell.get("VarCharValue", "") for cell in data]
    (
        icao_hex, registration, type_designator, military,
        operator_designator, ident, first_message, last_message, s3_key,
    ) = fields
    return {
        "uuid": _uuid_from_s3_key(s3_key),
        "icao_hex": icao_hex,
        "registration": registration,
        "type_designator": type_designator,
        "military": military.strip().lower() == "true",
        "operator_designator": operator_designator,
        "ident": ident,
        "first_message": first_message,
        "last_message": last_message,
        "token": _encrypt_s3_key(s3_key),
    }


@app.post(
    "/api/archive/search",
    tags=["archive"],
    status_code=202,
    responses={**_VALIDATION_ERROR, **_REDIS_ERROR, **_AWS_ERROR},
)
def create_archive_search(body: ArchiveSearchCreate):
    where_clause = body.where_clause.strip()
    if not where_clause:
        raise HTTPException(status_code=400, detail="where_clause must not be empty")
    _validate_where_clause(where_clause)

    today = datetime.now(timezone.utc).date()
    explicit_start = body.start_date or _ARCHIVE_EPOCH
    explicit_end = body.end_date or (today + timedelta(days=1))
    if explicit_start > explicit_end:
        raise HTTPException(status_code=400, detail="start_date must not be after end_date")

    start, end = _resolve_search_range(where_clause, explicit_start, explicit_end)

    search_uuid = str(uuid7())
    submitted_at = datetime.now(timezone.utc).isoformat()

    if start is None:
        # The operator's explicit range and what where_clause's own
        # predicates could ever match don't overlap -- a real, zero-row
        # answer (see _resolve_search_range's docstring), not an error.
        # Recorded as already COMPLETE with no Athena query ever started,
        # rather than spending a query to prove what's already provably
        # empty.
        record = {
            "name": body.name,
            "where_clause": where_clause,
            "status": "COMPLETE",
            "submitted_at": submitted_at,
            "query_execution_id": None,
            "start_date": explicit_start.isoformat(),
            "end_date": explicit_end.isoformat(),
            "requested_start_date": body.start_date.isoformat() if body.start_date else None,
            "requested_end_date": body.end_date.isoformat() if body.end_date else None,
        }
        _redis_set(archive_search_key(search_uuid), json.dumps(record), ex=ARCHIVE_SEARCH_TTL_SECONDS)
        try:
            _redis.sadd(archive_search_index_key(), search_uuid)
        except redis_lib.RedisError as exc:
            logger.warning("Failed to add search %s to the archive search index: %s", search_uuid, exc)
        return JSONResponse(status_code=202, content={"uuid": search_uuid})

    partition_predicate = _partition_predicate(start, end)
    query = _build_search_query(partition_predicate, where_clause)
    try:
        resp = _athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": _athena_cfg["database"]},
            WorkGroup=_athena_cfg["workgroup"],
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to start Athena query: {exc}") from exc
    query_execution_id = resp["QueryExecutionId"]

    record = {
        "name": body.name,
        "where_clause": where_clause,
        "status": "RUNNING",
        "submitted_at": submitted_at,
        "query_execution_id": query_execution_id,
        # The RESOLVED range actually queried (explicit input intersected
        # with where_clause's own derived bounds) -- persisted so a
        # resubmit reproduces this exact range rather than re-resolving
        # "tomorrow" against a later clock.
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        # What the operator actually typed, before the _ARCHIVE_EPOCH/tomorrow
        # substitution above -- None means the field was left blank.
        "requested_start_date": body.start_date.isoformat() if body.start_date else None,
        "requested_end_date": body.end_date.isoformat() if body.end_date else None,
    }
    _redis_set(archive_search_key(search_uuid), json.dumps(record), ex=ARCHIVE_SEARCH_TTL_SECONDS)
    try:
        _redis.sadd(archive_search_index_key(), search_uuid)
    except redis_lib.RedisError as exc:
        # Not fatal to the search itself -- worst case, this uuid is
        # missing from list/reconcile until the index is rebuilt some
        # other way, not a data-loss or correctness issue for the search
        # record itself (still readable directly by uuid).
        logger.warning("Failed to add search %s to the archive search index: %s", search_uuid, exc)

    threading.Thread(
        target=_poll_search_execution,
        args=(search_uuid, query_execution_id),
        daemon=True,
        name=f"archive-search-{search_uuid}",
    ).start()

    return JSONResponse(status_code=202, content={"uuid": search_uuid})


@app.get(
    "/api/archive/search",
    tags=["archive"],
    response_model=list[ArchiveSearchSummary],
    responses={**_REDIS_ERROR},
)
def list_archive_searches():
    summaries = [_search_summary(uuid, record) for uuid, record in _iter_active_searches()]
    summaries.sort(key=lambda s: s["submitted_at"], reverse=True)
    return JSONResponse(content=summaries)


@app.get(
    "/api/archive/search/{uuid}",
    tags=["archive"],
    response_model=ArchiveSearchDetail,
    responses={**_NOT_FOUND, **_REDIS_ERROR},
)
def get_archive_search(uuid: str):
    record = _get_search_record(uuid)
    return JSONResponse(content={
        **_search_summary(uuid, record),
        "where_clause": record["where_clause"],
        # .get() rather than direct indexing -- a record written before
        # this field existed still deserializes, just with no range shown.
        "start_date": record.get("start_date"),
        "end_date": record.get("end_date"),
        "requested_start_date": record.get("requested_start_date"),
        "requested_end_date": record.get("requested_end_date"),
    })


@app.get(
    "/api/archive/search/{uuid}/results",
    tags=["archive"],
    response_model=ArchiveSearchResultsPage,
    responses={**_NOT_FOUND, **_VALIDATION_ERROR, **_REDIS_ERROR, **_AWS_ERROR},
)
def get_archive_search_results(
    uuid: str,
    page: int = FastAPIQuery(default=1, ge=1),
    page_size: int = FastAPIQuery(default=_PAGE_SIZE, ge=_PAGE_SIZE_MIN, le=_PAGE_SIZE_MAX),
    sort_by: Optional[Literal[_SORTABLE_COLUMNS]] = FastAPIQuery(default=None),
    sort_dir: Literal["asc", "desc"] = FastAPIQuery(default="asc"),
):
    record = _get_search_record(uuid)
    if record["status"] != "COMPLETE":
        raise HTTPException(
            status_code=400,
            detail=f"Search '{uuid}' is not complete (status: {record['status']})",
        )
    if record.get("query_execution_id") is None:
        # Empty-intersection short-circuit from create_archive_search --
        # no Athena query was ever run because the resolved date range and
        # where_clause's own derived range don't overlap, so there is
        # nothing to fetch or paginate.
        return JSONResponse(content={"rows": [], "total_rows": 0, "truncated": False})
    rows, truncated = _fetch_and_cache_results(uuid, record["query_execution_id"])
    # Sort the whole cached result set (not just the requested page) so this
    # behaves like a real column sort -- _fetch_and_cache_results already
    # has every row in memory (up to the cache cap), so no new Athena call is
    # needed. sorted() returns a new list, leaving the cached one untouched
    # for other pages/sort orders to reuse.
    if sort_by is not None:
        rows = sorted(rows, key=lambda row: row[sort_by], reverse=sort_dir == "desc")
    total_pages = max(1, -(-len(rows) // page_size))  # ceil division
    if page > total_pages:
        raise HTTPException(
            status_code=400,
            detail=f"Page {page} exceeds the cached results ({total_pages} page(s) available)",
        )
    start = (page - 1) * page_size
    return JSONResponse(content={
        "rows": rows[start:start + page_size],
        "total_rows": len(rows),
        "truncated": truncated,
    })


_FILENAME_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _download_filename(name: str, uuid: str) -> str:
    """Filesystem-safe stand-in for a search's display name, used as the
    browser-facing download filename (see download_archive_search) --
    falls back to the uuid alone if the name is empty after stripping
    (e.g. all-punctuation)."""
    slug = _FILENAME_SLUG_RE.sub("-", name.strip().lower()).strip("-")
    return f"{slug or 'archive-search'}-{uuid}.csv"


def _search_query_range(record: dict) -> tuple[date, date]:
    """(start, end) to build a partition predicate from, for a search
    record that may predate start_date/end_date being persisted (see
    ArchiveSearchDetail's own Optional fields) -- falls back to the full
    archive range in that case, exactly like create_archive_search's own
    default for an omitted explicit bound."""
    today = datetime.now(timezone.utc).date()
    start = date.fromisoformat(record["start_date"]) if record.get("start_date") else _ARCHIVE_EPOCH
    end = date.fromisoformat(record["end_date"]) if record.get("end_date") else today + timedelta(days=1)
    return start, end


def _poll_until_terminal(query_execution_id: str) -> tuple[str, str]:
    """Synchronous variant of _poll_search_execution's loop, for a request
    handler that needs the answer inline rather than via a background
    thread + Redis write. Checks immediately before ever sleeping (unlike
    the background poller, a query already finished by the time this is
    called must not pay an up-front sleep first). Returns (state, reason);
    reason is only ever non-empty for FAILED/CANCELLED."""
    deadline = time.monotonic() + ATHENA_POLL_DEADLINE_SECONDS
    attempt = 0
    while True:
        resp = _athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return state, resp["QueryExecution"]["Status"].get("StateChangeReason", "")
        if time.monotonic() >= deadline:
            return state, "Deadline exceeded"
        delay = ATHENA_POLL_BACKOFF_SECONDS[min(attempt, len(ATHENA_POLL_BACKOFF_SECONDS) - 1)]
        attempt += 1
        time.sleep(min(delay, 30))


def _run_or_get_download_query_execution(uuid: str, record: dict) -> str:
    """Returns the S3 output location of the download query's results,
    running (and persisting) that query at most once per search -- a
    second call for the same uuid, whether from this same request or a
    later one, reuses download_output_location straight off the record
    with no new Athena call at all. Reuses the SAME partition predicate and
    where_clause the original search resolved (see _search_query_range),
    submitted fresh here rather than reusing query_execution_id: the
    download query's SELECT list differs (no s3_key -- see
    _build_download_query)."""
    output_location = record.get("download_output_location")
    if output_location:
        return output_location

    query_execution_id = record.get("download_query_execution_id")
    if query_execution_id is None:
        start, end = _search_query_range(record)
        query = _build_download_query(_partition_predicate(start, end), record["where_clause"])
        try:
            resp = _athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={"Database": _athena_cfg["database"]},
                WorkGroup=_athena_cfg["workgroup"],
            )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Failed to start download query: {exc}") from exc
        query_execution_id = resp["QueryExecutionId"]
        _update_search_record(uuid, download_query_execution_id=query_execution_id)

    state, reason = _poll_until_terminal(query_execution_id)
    if state == "SUCCEEDED":
        output_location = _result_output_location(query_execution_id)
        _update_search_record(uuid, download_output_location=output_location)
        return output_location
    if state in ("FAILED", "CANCELLED"):
        raise HTTPException(status_code=502, detail=f"Download query failed: {reason}")
    raise HTTPException(status_code=504, detail="Download query did not complete in time")


@app.get(
    "/api/archive/search/{uuid}/download",
    tags=["archive"],
    responses={
        307: {"description": "Redirect to a short-lived presigned S3 URL for the full result CSV"},
        **_NOT_FOUND, **_VALIDATION_ERROR, **_REDIS_ERROR, **_AWS_ERROR,
    },
)
def download_archive_search(uuid: str):
    """Always S3-direct, for every result size -- no branch on how many
    rows matched. The backend never reads the result bytes; it only submits
    a sanitized, s3_key-free query (see _build_download_query) and hands
    the browser a presigned URL to Athena's own S3 output for it. Because
    this reruns the search rather than reusing its original query
    execution, a flight archived after the search was submitted can appear
    in the download that wasn't in the paged view -- only affects the most
    recent days; see management-ui/README.md."""
    record = _get_search_record(uuid)
    if record["status"] != "COMPLETE":
        raise HTTPException(
            status_code=400,
            detail=f"Search '{uuid}' is not complete (status: {record['status']})",
        )

    output_location = _run_or_get_download_query_execution(uuid, record)
    bucket, key = _parse_s3_uri(output_location)
    try:
        presigned_url = _s3_client.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": bucket,
                "Key": key,
                # Overrides the response headers S3 actually serves for this
                # one presigned GET -- gives the browser a friendly filename
                # and a real download prompt, without the backend ever
                # touching the object's bytes.
                "ResponseContentDisposition": f'attachment; filename="{_download_filename(record["name"], uuid)}"',
                "ResponseContentType": "text/csv",
            },
            ExpiresIn=_DOWNLOAD_PRESIGN_TTL_SECONDS,
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to presign download: {exc}") from exc
    return RedirectResponse(url=presigned_url, status_code=307)


@app.delete(
    "/api/archive/search/{uuid}",
    tags=["archive"],
    status_code=204,
    responses={**_NOT_FOUND, **_REDIS_ERROR},
)
def delete_archive_search(uuid: str):
    record = _get_search_record(uuid)

    if record["status"] == "RUNNING":
        # Fire, don't wait -- the delete proceeds regardless of whether
        # Athena's cancellation has actually taken effect yet.
        try:
            _athena_client.stop_query_execution(QueryExecutionId=record["query_execution_id"])
        except Exception as exc:
            logger.warning("Best-effort stop_query_execution failed for search %s: %s", uuid, exc)

    if record["status"] == "COMPLETE" and record.get("query_execution_id") is not None:
        # query_execution_id is None for an empty-intersection short-circuit
        # (see create_archive_search) -- no query ever ran, so there is no
        # result file in S3 to clean up. Delete the risky/expensive side
        # (the S3 result file) before the Redis pointer to it otherwise,
        # matching archive-compaction's own write-then-delete ordering
        # principle.
        try:
            output_location = _result_output_location(record["query_execution_id"])
            bucket, key = _parse_s3_uri(output_location)
            _s3_client.delete_object(Bucket=bucket, Key=key)
        except Exception as exc:
            logger.warning("Failed to delete Athena result file for search %s: %s", uuid, exc)

    try:
        _redis.delete(archive_search_key(uuid))
        _redis.srem(archive_search_index_key(), uuid)
    except redis_lib.RedisError as exc:
        raise HTTPException(status_code=500, detail=f"Redis error: {exc}") from exc
    _result_cache.discard(uuid)

    return Response(status_code=204)


@app.get(
    "/api/archive/flights/{token}",
    tags=["archive"],
    responses={**_VALIDATION_ERROR, **_AWS_ERROR},
)
def get_archive_flight(token: str):
    s3_key = _decrypt_token(token)
    try:
        obj = _s3_client.get_object(Bucket=_s3_bucket, Key=s3_key)
        body = obj["Body"].read()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch flight: {exc}") from exc

    filename = s3_key.rsplit("/", 1)[-1]
    return Response(
        content=body,
        media_type="application/gzip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _fetch_flight_record(token: str) -> dict:
    """Decrypt `token` to an S3 key, fetch and gunzip the flight object, and
    return the parsed JSON dict (the CompletedFlight shape). Raises
    HTTPException(502) on any S3/decompress/parse failure."""
    s3_key = _decrypt_token(token)
    try:
        obj = _s3_client.get_object(Bucket=_s3_bucket, Key=s3_key)
        body = obj["Body"].read()
        return json.loads(gzip.decompress(body))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch flight: {exc}") from exc


def _resolve_airport(icao_code: Optional[str]) -> Optional[dict]:
    """Best-effort ICAO code -> airport doc lookup for display purposes.
    Returns None (never raises) if the code is absent or Redis has no
    matching record -- the caller omits the field entirely rather than
    showing a partially-resolved airport."""
    if not icao_code:
        return None
    return _redis_json_get(airport_key(icao_code.strip().upper()))


class FlightView(BaseModel):
    """
    Response shape for GET /api/archive/flights/{token}/view -- everything
    the History "View" modal needs, parsed and enriched server-side from the
    raw S3 flight object. Every field is optional except icao_hex/timestamps/
    total_messages, which every archived flight always carries; a field with
    nothing to show is simply absent from the JSON rather than null, so the
    frontend's "omit if absent" rendering rule works the same way it already
    does for the rest of the app's reference-data lookups.
    """

    ident: Optional[str] = None
    registration: Optional[str] = None
    icao_hex: str
    squawk: Optional[str] = None
    military: Optional[bool] = None
    type_designator: Optional[str] = None
    manufacturer_model: Optional[str] = None
    category: Optional[str] = None
    aircraft_type: Optional[str] = None   # merge_aircraft.lua's "type" (e.g. "Airplane")
    model: Optional[str] = None           # national-registry-specific designation, distinct from manufacturer_model
    serial_number: Optional[str] = None
    seats: Optional[int] = None
    powerplant: Optional[dict] = None     # count/type/manufacturer/model/power_type
    operator: Optional[dict] = None      # OperatorRecord fields (name, callsign, ...)
    registrant: Optional[dict] = None    # RegistrantInfo fields
    origin: Optional[dict] = None        # airport doc (icao_code, name, ...), resolved via Redis
    destination: Optional[dict] = None   # airport doc, resolved via Redis
    first_message: datetime
    last_message: datetime
    total_messages: int
    matched_rules: list[str] = []
    flight_path: Optional[dict] = None   # GeoJSON LineString Feature, or None for <2 positions


@app.get(
    "/api/archive/flights/{token}/view",
    tags=["archive"],
    response_model=FlightView,
    responses={**_VALIDATION_ERROR, **_AWS_ERROR},
)
def get_archive_flight_view(token: str):
    flight = _fetch_flight_record(token)
    # merge_aircraft.lua's output nests type/category/manufacturer/powerplant
    # under aircraft.aircraft (see _flatten_aircraft_doc) -- a copy so this
    # doesn't mutate the parsed flight record in place.
    aircraft = _flatten_aircraft_doc(dict(flight.get("aircraft") or {}))

    return FlightView(
        ident=flight.get("ident"),
        registration=aircraft.get("registration"),
        icao_hex=aircraft["icao_hex"],
        squawk=flight.get("squawk"),
        military=aircraft.get("military"),
        type_designator=aircraft.get("type_designator"),
        manufacturer_model=aircraft.get("manufacturer_model"),
        category=aircraft.get("category"),
        aircraft_type=aircraft.get("type"),
        model=aircraft.get("model"),
        serial_number=aircraft.get("serial_number"),
        seats=aircraft.get("seats"),
        powerplant=aircraft.get("powerplant"),
        operator=flight.get("operator"),
        registrant=aircraft.get("registrant"),
        origin=_resolve_airport(flight.get("origin")),
        destination=_resolve_airport(flight.get("destination")),
        first_message=flight["first_message"],
        last_message=flight["last_message"],
        total_messages=flight["total_messages"],
        matched_rules=flight.get("matched_rules", []),
        flight_path=build_flight_path(flight.get("positions", [])),
    )


@app.get(
    "/api/archive/flights/{token}/flight-path",
    tags=["archive"],
    responses={**_VALIDATION_ERROR, **_NOT_FOUND, **_AWS_ERROR},
)
def get_archive_flight_path(token: str):
    s3_key = _decrypt_token(token)
    flight = _fetch_flight_record(token)
    feature = build_flight_path(flight.get("positions", []))
    if feature is None:
        raise HTTPException(status_code=404, detail="Flight has fewer than 2 positions; no path to export")

    filename = f"{_uuid_from_s3_key(s3_key)}.geojson"
    return Response(
        content=json.dumps(feature),
        media_type="application/geo+json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
