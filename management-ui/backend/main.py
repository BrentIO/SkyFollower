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

import csv
import hashlib
import io
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
from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal, Optional, Union

import boto3
import redis as redis_lib
from cryptography.fernet import Fernet, InvalidToken
from fastapi import FastAPI, HTTPException, Response
from fastapi import Query as FastAPIQuery
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from redis.commands.search.field import TagField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query as RedisSearchQuery
from uuid_extensions import uuid7

# Add the repo root to sys.path so shared/ is importable when this module is
# run outside Docker (e.g. tests, local `uvicorn main:app`, OpenAPI export).
# In the Docker image PYTHONPATH=/app already covers this and _REPO_ROOT
# below resolves to "/", which is simply never used.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_HERE))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from shared.aws_setup import write_aws_setup_files  # noqa: E402
from shared.config import load_config  # noqa: E402
from shared.redis_client import build_redis_client  # noqa: E402
from shared.logging_setup import configure_logging  # noqa: E402
from shared.models import AircraftRecord, AirportRecord, OperatorRecord  # noqa: E402
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
    operator_key,
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
        "be skipped for being MLAT-only",
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
        "via 1090MHz ADS-B, not merely via MLAT -- it often suppresses its position and is "
        "then only visible via MLAT triangulation, which doesn't imply it's actually nearby. "
        "If it isn't showing up on 1090, skip the force-archive and let the normal MLAT-only "
        "archive skip apply.",
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
    # 1-2 of "1090"/"978"/"MLAT", no duplicates -- all 3 would be equivalent
    # to no filter at all (every flight has at least one), so RulesEngine
    # rejects that as dead weight rather than a real filter.
    value: list[Literal["1090", "978", "MLAT"]] = Field(min_length=1, max_length=2)

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

    - Redis has `key`, backup file exists: nothing to do.
    - Redis has `key`, backup file is missing -- an existing deployment
      upgrading to this feature has real data in Redis but has never
      written a backup file (only _save_rules_array/_save_areas_array do
      that, on save): seed the file from Redis's current value so it
      doesn't stay empty until the next edit. Never overwrites a backup
      file that already exists.
    - Redis is missing `key`, backup file exists: restore Redis from the
      file (and its `:version` hash, so RulesEngine's poll-based reload
      picks it up) -- a lost/corrupted Redis volume, or a fresh one.
    - Both missing: nothing to do -- same empty-array behavior as today.
    """
    existing = _redis.get(key)
    if existing is not None:
        if not os.path.exists(backup_path):
            _write_backup_file(backup_path, existing)
            logger.info("Seeded %s backup file %s from existing Redis data.", label, backup_path)
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
    _redis.set(key, body)
    _redis.set(version_key, version)
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

# Same dual-path story as _LUA_DIR above -- the Docker image copies
# specs/aws/ flat alongside main.py (see management-ui/Dockerfile).
_AWS_DIR = pathlib.Path(_HERE) / "specs" / "aws"
if not _AWS_DIR.is_dir():
    _AWS_DIR = pathlib.Path(_REPO_ROOT) / "specs" / "aws"

_AWS_SETUP_TEMPLATES = {
    str(_AWS_DIR / "iam-policies" / "management-ui.json"): "iam-policy.json",
}


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

    write_aws_setup_files(_data_dir(), _s3_bucket, _AWS_SETUP_TEMPLATES)
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
    without one.
    """

    ident: str
    origin: AirportRecord
    destination: AirportRecord
    stops: list[AirportRecord]
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


def _save_rules_array(rules: list[dict]) -> None:
    body = json.dumps(rules)
    if not _engine.load_rules_json(body):
        raise HTTPException(status_code=400, detail=_engine.last_error or "Invalid rules")

    version = hashlib.sha256(body.encode()).hexdigest()
    _redis_set(config_rules_key(), body)
    _redis_set(config_rules_version_key(), version)
    _write_backup_file(_rules_backup_path(), body)


@app.get("/api/rules", tags=["rules"], response_model=list[Rule], responses={**_REDIS_ERROR})
def list_rules():
    return JSONResponse(content=_load_rules_array())


@app.get(
    "/api/rules/{identifier}",
    tags=["rules"],
    response_model=Rule,
    responses={**_NOT_FOUND, **_REDIS_ERROR},
)
def get_rule(identifier: str):
    for rule in _load_rules_array():
        if rule.get("identifier") == identifier:
            return JSONResponse(content=rule)
    raise HTTPException(status_code=404, detail=f"Rule '{identifier}' not found")


@app.post(
    "/api/rules",
    tags=["rules"],
    status_code=201,
    response_model=Rule,
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
    return JSONResponse(status_code=201, content=rule_dict)


@app.put(
    "/api/rules/{identifier}",
    tags=["rules"],
    response_model=Rule,
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
    return JSONResponse(content=rules[idx])


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
    _redis_set(config_areas_key(), body)
    _redis_set(config_areas_version_key(), version)
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
    responses={**_NOT_FOUND, **_VALIDATION_ERROR, **_REDIS_ERROR},
)
def delete_area(identifier: str):
    areas = _load_areas_array()
    remaining = [a for a in areas if a.get("identifier") != identifier]
    if len(remaining) == len(areas):
        raise HTTPException(status_code=404, detail=f"Area '{identifier}' not found")

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


@app.get(
    "/api/routes/{ident}",
    tags=["reference-data"],
    response_model=RouteLookup,
    responses={**_NOT_FOUND, **_REDIS_ERROR},
)
def get_route(ident: str):
    raw = _redis_evalsha(_route_airports_sha, ident.upper())
    airports = json.loads(raw) if raw else []
    if not airports:
        raise HTTPException(status_code=404, detail=f"No route data found for '{ident}'")

    # Best-effort operator enrichment -- same ICAO airline-designator
    # extraction message-processor's _enrich_operator uses (letters before
    # the first digit). A too-short/missing prefix or no matching
    # operator:{designator} record just omits `operator`; it never fails
    # the route lookup itself.
    operator = None
    prefix = re.split(r"[^a-zA-Z]", ident)[0]
    if len(prefix) >= 2:
        operator = _redis_json_get(operator_key(prefix))

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
# Redis for a fixed 7 days from creation (never refreshed on access);
# result rows themselves are never cached in Redis, only a pointer to
# where Athena wrote them in S3 -- see _fetch_and_cache_results below.
# ---------------------------------------------------------------------------

_SEARCH_TTL_SECONDS = 7 * 86400
_PAGE_SIZE = 100
_POLL_BACKOFF_SECONDS = [1, 2, 4, 8, 16]
_POLL_DEADLINE_SECONDS = 120
_RESULT_CACHE_MAX_ENTRIES = 10

# Column order here is exactly what the Athena SELECT below returns, so
# _fetch_and_cache_results can map each CSV row positionally without
# needing to consult the header row Athena also writes. s3_key IS selected
# (needed server-side to mint each row's fetch token and derive its flight
# UUID -- see _row_from_csv_fields) but is never included in the dict a
# response actually returns to the browser.
_SEARCH_SELECT_COLUMNS = [
    "icao_hex", "registration", "type_designator", "military",
    "operator_designator", "ident", "first_message", "last_message", "s3_key",
]

# Cheap early rejection before ever calling Athena -- not a real security
# boundary (the querying IAM identity is already read-only on just this one
# table), purely so a mistake produces an instant, clear 400 instead of a
# slower, more opaque Athena AccessDenied. Word-boundary so a legitimate
# value that happens to contain one of these words (e.g. ident = 'INSERT1')
# doesn't false-positive.
_FORBIDDEN_WHERE_CLAUSE_RE = re.compile(
    r"\b(DROP|CREATE|ALTER|INSERT|DELETE|UPDATE|GRANT)\b", re.IGNORECASE
)

_AWS_ERROR = {502: {"description": "AWS (Athena/S3) error", "model": ErrorDetail}}


class ArchiveSearchCreate(BaseModel):
    name: str = Field(..., min_length=1)
    where_clause: str = Field(..., min_length=1)


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


class ArchiveSearchResultsPage(BaseModel):
    rows: list[ArchiveSearchResultRow]
    # The full match count, not just len(rows) -- rows is only this page's
    # slice. The backend already has this for free: _fetch_and_cache_results
    # downloads and fully parses the CSV before any pagination slicing.
    total_rows: int


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


def _build_search_query(where_clause: str) -> str:
    """The SELECT list and FROM table are always backend-controlled, never
    influenced by user input -- where_clause only ever fills the WHERE
    fragment, parenthesized so it can't prematurely close the clause and
    inject a sibling SQL construct."""
    columns = ", ".join(_SEARCH_SELECT_COLUMNS)
    table = f'{_athena_cfg["database"]}.{_athena_cfg["table"]}'
    return f"SELECT {columns} FROM {table} WHERE ({where_clause})"


def _expires_at(submitted_at_iso: str) -> str:
    submitted = datetime.fromisoformat(submitted_at_iso)
    return (submitted + timedelta(seconds=_SEARCH_TTL_SECONDS)).isoformat()


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
    deadline = time.monotonic() + _POLL_DEADLINE_SECONDS
    attempt = 0
    while time.monotonic() < deadline:
        delay = (
            _POLL_BACKOFF_SECONDS[attempt]
            if attempt < len(_POLL_BACKOFF_SECONDS)
            else _POLL_BACKOFF_SECONDS[-1] * 2  # 30s cap, per design
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
        self._data: OrderedDict[str, list[dict]] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[list[dict]]:
        with self._lock:
            if key not in self._data:
                return None
            self._data.move_to_end(key)
            return self._data[key]

    def put(self, key: str, value: list[dict]) -> None:
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


def _uuid_from_s3_key(s3_key: str) -> str:
    """flights/{YYYY}/{MM}/{DD}/{icao_hex}_{ident}_{uuid}.json.gz -- icao_hex
    and ident never contain underscores (icao_hex is hex digits; ident is
    sanitized to alnum-only by archive-processor's build_s3_key), so the
    final underscore-separated segment is always the uuid."""
    filename = s3_key.rsplit("/", 1)[-1]
    stem = filename.removesuffix(".json.gz")
    return stem.rsplit("_", 1)[-1]


def _encrypt_s3_key(s3_key: str) -> str:
    return _fernet.encrypt(s3_key.encode()).decode()


def _decrypt_token(token: str) -> str:
    try:
        return _fernet.decrypt(token.encode()).decode()
    except InvalidToken as exc:
        raise HTTPException(status_code=400, detail="Invalid or expired flight token") from exc


def _fetch_and_cache_results(uuid: str, query_execution_id: str) -> list[dict]:
    cached = _result_cache.get(uuid)
    if cached is not None:
        return cached

    try:
        output_location = _result_output_location(query_execution_id)
        bucket, key = _parse_s3_uri(output_location)
        obj = _s3_client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch search results: {exc}") from exc

    reader = csv.reader(io.StringIO(body))
    data_rows = list(reader)[1:]  # skip Athena's own header row
    rows = [_row_from_csv_fields(fields) for fields in data_rows]
    _result_cache.put(uuid, rows)
    return rows


def _row_from_csv_fields(fields: list[str]) -> dict:
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

    query = _build_search_query(where_clause)
    try:
        resp = _athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": _athena_cfg["database"]},
            WorkGroup=_athena_cfg["workgroup"],
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to start Athena query: {exc}") from exc
    query_execution_id = resp["QueryExecutionId"]

    search_uuid = str(uuid7())
    submitted_at = datetime.now(timezone.utc).isoformat()
    record = {
        "name": body.name,
        "where_clause": where_clause,
        "status": "RUNNING",
        "submitted_at": submitted_at,
        "query_execution_id": query_execution_id,
    }
    _redis_set(archive_search_key(search_uuid), json.dumps(record), ex=_SEARCH_TTL_SECONDS)
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
    return JSONResponse(content={**_search_summary(uuid, record), "where_clause": record["where_clause"]})


@app.get(
    "/api/archive/search/{uuid}/results",
    tags=["archive"],
    response_model=ArchiveSearchResultsPage,
    responses={**_NOT_FOUND, **_VALIDATION_ERROR, **_REDIS_ERROR, **_AWS_ERROR},
)
def get_archive_search_results(uuid: str, page: int = FastAPIQuery(default=1, ge=1)):
    record = _get_search_record(uuid)
    if record["status"] != "COMPLETE":
        raise HTTPException(
            status_code=400,
            detail=f"Search '{uuid}' is not complete (status: {record['status']})",
        )
    rows = _fetch_and_cache_results(uuid, record["query_execution_id"])
    start = (page - 1) * _PAGE_SIZE
    return JSONResponse(content={"rows": rows[start:start + _PAGE_SIZE], "total_rows": len(rows)})


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

    if record["status"] == "COMPLETE":
        # Delete the risky/expensive side (the S3 result file) before the
        # Redis pointer to it, matching archive-compaction's own
        # write-then-delete ordering principle.
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
