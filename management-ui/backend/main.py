#!/usr/bin/env python3
"""
SkyFollower Management UI Backend

FastAPI service that is the sole write path for the rules and areas
configuration read by every message processor (config:rules / config:areas
in Redis, polled every 30 seconds). No authentication — home lab deployment.

Named "management" to leave room for a future, separate UI for viewing live
aircraft movement, distinct from this configuration-focused one.

Runs standalone on port 8000 for now. Once the React frontend (#15, #16)
exists, the Dockerfile will grow a node build stage and nginx will proxy
/api/* to this process and serve the built frontend at /.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import Literal, Optional, Union

import redis as redis_lib
from fastapi import FastAPI, HTTPException, Response
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Add the repo root to sys.path so shared/ is importable when this module is
# run outside Docker (e.g. tests, local `uvicorn main:app`, OpenAPI export).
# In the Docker image PYTHONPATH=/app already covers this and _REPO_ROOT
# below resolves to "/", which is simply never used.
_HERE = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_HERE))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from shared.logging_setup import configure_logging  # noqa: E402
from shared.redis_keys import (  # noqa: E402
    config_areas_key,
    config_areas_version_key,
    config_rules_key,
    config_rules_version_key,
)

try:
    from message_processor.rules_engine import RulesEngine
except ModuleNotFoundError:
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
# Schema models -- documentation only, matching SkyFollower-legacy's
# rules.example.json / areas.example.geojson shape (condition values are
# strings even for numeric fields, e.g. altitude "10000", military "true";
# only matched_rules is a real array). Not used as the actual route
# parameter types: the routes below keep plain dict/list[dict] so
# RulesEngine (message-processor/rules_engine.py) stays the single source
# of truth for validation -- these models exist so /docs and
# specs/openapi.yaml describe the real shape instead of an empty
# "additionalProperties: true" object, without a second, stricter
# validation layer fighting the engine's own (more permissive) rules, e.g.
# a disabled placeholder rule like {"enabled": false} with no identifier or
# conditions at all is valid and simply skipped, not rejected.
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
}


class Condition(BaseModel):
    """
    One rule condition; every condition in a rule is AND'd together.
    `value`'s shape depends on `type` -- see CLAUDE.md's Conditions table.
    Matching SkyFollower-legacy's convention, this is a string even for
    numeric fields (altitude "10000", heading "340,020" for min,max
    wrap-around, military "true"/"false") -- matched_rules is the one
    exception, taking a real list of rule identifiers. A `date` value with
    a `T` is a datetime and must carry a timezone designator -- `Z` (UTC)
    or any ISO 8601 offset, e.g. `2026-01-15T23:31:00-05:00` -- stored and
    echoed back exactly as submitted, not normalised to `Z`.
    """

    type: Literal[
        "altitude", "heading", "velocity", "vertical_speed", "area", "date",
        "ident", "squawk", "military", "operator_airline_designator",
        "aircraft_type_designator", "aircraft_registration", "aircraft_icao_hex",
        "aircraft_powerplant_count", "wake_turbulence_category", "matched_rules",
    ]
    operator: Literal["equals", "minimum", "maximum", "in_list", "not_in_list"]
    value: Union[str, list[str]]


class Rule(BaseModel):
    """
    A notification rule. Fires at most once per flight per `identifier`.
    `identifier` is the routing key used in /api/rules/{identifier} and must
    not contain spaces; `name` is a free-text display label and may.
    Documents the shape of a normal (enabled) rule -- the rules engine
    additionally tolerates a disabled placeholder rule with only
    `enabled: false` and nothing else, silently skipping it rather than
    validating it; that leniency is a narrow exception, not reflected here.
    """

    model_config = {"json_schema_extra": {"examples": list(_RULE_EXAMPLES.values())}}

    name: str = ""
    description: str = ""
    identifier: str = Field(pattern=_IDENTIFIER_PATTERN)
    enabled: bool
    force_archive: bool = False
    conditions: list[Condition] = Field(min_length=1)


class AreaGeometry(BaseModel):
    type: Literal["Polygon"]
    coordinates: list[list[list[float]]]


class Area(BaseModel):
    """
    A named GeoJSON polygon area, referenced by rules' `area` condition
    (matched against `identifier`, not `name`). `identifier` is the routing
    key used in /api/areas/{identifier} and must not contain spaces; `name`
    is a free-text display label and may.
    """

    model_config = {"json_schema_extra": {"examples": list(_AREA_EXAMPLES.values())}}

    identifier: str = Field(pattern=_IDENTIFIER_PATTERN)
    name: str = ""
    geometry: AreaGeometry


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


def _load_config() -> dict:
    path = os.environ.get("SETTINGS_PATH", "/app/settings.json")
    with open(path) as f:
        return json.load(f)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _redis, _engine
    config = _load_config()
    configure_logging(config.get("log_level"))

    redis_config = config.get("redis", {})
    _redis = redis_lib.Redis(
        host=redis_config.get("host", "localhost"),
        port=redis_config.get("port", 6379),
        decode_responses=True,
    )
    _engine = RulesEngine(_redis)
    _engine.reload_if_changed()

    logger.info("Management UI backend started.")
    yield
    logger.info("Management UI backend shutting down.")


app = FastAPI(
    title="SkyFollower Management",
    description="Rules and areas configuration API. Message processors poll "
    "config:rules/config:areas in Redis every 30 seconds for changes written here.",
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

    FastAPI infers a request body schema from the route's actual parameter
    type (plain dict here, kept that way so RulesEngine -- not Pydantic --
    stays the one place validation happens; see the Schema models comment
    above). route(openapi_extra=...) can't clean this up: FastAPI merges it
    into the auto-generated operation via a recursive dict merge
    (fastapi.openapi.utils.deep_dict_update), not a replace, which leaves
    stale "additionalProperties: true" sitting alongside the injected $ref.
    Overwriting the generated schema's dict keys directly, after the fact,
    is a plain assignment instead, so it actually replaces.
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


def _redis_set(key: str, value: str) -> None:
    try:
        _redis.set(key, value)
    except redis_lib.RedisError as exc:
        raise HTTPException(status_code=500, detail=f"Redis error: {exc}") from exc


_NOT_FOUND = {404: {"description": "Not found", "model": ErrorDetail}}
_CONFLICT = {409: {"description": "Identifier already exists", "model": ErrorDetail}}
_REDIS_ERROR = {500: {"description": "Redis error", "model": ErrorDetail}}
_VALIDATION_ERROR = {400: {"description": "Validation error", "model": ErrorDetail}}


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
def create_rule(rule: dict):
    rules = _load_rules_array()
    identifier = rule.get("identifier")
    if any(r.get("identifier") == identifier for r in rules):
        raise HTTPException(status_code=409, detail=f"Rule '{identifier}' already exists")

    rules.append(rule)
    _save_rules_array(rules)
    return JSONResponse(status_code=201, content=rule)


@app.put(
    "/api/rules/{identifier}",
    tags=["rules"],
    response_model=Rule,
    responses={**_NOT_FOUND, **_VALIDATION_ERROR, **_REDIS_ERROR},
)
def update_rule(identifier: str, rule: dict):
    rules = _load_rules_array()
    idx = next((i for i, r in enumerate(rules) if r.get("identifier") == identifier), None)
    if idx is None:
        raise HTTPException(status_code=404, detail=f"Rule '{identifier}' not found")

    body_identifier = rule.get("identifier", identifier)
    if body_identifier != identifier:
        raise HTTPException(
            status_code=400,
            detail=f"Body identifier '{body_identifier}' does not match path identifier '{identifier}'",
        )

    rules[idx] = {**rule, "identifier": identifier}
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

def _feature_to_area(feature: dict) -> dict:
    props = feature.get("properties", {})
    return {
        "identifier": props.get("identifier", ""),
        "name": props.get("name", ""),
        "geometry": feature.get("geometry", {}),
    }


def _area_to_feature(area: dict) -> dict:
    return {
        "type": "Feature",
        "properties": {
            "identifier": area.get("identifier", ""),
            "name": area.get("name", ""),
        },
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
    # guarantee the item this call cares about actually survived it.
    if expect_identifier is not None:
        if not any(a["identifier"] == expect_identifier for a in _engine._areas):
            raise HTTPException(
                status_code=400,
                detail=f"Area '{expect_identifier}' failed validation "
                "(check identifier has no spaces and geometry is a valid Polygon)",
            )

    version = hashlib.sha256(body.encode()).hexdigest()
    _redis_set(config_areas_key(), body)
    _redis_set(config_areas_version_key(), version)


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
def create_area(area: dict):
    areas = _load_areas_array()
    identifier = area.get("identifier")
    if any(a.get("identifier") == identifier for a in areas):
        raise HTTPException(status_code=409, detail=f"Area '{identifier}' already exists")

    areas.append(area)
    _save_areas_array(areas, expect_identifier=identifier)
    return JSONResponse(status_code=201, content=area)


@app.put(
    "/api/areas/{identifier}",
    tags=["areas"],
    response_model=Area,
    responses={**_NOT_FOUND, **_VALIDATION_ERROR, **_REDIS_ERROR},
)
def update_area(identifier: str, area: dict):
    areas = _load_areas_array()
    idx = next((i for i, a in enumerate(areas) if a.get("identifier") == identifier), None)
    if idx is None:
        raise HTTPException(status_code=404, detail=f"Area '{identifier}' not found")

    body_identifier = area.get("identifier", identifier)
    if body_identifier != identifier:
        raise HTTPException(
            status_code=400,
            detail=f"Body identifier '{body_identifier}' does not match path identifier '{identifier}'",
        )

    areas[idx] = {**area, "identifier": identifier}
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
