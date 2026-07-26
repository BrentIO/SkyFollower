#!/usr/bin/env python3
"""
SkyFollower Management UI Backend

FastAPI service that is the sole write path for the rules and areas
configuration read by every message processor (config:rules / config:areas
in Redis, polled every 5 seconds). No authentication — home lab deployment.

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
from typing import Optional

import redis as redis_lib
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import JSONResponse

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
    title="SkyFollower Management UI Backend",
    description="Rules and areas configuration API. Message processors poll "
    "config:rules/config:areas in Redis every 5 seconds for changes written here.",
    version="9999.99.99",
    lifespan=lifespan,
)


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


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

_NO_CONTENT = {204: {"description": "No configuration saved yet"}}
_REDIS_ERROR = {500: {"description": "Redis error"}}
_VALIDATION_ERROR = {400: {"description": "Validation error"}}


@app.get(
    "/api/rules",
    tags=["rules"],
    responses={**_NO_CONTENT, **_REDIS_ERROR},
)
def get_rules():
    raw = _redis_get(config_rules_key())
    if not raw:
        return Response(status_code=204)
    return JSONResponse(content=json.loads(raw))


@app.put(
    "/api/rules",
    tags=["rules"],
    responses={**_VALIDATION_ERROR, **_REDIS_ERROR},
)
def put_rules(rules: list[dict]):
    body = json.dumps(rules)
    if not _engine.load_rules_json(body):
        raise HTTPException(status_code=400, detail=_engine.last_error or "Invalid rules")

    version = hashlib.sha256(body.encode()).hexdigest()
    _redis_set(config_rules_key(), body)
    _redis_set(config_rules_version_key(), version)
    return JSONResponse(content=rules)


# ---------------------------------------------------------------------------
# Areas
# ---------------------------------------------------------------------------

@app.get(
    "/api/areas",
    tags=["areas"],
    responses={**_NO_CONTENT, **_REDIS_ERROR},
)
def get_areas():
    raw = _redis_get(config_areas_key())
    if not raw:
        return Response(status_code=204)
    return JSONResponse(content=json.loads(raw))


@app.put(
    "/api/areas",
    tags=["areas"],
    responses={**_VALIDATION_ERROR, **_REDIS_ERROR},
)
def put_areas(areas: dict):
    body = json.dumps(areas)
    if not _engine.load_areas_json(body):
        raise HTTPException(status_code=400, detail=_engine.last_error or "Invalid areas")

    version = hashlib.sha256(body.encode()).hexdigest()
    _redis_set(config_areas_key(), body)
    _redis_set(config_areas_version_key(), version)
    return JSONResponse(content=areas)
