"""
Shared Redis client construction for SkyFollower.

Every component that talks to Redis builds its redis.Redis through
build_redis_client() so the required password is applied consistently
instead of being reimplemented at each call site.
"""

from __future__ import annotations

import logging

import redis as redis_lib

logger = logging.getLogger(__name__)


def build_redis_client(redis_config: dict) -> redis_lib.Redis:
    """
    Build a redis.Redis from a component's `redis` config block.

    `password` is passed unconditionally since shared/config.py's
    redis_config() treats REDIS_PASSWORD as required. Every component
    authenticates as the "default" user -- no per-component ACL users.
    """
    return redis_lib.Redis(
        host=redis_config["host"],
        port=redis_config.get("port", 6379),
        password=redis_config.get("password"),
        decode_responses=True,
    )


def evalsha_with_reload(client: redis_lib.Redis, sha: str, script: str, numkeys: int, *args):
    """EVALSHA that reloads the script and retries once on NoScriptError,
    since a Redis restart flushes the script cache."""
    try:
        return client.evalsha(sha, numkeys, *args)
    except redis_lib.exceptions.NoScriptError:
        logger.warning("Lua script %s missing from Redis script cache; reloading", sha)
    try:
        return client.evalsha(client.script_load(script), numkeys, *args)
    except Exception as exc:
        logger.warning("Lua script %s failed after reload: %s", sha, exc)
        raise
