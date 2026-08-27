"""
Shared Redis client construction for SkyFollower.

Every component that talks to Redis (message processor, archive processor,
management-UI backend, and all 42 runners) builds its redis.Redis through
build_redis_client() so the required password is applied consistently,
instead of being reimplemented -- or missed -- at each of the ~45
independent call sites.
"""

from __future__ import annotations

import redis as redis_lib


def build_redis_client(redis_config: dict) -> redis_lib.Redis:
    """
    Build a redis.Redis from a component's `redis` config block.

    `password` is passed unconditionally: shared/config.py's redis_config()
    treats REDIS_PASSWORD as required, so every caller already has one by
    the time this runs.

    `username` is optional and defaults to None (redis-py's own default),
    which authenticates as the "default" user -- every component before
    core-health. core-health is the first caller to pass a `username`,
    authenticating as its own scoped Redis ACL user instead.
    """
    return redis_lib.Redis(
        host=redis_config["host"],
        port=redis_config.get("port", 6379),
        username=redis_config.get("username") or None,
        password=redis_config.get("password"),
        decode_responses=True,
    )
