"""
Shared write path for RedisJSON documents.

Writing a dict straight to Redis stores literal JSON `null` for any field
left as `None`, forcing every consumer to distinguish "null" from
"missing". redis-py's JSON client also defaults to
`ensure_ascii=True`, escaping non-ASCII characters unreadably.

`set_json()` is the single choke point every runner writes enrichment
records through, enforcing both invariants -- no null values, UTF-8
stored as-is -- once instead of in each runner.
"""

from __future__ import annotations

import json
from typing import Any

_ENCODER = json.JSONEncoder(ensure_ascii=False)


def prune_none(value: Any) -> Any:
    """Recursively drop dict keys whose value is None. Lists are walked
    element-wise (so dicts nested inside lists are pruned too); their own
    entries are otherwise left as-is, including empty lists."""
    if isinstance(value, dict):
        return {k: prune_none(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [prune_none(v) for v in value]
    return value


def set_json(client: Any, key: str, obj: Any, path: str = "$", nx: bool = False) -> Any:
    """Write `obj` to Redis as a JSON document at `key`/`path`, omitting
    any field whose value is None and preserving non-ASCII characters
    as-is. `client` may be a redis client or a pipeline.

    `nx=True` writes only if `key` does not already exist (Redis
    JSON.SET's native NX option), for a runner that must never overwrite
    another source's existing record. Returns the underlying JSON.SET
    result so a caller can tell whether the write happened."""
    return client.json(encoder=_ENCODER).set(key, path, prune_none(obj), nx=nx)
