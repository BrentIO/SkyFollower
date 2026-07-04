"""
Shared write path for RedisJSON documents.

All data runners build an enrichment record as a plain dict, often leaving
`None` in place for fields with no data (e.g. `row["model"] or None`). Writing
that dict straight to Redis stores literal JSON `null` for those fields,
which forces every consumer to distinguish "null" from "missing" (issue #289).

`set_json()` is the single choke point every runner writes enrichment records
through, so the "no null values in Redis" invariant is enforced once here
rather than needing to be re-implemented in each runner's record-building
logic.
"""

from __future__ import annotations

from typing import Any


def prune_none(value: Any) -> Any:
    """Recursively drop dict keys whose value is None. Lists are walked
    element-wise (so dicts nested inside lists are pruned too); their own
    entries are otherwise left as-is, including empty lists."""
    if isinstance(value, dict):
        return {k: prune_none(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [prune_none(v) for v in value]
    return value


def set_json(client: Any, key: str, obj: Any, path: str = "$") -> None:
    """Write `obj` to Redis as a JSON document at `key`/`path`, omitting any
    field whose value is None. `client` may be a redis client or a pipeline."""
    client.json().set(key, path, prune_none(obj))
