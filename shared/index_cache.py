"""
Shared local cache for per-flight Parquet index rows.

archive-processor uploads a small Parquet index row per flight to S3
(index/year={YYYY}/month={MM}/day={DD}/{uuid}.parquet -- see its own
build_index_s3_key()). archive-compaction re-reads every one of those rows
days later to fold them into a single compacted file per partition.
Since both components run on the same host (see docker-compose.archive.yaml)
and already have the bytes in hand at upload time, archive-processor also
writes a local copy here, under a directory both containers bind-mount from
the same host path. archive-compaction reads from this local copy instead
of downloading the row again, falling back to a real S3 GetObject only when
the local copy is unexpectedly missing.
"""

from __future__ import annotations

import os

# Fixed, like shared.config.DATA_DIR -- both archive-processor and
# archive-compaction mount the same host directory here (see
# docker-compose.archive.yaml), so a file written by one is immediately
# visible to the other. No deployment has a reason to vary it.
INDEX_CACHE_DIR = "/app/index-cache"


def local_index_path(index_key: str, base_dir: str = INDEX_CACHE_DIR) -> str:
    """
    Map a Parquet index S3 key
    (index/year={YYYY}/month={MM}/day={DD}/{uuid}.parquet) to its path
    under the shared local cache -- dropping the leading "index/" segment
    since the cache directory root already represents that prefix.
    """
    relative = index_key[len("index/"):] if index_key.startswith("index/") else index_key
    return os.path.join(base_dir, relative)


def write_local_index(index_key: str, data: bytes, base_dir: str = INDEX_CACHE_DIR) -> None:
    """
    Write `data` to the local cache path for `index_key`, creating any
    needed parent directories. Written to a temp file and atomically
    renamed into place, so a concurrent reader can never observe a
    partially-written file. Raises on failure -- callers treat this as a
    best-effort mirror of an S3 write that already succeeded and catch
    around it.
    """
    path = local_index_path(index_key, base_dir)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = f"{path}.tmp-{os.getpid()}"
    with open(tmp_path, "wb") as f:
        f.write(data)
    os.replace(tmp_path, path)


def delete_local_index(index_key: str, base_dir: str = INDEX_CACHE_DIR) -> None:
    """Remove the local cache copy for `index_key` if present -- a no-op,
    not an error, when it's already gone."""
    path = local_index_path(index_key, base_dir)
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
