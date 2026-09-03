"""
Tests for shared/index_cache.py -- the local mirror of a per-flight
Parquet index row, shared between archive-processor (writer) and
archive-compaction (reader), both bind-mounted from the same host
directory (see docker-compose.archive.yaml).
"""

from __future__ import annotations

import os

from shared.index_cache import delete_local_index, local_index_path, write_local_index


class TestLocalIndexPath:
    def test_strips_leading_index_prefix(self):
        key = "index/year=2026/month=07/day=23/uuid-a.parquet"
        path = local_index_path(key, "/base")
        assert path == "/base/year=2026/month=07/day=23/uuid-a.parquet"

    def test_key_without_index_prefix_used_as_is(self):
        key = "year=2026/month=07/day=23/uuid-a.parquet"
        path = local_index_path(key, "/base")
        assert path == "/base/year=2026/month=07/day=23/uuid-a.parquet"


class TestWriteLocalIndex:
    def test_creates_parent_directories(self, tmp_path):
        key = "index/year=2026/month=07/day=23/uuid-a.parquet"
        write_local_index(key, b"payload", base_dir=str(tmp_path))
        path = local_index_path(key, str(tmp_path))
        assert os.path.exists(path)
        with open(path, "rb") as f:
            assert f.read() == b"payload"

    def test_overwrite_leaves_no_temp_file_behind(self, tmp_path):
        key = "index/year=2026/month=07/day=23/uuid-a.parquet"
        write_local_index(key, b"first", base_dir=str(tmp_path))
        write_local_index(key, b"second", base_dir=str(tmp_path))
        day_dir = os.path.dirname(local_index_path(key, str(tmp_path)))
        assert os.listdir(day_dir) == ["uuid-a.parquet"]
        with open(local_index_path(key, str(tmp_path)), "rb") as f:
            assert f.read() == b"second"


class TestDeleteLocalIndex:
    def test_removes_existing_file(self, tmp_path):
        key = "index/year=2026/month=07/day=23/uuid-a.parquet"
        write_local_index(key, b"payload", base_dir=str(tmp_path))
        delete_local_index(key, base_dir=str(tmp_path))
        assert not os.path.exists(local_index_path(key, str(tmp_path)))

    def test_missing_file_is_a_silent_no_op(self, tmp_path):
        key = "index/year=2026/month=07/day=23/never-written.parquet"
        delete_local_index(key, base_dir=str(tmp_path))  # must not raise
