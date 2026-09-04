"""
pytest configuration: make tools/legacy-migration's own modules (common,
producer, worker, verify) importable as plain top-level modules, the same
way running `python main.py` from that directory would see them (its
directory is auto-added to sys.path[0]), plus the repo root for `shared`.
"""

from __future__ import annotations

import os
import sys

_TOOL_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_REPO_ROOT = os.path.abspath(os.path.join(_TOOL_DIR, "..", ".."))

for _p in (_REPO_ROOT, _TOOL_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)
