"""
pytest configuration for map/tests/.

Unlike message-processor/ and archive-processor/, "map" has no hyphen, so
it's already a valid Python package name -- no importlib-spec workaround is
needed, just the repo root on sys.path so `import map...` and `import
shared...` both resolve when pytest collects this directory directly
(--import-mode=importlib doesn't add the rootdir to sys.path on its own).
"""

from __future__ import annotations

import os
import sys

_MAP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_REPO_ROOT = os.path.abspath(os.path.join(_MAP_DIR, ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
