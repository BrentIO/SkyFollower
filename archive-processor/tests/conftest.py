"""
pytest configuration: expose archive-processor/ as the 'archive_processor'
Python package.  The directory name contains a hyphen which Python can't
import directly, so we register a finder that maps the dotted name
'archive_processor' to the hyphenated directory.
"""

from __future__ import annotations

import importlib
import importlib.machinery
import importlib.util
import os
import sys
import types

_ARCHIVE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_REPO_ROOT = os.path.abspath(os.path.join(_ARCHIVE_DIR, ".."))

# Ensure repo root is on sys.path so 'shared' is importable.
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _register_archive_processor_package() -> None:
    """Register archive-processor/ as the Python package 'archive_processor'."""
    pkg_name = "archive_processor"
    if pkg_name in sys.modules:
        return  # Already registered

    # Create a package object pointing at the hyphenated directory.
    spec = importlib.util.spec_from_file_location(
        pkg_name,
        os.path.join(_ARCHIVE_DIR, "__init__.py"),
        submodule_search_locations=[_ARCHIVE_DIR],
    )
    pkg = importlib.util.module_from_spec(spec)
    pkg.__path__ = [_ARCHIVE_DIR]
    pkg.__package__ = pkg_name
    sys.modules[pkg_name] = pkg
    spec.loader.exec_module(pkg)


_register_archive_processor_package()
