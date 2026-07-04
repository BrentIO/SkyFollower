"""
Guards the convention from issue #305/#306: every spec file's version field
is a checked-in 9999.99.99 placeholder on main, substituted with the real
release version only in build/release output (see .github/workflows/release.yaml),
never committed back to main.
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

_SPECS_DIR = Path(__file__).resolve().parents[2] / "specs"

_PLACEHOLDER = "9999.99.99"


class TestSpecVersionPlaceholders:
    def test_data_dictionary_version_is_placeholder(self):
        data = yaml.safe_load((_SPECS_DIR / "data-dictionary.yaml").read_text())
        assert data["version"] == _PLACEHOLDER

    def test_asyncapi_version_is_placeholder(self):
        data = yaml.safe_load((_SPECS_DIR / "asyncapi.yaml").read_text())
        assert data["info"]["version"] == _PLACEHOLDER

    def test_no_spec_file_has_a_real_version_checked_in(self):
        """Catches any spec file (including future ones, e.g. openapi.yaml)
        that carries a version-shaped string other than the placeholder."""
        version_line = re.compile(r'^\s*version:\s*[\'"]?([0-9]{4}\.[0-9]{2}\.[0-9]{2})[\'"]?\s*$')
        offenders = []
        for spec in _SPECS_DIR.glob("*.yaml"):
            for line in spec.read_text().splitlines():
                match = version_line.match(line)
                if match and match.group(1) != _PLACEHOLDER:
                    offenders.append(f"{spec.name}: {line.strip()}")
        assert offenders == []
