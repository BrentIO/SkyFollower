"""
Guards docker-compose.receiver.yaml's anchor template and
scripts/install.sh's receiver service-block append path.

The receiver compose file went from a single static `receiver:` service to
the same shape docker-compose.message-processor.yaml uses: a fixed
top-level `name:`, two YAML anchors, and one generated service block per
instance appended by install.sh. Compose performs no validation of a
merged-anchor block until `up`, so these assertions cover what a fresh
install would otherwise only discover at runtime.
"""

from __future__ import annotations

import re
import subprocess
import textwrap
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_TEMPLATE_PATH = _REPO_ROOT / "docker-compose.receiver.yaml"
_INSTALL_SH_PATH = _REPO_ROOT / "scripts" / "install.sh"

_TEMPLATE_RAW = _TEMPLATE_PATH.read_text()
_INSTALL_SH_RAW = _INSTALL_SH_PATH.read_text()


def _extract_function(name: str) -> str:
    m = re.search(rf"^{re.escape(name)}\(\) \{{.*?^\}}", _INSTALL_SH_RAW, re.DOTALL | re.MULTILINE)
    assert m, f"{name}() not found in scripts/install.sh"
    return m.group(0)


_FUNCS = "\n".join(
    _extract_function(n)
    for n in ("sanitize_identifier", "existing_receiver_slugs", "append_receiver_service")
)


def _run(script: str) -> str:
    result = subprocess.run(
        ["bash", "-c", f"set -eu\n{_FUNCS}\n{script}"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


# ---------------------------------------------------------------------------
# The repo-provided template
# ---------------------------------------------------------------------------

class TestTemplate:
    def test_parses_as_yaml(self):
        assert yaml.safe_load(_TEMPLATE_RAW) is not None

    def test_has_fixed_project_name(self):
        assert yaml.safe_load(_TEMPLATE_RAW)["name"] == "skyfollower-receiver"

    def test_defines_both_anchors_and_no_services(self):
        assert "&receiver-environment" in _TEMPLATE_RAW
        assert "&receiver:" in _TEMPLATE_RAW or "&receiver\n" in _TEMPLATE_RAW
        doc = yaml.safe_load(_TEMPLATE_RAW)
        # `services:` present but empty (install.sh appends into it).
        assert doc.get("services") in (None, {})

    def test_env_anchor_omits_the_per_instance_values(self):
        env = yaml.safe_load(_TEMPLATE_RAW)["x-receiver-environment"]
        assert "RECEIVER_NAME" not in env
        assert "RECEIVER_SOURCES" not in env
        assert "RABBITMQ_HOST" in env and "REDIS_HOST" in env

    def test_project_name_derivation_agrees_with_the_template(self):
        m = re.search(r"^project_name_for_folder\(\) \{.*?^\}", _INSTALL_SH_RAW, re.DOTALL | re.MULTILINE)
        assert m and "receiver) sanitized" not in m.group(0), (
            "project_name_for_folder still has a receiver carve-out"
        )
        slug = _run("sanitize_identifier receiver").strip()
        assert f"skyfollower-{slug}" == yaml.safe_load(_TEMPLATE_RAW)["name"] == "skyfollower-receiver"


# ---------------------------------------------------------------------------
# append_receiver_service / existing_receiver_slugs
# ---------------------------------------------------------------------------

class TestAppend:
    def _seeded(self, tmp_path, *instances):
        compose = tmp_path / "docker-compose.receiver.yaml"
        compose.write_text(_TEMPLATE_RAW)
        for name, sources in instances:
            _run(f'append_receiver_service {compose!s} "{name}" "{sources}"')
        return compose

    def test_single_instance_block_is_valid_and_correct(self, tmp_path):
        compose = self._seeded(
            tmp_path, ("ATTIC-PI", "192.168.1.10:30002:1090,192.168.1.10:30978:978")
        )
        doc = yaml.safe_load(compose.read_text())
        svc = doc["services"]["skyfollower-receiver-attic-pi"]
        assert svc["container_name"] == "skyfollower-receiver-attic-pi"
        assert svc["volumes"] == ["./data/skyfollower-receiver-attic-pi:/app/data"]
        assert svc["environment"]["RECEIVER_NAME"] == "ATTIC-PI"  # original casing kept
        assert (
            svc["environment"]["RECEIVER_SOURCES"]
            == "192.168.1.10:30002:1090,192.168.1.10:30978:978"
        )
        # Merge key resolved -> anchor fields present.
        assert svc["restart"] == "unless-stopped"
        assert svc["image"].startswith("ghcr.io/brentio/skyfollower-receiver:")

    def test_multiple_instances_stay_independent(self, tmp_path):
        compose = self._seeded(
            tmp_path,
            ("ATTIC-PI", "192.168.1.10:30002:1090"),
            ("MLAT-VPS", "mlat.example:30003:EXTERNAL"),
        )
        doc = yaml.safe_load(compose.read_text())
        assert set(doc["services"]) == {
            "skyfollower-receiver-attic-pi",
            "skyfollower-receiver-mlat-vps",
        }
        assert (
            doc["services"]["skyfollower-receiver-mlat-vps"]["volumes"]
            == ["./data/skyfollower-receiver-mlat-vps:/app/data"]
        )

    def test_existing_receiver_slugs_lists_appended_blocks(self, tmp_path):
        compose = self._seeded(
            tmp_path, ("ATTIC-PI", "h:1:1090"), ("Shed_2", "h:2:1090")
        )
        slugs = _run(f"existing_receiver_slugs {compose!s}").split()
        assert sorted(slugs) == ["attic-pi", "shed_2"]

    def test_existing_receiver_slugs_empty_for_untouched_template(self, tmp_path):
        compose = tmp_path / "docker-compose.receiver.yaml"
        compose.write_text(_TEMPLATE_RAW)
        assert _run(f"existing_receiver_slugs {compose!s}").strip() == ""
