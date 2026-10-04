"""
Guards docker-compose.receiver.yaml's static template and scripts/install.sh's
per-instance env file and generated-service path.

The compose file holds structure only: a fixed `name:`, the shared
environment anchor, and one profile-gated `receiver` template service. Each
instance is a generated service in docker-compose.instances.yaml that extends
the template and points at its own receivers/{slug}.env.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
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


_FUNCS = 'INSTANCES_COMPOSE_FILE="docker-compose.instances.yaml"\n' + "\n".join(
    _extract_function(n)
    for n in (
        "sanitize_identifier",
        "instance_ids",
        "init_instances_compose",
        "write_instance_env",
        "write_receiver_env",
        "append_receiver_service",
    )
)


class _ComposeLoader(yaml.SafeLoader):
    pass


_ComposeLoader.add_constructor("!reset", lambda loader, node: None)


def _load(text: str):
    return yaml.load(text, Loader=_ComposeLoader)


def _run(script: str) -> str:
    result = subprocess.run(
        ["bash", "-c", f"set -eu\n{_FUNCS}\n{script}"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


class TestTemplate:
    def test_parses_as_yaml(self):
        assert yaml.safe_load(_TEMPLATE_RAW) is not None

    def test_has_fixed_project_name(self):
        assert yaml.safe_load(_TEMPLATE_RAW)["name"] == "skyfollower-receiver"

    def test_only_service_is_the_profile_gated_template(self):
        services = yaml.safe_load(_TEMPLATE_RAW)["services"]
        assert set(services) == {"receiver"}
        assert services["receiver"]["profiles"] == ["template"]

    def test_holds_no_per_instance_values(self):
        doc = yaml.safe_load(_TEMPLATE_RAW)
        env = doc["x-receiver-environment"]
        assert "RECEIVER_NAME" not in env
        assert "RECEIVER_SOURCES" not in env
        assert "RABBITMQ_HOST" in env and "REDIS_HOST" in env
        assert "container_name" not in doc["services"]["receiver"]

    def test_project_name_derivation_agrees_with_the_template(self):
        m = re.search(r"^project_name_for_folder\(\) \{.*?^\}", _INSTALL_SH_RAW, re.DOTALL | re.MULTILINE)
        assert m and "receiver) sanitized" not in m.group(0), (
            "project_name_for_folder still has a receiver carve-out"
        )
        slug = _run("sanitize_identifier receiver").strip()
        assert f"skyfollower-{slug}" == yaml.safe_load(_TEMPLATE_RAW)["name"] == "skyfollower-receiver"


class TestInstances:
    def _seeded(self, tmp_path, *instances):
        _run(f"init_instances_compose {tmp_path!s}")
        for name, sources in instances:
            slug = _run(f'sanitize_identifier "{name}"').strip()
            _run(f'write_receiver_env {tmp_path!s} "{slug}" "{name}" "{sources}"')
            _run(f"append_receiver_service {tmp_path!s} {slug}")
        return tmp_path / "docker-compose.instances.yaml"

    def test_single_instance_service_and_env_file(self, tmp_path):
        instances = self._seeded(
            tmp_path, ("ATTIC-PI", "192.168.1.10:30002:1090,192.168.1.10:30978:978")
        )
        svc = _load(instances.read_text())["services"]["skyfollower-receiver-attic-pi"]
        assert svc["container_name"] == "skyfollower-receiver-attic-pi"
        assert svc["volumes"] == ["./data/skyfollower-receiver-attic-pi:/app/data"]
        assert svc["env_file"] == "./receivers/attic-pi.env"
        assert "environment" not in svc
        assert (tmp_path / "receivers" / "attic-pi.env").read_text() == (
            "RECEIVER_NAME='ATTIC-PI'\n"
            "RECEIVER_SOURCES='192.168.1.10:30002:1090,192.168.1.10:30978:978'\n"
        )

    def test_env_file_is_private(self, tmp_path):
        self._seeded(tmp_path, ("ATTIC-PI", "h:1:1090"))
        assert (tmp_path / "receivers" / "attic-pi.env").stat().st_mode & 0o777 == 0o600

    def test_multiple_instances_stay_independent(self, tmp_path):
        instances = self._seeded(
            tmp_path,
            ("ATTIC-PI", "192.168.1.10:30002:1090"),
            ("MLAT-VPS", "mlat.example:30003:EXTERNAL"),
        )
        doc = _load(instances.read_text())
        assert set(doc["services"]) == {
            "skyfollower-receiver-attic-pi",
            "skyfollower-receiver-mlat-vps",
        }
        assert "mlat.example" in (tmp_path / "receivers" / "mlat-vps.env").read_text()

    def test_instance_ids_lists_env_files(self, tmp_path):
        self._seeded(tmp_path, ("ATTIC-PI", "h:1:1090"), ("Shed_2", "h:2:1090"))
        assert sorted(_run(f"instance_ids {tmp_path!s}/receivers").split()) == ["attic-pi", "shed_2"]

    def test_instance_ids_empty_when_no_env_files(self, tmp_path):
        assert _run(f"instance_ids {tmp_path!s}/receivers").strip() == ""

    @pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
    def test_compose_resolves_instance_with_template_excluded(self, tmp_path):
        (tmp_path / "docker-compose.receiver.yaml").write_text(_TEMPLATE_RAW)
        self._seeded(tmp_path, ("ATTIC-PI", "h:1:1090"))
        result = subprocess.run(
            [
                "docker", "compose",
                "-f", "docker-compose.receiver.yaml",
                "-f", "docker-compose.instances.yaml",
                "config", "--format", "json",
            ],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "RABBITMQ_HOST": "r",
                "RABBITMQ_USERNAME": "u",
                "RABBITMQ_PASSWORD": "p",
                "MQTT_HOST": "m",
                "MQTT_USERNAME": "",
                "MQTT_PASSWORD": "",
            },
        )
        assert result.returncode == 0, result.stderr
        services = json.loads(result.stdout)["services"]
        assert list(services) == ["skyfollower-receiver-attic-pi"]
        env = services["skyfollower-receiver-attic-pi"]["environment"]
        assert env["RECEIVER_NAME"] == "ATTIC-PI"
        assert env["RECEIVER_SOURCES"] == "h:1:1090"
        assert env["RABBITMQ_HOST"] == "r"
