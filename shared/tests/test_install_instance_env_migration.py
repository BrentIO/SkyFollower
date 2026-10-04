"""
Guards scripts/install.sh's conversion of compose files that still hold
generated per-instance service blocks (inline RECEIVER_NAME/RECEIVER_SOURCES
or MESSAGE_PROCESSOR_ID) into per-instance env files plus
docker-compose.instances.yaml, run by `--upgrade` before the compose file is
replaced by the static one.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_INSTALL_SH_RAW = (_REPO_ROOT / "scripts" / "install.sh").read_text()


def _extract_function(name: str) -> str:
    m = re.search(rf"^{re.escape(name)}\(\) \{{.*?^\}}", _INSTALL_SH_RAW, re.DOTALL | re.MULTILINE)
    assert m, f"{name}() not found in scripts/install.sh"
    return m.group(0)


_FUNCS = "\n".join(
    _extract_function(n)
    for n in (
        "fetch_role", "role_files", "role_compose_files", "role_data_dirs", "do_upgrade",
        "convert_generated_blocks", "init_instances_compose", "instance_ids",
        "write_instance_env", "write_receiver_env", "write_message_processor_env",
        "append_receiver_service", "append_message_processor_service",
    )
)

_HARNESS = f"""
set -eu
DEV_BUILD=0
BRANCH=""
REF="main"
INSTANCES_COMPOSE_FILE="docker-compose.instances.yaml"
docker() {{ return 0; }}
http_get() {{ cat "{_REPO_ROOT}/${{1##*/main/}}"; }}
""" + _FUNCS


def _upgrade(install_root: Path) -> subprocess.CompletedProcess:
    script = _HARNESS + f"\nINSTALL_ROOT={install_root!s}\nIMAGE_VERSION=2026.09.99\ndo_upgrade\n"
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True)


class _Loader(yaml.SafeLoader):
    pass


_Loader.add_constructor("!reset", lambda loader, node: None)


def _legacy_receiver_service(slug: str, name: str, sources: str) -> str:
    return f"""
  skyfollower-receiver-{slug}:
    <<: *receiver
    container_name: skyfollower-receiver-{slug}
    volumes:
      - ./data/skyfollower-receiver-{slug}:/app/data
    environment:
      <<: *receiver-environment
      RECEIVER_NAME: {name}
      RECEIVER_SOURCES: "{sources}"
"""


def _legacy_processor_service(id_: str) -> str:
    return f"""
  skyfollower-message-processor-{id_}:
    <<: *message-processor
    container_name: skyfollower-message-processor-{id_}
    volumes:
      - ./data/skyfollower-message-processor-{id_}:/app/data
    environment:
      <<: *message-processor-environment
      MESSAGE_PROCESSOR_ID: {id_}
"""


def _seed_receiver(root: Path, *instances) -> Path:
    role_dir = root / "receiver"
    role_dir.mkdir()
    (role_dir / ".env").write_text(
        "SKYFOLLOWER_VERSION=2026.01.01\nCOMPOSE_FILE=docker-compose.receiver.yaml\nLOG_LEVEL=info\n"
    )
    legacy = (
        "name: skyfollower-receiver\nx-receiver-environment: &receiver-environment\n  LOG_LEVEL: info\n"
        "x-receiver: &receiver\n  restart: unless-stopped\nservices:"
        + "".join(_legacy_receiver_service(*i) for i in instances)
    )
    (role_dir / "docker-compose.receiver.yaml").write_text(legacy)
    return role_dir


def _seed_processors(root: Path, *ids) -> Path:
    role_dir = root / "message-processor"
    role_dir.mkdir()
    (role_dir / ".env").write_text(
        "SKYFOLLOWER_VERSION=2026.01.01\nCOMPOSE_FILE=docker-compose.message-processor.yaml\n"
    )
    legacy = (
        "name: skyfollower-message-processor\nx-message-processor-environment: &message-processor-environment\n"
        "  LOG_LEVEL: info\nx-message-processor: &message-processor\n  restart: unless-stopped\n# Slots\nservices:"
        + "".join(_legacy_processor_service(i) for i in ids)
    )
    (role_dir / "docker-compose.message-processor.yaml").write_text(legacy)
    return role_dir


class TestReceiverConversion:
    def test_single_instance(self, tmp_path):
        role_dir = _seed_receiver(tmp_path, ("attic-pi", "ATTIC-PI", "10.0.0.5:30002:1090,10.0.0.5:30978:978"))
        original = (role_dir / "docker-compose.receiver.yaml").read_text()

        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        env = role_dir / "receivers" / "attic-pi.env"
        assert env.read_text() == (
            "RECEIVER_NAME='ATTIC-PI'\nRECEIVER_SOURCES='10.0.0.5:30002:1090,10.0.0.5:30978:978'\n"
        )
        assert stat.S_IMODE(env.stat().st_mode) == 0o600
        assert (role_dir / "docker-compose.receiver.yaml.bak").read_text() == original

        services = yaml.load((role_dir / "docker-compose.instances.yaml").read_text(), Loader=_Loader)["services"]
        svc = services["skyfollower-receiver-attic-pi"]
        assert svc["container_name"] == "skyfollower-receiver-attic-pi"
        assert svc["volumes"] == ["./data/skyfollower-receiver-attic-pi:/app/data"]
        assert svc["env_file"] == "./receivers/attic-pi.env"

        static = (role_dir / "docker-compose.receiver.yaml").read_text()
        assert "skyfollower-receiver-attic-pi" not in static
        assert "COMPOSE_FILE=docker-compose.receiver.yaml:docker-compose.instances.yaml" in (role_dir / ".env").read_text()

    def test_multiple_instances_with_spaces_in_name(self, tmp_path):
        role_dir = _seed_receiver(
            tmp_path,
            ("attic-pi", "ATTIC-PI", "10.0.0.5:30002:1090"),
            ("mlat-vps", "MLAT VPS", "mlat.example:30003:EXTERNAL"),
        )

        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        assert (role_dir / "receivers" / "mlat-vps.env").read_text() == (
            "RECEIVER_NAME='MLAT VPS'\nRECEIVER_SOURCES='mlat.example:30003:EXTERNAL'\n"
        )
        services = yaml.load((role_dir / "docker-compose.instances.yaml").read_text(), Loader=_Loader)["services"]
        assert set(services) == {"skyfollower-receiver-attic-pi", "skyfollower-receiver-mlat-vps"}

    def test_idempotent_rerun(self, tmp_path):
        role_dir = _seed_receiver(tmp_path, ("attic-pi", "ATTIC-PI", "10.0.0.5:30002:1090"))
        assert _upgrade(tmp_path).returncode == 0
        before = {
            p: p.read_text()
            for p in [
                role_dir / "docker-compose.instances.yaml",
                role_dir / "docker-compose.receiver.yaml.bak",
                role_dir / "receivers" / "attic-pi.env",
                role_dir / ".env",
            ]
        }

        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr
        assert {p: p.read_text() for p in before} == before

    def test_interrupted_conversion_resumes_without_duplicates(self, tmp_path):
        role_dir = _seed_receiver(
            tmp_path,
            ("attic-pi", "ATTIC-PI", "10.0.0.5:30002:1090"),
            ("shed", "SHED", "10.0.0.6:30002:1090"),
        )
        (role_dir / "receivers").mkdir()
        (role_dir / "receivers" / "attic-pi.env").write_text("RECEIVER_NAME='ATTIC-PI'\nRECEIVER_SOURCES='10.0.0.5:30002:1090'\n")

        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr
        instances = (role_dir / "docker-compose.instances.yaml").read_text()
        assert len(re.findall(r"^  skyfollower-receiver-attic-pi:", instances, re.M)) == 1
        assert len(re.findall(r"^  skyfollower-receiver-shed:", instances, re.M)) == 1
        assert (role_dir / "receivers" / "shed.env").exists()

    @pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
    def test_converted_install_resolves_to_same_services(self, tmp_path):
        role_dir = _seed_receiver(tmp_path, ("attic-pi", "ATTIC-PI", "10.0.0.5:30002:1090"))
        assert _upgrade(tmp_path).returncode == 0
        result = subprocess.run(
            ["docker", "compose", "config", "--format", "json"],
            cwd=role_dir,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "COMPOSE_FILE": "docker-compose.receiver.yaml:docker-compose.instances.yaml",
                "RABBITMQ_HOST": "r", "RABBITMQ_USERNAME": "u", "RABBITMQ_PASSWORD": "p",
                "MQTT_HOST": "m", "MQTT_USERNAME": "", "MQTT_PASSWORD": "",
            },
        )
        assert result.returncode == 0, result.stderr
        services = json.loads(result.stdout)["services"]
        assert list(services) == ["skyfollower-receiver-attic-pi"]
        env = services["skyfollower-receiver-attic-pi"]["environment"]
        assert env["RECEIVER_NAME"] == "ATTIC-PI"
        assert env["RECEIVER_SOURCES"] == "10.0.0.5:30002:1090"


class TestMessageProcessorConversion:
    def test_multiple_instances(self, tmp_path):
        role_dir = _seed_processors(tmp_path, "1", "2", "3")

        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        for i in ("1", "2", "3"):
            assert (role_dir / "message-processors" / f"{i}.env").read_text() == f"MESSAGE_PROCESSOR_ID={i}\n"
        services = yaml.load((role_dir / "docker-compose.instances.yaml").read_text(), Loader=_Loader)["services"]
        assert set(services) == {f"skyfollower-message-processor-{i}" for i in "123"}
        svc = services["skyfollower-message-processor-2"]
        assert svc["container_name"] == "skyfollower-message-processor-2"
        assert svc["volumes"] == ["./data/skyfollower-message-processor-2:/app/data"]
        assert svc["env_file"] == "./message-processors/2.env"
        assert (role_dir / "docker-compose.message-processor.yaml.bak").exists()
        assert "skyfollower-message-processor-1" not in (role_dir / "docker-compose.message-processor.yaml").read_text()

    def test_single_instance_and_idempotent_rerun(self, tmp_path):
        role_dir = _seed_processors(tmp_path, "1")
        assert _upgrade(tmp_path).returncode == 0
        snapshot = (role_dir / "docker-compose.instances.yaml").read_text()

        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr
        assert (role_dir / "docker-compose.instances.yaml").read_text() == snapshot
        assert (role_dir / "message-processors" / "1.env").read_text() == "MESSAGE_PROCESSOR_ID=1\n"
