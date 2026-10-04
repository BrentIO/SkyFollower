"""
Guards scripts/install.sh's `--upgrade` re-fetch of each role's compose
file: do_upgrade() calls fetch_role() per role directory before the
pull/up step.

These assertions exercise do_upgrade() + fetch_role() together (stubbing
`docker` and `http_get` so no real compose/pull/up or network fetch
happens) and confirm every compose file is refreshed while per-instance env files and
any config/* file already derived from a .example template are not.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

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

# docker is stubbed to a no-op: these tests only care which files
# fetch_role touches, not the real pull/up sequence. http_get is stubbed
# so no real network fetch happens; the stub writes a marker recording
# the exact URL requested, so a test can tell a file really went through
# fetch_role's fetch path rather than being pre-seeded.
_HARNESS = """
set -eu
DEV_BUILD=0
BRANCH=""
REF="main"
INSTANCES_COMPOSE_FILE="docker-compose.instances.yaml"
docker() { return 0; }
http_get() { echo "FETCHED:${1}"; }
""" + _FUNCS


def _run_upgrade(install_root: Path, image_version: str = "2026.09.99") -> subprocess.CompletedProcess:
    script = (
        _HARNESS
        + f'\nINSTALL_ROOT={install_root!s}\n'
        + f'IMAGE_VERSION={image_version!r}\n'
        + "\ndo_upgrade\n"
    )
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True)


class TestComposeRefetchedOnUpgrade:
    def test_core_compose_file_is_refetched(self, tmp_path):
        """core has no per-instance state -- its compose file is meant to
        be overwritten wholesale on every upgrade, same as a first install."""
        role_dir = tmp_path / "core"
        role_dir.mkdir()
        (role_dir / ".env").write_text("SKYFOLLOWER_VERSION=2026.01.01\n")
        (role_dir / "docker-compose.core.yaml").write_text("# stale compose file\n")

        result = _run_upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        content = (role_dir / "docker-compose.core.yaml").read_text()
        assert content == "FETCHED:https://raw.githubusercontent.com/BrentIO/SkyFollower/main/docker-compose.core.yaml\n"

    def test_map_compose_file_is_refetched(self, tmp_path):
        """Same as core: no per-instance state, meant to be overwritten."""
        role_dir = tmp_path / "map"
        role_dir.mkdir()
        (role_dir / ".env").write_text("SKYFOLLOWER_VERSION=2026.01.01\n")
        (role_dir / "docker-compose.map.yaml").write_text("# stale compose file\n")

        result = _run_upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        content = (role_dir / "docker-compose.map.yaml").read_text()
        assert content.startswith("FETCHED:")
        assert content.endswith("docker-compose.map.yaml\n")

    def test_core_config_example_template_is_refetched(self, tmp_path):
        role_dir = tmp_path / "core"
        role_dir.mkdir()
        (role_dir / ".env").write_text("SKYFOLLOWER_VERSION=2026.01.01\n")

        result = _run_upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        example = role_dir / "config" / "rabbitmq" / "rabbitmq.conf.example"
        assert example.exists()
        assert example.read_text().startswith("FETCHED:")


class TestInstanceRoleComposeRefetched:
    def test_message_processor_compose_is_refetched_and_instances_kept(self, tmp_path):
        role_dir = tmp_path / "message-processor"
        role_dir.mkdir()
        (role_dir / ".env").write_text("SKYFOLLOWER_VERSION=2026.01.01\nCOMPOSE_FILE=docker-compose.message-processor.yaml\n")
        (role_dir / "message-processors").mkdir()
        (role_dir / "message-processors" / "1.env").write_text("MESSAGE_PROCESSOR_ID=1\n")
        instances = role_dir / "docker-compose.instances.yaml"
        instances.write_text("services:\n  skyfollower-message-processor-1: {}\n")
        (role_dir / "docker-compose.message-processor.yaml").write_text("# stale\n")

        result = _run_upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        compose = (role_dir / "docker-compose.message-processor.yaml").read_text()
        assert compose.startswith("FETCHED:")
        assert instances.read_text() == "services:\n  skyfollower-message-processor-1: {}\n"
        assert (role_dir / "message-processors" / "1.env").read_text() == "MESSAGE_PROCESSOR_ID=1\n"
        assert "COMPOSE_FILE=docker-compose.message-processor.yaml:docker-compose.instances.yaml" in (role_dir / ".env").read_text()

    def test_receiver_compose_is_refetched_and_instances_kept(self, tmp_path):
        role_dir = tmp_path / "receiver"
        role_dir.mkdir()
        (role_dir / ".env").write_text("SKYFOLLOWER_VERSION=2026.01.01\n")
        (role_dir / "receivers").mkdir()
        (role_dir / "receivers" / "attic-pi.env").write_text("RECEIVER_NAME='ATTIC-PI'\n")
        (role_dir / "docker-compose.receiver.yaml").write_text("# stale\n")

        result = _run_upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        assert (role_dir / "docker-compose.receiver.yaml").read_text().startswith("FETCHED:")
        assert (role_dir / "receivers" / "attic-pi.env").read_text() == "RECEIVER_NAME='ATTIC-PI'\n"


class TestNoClobberPreservedDuringUpgrade:
    def test_config_file_already_derived_from_example_is_not_clobbered(self, tmp_path):
        """A config/* file the operator has (or install already has) turned
        into a real file from its .example template must survive -- only
        the .example template itself is refreshed."""
        role_dir = tmp_path / "core"
        role_dir.mkdir()
        (role_dir / ".env").write_text("SKYFOLLOWER_VERSION=2026.01.01\n")
        config_dir = role_dir / "config" / "rabbitmq"
        config_dir.mkdir(parents=True)
        derived = config_dir / "rabbitmq.conf"
        derived.write_text("# operator-edited rabbitmq.conf, do not lose\n")

        result = _run_upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        assert derived.read_text() == "# operator-edited rabbitmq.conf, do not lose\n"
        # The .example sibling is still refreshed -- only the derived real
        # file is protected.
        example = config_dir / "rabbitmq.conf.example"
        assert example.read_text().startswith("FETCHED:")


class TestEnvRewriteStillHappensAlongsideRefetch:
    def test_version_and_map_center_rewrite_still_applied(self, tmp_path):
        """The pre-existing .env rewrite (SKYFOLLOWER_VERSION bump,
        MAP_HOME_*->MAP_CENTER_* migration) must keep working once
        do_upgrade() also calls fetch_role() -- see
        test_install_map_center_migration.py for the dedicated coverage of
        this rewrite in isolation."""
        role_dir = tmp_path / "map"
        role_dir.mkdir()
        (role_dir / ".env").write_text(
            "SKYFOLLOWER_VERSION=2026.01.01\n"
            "MAP_HOME_LATITUDE=33.9425\n"
            "MAP_HOME_LONGITUDE=-118.4081\n"
        )

        result = _run_upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        content = (role_dir / ".env").read_text()
        assert "SKYFOLLOWER_VERSION=2026.09.99" in content
        assert "MAP_CENTER_LATITUDE=33.9425" in content
        assert "MAP_CENTER_LONGITUDE=-118.4081" in content
        # And the compose refetch happened in the same run.
        assert (role_dir / "docker-compose.map.yaml").read_text().startswith("FETCHED:")
