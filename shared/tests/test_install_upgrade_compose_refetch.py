"""
Guards scripts/install.sh's `--upgrade` re-fetch of each role's compose
file (#1961).

Before this fix, do_upgrade()'s entire body was an .env rewrite followed by
`docker compose pull && up -d` against whatever compose file already
happened to be on disk -- a compose-file-only change (a new service, a new
label, a port mapping) merged into the repo could never reach an existing
deployment no matter how many times --upgrade ran. The fix has do_upgrade()
call the existing fetch_role() per role directory, before the pull/up step,
reusing its no-clobber logic rather than reinventing it. These assertions
exercise do_upgrade() + fetch_role() together (stubbing `docker` and
`http_get` so no real compose/pull/up or network fetch happens) and confirm
the two compose files with per-instance generated service blocks --
docker-compose.message-processor.yaml and docker-compose.receiver.yaml --
and any config/* file already derived from a .example template are still
never clobbered by an upgrade.
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
    _extract_function(n) for n in ("fetch_role", "role_files", "role_data_dirs", "do_upgrade")
)

# docker is stubbed to a no-op -- these tests only care about which files
# fetch_role touches, not the real pull/up sequence (no daemon available in
# CI/local test runs anyway). http_get is stubbed instead of curl/wget
# directly (install.sh picks one of those at parse time, before fetch_role
# is even reachable) so no real network fetch happens; the stub writes a
# marker recording the exact URL requested, so a test can tell a given file
# really went through fetch_role's fetch path rather than being pre-seeded.
_HARNESS = """
set -eu
DEV_BUILD=0
BRANCH=""
REF="main"
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
        (role_dir / "docker-compose.core.yaml").write_text("# stale pre-#1907 compose file\n")

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


class TestNoClobberPreservedDuringUpgrade:
    def test_message_processor_generated_blocks_survive_upgrade(self, tmp_path):
        """The whole reason fetch_role() no-clobbers this file: blind
        re-fetching would silently discard every already-running instance's
        generated service block. An upgrade must not regress that."""
        role_dir = tmp_path / "message-processor"
        role_dir.mkdir()
        (role_dir / ".env").write_text("SKYFOLLOWER_VERSION=2026.01.01\n")
        compose = role_dir / "docker-compose.message-processor.yaml"
        generated = (
            "name: skyfollower-message-processor\n"
            "services:\n"
            "  skyfollower-message-processor-1:\n"
            "    environment:\n"
            "      MESSAGE_PROCESSOR_ID: 1\n"
        )
        compose.write_text(generated)

        result = _run_upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        assert compose.read_text() == generated
        assert "FETCHED:" not in compose.read_text()

    def test_receiver_generated_blocks_survive_upgrade(self, tmp_path):
        role_dir = tmp_path / "receiver"
        role_dir.mkdir()
        (role_dir / ".env").write_text("SKYFOLLOWER_VERSION=2026.01.01\n")
        compose = role_dir / "docker-compose.receiver.yaml"
        generated = (
            "name: skyfollower-receiver\n"
            "services:\n"
            "  skyfollower-receiver-attic-pi:\n"
            "    environment:\n"
            "      RECEIVER_NAME: ATTIC-PI\n"
        )
        compose.write_text(generated)

        result = _run_upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        assert compose.read_text() == generated
        assert "FETCHED:" not in compose.read_text()

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
