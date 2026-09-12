"""
Guards scripts/install.sh's `--upgrade` migration of the map role's
"center" reference point env vars.

MAP_HOME_LATITUDE/MAP_HOME_LONGITUDE were renamed to MAP_CENTER_LATITUDE/
MAP_CENTER_LONGITUDE with no dual-read period -- an operator's existing
`.env` must be rewritten in place by `--upgrade`, the same way it already
rewrites SKYFOLLOWER_VERSION in place, so nothing needs a manual edit for
the rename to take effect. These assertions exercise do_upgrade()'s env-file
rewrite directly (stubbing `docker` so no real compose/pull/up happens).
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


_DO_UPGRADE = _extract_function("do_upgrade")

# docker is stubbed to a no-op -- these tests only care about the .env
# rewrite, not the real pull/up sequence (no daemon available in CI/local
# test runs anyway).
_HARNESS = """
set -eu
DEV_BUILD=0
BRANCH=""
docker() { return 0; }
"""


def _run_upgrade(install_root: Path, image_version: str = "2026.09.99") -> subprocess.CompletedProcess:
    script = (
        _HARNESS
        + f'INSTALL_ROOT={install_root!s}\n'
        + f'IMAGE_VERSION={image_version!r}\n'
        + _DO_UPGRADE
        + "\ndo_upgrade\n"
    )
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True)


class TestMapCenterMigration:
    def test_rewrites_map_home_keys_to_center_preserving_values(self, tmp_path):
        role_dir = tmp_path / "map"
        role_dir.mkdir()
        env_file = role_dir / ".env"
        env_file.write_text(
            "SKYFOLLOWER_VERSION=2026.01.01\n"
            "MAP_HOME_LATITUDE=33.9425\n"
            "MAP_HOME_LONGITUDE=-118.4081\n"
            "MQTT_HOST=mqtt.example\n"
        )

        result = _run_upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        content = env_file.read_text()
        assert "MAP_CENTER_LATITUDE=33.9425" in content
        assert "MAP_CENTER_LONGITUDE=-118.4081" in content
        assert "MAP_HOME_LATITUDE" not in content
        assert "MAP_HOME_LONGITUDE" not in content
        # Unrelated lines are untouched, including the version bump that
        # already happens on every --upgrade.
        assert "SKYFOLLOWER_VERSION=2026.09.99" in content
        assert "MQTT_HOST=mqtt.example" in content

    def test_negative_coordinate_values_survive_the_rewrite(self, tmp_path):
        role_dir = tmp_path / "map"
        role_dir.mkdir()
        env_file = role_dir / ".env"
        env_file.write_text(
            "SKYFOLLOWER_VERSION=2026.01.01\n"
            "MAP_HOME_LATITUDE=-33.9425\n"
            "MAP_HOME_LONGITUDE=-118.4081\n"
        )

        result = _run_upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        content = env_file.read_text()
        assert "MAP_CENTER_LATITUDE=-33.9425" in content
        assert "MAP_CENTER_LONGITUDE=-118.4081" in content

    def test_role_dir_without_map_center_keys_is_a_no_op(self, tmp_path):
        """A non-map role's .env (e.g. message-processor) never had these
        keys -- the rewrite must not add them or otherwise choke on their
        absence."""
        role_dir = tmp_path / "message-processor"
        role_dir.mkdir()
        env_file = role_dir / ".env"
        env_file.write_text("SKYFOLLOWER_VERSION=2026.01.01\nMESSAGE_PROCESSOR_PREFIX=mp\n")

        result = _run_upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        content = env_file.read_text()
        assert "MAP_CENTER_LATITUDE" not in content
        assert "MAP_CENTER_LONGITUDE" not in content
        assert "SKYFOLLOWER_VERSION=2026.09.99" in content

    def test_already_migrated_env_is_left_alone(self, tmp_path):
        """Idempotent: an .env already using the new names is untouched by
        a second --upgrade run."""
        role_dir = tmp_path / "map"
        role_dir.mkdir()
        env_file = role_dir / ".env"
        env_file.write_text(
            "SKYFOLLOWER_VERSION=2026.01.01\n"
            "MAP_CENTER_LATITUDE=33.9425\n"
            "MAP_CENTER_LONGITUDE=-118.4081\n"
        )

        result = _run_upgrade(tmp_path)
        assert result.returncode == 0, result.stderr

        content = env_file.read_text()
        assert "MAP_CENTER_LATITUDE=33.9425" in content
        assert "MAP_CENTER_LONGITUDE=-118.4081" in content
