from __future__ import annotations

import re
import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_INSTALL_SH = (_REPO_ROOT / "scripts" / "install.sh").read_text()


def _extract_function(name: str) -> str:
    m = re.search(rf"^{re.escape(name)}\(\) \{{.*?^\}}", _INSTALL_SH, re.DOTALL | re.MULTILINE)
    assert m, f"{name}() not found"
    return m.group(0)


_HARNESS = """
set -euo pipefail
DEV_BUILD=0
BRANCH=""
FAIL_PULL="${FAIL_PULL:-}"
FAIL_UP="${FAIL_UP:-}"
FAIL_FETCH="${FAIL_FETCH:-}"
docker() {
  local role; role="$(basename "$PWD")"
  echo "$role ${SKYFOLLOWER_VERSION:-} $*" >> "$DOCKER_LOG"
  case "$*" in
    *" pull"*) case " $FAIL_PULL " in *" $role "*) echo "crc32 mismatch" >&2; return 1 ;; esac ;;
    *"up -d"*) case " $FAIL_UP " in *" $role "*) return 1 ;; esac ;;
  esac
  return 0
}
fetch_role() {
  case " $FAIL_FETCH " in *" $1 "*) echo "Not found." >&2; exit 1 ;; esac
  return 0
}
role_compose_files() { echo "docker-compose.$1.yaml"; }
provision_rabbitmq_users() { echo "PROVISION $1" >> "$DOCKER_LOG"; return "${PROVISION_RC:-0}"; }
"""


def _upgrade(root: Path, **fail: str) -> subprocess.CompletedProcess:
    log = root / "docker.log"
    env = {"PATH": "/usr/bin:/bin", "DOCKER_LOG": str(log)}
    env.update({k.upper(): v for k, v in fail.items()})
    script = (
        _HARNESS
        + f"INSTALL_ROOT={root!s}\nIMAGE_VERSION=2026.09.99\n"
        + _extract_function("do_upgrade")
        + "\ndo_upgrade\n"
    )
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env)


def _roles(tmp_path: Path, *roles: str) -> dict[str, Path]:
    out = {}
    for role in roles:
        d = tmp_path / role
        d.mkdir()
        f = d / ".env"
        f.write_text("SKYFOLLOWER_VERSION=2026.01.01\nLOG_LEVEL=info\n")
        out[role] = f
    return out


def _version(env: Path) -> str:
    return re.search(r"^SKYFOLLOWER_VERSION=(.*)$", env.read_text(), re.M).group(1)


class TestUpgradeResume:
    def test_all_succeed_writes_version_and_exits_zero(self, tmp_path):
        envs = _roles(tmp_path, "core", "map")
        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr
        assert all(_version(e) == "2026.09.99" for e in envs.values())
        assert "Upgrade complete." in result.stdout
        assert f"✓ {tmp_path / 'core'}: 2026.09.99" in result.stdout

    def test_pull_and_up_see_the_new_version_before_env_is_written(self, tmp_path):
        _roles(tmp_path, "core")
        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr
        log = (tmp_path / "docker.log").read_text().splitlines()
        assert any(" pull" in line and line.startswith("core 2026.09.99 ") for line in log)
        assert any("up -d" in line and line.startswith("core 2026.09.99 ") for line in log)

    def test_failed_pull_keeps_old_version_and_continues(self, tmp_path):
        envs = _roles(tmp_path, "archive", "core", "map")
        result = _upgrade(tmp_path, fail_pull="core")
        assert result.returncode != 0
        assert _version(envs["core"]) == "2026.01.01"
        assert _version(envs["archive"]) == "2026.09.99"
        assert _version(envs["map"]) == "2026.09.99"
        log = (tmp_path / "docker.log").read_text()
        assert not any(line.startswith("core ") and "up -d" in line for line in log.splitlines())
        assert any(line.startswith("map ") and "up -d" in line for line in log.splitlines())

    def test_failed_up_keeps_old_version(self, tmp_path):
        envs = _roles(tmp_path, "core", "map")
        result = _upgrade(tmp_path, fail_up="map")
        assert result.returncode != 0
        assert _version(envs["map"]) == "2026.01.01"
        assert _version(envs["core"]) == "2026.09.99"

    def test_failed_fetch_is_contained_to_its_role(self, tmp_path):
        envs = _roles(tmp_path, "core", "map")
        result = _upgrade(tmp_path, fail_fetch="core")
        assert result.returncode != 0
        assert _version(envs["core"]) == "2026.01.01"
        assert _version(envs["map"]) == "2026.09.99"

    def test_summary_lists_every_role_and_what_to_rerun(self, tmp_path):
        _roles(tmp_path, "core", "map")
        result = _upgrade(tmp_path, fail_pull="core")
        assert "Upgrade summary:" in result.stdout
        assert f"✗ {tmp_path / 'core'}: FAILED -- .env still says 2026.01.01" in result.stdout
        assert f"✓ {tmp_path / 'map'}: 2026.09.99" in result.stdout
        assert "1 role(s) failed" in result.stderr
        assert "Upgrade complete." not in result.stdout

    def test_other_env_lines_untouched_on_failure(self, tmp_path):
        envs = _roles(tmp_path, "core")
        _upgrade(tmp_path, fail_pull="core")
        assert "LOG_LEVEL=info" in envs["core"].read_text()


class TestUpgradeReconcilesRabbitmqUsers:
    def _provisioned(self, tmp_path: Path) -> list[str]:
        return [ln for ln in (tmp_path / "docker.log").read_text().splitlines() if ln.startswith("PROVISION ")]

    def test_core_upgrade_reconciles_users(self, tmp_path):
        _roles(tmp_path, "core", "map")
        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr
        assert self._provisioned(tmp_path) == [f"PROVISION {tmp_path / 'core'}"]

    def test_reconcile_runs_even_when_core_pull_failed(self, tmp_path):
        _roles(tmp_path, "core")
        _upgrade(tmp_path, fail_pull="core")
        assert self._provisioned(tmp_path) == [f"PROVISION {tmp_path / 'core'}"]

    def test_reconcile_failure_never_fails_the_upgrade(self, tmp_path):
        envs = _roles(tmp_path, "core", "map")
        result = _upgrade(tmp_path, provision_rc="1")
        assert result.returncode == 0, result.stderr
        assert _version(envs["map"]) == "2026.09.99"
        assert "reconcile did not complete" in result.stderr
