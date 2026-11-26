"""
Guards the uk-caa-registry steps scripts/install.sh runs during --upgrade,
and the compose declarations they depend on.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_INSTALL_SH = (_REPO_ROOT / "scripts" / "install.sh").read_text()
_COMPOSE = yaml.safe_load((_REPO_ROOT / "docker-compose.core.yaml").read_text())
_IMAGE = "ghcr.io/brentio/skyfollower-runner-uk-caa-registry"


def _extract_function(name: str) -> str:
    m = re.search(rf"^{re.escape(name)}\(\) \{{.*?^\}}", _INSTALL_SH, re.DOTALL | re.MULTILINE)
    assert m, f"{name}() not found"
    return m.group(0)


_HARNESS = r"""
set -euo pipefail
existing_env_value() { echo "secret"; }
sleep() { :; }
docker() {
  echo "$*" >> "$DOCKER_LOG"
  case "$*" in
    ps\ *) printf '%s\n' "$DOCKER_PS" ;;
    *"redis-cli EXISTS"*) echo "$EXISTS" ;;
    *"run -d"*) return "$RUN_RC" ;;
  esac
  return 0
}
"""


def _run(tmp_path: Path, function: str, call: str, exists="0", run_rc="0", ps=""):
    log = tmp_path / "docker.log"
    log.touch()
    env = {
        "PATH": "/usr/bin:/bin", "DOCKER_LOG": str(log),
        "EXISTS": exists, "RUN_RC": run_rc, "DOCKER_PS": ps,
    }
    script = _HARNESS + _extract_function(function) + f"\nIMAGE_VERSION=2026.12.01\n{call}\n"
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env)
    return result, log.read_text()


def _prime(tmp_path, **kwargs):
    return _run(tmp_path, "prime_uk_caa_queue", f"prime_uk_caa_queue {tmp_path}", **kwargs)


class TestPrimeQueue:
    def test_starts_a_detached_search_when_none_has_run(self, tmp_path):
        result, log = _prime(tmp_path, exists="0")
        assert result.returncode == 0, result.stderr
        assert "compose run -d --rm runner-uk-caa-registry-search" in log

    def test_does_nothing_once_a_search_has_run(self, tmp_path):
        result, log = _prime(tmp_path, exists="1")
        assert result.returncode == 0, result.stderr
        assert "run -d" not in log

    @pytest.mark.parametrize("kwargs", [{"run_rc": "1"}, {"exists": "NOAUTH"}])
    def test_failure_is_reported_with_the_command_and_never_fails(self, tmp_path, kwargs):
        result, _ = _prime(tmp_path, **kwargs)
        assert result.returncode == 0
        assert "docker compose run -d --rm runner-uk-caa-registry-search" in result.stderr


class TestStopOldRuns:
    def test_stops_only_other_versions_of_this_runner(self, tmp_path):
        ps = "\n".join([
            f"aaa {_IMAGE}:2026.11.01",
            f"bbb {_IMAGE}:2026.12.01",
            "ccc ghcr.io/brentio/skyfollower-runner-mictronics:2026.11.01",
        ])
        result, log = _run(tmp_path, "stop_old_uk_caa_runs", "stop_old_uk_caa_runs", ps=ps)
        assert result.returncode == 0, result.stderr
        assert re.findall(r"^stop (\w+)$", log, re.M) == ["aaa"]


class TestComposeJobs:
    def test_each_job_has_a_service_and_a_schedule(self):
        labels = _COMPOSE["services"]["ofelia"]["labels"]
        for job in ("search", "details"):
            name = f"runner-uk-caa-registry-{job}"
            command = ["python", "uk-caa-registry/main.py", job]
            assert _COMPOSE["services"][name]["command"] == command
            assert labels[f"ofelia.job-run.{name}.command"] == " ".join(command)
            assert labels[f"ofelia.job-run.{name}.schedule"]

    def test_single_weekly_job_is_gone(self):
        assert "runner-uk-caa-registry" not in _COMPOSE["services"]
        assert not any(
            key.startswith("ofelia.job-run.runner-uk-caa-registry.")
            for key in _COMPOSE["services"]["ofelia"]["labels"]
        )

    def test_upgrade_calls_both_steps_for_core(self):
        body = _extract_function("do_upgrade")
        assert body.index("stop_old_uk_caa_runs") < body.index("docker compose up -d")
        assert body.index("docker compose up -d") < body.index('prime_uk_caa_queue "$role_dir"')
