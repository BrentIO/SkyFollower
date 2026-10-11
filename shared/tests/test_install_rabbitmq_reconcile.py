"""
Guards scripts/install.sh's RabbitMQ user reconcile (#2197): missing users
are created, existing users get change_password so the broker matches
.env, and an unreachable broker warns without failing.
"""

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


_FUNCS = "\n".join(
    _extract_function(n)
    for n in ("existing_env_value", "reconcile_rabbitmq_user", "provision_rabbitmq_users")
)

# Fake broker: USERS_FILE holds one "name<TAB>[tags]" line per user;
# every rabbitmqctl call is logged to CALL_LOG.
_HARNESS = r"""
set -euo pipefail
docker() {
  case "$1 $2" in
    "compose ps") [ -n "${BROKER_DOWN:-}" ] || echo "cid123"; return 0 ;;
    "inspect --format") echo healthy; return 0 ;;
  esac
  shift 5  # compose exec -T rabbitmq rabbitmqctl
  echo "$*" >> "$CALL_LOG"
  case "$1" in
    list_users)
      [ -z "${LIST_FAILS:-}" ] || return 1
      printf 'Listing users ...\nuser\ttags\n'; cat "$USERS_FILE" ;;
    add_user) printf '%s\t[]\n' "$2" >> "$USERS_FILE" ;;
    change_password) [ -z "${CHPW_FAILS:-}" ] || return 1 ;;
  esac
  return 0
}
sleep() { :; }
"""

_ENV = (
    "RABBITMQ_USERNAME=skyfollower\n"
    "RABBITMQ_PASSWORD=app-pw\n"
    "RABBITMQ_ADMIN_USERNAME=skyfollower-admin\n"
    "RABBITMQ_ADMIN_PASSWORD=admin-pw\n"
    "RABBITMQ_MONITORING_USERNAME=skyfollower-monitoring\n"
    "RABBITMQ_MONITORING_PASSWORD=mon-pw\n"
)


def _provision(tmp_path: Path, existing_users: list[str], **env: str) -> tuple[subprocess.CompletedProcess, list[str]]:
    role_dir = tmp_path / "core"
    role_dir.mkdir(exist_ok=True)
    (role_dir / ".env").write_text(_ENV)
    users = tmp_path / "users"
    users.write_text("".join(f"{u}\t[]\n" for u in existing_users))
    log = tmp_path / "calls"
    log.write_text("")
    full_env = {"PATH": "/usr/bin:/bin", "USERS_FILE": str(users), "CALL_LOG": str(log)}
    full_env.update({k.upper(): v for k, v in env.items()})
    script = _HARNESS + _FUNCS + f"\nprovision_rabbitmq_users {role_dir!s}\n"
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=full_env)
    return result, log.read_text().splitlines()


class TestReconcile:
    def test_missing_users_are_created(self, tmp_path):
        result, calls = _provision(tmp_path, ["skyfollower"])
        assert result.returncode == 0, result.stderr
        assert "add_user skyfollower-admin admin-pw" in calls
        assert "add_user skyfollower-monitoring mon-pw" in calls

    def test_existing_users_get_password_reset_to_env(self, tmp_path):
        result, calls = _provision(tmp_path, ["skyfollower", "skyfollower-admin", "skyfollower-monitoring"])
        assert result.returncode == 0, result.stderr
        assert "change_password skyfollower app-pw" in calls
        assert "change_password skyfollower-admin admin-pw" in calls
        assert "change_password skyfollower-monitoring mon-pw" in calls
        assert not any(c.startswith("add_user") for c in calls)

    def test_missing_application_user_is_created(self, tmp_path):
        result, calls = _provision(tmp_path, [])
        assert result.returncode == 0, result.stderr
        assert "add_user skyfollower app-pw" in calls

    def test_prefix_named_user_does_not_count_as_existing(self, tmp_path):
        result, calls = _provision(tmp_path, ["skyfollower", "skyfollower-admin-old"])
        assert "add_user skyfollower-admin admin-pw" in calls

    def test_tags_and_permissions_still_applied(self, tmp_path):
        _, calls = _provision(tmp_path, ["skyfollower"])
        assert "set_user_tags skyfollower" in calls
        assert "set_user_tags skyfollower-admin administrator" in calls
        assert "set_user_tags skyfollower-monitoring monitoring" in calls

    def test_passwords_never_printed(self, tmp_path):
        result, _ = _provision(tmp_path, ["skyfollower"])
        for pw in ("app-pw", "admin-pw", "mon-pw"):
            assert pw not in result.stdout + result.stderr


class TestUnreachableBroker:
    def test_broker_not_running_warns_and_returns_zero(self, tmp_path):
        result, calls = _provision(tmp_path, ["skyfollower"], broker_down="1")
        assert result.returncode == 0, result.stderr
        assert "isn't running" in result.stderr
        assert calls == []

    def test_list_users_failure_warns_and_changes_nothing(self, tmp_path):
        result, calls = _provision(tmp_path, ["skyfollower"], list_fails="1")
        assert result.returncode == 0, result.stderr
        assert "Could not list RabbitMQ users" in result.stderr
        assert not any(c.startswith(("add_user", "change_password")) for c in calls)

    def test_change_password_failure_warns_and_continues(self, tmp_path):
        result, calls = _provision(tmp_path, ["skyfollower", "skyfollower-admin"], chpw_fails="1")
        assert result.returncode == 0, result.stderr
        assert "Could not update skyfollower's password" in result.stderr
        assert "add_user skyfollower-monitoring mon-pw" in calls
