"""
Guards the management-ui/map HTTPS port defaults and their .env handling in
scripts/install.sh.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_INSTALL_SH = (_REPO_ROOT / "scripts" / "install.sh").read_text()
_MUI_COMPOSE = (_REPO_ROOT / "docker-compose.management-ui.yaml").read_text()
_MAP_COMPOSE = (_REPO_ROOT / "docker-compose.map.yaml").read_text()
_NGINX = (_REPO_ROOT / "management-ui" / "nginx.conf").read_text()


def _extract_function(name: str) -> str:
    m = re.search(rf"^{re.escape(name)}\(\) \{{.*?^\}}", _INSTALL_SH, re.DOTALL | re.MULTILINE)
    assert m, f"{name}() not found"
    return m.group(0)


_HARNESS = """
set -eu
DEV_BUILD=0
BRANCH=""
docker() { return 0; }
fetch_role() { return 0; }
role_compose_files() { echo "docker-compose.$1.yaml"; }
"""


def _upgrade(root: Path) -> subprocess.CompletedProcess:
    script = (
        _HARNESS
        + f"INSTALL_ROOT={root!s}\nIMAGE_VERSION=2026.09.99\n"
        + _extract_function("do_upgrade")
        + "\ndo_upgrade\n"
    )
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True)


def _env(tmp_path: Path, role: str, body: str) -> Path:
    d = tmp_path / role
    d.mkdir()
    f = d / ".env"
    f.write_text(body)
    return f


_PROMPT_FUNCS = "\n".join(
    _extract_function(n)
    for n in ("existing_env_value", "existing_env_value_or", "record_problem", "prompt_int_range")
)


def _prompt_default(env_file: Path, expr: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess:
    script = (
        "set -eu\nNON_INTERACTIVE=1\nPROBLEMS_FILE=$(mktemp)\n"
        + _PROMPT_FUNCS
        + f"\nenv_file={env_file!s}\n"
        + f"printf '%s' \"$({expr})\"\n"
    )
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True, env=env or {"PATH": "/usr/bin:/bin"})


class TestComposeAndNginx:
    def test_management_ui_publishes_only_configurable_https_port(self):
        assert '"${MANAGEMENT_UI_HTTPS_PORT:-18443}:443"' in _MUI_COMPOSE
        assert '"80:80"' not in _MUI_COMPOSE
        assert '"443:443"' not in _MUI_COMPOSE

    def test_nginx_has_no_port_80_listener_and_redirects_plain_http(self):
        assert "listen 80" not in _NGINX
        assert "error_page 497 =301 https://$http_host$request_uri;" in _NGINX

    def test_map_uses_renamed_https_port_with_new_default(self):
        assert '"${MAP_HTTPS_PORT:-19443}:${MAP_HTTPS_PORT:-19443}"' in _MAP_COMPOSE
        assert "MAP_HTTPS_PORT: ${MAP_HTTPS_PORT:-19443}" in _MAP_COMPOSE
        assert "MAP_HTTP_PORT" not in _MAP_COMPOSE


class TestCollectEnv:
    def test_management_ui_prompts_and_writes_https_port(self):
        block = _extract_function("collect_management_ui_env")
        assert 'existing_env_value_or "$env_file" MANAGEMENT_UI_HTTPS_PORT 18443' in block
        assert "MANAGEMENT_UI_HTTPS_PORT=${MANAGEMENT_UI_HTTPS_PORT}" in block

    def test_map_writes_only_renamed_key(self):
        block = _extract_function("collect_map_env")
        assert "MAP_HTTPS_PORT=${MAP_HTTPS_PORT}" in block
        assert "MAP_HTTP_PORT=$" not in block

    def _map_port_expr(self) -> str:
        line = next(
            ln for ln in _extract_function("collect_map_env").splitlines()
            if ln.strip().startswith('MAP_HTTPS_PORT="$(prompt_int_range')
        )
        return line.split('MAP_HTTPS_PORT="$(', 1)[1].rsplit(')"', 1)[0]

    def test_map_default_is_19443_on_fresh_install(self, tmp_path):
        result = _prompt_default(tmp_path / "missing.env", self._map_port_expr())
        assert result.returncode == 0, result.stderr
        assert result.stdout == "19443"

    def test_map_default_falls_back_to_legacy_http_port(self, tmp_path):
        env = tmp_path / ".env"
        env.write_text("MAP_HTTP_PORT=443\n")
        result = _prompt_default(env, self._map_port_expr())
        assert result.stdout == "443"

    def test_map_default_prefers_new_key_over_legacy(self, tmp_path):
        env = tmp_path / ".env"
        env.write_text("MAP_HTTP_PORT=443\nMAP_HTTPS_PORT=9443\n")
        result = _prompt_default(env, self._map_port_expr())
        assert result.stdout == "9443"


class TestUpgradeMigration:
    def test_map_http_port_renamed_keeping_value(self, tmp_path):
        env = _env(tmp_path, "map", "SKYFOLLOWER_VERSION=2026.01.01\nMAP_HTTP_PORT=443\nMAP_LISTEN_PORT=30500\n")
        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr
        content = env.read_text()
        assert "MAP_HTTPS_PORT=443\n" in content
        assert "MAP_HTTP_PORT" not in content
        assert "MAP_LISTEN_PORT=30500" in content

    def test_legacy_key_dropped_when_new_key_already_present(self, tmp_path):
        env = _env(tmp_path, "map", "SKYFOLLOWER_VERSION=2026.01.01\nMAP_HTTP_PORT=443\nMAP_HTTPS_PORT=9443\n")
        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr
        content = env.read_text()
        assert content.count("MAP_HTTPS_PORT=") == 1
        assert "MAP_HTTPS_PORT=9443" in content
        assert "MAP_HTTP_PORT" not in content

    def test_management_ui_port_appended_when_missing(self, tmp_path):
        env = _env(tmp_path, "management-ui", "SKYFOLLOWER_VERSION=2026.01.01\n")
        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr
        assert "MANAGEMENT_UI_HTTPS_PORT=18443" in env.read_text()

    def test_management_ui_port_preserved_across_upgrade(self, tmp_path):
        env = _env(tmp_path, "management-ui", "SKYFOLLOWER_VERSION=2026.01.01\nMANAGEMENT_UI_HTTPS_PORT=8443\n")
        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr
        content = env.read_text()
        assert content.count("MANAGEMENT_UI_HTTPS_PORT=") == 1
        assert "MANAGEMENT_UI_HTTPS_PORT=8443" in content

    def test_other_roles_untouched(self, tmp_path):
        env = _env(tmp_path, "core", "SKYFOLLOWER_VERSION=2026.01.01\n")
        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr
        content = env.read_text()
        assert "MANAGEMENT_UI_HTTPS_PORT" not in content
        assert "MAP_HTTPS_PORT" not in content
