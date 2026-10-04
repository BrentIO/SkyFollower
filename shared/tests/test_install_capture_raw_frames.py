from __future__ import annotations

import re
import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_INSTALL_SH = (_REPO_ROOT / "scripts" / "install.sh").read_text()
_COMPOSE = (_REPO_ROOT / "docker-compose.message-processor.yaml").read_text()


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


class TestCaptureRawFramesEnv:
    def test_compose_has_no_default(self):
        assert "CAPTURE_RAW_FRAMES: ${CAPTURE_RAW_FRAMES}" in _COMPOSE
        assert "CAPTURE_RAW_FRAMES:-" not in _COMPOSE

    def test_template_writes_the_setting_with_a_comment(self):
        block = _extract_function("collect_message_processor_env")
        assert "CAPTURE_RAW_FRAMES=${CAPTURE_RAW_FRAMES}" in block
        assert 'existing_env_value_or "$env_file" CAPTURE_RAW_FRAMES false' in block
        assert "forensic raw-frame capture" in block

    def test_upgrade_appends_when_missing(self, tmp_path):
        env = _env(tmp_path, "message-processor", "SKYFOLLOWER_VERSION=2026.01.01\n")
        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr
        assert env.read_text().count("CAPTURE_RAW_FRAMES=false") == 1

    def test_upgrade_leaves_existing_value_alone(self, tmp_path):
        env = _env(tmp_path, "message-processor", "SKYFOLLOWER_VERSION=2026.01.01\nCAPTURE_RAW_FRAMES=true\n")
        result = _upgrade(tmp_path)
        assert result.returncode == 0, result.stderr
        content = env.read_text()
        assert "CAPTURE_RAW_FRAMES=true" in content
        assert "CAPTURE_RAW_FRAMES=false" not in content

    def test_upgrade_does_not_touch_other_roles(self, tmp_path):
        env = _env(tmp_path, "receiver", "SKYFOLLOWER_VERSION=2026.01.01\n")
        assert _upgrade(tmp_path).returncode == 0
        assert "CAPTURE_RAW_FRAMES" not in env.read_text()
