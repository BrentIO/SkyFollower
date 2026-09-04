"""
Guards scripts/install.sh's single-variable dev-build path (`branch=<name>`).

A dev install/upgrade takes exactly one variable, `branch`. Its presence
makes the run a dev build; its value picks BOTH the git ref for
config/compose files (REF) and the image tag (IMAGE_VERSION =
dev-<sanitized-branch>), so the two can never desync. A value shaped like
a real release tag is rejected. These assertions cover resolve_ref()'s
branch handling without hitting the network, by stubbing ghcr_tag_exists.
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


_RESOLVE_REF = _extract_function("resolve_ref")

# Stubs replacing the real network/API helpers. ghcr_tag_exists's return
# code is driven by the GHCR_STUB_RC env var so each test picks the
# published / not-published / unreachable case.
_HARNESS = """
set -u
DEV_BUILD=0
BRANCH=""
REF=""
IMAGE_VERSION=""
ghcr_tag_exists() { return "${GHCR_STUB_RC:-0}"; }
http_get() { echo '{"tag_name":"2099.01.01"}'; }
"""


def _run(branch: str | None, ghcr_rc: int = 0) -> subprocess.CompletedProcess:
    env_line = f'export branch={branch!r}\n' if branch is not None else ""
    script = (
        _HARNESS
        + f'export GHCR_STUB_RC={ghcr_rc}\n'
        + env_line
        + _RESOLVE_REF
        + "\nresolve_ref\n"
        + 'echo "DEV_BUILD=$DEV_BUILD REF=$REF IMAGE_VERSION=$IMAGE_VERSION BRANCH=$BRANCH"\n'
    )
    return subprocess.run(["bash", "-c", script], capture_output=True, text=True)


def _parse(stdout: str) -> dict[str, str]:
    line = [ln for ln in stdout.splitlines() if ln.startswith("DEV_BUILD=")][-1]
    return dict(part.split("=", 1) for part in line.split())


class TestBranchSetResolvesDevBuild:
    def test_branch_main_resolves_dev_main(self):
        res = _run("main")
        assert res.returncode == 0, res.stderr
        vals = _parse(res.stdout)
        assert vals == {
            "DEV_BUILD": "1",
            "REF": "main",
            "IMAGE_VERSION": "dev-main",
            "BRANCH": "main",
        }

    def test_slash_in_branch_is_sanitized_for_the_image_tag_only(self):
        vals = _parse(_run("feature/foo").stdout)
        assert vals["REF"] == "feature/foo", "config files still come from the real ref"
        assert vals["IMAGE_VERSION"] == "dev-feature-foo", "image tag has / -> -"
        assert vals["BRANCH"] == "feature/foo"


class TestBranchRejectsReleaseTag:
    def test_release_shaped_value_is_a_hard_error(self):
        res = _run("2026.09.01")
        assert res.returncode != 0
        assert "looks like a release tag" in res.stderr
        assert "Omit 'branch'" in res.stderr

    def test_release_shaped_value_never_reaches_the_ghcr_check(self):
        # GHCR stub would say "published" (rc 0); the release-shape guard
        # must fire before it regardless.
        assert _run("2026.12.31", ghcr_rc=0).returncode != 0


class TestUnpublishedBranch:
    def test_missing_dev_image_stops_with_the_publish_command(self):
        res = _run("nope-not-built", ghcr_rc=1)
        assert res.returncode != 0
        assert "No dev build published for 'nope-not-built'" in res.stderr
        assert "gh workflow run build-container-images.yaml --ref nope-not-built -f dev_mode=true" in res.stderr

    def test_ghcr_unreachable_warns_but_continues(self):
        res = _run("some-branch", ghcr_rc=2)
        assert res.returncode == 0, res.stderr
        assert "could not reach GHCR" in res.stderr
        assert _parse(res.stdout)["IMAGE_VERSION"] == "dev-some-branch"


class TestNoBranchIsAReleaseInstall:
    def test_unset_branch_resolves_latest_release_tag(self):
        res = _run(None)
        assert res.returncode == 0, res.stderr
        vals = _parse(res.stdout)
        assert vals["DEV_BUILD"] == "0"
        assert vals["REF"] == "2099.01.01"
        assert vals["IMAGE_VERSION"] == "2099.01.01"
