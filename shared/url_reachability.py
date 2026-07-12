"""
Shared HTTP liveness check used by each data runner's @pytest.mark.network test
(issue #405) to confirm the runner's source URL is reachable.

Not a content guarantee — a 2xx response proves the URL responds, not that the
runner will successfully parse whatever is there (the runner's own error
handling already covers that).
"""

from __future__ import annotations

import requests


def assert_url_reachable(
    url: str,
    runner_name: str,
    headers: dict | None = None,
    verify: bool = True,
    timeout: float = 10,
) -> None:
    """Assert that `url` responds with a 2xx status to an HTTP HEAD request.

    Follows redirects. A 405 (Method Not Allowed) is treated as a hard
    failure — no fallback to GET. Raises AssertionError with the runner
    name, URL, and status code (or request exception) on any other
    non-2xx outcome.
    """
    try:
        response = requests.head(
            url, headers=headers, verify=verify, timeout=timeout, allow_redirects=True,
        )
    except requests.RequestException as exc:
        raise AssertionError(f"{runner_name}: request to {url} failed: {exc}") from exc

    if not (200 <= response.status_code < 300):
        raise AssertionError(
            f"{runner_name}: HEAD {url} returned HTTP {response.status_code} (expected 2xx)"
        )
