"""
Shared HTTP liveness check used by each data runner's @pytest.mark.network test
(issue #405) to confirm the runner's source URL is reachable.

Uses GET rather than HEAD: several source sites (e.g. is-samgongustofa)
reject HEAD with 405 while GET works fine, since GET is the method each
runner's real download actually uses. The request is streamed and the
response body is never read, so checking a multi-hundred-MB source file
doesn't pull it over the wire just to confirm liveness.

Not a content guarantee — a 2xx response proves the URL responds, not that the
runner will successfully parse whatever is there (the runner's own error
handling already covers that).

Default timeout is 30s rather than a tighter value: some sources (e.g.
br-anac) build their response server-side before writing anything to the
socket, so streaming doesn't save us from that server-side latency, and this
runs on a non-blocking weekly schedule with no reason to hold flaky gov
sites to a tight SLA.
"""

from __future__ import annotations

import requests


def assert_url_reachable(
    url: str,
    runner_name: str,
    headers: dict | None = None,
    verify: bool = True,
    timeout: float = 30,
) -> None:
    """Assert that `url` responds with a 2xx status to a streamed GET request.

    Follows redirects. The response body is never read — the connection is
    closed as soon as the status code is available. Raises AssertionError
    with the runner name, URL, and status code (or request exception) on
    any non-2xx outcome.
    """
    try:
        with requests.get(
            url, headers=headers, verify=verify, timeout=timeout,
            allow_redirects=True, stream=True,
        ) as response:
            status_code = response.status_code
    except requests.RequestException as exc:
        raise AssertionError(f"{runner_name}: request to {url} failed: {exc}") from exc

    if not (200 <= status_code < 300):
        raise AssertionError(
            f"{runner_name}: GET {url} returned HTTP {status_code} (expected 2xx)"
        )
