"""
Latest-version lookup against the GitHub Container Registry.

`core-health` compares the running version of every SkyFollower component
(already on the MQTT broker via each component's HA discovery `device`
block) against the newest calendar-versioned image tag published to GHCR,
and surfaces the difference as a Home Assistant `update` entity. This
module is the GHCR half of that: given an image name, return the newest
`YYYY.MM.BB` tag or `None`.

It is deliberately best-effort. Every failure mode -- a network error, a
non-200 response, malformed JSON, a rate-limit answer, an image with no
parseable tags -- is logged and turned into `None`. Nothing here raises,
so a caller looping over every component's image never has to guard an
individual lookup.

GHCR's Registry v2 API requires a bearer token even for a public image:
the caller first fetches an anonymous pull token, then presents it on the
tags-list request. The token is memoised in-process with a short TTL so a
single poll pass over dozens of images reuses one token instead of
fetching a fresh one per image. The poll cadence itself
(`GHCR_VERSION_CHECK_INTERVAL_SECONDS`) lives in the caller, not here.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from typing import Optional

import requests

from shared.timing import HTTP_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)

# Every SkyFollower image is published under this GHCR owner, lower-cased
# (the Registry v2 API path is case-sensitive and rejects the mixed-case
# spelling used elsewhere in the project).
GHCR_OWNER = "brentio"

_TOKEN_URL = "https://ghcr.io/token"
_REGISTRY_BASE = "https://ghcr.io/v2"

# A published release tag: four-digit year, one- or two-digit month, and a
# build number of one or more digits. Single-digit months and builds are
# why this exists at all -- "2026.9.10" sorts before "2026.9.9" as a
# string, so every tag is compared as an integer tuple instead.
_TAG_RE = re.compile(r"^(\d{4})\.(\d{1,2})\.(\d+)$")

# How long a fetched anonymous pull token is trusted before it is
# re-fetched. GHCR's tokens live several minutes; this stays well inside
# that while still covering a whole multi-image poll pass with one fetch.
_TOKEN_TTL_SECONDS = 240

_token_lock = threading.Lock()
# (token, monotonic-expiry). Anonymous pull tokens issued by GHCR for a
# public repository are accepted for other public repositories too, so one
# cached token serves every image in a pass.
_token_cache: tuple[str, float] | None = None


def _cached_token() -> Optional[str]:
    global _token_cache
    with _token_lock:
        if _token_cache is not None and _token_cache[1] > time.monotonic():
            return _token_cache[0]
    return None


def _store_token(token: str) -> None:
    global _token_cache
    with _token_lock:
        _token_cache = (token, time.monotonic() + _TOKEN_TTL_SECONDS)


def _clear_token() -> None:
    global _token_cache
    with _token_lock:
        _token_cache = None


def _fetch_token(image: str) -> Optional[str]:
    """Fetch an anonymous pull token scoped to one image, or `None`."""
    try:
        response = requests.get(
            _TOKEN_URL,
            params={
                "service": "ghcr.io",
                "scope": f"repository:{GHCR_OWNER}/{image}:pull",
            },
            timeout=HTTP_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        logger.info("GHCR token request for %s failed: %s", image, exc)
        return None
    if response.status_code != 200:
        logger.info(
            "GHCR token request for %s returned HTTP %s", image, response.status_code
        )
        return None
    try:
        token = response.json().get("token")
    except ValueError as exc:
        logger.info("GHCR token response for %s was not JSON: %s", image, exc)
        return None
    if not token:
        logger.info("GHCR token response for %s contained no token", image)
        return None
    return token


def _fetch_tags(image: str, token: str) -> Optional[list[str]]:
    """Fetch the raw tag list for one image, or `None` on any failure.

    A 401 clears the cached token and returns `None`; the next call
    re-fetches a token from scratch.
    """
    try:
        response = requests.get(
            f"{_REGISTRY_BASE}/{GHCR_OWNER}/{image}/tags/list",
            headers={"Authorization": f"Bearer {token}"},
            timeout=HTTP_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        logger.info("GHCR tags request for %s failed: %s", image, exc)
        return None
    if response.status_code == 401:
        logger.info("GHCR rejected the cached token for %s; will re-fetch", image)
        _clear_token()
        return None
    if response.status_code != 200:
        logger.info(
            "GHCR tags request for %s returned HTTP %s", image, response.status_code
        )
        return None
    try:
        tags = response.json().get("tags")
    except ValueError as exc:
        logger.info("GHCR tags response for %s was not JSON: %s", image, exc)
        return None
    if not isinstance(tags, list):
        logger.info("GHCR tags response for %s had no tag list", image)
        return None
    return [tag for tag in tags if isinstance(tag, str)]


def _parse_tag(tag: str) -> Optional[tuple[int, int, int]]:
    """`"2026.9.10"` -> `(2026, 9, 10)`; `None` for anything that is not a
    bare `YYYY.MM.BB` release tag (`dev`, `latest`, PR-preview tags)."""
    match = _TAG_RE.match(tag)
    if match is None:
        return None
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


def latest_calendar_tag(tags: list[str]) -> Optional[str]:
    """The newest `YYYY.MM.BB` tag in `tags`, compared as integer tuples.

    Returns `None` if `tags` is empty or contains no parseable release
    tag. Exposed separately from the network path so the ordering rules
    can be tested without HTTP.
    """
    best_tag: Optional[str] = None
    best_key: Optional[tuple[int, int, int]] = None
    for tag in tags:
        key = _parse_tag(tag)
        if key is None:
            continue
        if best_key is None or key > best_key:
            best_key = key
            best_tag = tag
    return best_tag


def get_latest_ghcr_tag(image: str) -> Optional[str]:
    """The latest calendar-versioned tag of `ghcr.io/BrentIO/<image>`.

    `image` is the bare repository name, e.g. `"skyfollower-core-health"`.
    Returns the newest `YYYY.MM.BB` tag, or `None` on any failure or when
    the image has no release tags. Never raises.
    """
    token = _cached_token()
    if token is not None:
        tags = _fetch_tags(image, token)
    else:
        tags = None

    if tags is None:
        token = _fetch_token(image)
        if token is None:
            return None
        _store_token(token)
        tags = _fetch_tags(image, token)

    if tags is None:
        return None

    latest = latest_calendar_tag(tags)
    if latest is None:
        logger.info("GHCR image %s has no calendar-versioned tags", image)
    return latest
