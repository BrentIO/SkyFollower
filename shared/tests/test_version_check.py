"""
Tests for shared/version_check.py -- the GHCR latest-tag lookup that
drives core-health's Home Assistant "update available" entities.

Two concerns: the pure calendar-version ordering (including the
single-digit month/build case that a plain string compare gets wrong),
and the two-step token-then-tags network path, which must degrade to
`None` on every failure mode without ever raising.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from shared import version_check
from shared.version_check import (
    get_latest_ghcr_tag,
    latest_calendar_tag,
)


@pytest.fixture(autouse=True)
def _reset_token_cache():
    version_check._clear_token()
    yield
    version_check._clear_token()


def _response(status_code: int, json_body=None, raise_json: bool = False):
    response = MagicMock()
    response.status_code = status_code
    if raise_json:
        response.json.side_effect = ValueError("no json")
    else:
        response.json.return_value = json_body
    return response


class TestLatestCalendarTag:
    def test_picks_the_newest_tuple_ignoring_non_release_tags(self):
        tags = ["2026.9.9", "2026.9.10", "2026.10.1", "dev", "latest"]
        assert latest_calendar_tag(tags) == "2026.10.1"

    def test_single_digit_build_is_not_string_compared(self):
        # "2026.9.10" < "2026.9.9" lexicographically -- the tuple compare
        # is the whole point of this module.
        assert latest_calendar_tag(["2026.9.9", "2026.9.10"]) == "2026.9.10"

    def test_single_digit_month_ordering(self):
        assert latest_calendar_tag(["2026.9.30", "2026.10.1"]) == "2026.10.1"

    def test_zero_padded_tags_also_parse(self):
        assert latest_calendar_tag(["2026.08.20", "2026.09.01"]) == "2026.09.01"

    def test_all_unparseable_returns_none(self):
        assert latest_calendar_tag(["dev", "latest", "dev-main", "pr-123"]) is None

    def test_empty_returns_none(self):
        assert latest_calendar_tag([]) is None


def _token_ok():
    return _response(200, {"token": "tok-abc"})


def _tags_ok(tags):
    return _response(200, {"tags": tags})


class TestGetLatestGhcrTag:
    def test_happy_path_token_then_tags(self):
        with patch.object(version_check, "requests") as mock_requests:
            mock_requests.RequestException = requests.RequestException
            mock_requests.get.side_effect = [
                _token_ok(),
                _tags_ok(["2026.9.9", "2026.9.10", "latest"]),
            ]
            assert get_latest_ghcr_tag("skyfollower-core-health") == "2026.9.10"
        # token URL first, then the tags-list URL
        first_call, second_call = mock_requests.get.call_args_list
        assert first_call.args[0] == version_check._TOKEN_URL
        assert "skyfollower-core-health/tags/list" in second_call.args[0]
        assert second_call.kwargs["headers"]["Authorization"] == "Bearer tok-abc"

    def test_token_is_reused_across_images_within_a_pass(self):
        with patch.object(version_check, "requests") as mock_requests:
            mock_requests.RequestException = requests.RequestException
            mock_requests.get.side_effect = [
                _token_ok(),
                _tags_ok(["2026.1.1"]),
                _tags_ok(["2026.2.2"]),
            ]
            assert get_latest_ghcr_tag("skyfollower-a") == "2026.1.1"
            assert get_latest_ghcr_tag("skyfollower-b") == "2026.2.2"
        # 1 token fetch + 2 tags fetches, not 2 token fetches
        assert mock_requests.get.call_count == 3

    def test_no_release_tags_returns_none(self):
        with patch.object(version_check, "requests") as mock_requests:
            mock_requests.RequestException = requests.RequestException
            mock_requests.get.side_effect = [_token_ok(), _tags_ok(["dev", "latest"])]
            assert get_latest_ghcr_tag("skyfollower-core-health") is None

    def test_rate_limited_tags_list_returns_none(self):
        with patch.object(version_check, "requests") as mock_requests:
            mock_requests.RequestException = requests.RequestException
            mock_requests.get.side_effect = [_token_ok(), _response(429)]
            assert get_latest_ghcr_tag("skyfollower-core-health") is None

    def test_image_not_found_returns_none(self):
        with patch.object(version_check, "requests") as mock_requests:
            mock_requests.RequestException = requests.RequestException
            mock_requests.get.side_effect = [_token_ok(), _response(404)]
            assert get_latest_ghcr_tag("skyfollower-missing") is None

    def test_token_request_timeout_returns_none(self):
        with patch.object(version_check, "requests") as mock_requests:
            mock_requests.RequestException = requests.RequestException
            mock_requests.get.side_effect = requests.exceptions.Timeout("slow")
            assert get_latest_ghcr_tag("skyfollower-core-health") is None

    def test_tags_request_timeout_returns_none(self):
        with patch.object(version_check, "requests") as mock_requests:
            mock_requests.RequestException = requests.RequestException
            mock_requests.get.side_effect = [
                _token_ok(),
                requests.exceptions.Timeout("slow"),
            ]
            assert get_latest_ghcr_tag("skyfollower-core-health") is None

    def test_token_non_200_returns_none(self):
        with patch.object(version_check, "requests") as mock_requests:
            mock_requests.RequestException = requests.RequestException
            mock_requests.get.side_effect = [_response(503)]
            assert get_latest_ghcr_tag("skyfollower-core-health") is None

    def test_token_missing_from_body_returns_none(self):
        with patch.object(version_check, "requests") as mock_requests:
            mock_requests.RequestException = requests.RequestException
            mock_requests.get.side_effect = [_response(200, {})]
            assert get_latest_ghcr_tag("skyfollower-core-health") is None

    def test_malformed_tags_json_returns_none(self):
        with patch.object(version_check, "requests") as mock_requests:
            mock_requests.RequestException = requests.RequestException
            mock_requests.get.side_effect = [
                _token_ok(),
                _response(200, raise_json=True),
            ]
            assert get_latest_ghcr_tag("skyfollower-core-health") is None

    def test_401_clears_token_and_refetches_next_call(self):
        with patch.object(version_check, "requests") as mock_requests:
            mock_requests.RequestException = requests.RequestException
            mock_requests.get.side_effect = [
                _token_ok(),
                _response(401),
                _token_ok(),
                _tags_ok(["2026.5.5"]),
            ]
            assert get_latest_ghcr_tag("skyfollower-core-health") is None
            assert get_latest_ghcr_tag("skyfollower-core-health") == "2026.5.5"
        assert mock_requests.get.call_count == 4

    def test_never_raises_on_unexpected_error(self):
        with patch.object(version_check, "requests") as mock_requests:
            mock_requests.RequestException = requests.RequestException
            mock_requests.get.side_effect = requests.exceptions.ConnectionError("boom")
            assert get_latest_ghcr_tag("skyfollower-core-health") is None
