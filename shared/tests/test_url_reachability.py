from unittest.mock import MagicMock, patch

import pytest
import requests

from shared.url_reachability import assert_url_reachable


def _mock_response(status_code):
    resp = MagicMock()
    resp.status_code = status_code
    resp.__enter__.return_value = resp
    resp.__exit__.return_value = False
    return resp


class TestAssertUrlReachable:
    def test_200_passes(self):
        with patch("shared.url_reachability.requests.get", return_value=_mock_response(200)):
            assert_url_reachable("https://example.com", "example-runner")

    def test_2xx_passes(self):
        with patch("shared.url_reachability.requests.get", return_value=_mock_response(204)):
            assert_url_reachable("https://example.com", "example-runner")

    def test_404_fails(self):
        with patch("shared.url_reachability.requests.get", return_value=_mock_response(404)):
            with pytest.raises(AssertionError, match="404"):
                assert_url_reachable("https://example.com", "example-runner")

    def test_500_fails(self):
        with patch("shared.url_reachability.requests.get", return_value=_mock_response(500)):
            with pytest.raises(AssertionError, match="500"):
                assert_url_reachable("https://example.com", "example-runner")

    def test_405_fails(self):
        with patch("shared.url_reachability.requests.get", return_value=_mock_response(405)):
            with pytest.raises(AssertionError, match="405"):
                assert_url_reachable("https://example.com", "example-runner")

    def test_failure_message_includes_runner_name_and_url(self):
        with patch("shared.url_reachability.requests.get", return_value=_mock_response(503)):
            with pytest.raises(AssertionError, match="example-runner.*https://example.com.*503"):
                assert_url_reachable("https://example.com", "example-runner")

    def test_request_exception_fails(self):
        with patch("shared.url_reachability.requests.get", side_effect=requests.ConnectionError("refused")):
            with pytest.raises(AssertionError, match="example-runner"):
                assert_url_reachable("https://example.com", "example-runner")

    def test_timeout_exception_fails(self):
        with patch("shared.url_reachability.requests.get", side_effect=requests.Timeout("timed out")):
            with pytest.raises(AssertionError):
                assert_url_reachable("https://example.com", "example-runner")

    def test_response_body_never_read(self):
        resp = _mock_response(200)
        with patch("shared.url_reachability.requests.get", return_value=resp):
            assert_url_reachable("https://example.com", "example-runner")
        resp.__exit__.assert_called_once()
        resp.iter_content.assert_not_called()

    def test_passes_headers_and_verify_through(self):
        with patch("shared.url_reachability.requests.get", return_value=_mock_response(200)) as mock_get:
            assert_url_reachable(
                "https://example.com", "example-runner",
                headers={"User-Agent": "test-agent"}, verify=False, timeout=5,
            )
        mock_get.assert_called_once_with(
            "https://example.com", headers={"User-Agent": "test-agent"},
            verify=False, timeout=5, allow_redirects=True, stream=True,
        )

    def test_default_headers_none_and_verify_true(self):
        with patch("shared.url_reachability.requests.get", return_value=_mock_response(200)) as mock_get:
            assert_url_reachable("https://example.com", "example-runner")
        mock_get.assert_called_once_with(
            "https://example.com", headers=None, verify=True, timeout=30,
            allow_redirects=True, stream=True,
        )

    def test_allow_redirects_is_true(self):
        with patch("shared.url_reachability.requests.get", return_value=_mock_response(200)) as mock_get:
            assert_url_reachable("https://example.com", "example-runner")
        assert mock_get.call_args.kwargs["allow_redirects"] is True

    def test_stream_is_true(self):
        with patch("shared.url_reachability.requests.get", return_value=_mock_response(200)) as mock_get:
            assert_url_reachable("https://example.com", "example-runner")
        assert mock_get.call_args.kwargs["stream"] is True
