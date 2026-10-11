from unittest.mock import MagicMock, patch

import pytest
import redis

from shared.redis_client import build_redis_client, evalsha_with_reload


class TestBuildRedisClient:
    def test_passes_host_port_password(self):
        with patch("shared.redis_client.redis_lib.Redis") as mock_cls:
            build_redis_client(
                {"host": "redis.example.com", "port": 6380, "password": "secret"}
            )
            mock_cls.assert_called_once_with(
                host="redis.example.com",
                port=6380,
                password="secret",
                decode_responses=True,
            )

    def test_defaults_port_when_absent(self):
        with patch("shared.redis_client.redis_lib.Redis") as mock_cls:
            build_redis_client({"host": "redis.example.com", "password": "secret"})
            mock_cls.assert_called_once_with(
                host="redis.example.com",
                port=6379,
                password="secret",
                decode_responses=True,
            )

    def test_returns_the_constructed_client(self):
        with patch("shared.redis_client.redis_lib.Redis") as mock_cls:
            client = build_redis_client(
                {"host": "redis.example.com", "password": "secret"}
            )
            assert client is mock_cls.return_value


class TestEvalshaWithReload:
    def test_returns_result_without_reload_when_script_cached(self):
        client = MagicMock()
        client.evalsha.return_value = "ok"
        assert evalsha_with_reload(client, "sha1", "return 1", 0, "a") == "ok"
        client.evalsha.assert_called_once_with("sha1", 0, "a")
        client.script_load.assert_not_called()

    def test_reloads_and_retries_once_on_noscript(self, caplog):
        client = MagicMock()
        client.script_load.return_value = "sha1"
        client.evalsha.side_effect = [redis.exceptions.NoScriptError("NOSCRIPT"), "ok"]
        with caplog.at_level("WARNING", logger="shared.redis_client"):
            assert evalsha_with_reload(client, "sha1", "return 1", 0, "a") == "ok"
        client.script_load.assert_called_once_with("return 1")
        assert client.evalsha.call_count == 2
        assert any(r.levelname == "WARNING" for r in caplog.records)

    def test_second_failure_propagates_and_is_logged(self, caplog):
        client = MagicMock()
        client.evalsha.side_effect = redis.exceptions.NoScriptError("NOSCRIPT")
        with caplog.at_level("WARNING", logger="shared.redis_client"), \
             pytest.raises(redis.exceptions.NoScriptError):
            evalsha_with_reload(client, "sha1", "return 1", 0, "a")
        assert client.evalsha.call_count == 2
        assert "failed after reload" in caplog.text

    def test_other_errors_are_not_retried(self):
        client = MagicMock()
        client.evalsha.side_effect = redis.exceptions.ConnectionError("down")
        with pytest.raises(redis.exceptions.ConnectionError):
            evalsha_with_reload(client, "sha1", "return 1", 0, "a")
        client.script_load.assert_not_called()
