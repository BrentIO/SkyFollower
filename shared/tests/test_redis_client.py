from unittest.mock import patch

from shared.redis_client import build_redis_client


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
