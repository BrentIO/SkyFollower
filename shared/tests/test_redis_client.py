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
                username=None,
                password="secret",
                decode_responses=True,
            )

    def test_defaults_port_when_absent(self):
        with patch("shared.redis_client.redis_lib.Redis") as mock_cls:
            build_redis_client({"host": "redis.example.com", "password": "secret"})
            mock_cls.assert_called_once_with(
                host="redis.example.com",
                port=6379,
                username=None,
                password="secret",
                decode_responses=True,
            )

    def test_returns_the_constructed_client(self):
        with patch("shared.redis_client.redis_lib.Redis") as mock_cls:
            client = build_redis_client(
                {"host": "redis.example.com", "password": "secret"}
            )
            assert client is mock_cls.return_value

    def test_username_absent_defaults_to_none(self):
        """Every caller before core-health omits `username` entirely --
        this must be indistinguishable from redis-py's own default (the
        "default" user), not a behavior change for any existing caller."""
        with patch("shared.redis_client.redis_lib.Redis") as mock_cls:
            build_redis_client({"host": "redis.example.com", "password": "secret"})
            assert mock_cls.call_args.kwargs["username"] is None

    def test_username_passed_through_when_present(self):
        """core-health's ACL-scoped monitoring user is the first caller to
        set this."""
        with patch("shared.redis_client.redis_lib.Redis") as mock_cls:
            build_redis_client(
                {
                    "host": "redis.example.com",
                    "username": "skyfollower-monitoring",
                    "password": "secret",
                }
            )
            mock_cls.assert_called_once_with(
                host="redis.example.com",
                port=6379,
                username="skyfollower-monitoring",
                password="secret",
                decode_responses=True,
            )

    def test_empty_string_username_treated_as_absent(self):
        """shared/config.py's redis_monitoring_config() always populates a
        `username` key (possibly ""), unlike every other caller's config
        block which omits the key entirely -- both must resolve to the
        same "no explicit username" behavior."""
        with patch("shared.redis_client.redis_lib.Redis") as mock_cls:
            build_redis_client(
                {"host": "redis.example.com", "username": "", "password": "secret"}
            )
            assert mock_cls.call_args.kwargs["username"] is None
