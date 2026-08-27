"""
Environment-variable configuration for every SkyFollower component.

`load_config()` returns the same nested dictionary shape the components
have always consumed (`cfg["redis"]["host"]`, `cfg.get("log_level")`, ...),
built from the per-host `.env` Compose interpolates into each service's
`environment:` block rather than from a bind-mounted file. A host running
several components therefore states each credential once, and the RabbitMQ
broker's own username/password come from the same two variables its clients
authenticate with.

A component names the blocks it needs:

    from shared.config import load_config

    cfg = load_config("rabbitmq", "mqtt", "telemetry", "receiver")

Naming them is what keeps a runner from being asked for RabbitMQ
credentials it never opens a connection with, and what lets the loader
report *every* missing variable that component actually needs in one
error -- an operator filling in a fresh `.env` should learn the whole list
on the first start, not one name per restart.
"""

from __future__ import annotations

import os
from typing import Callable, Optional

# Fixed by every compose file's bind mount. It was only ever a path inside
# the container, and no deployment has a reason to vary it.
DATA_DIR = "/app/data"

RECEIVER_SOURCE_TAGS = ("1090", "978", "EXTERNAL")

_REQUIRED = object()


class ConfigError(RuntimeError):
    """Raised when the environment is missing or malformed.

    Carries every problem found, not just the first one.
    """

    def __init__(self, problems: list[str]):
        self.problems = list(problems)
        super().__init__(
            "Invalid configuration:\n"
            + "\n".join(f"  - {p}" for p in self.problems)
        )


class ConfigLoader:
    """Reads environment variables, accumulating problems instead of raising.

    Every block helper below takes one of these so a single component's
    blocks share one problem list, and `raise_for_problems()` is called
    once at the end.
    """

    def __init__(self, environ: Optional[dict] = None):
        self._environ = environ if environ is not None else os.environ
        self.problems: list[str] = []

    def raise_for_problems(self) -> None:
        if self.problems:
            raise ConfigError(self.problems)

    def string(self, name: str, default=_REQUIRED) -> str:
        raw = self._environ.get(name, "").strip()
        if raw:
            return raw
        if default is _REQUIRED:
            self.problems.append(f"{name} is required but is not set")
            return ""
        return default

    def integer(self, name: str, default=_REQUIRED) -> int:
        raw = self._environ.get(name, "").strip()
        if not raw:
            if default is _REQUIRED:
                self.problems.append(f"{name} is required but is not set")
                return 0
            return default
        try:
            return int(raw)
        except ValueError:
            self.problems.append(f"{name} must be a whole number (got {raw!r})")
            return 0

    def number(self, name: str, default=_REQUIRED) -> float:
        raw = self._environ.get(name, "").strip()
        if not raw:
            if default is _REQUIRED:
                self.problems.append(f"{name} is required but is not set")
                return 0.0
            return default
        try:
            return float(raw)
        except ValueError:
            self.problems.append(f"{name} must be a number (got {raw!r})")
            return 0.0

    def present(self, name: str) -> None:
        """Records a problem if `name` is unset, without returning its value.

        Used for the AWS credential variables, which boto3 reads from the
        environment itself. Checking them here turns "no credentials" into a
        startup error naming the variable, rather than an opaque failure at
        the first API call.
        """
        if not self._environ.get(name, "").strip():
            self.problems.append(f"{name} is required but is not set")


# ---------------------------------------------------------------------------
# Blocks
# ---------------------------------------------------------------------------


def _own_loader(loader: Optional[ConfigLoader]) -> tuple[ConfigLoader, bool]:
    if loader is not None:
        return loader, False
    return ConfigLoader(), True


def mqtt_config(loader: Optional[ConfigLoader] = None) -> dict:
    """MQTT is optional everywhere -- host/username/password all default to
    blank rather than being required, so a component with no MQTT_HOST set
    still starts. build_mqtt_client() reads this block's `host` to decide
    whether to skip MQTT entirely."""
    loader, own = _own_loader(loader)
    block = {
        "host": loader.string("MQTT_HOST", ""),
        "port": loader.integer("MQTT_PORT", 1883),
        "username": loader.string("MQTT_USERNAME", ""),
        "password": loader.string("MQTT_PASSWORD", ""),
    }
    if own:
        loader.raise_for_problems()
    return block


def rabbitmq_config(loader: Optional[ConfigLoader] = None) -> dict:
    loader, own = _own_loader(loader)
    block = {
        "host": loader.string("RABBITMQ_HOST"),
        "port": loader.integer("RABBITMQ_PORT", 5672),
        "username": loader.string("RABBITMQ_USERNAME"),
        "password": loader.string("RABBITMQ_PASSWORD"),
    }
    if own:
        loader.raise_for_problems()
    return block


def redis_config(loader: Optional[ConfigLoader] = None) -> dict:
    loader, own = _own_loader(loader)
    block = {
        "host": loader.string("REDIS_HOST"),
        "port": loader.integer("REDIS_PORT", 6379),
        "password": loader.string("REDIS_PASSWORD"),
    }
    if own:
        loader.raise_for_problems()
    return block


def rabbitmq_management_config(loader: Optional[ConfigLoader] = None) -> dict:
    """RabbitMQ's HTTP Management API, polled only by core-health -- a
    separate port/credential pair from rabbitmq_config()'s AMQP block above,
    since core-health authenticates as its own `monitoring`-tagged user
    (broker-wide read-only), never as the scoped application user every
    other component connects with."""
    loader, own = _own_loader(loader)
    block = {
        "host": loader.string("RABBITMQ_HOST"),
        "port": loader.integer("RABBITMQ_MANAGEMENT_PORT", 15672),
        "username": loader.string("RABBITMQ_MONITORING_USERNAME"),
        "password": loader.string("RABBITMQ_MONITORING_PASSWORD"),
    }
    if own:
        loader.raise_for_problems()
    return block


def s3_config(loader: Optional[ConfigLoader] = None) -> dict:
    """The bucket name, plus a presence check on boto3's own credential
    variables.

    Only the bucket is returned: `AWS_DEFAULT_REGION`, `AWS_ACCESS_KEY_ID`
    and `AWS_SECRET_ACCESS_KEY` are boto3's documented variable names, so
    every client is constructed with no credential arguments at all and
    picks them up from its default credential chain. That is also what
    leaves room for an instance role or short-lived credentials later,
    where an explicit key/secret pair could not be supplied.
    """
    loader, own = _own_loader(loader)
    block = {"bucket": loader.string("S3_BUCKET")}
    loader.present("AWS_DEFAULT_REGION")
    loader.present("AWS_ACCESS_KEY_ID")
    loader.present("AWS_SECRET_ACCESS_KEY")
    if own:
        loader.raise_for_problems()
    return block


def athena_config(loader: Optional[ConfigLoader] = None) -> dict:
    loader, own = _own_loader(loader)
    block = {
        "workgroup": loader.string("ATHENA_WORKGROUP", "skyfollower"),
        "database": loader.string("ATHENA_DATABASE", "skyfollower"),
        "table": loader.string("ATHENA_TABLE", "archive_flights"),
    }
    if own:
        loader.raise_for_problems()
    return block


def parse_receiver_sources(raw: str) -> list[dict]:
    """Parses `RECEIVER_SOURCES` into the list-of-dicts shape the receiver
    reads.

    Comma-separated `host:port:source` triples:

        192.168.10.5:30002:1090,192.168.10.5:30978:978
        out.adsb.lol:1366:EXTERNAL

    Raises ValueError naming the offending triple, since a host commonly
    lists several and "parse failed" would leave the operator to find which.
    """
    triples = [t.strip() for t in raw.split(",")]
    triples = [t for t in triples if t]
    if not triples:
        raise ValueError("RECEIVER_SOURCES must list at least one host:port:source triple")

    sources: list[dict] = []
    for triple in triples:
        parts = triple.split(":")
        if len(parts) != 3:
            raise ValueError(
                f"RECEIVER_SOURCES entry {triple!r} must be host:port:source"
            )
        host, port, tag = (p.strip() for p in parts)
        if not host:
            raise ValueError(f"RECEIVER_SOURCES entry {triple!r} has an empty host")
        if not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ValueError(
                f"RECEIVER_SOURCES entry {triple!r} has an invalid port {port!r}"
            )
        canonical = {t.casefold(): t for t in RECEIVER_SOURCE_TAGS}.get(tag.casefold())
        if canonical is None:
            raise ValueError(
                f"RECEIVER_SOURCES entry {triple!r} has an invalid source {tag!r}; "
                f"expected one of {', '.join(RECEIVER_SOURCE_TAGS)}"
            )
        sources.append({"host": host, "port": int(port), "source": canonical})
    return sources


def receiver_config(loader: Optional[ConfigLoader] = None) -> dict:
    loader, own = _own_loader(loader)
    block: dict = {"name": loader.string("RECEIVER_NAME")}
    raw_sources = loader.string("RECEIVER_SOURCES")
    if raw_sources:
        try:
            block["sources"] = parse_receiver_sources(raw_sources)
        except ValueError as exc:
            loader.problems.append(str(exc))
            block["sources"] = []
    else:
        block["sources"] = []
    if own:
        loader.raise_for_problems()
    return block


def message_processor_config(loader: Optional[ConfigLoader] = None) -> dict:
    """Never coerces `MESSAGE_PROCESSOR_ID` to an integer: the consistent-hash
    exchange makes it any string unique across the deployment, not an ordinal."""
    loader, own = _own_loader(loader)
    block = {
        "message_processor_id": loader.string("MESSAGE_PROCESSOR_ID"),
        "latitude": loader.number("LATITUDE"),
        "longitude": loader.number("LONGITUDE"),
        "rule_notification_max_lag_seconds": loader.integer(
            "RULE_NOTIFICATION_MAX_LAG_SECONDS", 30
        ),
    }
    if own:
        loader.raise_for_problems()
    return block


def runner_config(loader: Optional[ConfigLoader] = None) -> dict:
    loader, own = _own_loader(loader)
    block = {"redis_ttl_days": loader.integer("REDIS_TTL_DAYS", 14)}
    if own:
        loader.raise_for_problems()
    return block


def telemetry_config(loader: Optional[ConfigLoader] = None) -> dict:
    loader, own = _own_loader(loader)
    block = {
        "telemetry_interval_seconds": loader.integer("TELEMETRY_INTERVAL_SECONDS", 30)
    }
    if own:
        loader.raise_for_problems()
    return block


# Blocks that land under a key of their own, and blocks whose values are
# top-level fields of the component's config.
_NESTED_BLOCKS: dict[str, Callable[[ConfigLoader], dict]] = {
    "mqtt": mqtt_config,
    "rabbitmq": rabbitmq_config,
    "rabbitmq_management": rabbitmq_management_config,
    "redis": redis_config,
    "s3": s3_config,
    "athena": athena_config,
}

_FLAT_BLOCKS: dict[str, Callable[[ConfigLoader], dict]] = {
    "receiver": receiver_config,
    "message_processor": message_processor_config,
    "runner": runner_config,
    "telemetry": telemetry_config,
}


def load_config(*blocks: str, environ: Optional[dict] = None) -> dict:
    """Builds a component's configuration from the environment.

    `blocks` names the sections this component reads; `log_level` is always
    present. Unknown block names are a programming error and raise
    immediately.
    """
    unknown = [b for b in blocks if b not in _NESTED_BLOCKS and b not in _FLAT_BLOCKS]
    if unknown:
        raise ValueError(f"Unknown config block(s): {', '.join(sorted(unknown))}")

    loader = ConfigLoader(environ)
    cfg: dict = {"log_level": loader.string("LOG_LEVEL", "info")}

    for block in blocks:
        if block in _NESTED_BLOCKS:
            cfg[block] = _NESTED_BLOCKS[block](loader)
        else:
            cfg.update(_FLAT_BLOCKS[block](loader))

    loader.raise_for_problems()
    return cfg
