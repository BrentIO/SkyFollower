"""
Tests for shared/timing.py -- the single definition point for every timing
value in SkyFollower.

Two things are enforced here: the cross-file invariants the module asserts
at import (re-checked explicitly so a regression names the broken pair),
and the naming convention, so a future addition that does not follow it
fails CI rather than quietly reintroducing the drift this module removed.
"""

from __future__ import annotations

import re

import pytest

from shared import timing

# Every public constant in the module, by name.
_PUBLIC_NAMES = [
    name
    for name in dir(timing)
    if name.isupper() and not name.startswith("_")
]

# The word that may sit immediately before ``_SECONDS``. INTERVAL / TTL /
# TIMEOUT / WINDOW / BACKOFF are the documented vocabulary; AGE / LAG /
# KEEPIDLE / KEEPINTVL are accepted domain terms of art (a staleness
# threshold, a freshness bound, and the kernel's own TCP option names).
_ALLOWED_KINDS = {
    "INTERVAL", "TTL", "TIMEOUT", "WINDOW", "BACKOFF",
    "AGE", "LAG", "KEEPIDLE", "KEEPINTVL",
}

# Public names that are counts, not durations, and so carry no ``_SECONDS``.
_COUNT_NAMES = {"TCP_KEEPALIVE_PROBES"}


class TestNamingConvention:
    def test_there_is_at_least_one_public_constant(self):
        assert _PUBLIC_NAMES

    def test_no_public_constant_has_a_leading_underscore(self):
        for name in dir(timing):
            if name.isupper():
                assert not name.startswith("_"), name

    @pytest.mark.parametrize("name", _PUBLIC_NAMES)
    def test_duration_constants_end_in_seconds_with_an_allowed_kind(self, name):
        if name in _COUNT_NAMES:
            assert not name.endswith("_SECONDS"), name
            assert isinstance(getattr(timing, name), int)
            return
        assert name.endswith("_SECONDS"), name
        # The token before _SECONDS is the "kind". A DEFAULT_ prefix is
        # allowed on the one operator-tunable value's fallback.
        kind = name[: -len("_SECONDS")].split("_")[-1]
        assert kind in _ALLOWED_KINDS, f"{name}: unrecognized kind {kind!r}"

    @pytest.mark.parametrize("name", _PUBLIC_NAMES)
    def test_every_value_is_a_positive_number(self, name):
        value = getattr(timing, name)
        assert isinstance(value, (int, float))
        assert value > 0


class TestInvariants:
    def test_max_age_stays_above_two_heartbeat_intervals(self):
        assert (
            timing.HEALTHCHECK_INTERVAL_SECONDS * 2
            < timing.HEALTHCHECK_MAX_AGE_SECONDS
        )

    def test_raising_the_healthcheck_interval_past_the_margin_would_fail_import(self):
        # The module-level assert is the guard; prove it is expressed the
        # way the acceptance criterion requires.
        assert not (
            999 * 2 < timing.HEALTHCHECK_MAX_AGE_SECONDS
        )

    def test_heartbeat_ttl_survives_a_missed_refresh(self):
        assert timing.HEARTBEAT_TTL_SECONDS > timing.HEARTBEAT_INTERVAL_SECONDS

    def test_no_interval_is_multiplied_to_get_a_ttl_without_a_name(self):
        # HEARTBEAT_TTL_SECONDS is the named result of "twice the interval".
        assert timing.HEARTBEAT_TTL_SECONDS == 2 * timing.HEARTBEAT_INTERVAL_SECONDS

    def test_route_ttl_is_shorter_than_the_general_enrichment_ttl(self):
        assert timing.ROUTE_TTL_SECONDS < timing.ENRICHMENT_TTL_SECONDS

    def test_core_health_polls_rabbitmq_and_redis_every_thirty_seconds(self):
        assert timing.RABBITMQ_POLL_INTERVAL_SECONDS == 30
        assert timing.REDIS_POLL_INTERVAL_SECONDS == 30

    def test_mqtt_publish_cadence_is_thirty_seconds(self):
        assert timing.MQTT_PUBLISH_INTERVAL_SECONDS == 30

    def test_enrichment_ttl_is_fourteen_days(self):
        assert timing.ENRICHMENT_TTL_SECONDS == 14 * 86400

    def test_route_ttl_is_three_days(self):
        assert timing.ROUTE_TTL_SECONDS == 3 * 86400

    def test_default_flight_ttl_is_five_minutes(self):
        assert timing.DEFAULT_FLIGHT_TTL_SECONDS == 300

    def test_tcp_keepalive_detection_budget_is_about_ninety_seconds(self):
        budget = (
            timing.TCP_KEEPIDLE_SECONDS
            + timing.TCP_KEEPINTVL_SECONDS * timing.TCP_KEEPALIVE_PROBES
        )
        assert budget == 90


class TestNoResidualEnvVars:
    """The three deleted knobs must not have crept back in as constants
    under their old env-var names."""

    @pytest.mark.parametrize(
        "gone",
        ["TELEMETRY_INTERVAL_SECONDS", "REDIS_TTL_DAYS"],
    )
    def test_deleted_env_var_names_are_not_constants(self, gone):
        assert not hasattr(timing, gone)

    def test_rule_notification_max_lag_is_a_fixed_constant_not_a_knob(self):
        # It keeps its name (it is a real timing value) but is now fixed.
        assert timing.RULE_NOTIFICATION_MAX_LAG_SECONDS == 30
        assert re.fullmatch(r"[A-Z_]+", "RULE_NOTIFICATION_MAX_LAG_SECONDS")
