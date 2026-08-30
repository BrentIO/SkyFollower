"""
The single definition point for every timing value SkyFollower depends on.

Loop cadences, key/record TTLs, I/O deadlines, retry backoffs and rolling
windows used to be scattered across the tree as bare integer literals,
per-file module constants under a dozen different names, and a handful of
environment variables that only governed internal behaviour. This module
collects everything that either crosses a component boundary or carries a
cross-file invariant, so there is exactly one place to read and one place
to change.

Naming convention
-----------------
``<SUBJECT>_<KIND>_SECONDS``

* ``INTERVAL`` -- a recurring loop cadence
* ``TTL``      -- a key or record expiry
* ``TIMEOUT``  -- a single I/O deadline
* ``WINDOW``   -- a rolling span measured backwards from now
* ``BACKOFF``  -- the delay between retry attempts

A few values keep a domain term of art in place of one of those words
(``MAX_AGE`` for a staleness threshold, ``MAX_LAG`` for a freshness bound,
``KEEPIDLE`` / ``KEEPINTVL`` for the kernel's own TCP-keepalive option
names) -- those spellings are deliberate, not drift.

No name in this module has a leading underscore: it is a public module and
every constant is meant to be imported by name. Values that are counts
rather than durations omit the ``_SECONDS`` suffix (``TCP_KEEPALIVE_PROBES``).

``flight_ttl_seconds`` is intentionally *not* here as a fixed value: it is
the one timing value an operator may tune per deployment, carried in the
``config:flight_ttl_seconds`` Redis key and read once at startup by the
message processor and the archive processor. Only its fallback default
lives here, as ``DEFAULT_FLIGHT_TTL_SECONDS``.

Stdlib-only, no imports: ``shared/healthcheck.py`` -- the dependency-free
Docker HEALTHCHECK entrypoint -- imports from here, so this module must
never grow a third-party dependency.
"""

from __future__ import annotations

# --- Liveness -------------------------------------------------------------

# How often each long-running component (receiver, message processor,
# archive processor, core-health) rewrites its /app/health/heartbeat file
# while genuinely connected to its upstreams.
HEALTHCHECK_INTERVAL_SECONDS = 15

# Docker's HEALTHCHECK treats the heartbeat file as stale -- and the
# container as unhealthy -- once it is older than this. A shade under three
# write intervals: one missed write is normal jitter, two in a row is not.
HEALTHCHECK_MAX_AGE_SECONDS = 40

# --- MQTT / telemetry ----------------------------------------------------

# Cadence at which the receiver, message processor and archive processor
# publish their MQTT statistic topics. Purely time-based -- no component
# publishes early on a message-count trigger.
MQTT_PUBLISH_INTERVAL_SECONDS = 30

# --- Redis identity heartbeat ------------------------------------------------

# How often the receiver and message processor refresh the TTL on their
# Redis identity/registration key (the duplicate-instance guard).
HEARTBEAT_INTERVAL_SECONDS = 30

# TTL set on that key. Twice the refresh interval, so a single missed
# refresh never drops the claim. Named outright rather than derived as
# ``HEARTBEAT_INTERVAL_SECONDS * 2`` at each call site.
HEARTBEAT_TTL_SECONDS = 60

# --- Config polling -----------------------------------------------------

# How often the message processor polls config:rules:version /
# config:areas:version and reloads on a change.
CONFIG_POLL_INTERVAL_SECONDS = 30

# --- Reconnect / retry backoff ----------------------------------------------

# Wait between reconnect attempts to RabbitMQ / Redis / S3 after a drop.
RECONNECT_BACKOFF_SECONDS = 10

# Deadline on a RabbitMQ connection sitting in the broker's blocked state --
# publishers halted by a resource alarm (disk-free or high memory) while the
# TCP connection itself stays up. pika tears the connection down once this
# elapses, so the receiver's sole publishing thread reconnects and
# re-validates instead of staying wedged inside a basic_publish that will
# never return. Comfortably longer than a brief alarm flap, short enough
# that a genuinely stuck alarm cannot hide behind a healthy-looking
# connection.
RABBITMQ_BLOCKED_CONNECTION_TIMEOUT_SECONDS = 30

# Minimum spacing between drain attempts against a single fallback-queue
# row, independent of how often the caller invokes drain() -- keeps a
# flapping connection from burning through the retry threshold in well
# under a real recovery window. Default of FallbackQueue's
# ``min_retry_interval_seconds`` parameter.
FALLBACK_RETRY_BACKOFF_SECONDS = 30

# --- Rolling windows --------------------------------------------------------

# Rolling window over which _RateTracker measures messages-per-second, in
# the receiver and the message processor.
RATE_WINDOW_SECONDS = 30

# Trailing window over which the message processor requires repeated
# sightings before trusting a reserved squawk / an ident sourced from a
# message it could not CRC-verify.
PARITY_ERROR_CONFIRM_WINDOW_SECONDS = 30

# --- Rule notifications ----------------------------------------------------

# Maximum age of the triggering message for a rule match to still be
# published to MQTT -- an older match is recorded but not announced, since
# the notification would no longer be actionable.
RULE_NOTIFICATION_MAX_LAG_SECONDS = 30

# --- core-health polling --------------------------------------------------

# How often core-health polls RabbitMQ's HTTP management API. RabbitMQ
# aggregates its stats broker-side on a ~5s internal interval, so this
# stays comfortably fresh without re-reading unchanged cached data.
RABBITMQ_POLL_INTERVAL_SECONDS = 30

# How often core-health issues Redis INFO / MEMORY STATS. Redis's health
# signals (memory, persistence status, error counts) do not move on a
# sub-minute timescale in ways that matter here.
REDIS_POLL_INTERVAL_SECONDS = 30

# Deadline on each core-health HTTP request to the RabbitMQ management API.
HTTP_TIMEOUT_SECONDS = 10

# --- Receiver source sockets ----------------------------------------------

# TCP keepalive timers on every readsb source socket. The receiver only
# ever reads from these, so a peer that vanishes without a clean FIN/RST is
# otherwise indistinguishable from a quiet feed. First probe after 60s
# idle, then 3 probes 10s apart -- a ~90s detection budget, far below the
# ~2-hour OS default, without false-positiving on a legitimately quiet
# feed (a live peer answers the probes).
TCP_KEEPIDLE_SECONDS = 60
TCP_KEEPINTVL_SECONDS = 10
TCP_KEEPALIVE_PROBES = 3

# Minimum spacing between "N unparseable lines" summary warnings, per
# source connection -- keeps a genuine format mismatch visible without
# flooding the log at high traffic volume.
UNPARSEABLE_WARNING_INTERVAL_SECONDS = 60

# --- Archive processor --------------------------------------------------

# TTL on the archive:last_segment:{icao_hex} pointer used for split-flight
# stitching. One day: long enough to bridge a mid-flight queue rebind,
# short enough that a genuinely completed flight's pointer clears itself.
STITCH_POINTER_TTL_SECONDS = 86400

# --- Runner enrichment TTLs ----------------------------------------------

# TTL every data runner sets on the enrichment keys it writes
# (registration / operator / type / airport / livery). Long enough that a
# single missed weekly run never expires live data.
ENRICHMENT_TTL_SECONDS = 14 * 86400

# TTL the vrs-standing-data runner sets on route:{ident}. Shorter than
# ENRICHMENT_TTL_SECONDS on purpose: the upstream route data refreshes
# daily, so a 3-day ceiling keeps it from silently going stale for over a
# week if a run or two is missed.
ROUTE_TTL_SECONDS = 3 * 86400

# --- Flight TTL default -------------------------------------------------

# Fallback for flight_ttl_seconds when config:flight_ttl_seconds is unset.
# Aircraft are held in the active store this long after their last message
# before the flight is considered complete. This is the one timing value
# an operator may override (via that Redis key), which is why only the
# default lives here.
DEFAULT_FLIGHT_TTL_SECONDS = 300


# --- Cross-file invariants ------------------------------------------------
# Checked at import so a later edit to one value cannot silently break the
# contract it shares with another.

# Two missed heartbeat writes must still leave the file inside the max-age
# window; the third is what legitimately trips the container to unhealthy.
assert HEALTHCHECK_INTERVAL_SECONDS * 2 < HEALTHCHECK_MAX_AGE_SECONDS, (
    "HEALTHCHECK_MAX_AGE_SECONDS must stay above two heartbeat intervals"
)

# A single missed refresh must not drop the Redis identity claim.
assert HEARTBEAT_TTL_SECONDS > HEARTBEAT_INTERVAL_SECONDS, (
    "HEARTBEAT_TTL_SECONDS must exceed HEARTBEAT_INTERVAL_SECONDS"
)

# Route data is deliberately the more perishable of the two runner TTLs.
assert ROUTE_TTL_SECONDS < ENRICHMENT_TTL_SECONDS, (
    "ROUTE_TTL_SECONDS is meant to be shorter than ENRICHMENT_TTL_SECONDS"
)
