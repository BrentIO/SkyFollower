"""
The single definition point for every timing value SkyFollower depends on:
loop cadences, key/record TTLs, I/O deadlines, retry backoffs, and rolling
windows that cross a component boundary or carry a cross-file invariant.

Naming convention: ``<SUBJECT>_<KIND>_SECONDS``, where KIND is one of
``INTERVAL`` (loop cadence), ``TTL`` (key/record expiry), ``TIMEOUT``
(I/O deadline), ``WINDOW`` (rolling span), or ``BACKOFF`` (retry delay). A
few values use a domain term instead (``MAX_AGE``, ``MAX_LAG``,
``KEEPIDLE``/``KEEPINTVL`` for TCP keepalive) -- deliberate, not drift.
Count constants omit ``_SECONDS`` (e.g. ``TCP_KEEPALIVE_PROBES``).

``flight_ttl_seconds`` is operator-tunable via the
``config:flight_ttl_seconds`` Redis key; only its fallback default lives
here, as ``DEFAULT_FLIGHT_TTL_SECONDS``.

Stdlib-only, no imports: ``shared/healthcheck.py`` imports from here and
must never gain a third-party dependency.
"""

from __future__ import annotations

# --- Liveness -------------------------------------------------------------

# How often each long-running component rewrites its heartbeat file.
HEALTHCHECK_INTERVAL_SECONDS = 15

# Docker's HEALTHCHECK treats the heartbeat file as stale past this age.
# Kept under 3x the write interval so one missed write is still tolerated.
HEALTHCHECK_MAX_AGE_SECONDS = 40

# --- MQTT / telemetry ----------------------------------------------------

# Cadence for the receiver/message-processor/archive-processor MQTT
# statistic topics. Purely time-based, not message-count triggered.
MQTT_PUBLISH_INTERVAL_SECONDS = 30

# --- Redis identity heartbeat ------------------------------------------------

# How often the receiver and message processor refresh their Redis
# identity/registration key (the duplicate-instance guard).
HEARTBEAT_INTERVAL_SECONDS = 30

# TTL on that key -- twice the refresh interval so one missed refresh never
# drops the claim.
HEARTBEAT_TTL_SECONDS = 60

# --- Config polling -----------------------------------------------------

# How often the message processor polls config:rules:version /
# config:areas:version and reloads on a change.
CONFIG_POLL_INTERVAL_SECONDS = 30

# --- Reconnect / retry backoff ----------------------------------------------

# Wait between reconnect attempts to RabbitMQ / Redis / S3 after a drop.
RECONNECT_BACKOFF_SECONDS = 10

# Minimum uptime before a receiver source connection's reconnect_count
# resets to zero, so the metric reflects the current flapping episode.
RECONNECT_COUNT_RESET_AGE_SECONDS = 30

# Deadline on a RabbitMQ connection sitting in the broker's blocked state
# (publishers halted by a resource alarm). pika tears the connection down
# once this elapses, so the receiver reconnects instead of staying wedged
# inside a basic_publish that would never return.
RABBITMQ_BLOCKED_CONNECTION_TIMEOUT_SECONDS = 30

# Minimum spacing between drain attempts against a single fallback-queue
# row, regardless of how often the caller invokes drain(). Default of
# FallbackQueue's ``min_retry_interval_seconds`` parameter.
FALLBACK_RETRY_BACKOFF_SECONDS = 30

# --- Rolling windows --------------------------------------------------------

# Rolling window over which _RateTracker measures messages-per-second in
# the receiver and the message processor.
RATE_WINDOW_SECONDS = 30

# Confirmation window for a reserved squawk sourced from a message that
# could not be CRC-verified. Ident has its own, independently-derived
# values below -- do not reuse this one for ident.
PARITY_ERROR_CONFIRM_WINDOW_SECONDS = 30

# Repeated-sightings confirmation for an ident sourced from a message the
# message processor could not CRC-verify. The window is set far larger
# than any realistic flight because DF20/21 Comm-B replies are
# interrogation-driven, not periodic -- a tight window could simply never
# see a second sighting for an aircraft not under frequent SSR polling.
# Only needs to outlast one flight; `Flight.pending_ident` is destroyed on
# eviction regardless.
IDENT_CONFIRM_COUNT = 2
IDENT_CONFIRM_WINDOW_SECONDS = 24 * 60 * 60

# --- Message-age gating -----------------------------------------------------

# Maximum age of a source message before a downstream emission based on it
# is suppressed -- an older match is still recorded, just not emitted.
MAX_MESSAGE_LAG_SECONDS = 30

# --- core-health polling --------------------------------------------------

# How often core-health polls RabbitMQ's HTTP management API. RabbitMQ
# aggregates stats broker-side on a ~5s interval, so this stays fresh
# without re-reading unchanged cached data.
RABBITMQ_POLL_INTERVAL_SECONDS = 30

# How often core-health issues Redis INFO / MEMORY STATS.
REDIS_POLL_INTERVAL_SECONDS = 30

# Read-half deadline on each core-health HTTP request to the RabbitMQ
# management API, passed as the (connect, read) tuple with
# HTTP_CONNECT_TIMEOUT_SECONDS below. shared/version_check.py also imports
# this name for its own single-float GHCR request timeout, so its
# shape/meaning must stay a bare float.
HTTP_TIMEOUT_SECONDS = 10

# Connect-half deadline for that same request, kept separate from
# HTTP_TIMEOUT_SECONDS so requests/urllib3 can bound each phase
# independently.
HTTP_CONNECT_TIMEOUT_SECONDS = 5

# Wall-clock deadline core-health places on a single RabbitMQ Management
# API GET, layered on top of the (connect, read) timeout above. A
# connection left half-open by a broker restart can go unnoticed by
# requests/urllib3's own timeout machinery and hang the polling thread
# indefinitely; the GET runs on a short-lived daemon thread that the
# poller joins with this deadline, discarding and recreating the session
# if it fires. Set above HTTP_CONNECT_TIMEOUT_SECONDS + HTTP_TIMEOUT_SECONDS
# (see the cross-file invariant below) so a well-behaved request always
# finishes first.
RABBITMQ_POLL_HANG_TIMEOUT_SECONDS = 25

# How often core-health polls the container registry for each component's
# latest published image tag, to drive Home Assistant's "update available"
# entities. Deliberately slow -- roughly fifty images per pass, and
# published tags only move on a release.
GHCR_VERSION_CHECK_INTERVAL_SECONDS = 86400

# Delay before core-health's first container-registry poll after startup,
# so retained Home Assistant discovery configs have time to arrive first.
GHCR_VERSION_CHECK_STARTUP_DELAY_SECONDS = 30

# --- Receiver source sockets ----------------------------------------------

# TCP keepalive timers on every readsb source socket -- the receiver only
# reads from these, so a peer that vanishes without FIN/RST is otherwise
# indistinguishable from a quiet feed. First probe after 60s idle, then 3
# probes 10s apart.
TCP_KEEPIDLE_SECONDS = 60
TCP_KEEPINTVL_SECONDS = 10
TCP_KEEPALIVE_PROBES = 3

# Minimum spacing between "N unparseable lines" summary warnings, per
# source connection.
UNPARSEABLE_WARNING_INTERVAL_SECONDS = 60

# --- Archive processor --------------------------------------------------

# TTL on the archive:last_segment:{icao_hex} pointer used for split-flight
# stitching.
STITCH_POINTER_TTL_SECONDS = 86400

# --- Runner enrichment TTLs ----------------------------------------------

# TTL every data runner sets on the enrichment keys it writes
# (registration / operator / type / airport / livery).
ENRICHMENT_TTL_SECONDS = 14 * 86400

# TTL the vrs-standing-data runner sets on route:{ident}. Shorter than
# ENRICHMENT_TTL_SECONDS because the upstream route data refreshes daily.
ROUTE_TTL_SECONDS = 3 * 86400

# --- Flight TTL default -------------------------------------------------

# Fallback for flight_ttl_seconds when config:flight_ttl_seconds is unset.
DEFAULT_FLIGHT_TTL_SECONDS = 300

# --- Map service ----------------------------------------------------------

# How often the map service flushes buffered position/metadata/stale/remove
# events to each connected WebSocket client, coalescing updates arriving
# within the window into one frame.
MAP_WS_BATCH_INTERVAL_SECONDS = 0.25

# Fallback for MAP_UDP_MIN_POSITION_INTERVAL_SECONDS when unset: minimum
# spacing, per icao_hex, between `position` datagrams the message
# processor's _MapUdpPublisher sends toward the map service.
DEFAULT_MAP_UDP_MIN_POSITION_INTERVAL_SECONDS = 1

# How often each message processor's _map_heartbeat_loop ticks. The loop
# skips sending a `heartbeat` datagram on a tick if a `position`/`metadata`
# datagram already went out within this window.
MAP_HEARTBEAT_INTERVAL_SECONDS = 5

# How often the message processor unconditionally resends every active
# flight's `metadata` datagram, regardless of field changes. The map
# service's own Redis carries no persistence, so this periodic sweep is
# what lets a map-service restart recover within one MAP_EVICT_SECONDS
# window.
MAP_METADATA_RESEND_INTERVAL_SECONDS = 60

# Per-processor status thresholds the map service applies to
# now - last_seen (updated by any map UDP message type carrying
# processor_id). green ("Connected") at or under this age -- three missed
# heartbeat intervals.
MAP_PROCESSOR_GREEN_MAX_AGE_SECONDS = 15

# amber ("Reconnecting") from the green threshold up to this age; red
# ("Disconnected") beyond it, or if never seen.
MAP_PROCESSOR_AMBER_MAX_AGE_SECONDS = 60

# How long finalised daily range-outline snapshots are kept on disk before
# being deleted on the next snapshot write; requests past this window
# return HTTP 404.
MAP_RANGE_OUTLINE_TTL_SECONDS = 30 * 86400

# How often the map service rewrites the in-progress day's range-outline
# snapshot to disk (only if changed) and checks for UTC-date rollover.
MAP_RANGE_OUTLINE_SNAPSHOT_INTERVAL_SECONDS = 60

# --- Rule trigger counters --------------------------------------------------

# TTL on each rule_triggers:{identifier}:{date} day key -- one day of
# margin past the 30-day window management-ui's backend sums. The lifetime
# key (rule_triggers:{identifier}:lifetime) never expires.
RULE_TRIGGER_DAY_TTL_SECONDS = 31 * 86400


# --- Raw frame capture (forensic, CAPTURE_RAW_FRAMES) --------------------

# TTL on the skyfollower-archive-raw-frames queue (an `x-message-ttl` queue
# argument, so message-processor multiplies this by 1000 for RabbitMQ's
# millisecond units) -- a short-lived, manually-drained forensic queue, not
# the permanent archive.
RAW_FRAMES_QUEUE_TTL_SECONDS = 8 * 3600


# --- Cross-file invariants ------------------------------------------------
# Checked at import so a later edit to one value cannot silently break the
# contract it shares with another.

assert HEALTHCHECK_INTERVAL_SECONDS * 2 < HEALTHCHECK_MAX_AGE_SECONDS, (
    "HEALTHCHECK_MAX_AGE_SECONDS must stay above two heartbeat intervals"
)

assert HEARTBEAT_TTL_SECONDS > HEARTBEAT_INTERVAL_SECONDS, (
    "HEARTBEAT_TTL_SECONDS must exceed HEARTBEAT_INTERVAL_SECONDS"
)

assert ROUTE_TTL_SECONDS < ENRICHMENT_TTL_SECONDS, (
    "ROUTE_TTL_SECONDS is meant to be shorter than ENRICHMENT_TTL_SECONDS"
)

assert MAP_PROCESSOR_GREEN_MAX_AGE_SECONDS > MAP_HEARTBEAT_INTERVAL_SECONDS, (
    "MAP_PROCESSOR_GREEN_MAX_AGE_SECONDS must exceed MAP_HEARTBEAT_INTERVAL_SECONDS"
)
assert MAP_PROCESSOR_AMBER_MAX_AGE_SECONDS > MAP_PROCESSOR_GREEN_MAX_AGE_SECONDS, (
    "MAP_PROCESSOR_AMBER_MAX_AGE_SECONDS must exceed MAP_PROCESSOR_GREEN_MAX_AGE_SECONDS"
)

assert RABBITMQ_POLL_HANG_TIMEOUT_SECONDS > HTTP_CONNECT_TIMEOUT_SECONDS + HTTP_TIMEOUT_SECONDS, (
    "RABBITMQ_POLL_HANG_TIMEOUT_SECONDS must exceed the (connect, read) timeout it backstops"
)
