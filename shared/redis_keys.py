"""
Centralised Redis key name functions for SkyFollower.

All components import from here so key names stay consistent across the
codebase. Functions are used instead of string constants so that parameters
are always explicit and typos in key names are caught by the type checker.
"""

import re

_VALID_PERIODS = frozenset({"hour", "today", "lifetime"})
_VALID_ARCHIVE_PERIODS = frozenset({"hour", "today"})
# Receiver per-connection message counts have no "lifetime": that total is
# a device-local, in-memory figure the receiver publishes directly (it
# resets on a receiver restart), never persisted to Redis.
_VALID_RECEIVER_PERIODS = frozenset({"hour", "today"})
_VALID_OPERATOR_MISSES_PERIODS = frozenset({"today", "lifetime"})

# RediSearch index over all aircraft:mictronics:{hex} JSON documents (Mictronics).
# Indexed fields: $.icao_hex, $.registration
AIRCRAFT_MICTRONICS_SEARCH_INDEX = "idx:aircraft:mictronics"

# RediSearch index over all aircraft:registry:{hex} JSON documents (country runners).
# Indexed fields: $.icao_hex, $.registration
AIRCRAFT_REGISTRY_SEARCH_INDEX = "idx:aircraft:registry"

# RediSearch index over all airport:{icao_code} JSON documents.
# Supports lookup by ICAO code or IATA code.
AIRPORT_SEARCH_INDEX = "idx:airport"


def aircraft_mictronics_key(icao_hex: str) -> str:
    """Mictronics aircraft enrichment record. aircraft:mictronics:{icao_hex}"""
    return f"aircraft:mictronics:{icao_hex.upper()}"


def aircraft_registry_key(icao_hex: str) -> str:
    """Country-runner aircraft enrichment record. aircraft:registry:{icao_hex}"""
    return f"aircraft:registry:{icao_hex.upper()}"


def aircraft_livery_key(icao_hex: str) -> str:
    """
    Special-livery enrichment record, written by the airportwebcams-special-liveries
    runner. Deep-merged last by shared/lua/merge_aircraft.lua, so it wins
    over both aircraft:mictronics and aircraft:registry on any field overlap.
    aircraft:livery:{icao_hex}
    """
    return f"aircraft:livery:{icao_hex.upper()}"


def operator_key(designator: str) -> str:
    """Airline operator record. operator:{designator}"""
    return f"operator:{designator.upper()}"


def aircraft_type_key(designator: str) -> str:
    """Aircraft type-designator reference record. aircraft:type:{designator}"""
    return f"aircraft:type:{designator.upper()}"


_FLIGHT_IDENT_PATTERN = re.compile(r"^([A-Za-z]+)(\d+)([A-Za-z]*)$")


def normalize_flight_ident(ident: str) -> str:
    """Strips leading zeros from a flight ident's numeric portion by
    round-tripping it through int() -- "AFR0096" -> "AFR96", "IBE0339" ->
    "IBE339", "VIR096K" -> "VIR96K". Idents that don't match the
    operator-prefix + flight-number (+ optional trailing suffix letter)
    shape (e.g. a hyphenated registration used as ident) are returned
    unchanged -- this is purely a route-lookup key normalization, never
    applied to the stored/displayed ident itself. Idempotent: an
    already-unpadded ident passes through unchanged, so it's safe to apply
    on both the write side (VRS ingestion) and the read side (every
    lookup) without the two ever disagreeing.
    """
    match = _FLIGHT_IDENT_PATTERN.match(ident)
    if not match:
        return ident
    prefix, digits, suffix = match.groups()
    return f"{prefix}{int(digits)}{suffix}"


def route_key(ident: str) -> str:
    """
    Raw VRS standing-data route string for a callsign, written by the
    vrs-standing-data runner. Plain Redis string (not JSON) — e.g. GET
    route:AAL15 -> "KMIA-KJFK-KMIA" — passed through unchanged from the
    source AirportCodes column. route:{ident}
    """
    return f"route:{ident.upper()}"


def airport_key(icao_code: str) -> str:
    """Airport metadata record. airport:{icao_code}"""
    return f"airport:{icao_code.upper()}"


def config_rules_key() -> str:
    """Active rules JSON array. config:rules"""
    return "config:rules"


def config_rules_version_key() -> str:
    """SHA-256 hash of config:rules content; processors poll this. config:rules:version"""
    return "config:rules:version"


def config_areas_key() -> str:
    """Active GeoJSON FeatureCollection of named areas. config:areas"""
    return "config:areas"


def config_areas_version_key() -> str:
    """SHA-256 hash of config:areas content; processors poll this. config:areas:version"""
    return "config:areas:version"


def config_flight_ttl_seconds_key() -> str:
    """
    Shared flight_ttl_seconds value, read by both the message processor and
    the archive processor. Read once at startup and cached, not hot-reloaded —
    a changed value takes effect on the next container restart. Callers
    should default to 300 if unset.
    config:flight_ttl_seconds
    """
    return "config:flight_ttl_seconds"


def message_processor_heartbeat_key(message_processor_id: str) -> str:
    """
    Message processor liveness key used to detect duplicate
    MESSAGE_PROCESSOR_ID on startup. Set with NX + TTL = 2 × telemetry_interval.
    Same fleet-wide flat ID used for the compose service/container name and
    the RabbitMQ queue name -- no separate per-key naming scheme.
    skyfollower-message-processor-{id}
    """
    return f"skyfollower-message-processor-{message_processor_id}"


def receiver_heartbeat_key(receiver_id: str) -> str:
    """
    Receiver liveness/claim key -- mirrors message_processor_heartbeat_key()
    exactly. Set with NX + TTL = 2 × telemetry_interval on first-ever claim
    of a RECEIVER_NAME, then refreshed (unconditional EXPIRE, never a
    second NX) by the receiver's own _heartbeat_loop for as long as it's
    running. A local identity already persisted to {data_dir}/receiver_id
    resumes heartbeating this same key without ever re-running SET NX
    against it.
    skyfollower-receiver-{id}
    """
    return f"skyfollower-receiver-{receiver_id}"


def receiver_registry_index_key() -> str:
    """
    SET of every currently-claimed receiver identity -- lets core-health
    enumerate live receivers in O(receiver count) via SMEMBERS instead of
    a keyspace SCAN (receivers have no RabbitMQ queue of their own to
    enumerate through, unlike message processors). Added to at claim time
    and re-added (idempotent SADD) on every heartbeat tick, so a restart
    that resumes an already-persisted identity (no claim call at all) still
    ends up back in the index within one MQTT_PUBLISH_INTERVAL_SECONDS. No TTL
    on the set itself -- self-heals the same way archive_search_index_key
    does: a caller that SMEMBERS this set and finds a member's
    receiver:registration:{id} entry already expired just SREMs that stale
    member itself.
    receiver:index
    """
    return "receiver:index"


def receiver_registration_key(receiver_id: str) -> str:
    """
    Per-receiver registration entry core-health reads to construct the
    exact HA discovery/telemetry payloads the receiver's own
    _publish_ha_autodiscovery()/_publish_telemetry() would have produced
    for the Redis-backed period-count sensors. JSON array
    of {host, port, source} triples -- the same shape as the receiver's own
    `sources[]` config -- since the claimed name already *is* the identity
    (rid in topic paths), there's no separate UUID left to carry alongside
    it. Refreshed alongside the heartbeat, TTL'd the same way (2 ×
    telemetry_interval); a missing entry means core-health should treat
    that name as no longer live, not as an error.
    receiver:registration:{id}
    """
    return f"receiver:registration:{receiver_id}"


def receiver_message_count_key(receiver_id: str, connection_id: str, period: str) -> str:
    """
    Redis-backed period counter for one receiver connection's message
    count -- populated via
    shared/lua/incr_period_counter.lua, flushed from the receiver's
    telemetry thread, never the per-message hot path. `connection_id` is
    the same sanitized `{host}_{port}` identifier already used in that
    connection's MQTT topic/HA entity segment (_sanitize_mqtt_id'd host and
    port), so a Redis key and its corresponding
    messages_{connection_id}_total_{period} MQTT field are trivially
    derivable from each other -- what core-health's publishing-parity
    mimicry depends on. period must be one of: hour, today. There is no
    "lifetime" period here: that total is a device-local, in-memory figure
    the receiver publishes directly (resets on its restart), never Redis.
    Missing key means the count is genuinely 0, not unavailable -- same
    principle as the message processor's own miss counters.
    metrics:receiver:{id}:{connection_id}:messages:{period}
    """
    if period not in _VALID_RECEIVER_PERIODS:
        raise ValueError(
            f"period must be one of {_VALID_RECEIVER_PERIODS}, got: {period!r}"
        )
    return f"metrics:receiver:{receiver_id}:{connection_id}:messages:{period}"


def metrics_registration_misses_key(message_processor_id: str, period: str) -> str:
    """
    Counter for aircraft enrichment (registration) lookup misses per message
    processor -- an icao_hex with no matching aircraft:mictronics/registry/
    livery record. Operator-lookup misses are a separate failure type with
    their own dedicated key, metrics_operator_misses_key() -- not counted
    here.
    period must be one of: hour, today, lifetime.
    metrics:message_processor:{id}:registration_misses:{period}
    """
    if period not in _VALID_PERIODS:
        raise ValueError(f"period must be one of {_VALID_PERIODS}, got: {period!r}")
    return f"metrics:message_processor:{message_processor_id}:registration_misses:{period}"


def metrics_operator_misses_key(message_processor_id: str, period: str) -> str:
    """
    Counter for operator:{designator} lookup misses per message processor --
    a distinct failure type from an aircraft registration miss (see
    metrics_registration_misses_key()). No "hour" period: operator misses
    are lower-volume and only tracked today/lifetime.
    period must be one of: today, lifetime.
    metrics:message_processor:{id}:operator_misses:{period}
    """
    if period not in _VALID_OPERATOR_MISSES_PERIODS:
        raise ValueError(
            f"period must be one of {_VALID_OPERATOR_MISSES_PERIODS}, got: {period!r}"
        )
    return f"metrics:message_processor:{message_processor_id}:operator_misses:{period}"


def metrics_total_messages_processed_key(message_processor_id: str, period: str) -> str:
    """
    Counter for every message a message processor attempted to decode
    (including CRC-corrupt/no-content messages) -- incremented at the same
    point messages_per_second's own _RateTracker.record() is, per message
    processor.
    period must be one of: hour, today, lifetime.
    metrics:message_processor:{id}:total_messages_processed:{period}
    """
    if period not in _VALID_PERIODS:
        raise ValueError(f"period must be one of {_VALID_PERIODS}, got: {period!r}")
    return f"metrics:message_processor:{message_processor_id}:total_messages_processed:{period}"


def rule_trigger_lifetime_key(identifier: str) -> str:
    """
    Lifetime count of times a rule has fired (once per flight, the first
    time it matches -- not once per message). Never expires. Keyed by rule
    identifier, so a deleted-then-recreated identifier starts clean;
    DELETE /api/rules/{identifier} removes this key explicitly.
    rule_triggers:{identifier}:lifetime
    """
    return f"rule_triggers:{identifier}:lifetime"


def rule_trigger_day_key(identifier: str, date: str) -> str:
    """
    Count of times a rule fired on one UTC day (`date` is YYYY-MM-DD). Set
    with a 31-day TTL (shared.timing.RULE_TRIGGER_DAY_TTL_SECONDS), so old
    days clean themselves up with no sweep job. The management-ui backend
    sums the last 30 of these for a rule's rolling-30-day figure.
    rule_triggers:{identifier}:{date}
    """
    return f"rule_triggers:{identifier}:{date}"


def metrics_flights_archived_key(period: str) -> str:
    """
    Counter for flights successfully written to S3 by the archive processor.
    period must be one of: hour, today.
    metrics:archive:flights_archived:{period}
    """
    if period not in _VALID_ARCHIVE_PERIODS:
        raise ValueError(f"period must be one of {_VALID_ARCHIVE_PERIODS}, got: {period!r}")
    return f"metrics:archive:flights_archived:{period}"


def metrics_flights_skipped_key(period: str) -> str:
    """
    Counter for external-only flights dropped by the archive processor instead
    of being written to S3. period must be one of: hour, today.
    metrics:archive:flights_skipped:{period}
    """
    if period not in _VALID_ARCHIVE_PERIODS:
        raise ValueError(f"period must be one of {_VALID_ARCHIVE_PERIODS}, got: {period!r}")
    return f"metrics:archive:flights_skipped:{period}"


def archive_last_segment_key(icao_hex: str) -> str:
    """
    Pointer to the most recently archived flight segment for an aircraft.
    Used to detect and stitch together flights that were artificially split
    by a processor-count resize. JSON {uuid, first_message, last_message,
    s3_key}. Set with a 1-day TTL.
    archive:last_segment:{icao_hex}
    """
    return f"archive:last_segment:{icao_hex.upper()}"


def archive_search_key(uuid: str) -> str:
    """
    Archive search record (management-ui backend's Athena query layer, see
    management-ui/README.md). JSON {name, where_clause, status, submitted_at,
    query_execution_id, error}. Set with a fixed 7-day TTL from creation,
    never refreshed on access.
    archive_search:{uuid}
    """
    return f"archive_search:{uuid}"


def archive_search_index_key() -> str:
    """
    SET of every archive_search:{uuid}'s uuid -- lets the backend list/
    reconcile active searches in O(active searches) via SMEMBERS, instead
    of an O(entire keyspace) SCAN MATCH archive_search:*. No TTL on the
    set itself -- it's small and actively maintained (SADD on create,
    SREM on delete), not a data record. A uuid whose backing
    archive_search:{uuid} key has since expired (7-day TTL) has no way to
    notify this set, so it self-heals opportunistically instead: any
    SMEMBERS caller that GETs a member and finds it already gone SREMs
    that stale uuid right there.
    archive_search:index
    """
    return "archive_search:index"
