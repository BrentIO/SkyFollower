"""
Centralised Redis key name functions for SkyFollower.

All components import from here so key names stay consistent across the
codebase. Functions are used instead of string constants so that parameters
are always explicit and typos in key names are caught by the type checker.
"""

import re

_VALID_PERIODS = frozenset({"hour", "today", "lifetime"})
_VALID_ARCHIVE_PERIODS = frozenset({"hour", "today"})
# No "lifetime": that total is device-local and resets on receiver restart,
# never persisted to Redis.
_VALID_RECEIVER_PERIODS = frozenset({"hour", "today"})
_VALID_OPERATOR_MISSES_PERIODS = frozenset({"today", "lifetime"})

# RediSearch index over all aircraft:mictronics:{hex} JSON documents.
# Indexed fields: $.icao_hex, $.registration
AIRCRAFT_MICTRONICS_SEARCH_INDEX = "idx:aircraft:mictronics"

# RediSearch index over all aircraft:registry:{hex} JSON documents.
# Indexed fields: $.icao_hex, $.registration
AIRCRAFT_REGISTRY_SEARCH_INDEX = "idx:aircraft:registry"

# RediSearch index over all airport:{icao_code} JSON documents.
AIRPORT_SEARCH_INDEX = "idx:airport"


def aircraft_mictronics_key(icao_hex: str) -> str:
    """Mictronics aircraft enrichment record. aircraft:mictronics:{icao_hex}"""
    return f"aircraft:mictronics:{icao_hex.upper()}"


def aircraft_registry_key(icao_hex: str) -> str:
    """Country-runner aircraft enrichment record. aircraft:registry:{icao_hex}"""
    return f"aircraft:registry:{icao_hex.upper()}"


def aircraft_livery_key(icao_hex: str) -> str:
    """Special-livery enrichment record. Deep-merged last by
    shared/lua/merge_aircraft.lua, so it wins over aircraft:mictronics and
    aircraft:registry on any field overlap."""
    return f"aircraft:livery:{icao_hex.upper()}"


def operator_key(designator: str) -> str:
    """Airline operator record. operator:{designator}"""
    return f"operator:{designator.upper()}"


def aircraft_type_key(designator: str) -> str:
    """Aircraft type-designator reference record. aircraft:type:{designator}"""
    return f"aircraft:type:{designator.upper()}"


_FLIGHT_IDENT_PATTERN = re.compile(r"^([A-Za-z]+)(\d+)([A-Za-z]*)$")


def normalize_flight_ident(ident: str) -> str:
    """Strips leading zeros from a flight ident's numeric portion, e.g.
    "AFR0096" -> "AFR96". Idempotent, so it's safe to apply on both the
    write side (VRS ingestion) and the read side without disagreement.
    Idents that don't match the prefix+number(+suffix) shape are returned
    unchanged; this is purely a route-lookup key normalization, never
    applied to the stored/displayed ident.
    """
    match = _FLIGHT_IDENT_PATTERN.match(ident)
    if not match:
        return ident
    prefix, digits, suffix = match.groups()
    return f"{prefix}{int(digits)}{suffix}"


def route_key(ident: str) -> str:
    """Raw VRS standing-data route string for a callsign. Plain Redis
    string (not JSON), e.g. GET route:AAL15 -> "KMIA-KJFK-KMIA"."""
    return f"route:{ident.upper()}"


def icao_code_blocks_key() -> str:
    """ICAO 24-bit (Mode S) address allocation table. RedisJSON array of
    {bitmask, significant_bitmask, country_code} objects, pre-sorted by
    descending significant_bitmask so shared/lua/merge_aircraft.lua's
    linear scan can take the first match without re-sorting per call."""
    return "lookup:icao-code-blocks"


def icao_countries_key() -> str:
    """ISO 3166-1 alpha-2 -> English country name map. RedisJSON object,
    e.g. {"US": "United States", ...}. Used by
    shared/lua/merge_aircraft.lua to resolve AircraftRecord.country so
    country names live in exactly one place."""
    return "lookup:icao-countries"


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
    """Shared flight_ttl_seconds value, read once at startup (not
    hot-reloaded) by the message processor and archive processor. Callers
    should default to 300 if unset."""
    return "config:flight_ttl_seconds"


def message_processor_heartbeat_key(message_processor_id: str) -> str:
    """Message processor liveness key, used to detect a duplicate
    MESSAGE_PROCESSOR_ID on startup. Set with NX + TTL = 2 ×
    telemetry_interval."""
    return f"skyfollower-message-processor-{message_processor_id}"


def receiver_heartbeat_key(receiver_id: str) -> str:
    """Receiver liveness/claim key, mirroring
    message_processor_heartbeat_key(). Set with NX + TTL = 2 ×
    telemetry_interval on first claim, then refreshed by unconditional
    EXPIRE (never a second NX) for as long as the receiver runs."""
    return f"skyfollower-receiver-{receiver_id}"


def receiver_registry_index_key() -> str:
    """SET of every currently-claimed receiver identity, letting
    core-health enumerate live receivers via SMEMBERS instead of a
    keyspace SCAN. No TTL on the set itself; a stale member (its
    receiver:registration:{id} entry already expired) is SREM'd
    opportunistically by whichever caller notices it."""
    return "receiver:index"


def receiver_registration_key(receiver_id: str) -> str:
    """Per-receiver registration entry core-health reads to reconstruct
    the HA discovery/telemetry payloads for the Redis-backed period-count
    sensors. JSON array of {host, port, source} triples. TTL'd alongside
    the heartbeat (2 × telemetry_interval); a missing entry means the name
    is no longer live, not an error."""
    return f"receiver:registration:{receiver_id}"


def receiver_message_count_key(receiver_id: str, connection_id: str, period: str) -> str:
    """Redis-backed period counter for one receiver connection's message
    count, populated via shared/lua/incr_period_counter.lua from the
    receiver's telemetry thread. `connection_id` is the same sanitized
    `{host}_{port}` identifier used in that connection's MQTT topic, so
    this key and its corresponding MQTT field are derivable from each
    other. Missing key means the count is genuinely 0, not unavailable.
    """
    if period not in _VALID_RECEIVER_PERIODS:
        raise ValueError(
            f"period must be one of {_VALID_RECEIVER_PERIODS}, got: {period!r}"
        )
    return f"metrics:receiver:{receiver_id}:{connection_id}:messages:{period}"


def metrics_registration_misses_key(message_processor_id: str, period: str) -> str:
    """Counter for aircraft enrichment (registration) lookup misses per
    message processor -- an icao_hex with no matching aircraft:mictronics/
    registry/livery record. Operator-lookup misses are counted separately,
    via metrics_operator_misses_key()."""
    if period not in _VALID_PERIODS:
        raise ValueError(f"period must be one of {_VALID_PERIODS}, got: {period!r}")
    return f"metrics:message_processor:{message_processor_id}:registration_misses:{period}"


def metrics_operator_misses_key(message_processor_id: str, period: str) -> str:
    """Counter for operator:{designator} lookup misses per message
    processor. No "hour" period: operator misses are lower-volume and
    only tracked today/lifetime."""
    if period not in _VALID_OPERATOR_MISSES_PERIODS:
        raise ValueError(
            f"period must be one of {_VALID_OPERATOR_MISSES_PERIODS}, got: {period!r}"
        )
    return f"metrics:message_processor:{message_processor_id}:operator_misses:{period}"


def metrics_total_messages_processed_key(message_processor_id: str, period: str) -> str:
    """Counter for every message a message processor attempted to decode
    (including CRC-corrupt/no-content messages), incremented at the same
    point as messages_per_second's own _RateTracker.record()."""
    if period not in _VALID_PERIODS:
        raise ValueError(f"period must be one of {_VALID_PERIODS}, got: {period!r}")
    return f"metrics:message_processor:{message_processor_id}:total_messages_processed:{period}"


def rule_trigger_lifetime_key(identifier: str) -> str:
    """
    Lifetime count of times a rule has fired (once per flight, the first
    time it matches -- not once per message). Never expires; keyed by rule
    identifier, so a deleted-then-recreated identifier starts clean.
    """
    return f"rule_triggers:{identifier}:lifetime"


def rule_trigger_day_key(identifier: str, date: str) -> str:
    """Count of times a rule fired on one UTC day (`date` is YYYY-MM-DD).
    Set with a 31-day TTL, so old days clean themselves up with no sweep
    job. management-ui sums the last 30 of these for a rolling figure."""
    return f"rule_triggers:{identifier}:{date}"


def metrics_flights_archived_key(period: str) -> str:
    """Counter for flights successfully written to S3 by the archive
    processor."""
    if period not in _VALID_ARCHIVE_PERIODS:
        raise ValueError(f"period must be one of {_VALID_ARCHIVE_PERIODS}, got: {period!r}")
    return f"metrics:archive:flights_archived:{period}"


def metrics_flights_skipped_key(period: str) -> str:
    """Counter for external-only flights dropped by the archive processor
    instead of being written to S3."""
    if period not in _VALID_ARCHIVE_PERIODS:
        raise ValueError(f"period must be one of {_VALID_ARCHIVE_PERIODS}, got: {period!r}")
    return f"metrics:archive:flights_skipped:{period}"


def archive_last_segment_key(icao_hex: str) -> str:
    """Pointer to the most recently archived flight segment for an
    aircraft, used to detect and stitch together flights artificially
    split by a processor-count resize."""
    return f"archive:last_segment:{icao_hex.upper()}"


def archive_search_key(uuid: str) -> str:
    """Archive search record (management-ui's Athena query layer). Set
    with a fixed 7-day TTL from creation, never refreshed on access."""
    return f"archive_search:{uuid}"


def archive_search_index_key() -> str:
    """SET of every archive_search:{uuid}'s uuid, letting the backend
    list/reconcile active searches via SMEMBERS instead of a keyspace
    SCAN. No TTL on the set itself; a uuid whose backing key has since
    expired is SREM'd opportunistically by whichever caller notices it."""
    return "archive_search:index"
