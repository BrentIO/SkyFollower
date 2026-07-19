"""
Centralised Redis key name functions for SkyFollower.

All components import from here so key names stay consistent across the
codebase. Functions are used instead of string constants so that parameters
are always explicit and typos in key names are caught by the type checker.
"""

_VALID_PERIODS = frozenset({"hour", "today", "lifetime"})
_VALID_ARCHIVE_PERIODS = frozenset({"hour", "today"})

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


def operator_key(designator: str) -> str:
    """Airline operator record. operator:{designator}"""
    return f"operator:{designator.upper()}"


def aircraft_type_key(designator: str) -> str:
    """Aircraft type-designator reference record. aircraft:type:{designator}"""
    return f"aircraft:type:{designator.upper()}"


def flight_key(ident: str) -> str:
    """Origin/destination enrichment record. flight:{ident}"""
    return f"flight:{ident.upper()}"


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


def processor_heartbeat_key(processor_id: int) -> str:
    """
    Processor liveness key used to detect duplicate PROCESSOR_ID on startup.
    Set with NX + TTL = 2 × telemetry_interval.
    processor:{id}:heartbeat
    """
    return f"processor:{processor_id}:heartbeat"


def metrics_registration_misses_key(processor_id: int, period: str) -> str:
    """
    Counter for Redis enrichment misses (aircraft not found) per processor.
    period must be one of: hour, today, lifetime.
    metrics:processor:{id}:registration_misses:{period}
    """
    if period not in _VALID_PERIODS:
        raise ValueError(f"period must be one of {_VALID_PERIODS}, got: {period!r}")
    return f"metrics:processor:{processor_id}:registration_misses:{period}"


def metrics_aircraft_type_misses_key(processor_id: int, period: str) -> str:
    """
    Counter for aircraft type lookup misses per processor.
    period must be one of: hour, today, lifetime.
    metrics:processor:{id}:aircraft_type_misses:{period}
    """
    if period not in _VALID_PERIODS:
        raise ValueError(f"period must be one of {_VALID_PERIODS}, got: {period!r}")
    return f"metrics:processor:{processor_id}:aircraft_type_misses:{period}"


def metrics_flights_archived_key(period: str) -> str:
    """
    Counter for flights successfully written to S3 by the archive processor.
    period must be one of: hour, today.
    metrics:archive:flights_archived:{period}
    """
    if period not in _VALID_ARCHIVE_PERIODS:
        raise ValueError(f"period must be one of {_VALID_ARCHIVE_PERIODS}, got: {period!r}")
    return f"metrics:archive:flights_archived:{period}"


def archive_last_segment_key(icao_hex: str) -> str:
    """
    Pointer to the most recently archived flight segment for an aircraft.
    Used to detect and stitch together flights that were artificially split
    by a processor-count resize. JSON {uuid, first_message, last_message,
    s3_key}. Set with a 1-day TTL.
    archive:last_segment:{icao_hex}
    """
    return f"archive:last_segment:{icao_hex.upper()}"
