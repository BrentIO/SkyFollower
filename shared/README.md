# Shared Data Models

The `shared` package contains the Pydantic data models, Redis key name
functions, and other common primitives used by all SkyFollower components.
Keeping these in one place ensures that the message envelope format, Redis
key naming, and fault-tolerance behavior stay consistent across the
receiver, processor, archive processor, data runners, and UI backend.
Components that run in their own containers install the package at build
time via a relative path reference in their `requirements.txt`.

## Contents

- **`models.py`** — Pydantic models for the RabbitMQ message envelope
  (`InboundMessage`), in-flight telemetry (`Position`, `Velocity`), Redis
  enrichment records (`AircraftRecord`, `OperatorRecord`, `AirportRecord`),
  and the completed flight record published to the archive queue
  (`CompletedFlight`). Also exports `generate_flight_id()` which returns
  a UUID-v7 string used as the `_id` of each archived flight.

- **`redis_keys.py`** — Functions (not string constants) for every Redis key
  used in the system. Using functions makes key parameters explicit and allows
  the type checker to catch typos.

- **`fallback_queue.py`** — `FallbackQueue`, the SQLite-backed local
  retry queue every component that talks to an external dependency
  (RabbitMQ, S3) uses when that dependency is unreachable. Below a
  per-row retry threshold (default 5), a failure behaves like a plain
  outage — stop the drain pass, retry from the top next time. At the
  threshold, the row is judged permanently poison: it's written out as a
  standalone JSON file under `{dirname(db_path)}/dead_letters/{table_name}/`
  (size-capped at 100MB, oldest-evicted-first) for an operator to inspect
  or discard out-of-band, and the drain pass continues past it instead of
  stopping — so one permanently-failing item can no longer silently block
  everything queued behind it forever. A row also can't accumulate retries
  faster than once per `min_retry_interval_seconds` (default 30), no
  matter how often a caller invokes `drain()` — otherwise a caller whose
  own retry trigger fires in rapid bursts (e.g. a flapping connection
  reconnecting every few seconds) could dead-letter a row within seconds,
  even though it was never actually poison. See each component's own
  README (Fault Tolerance section) for how it's wired in.

## Usage

```python
from shared.models import InboundMessage, CompletedFlight, AircraftRecord

# Parse a message received from RabbitMQ
msg = InboundMessage.model_validate_json(body)
print(msg.icao_hex, msg.source)
```

```python
from shared.redis_keys import aircraft_simple_key, config_rules_key

# Build a Redis key for a specific aircraft
key = aircraft_simple_key("A8AE7F")    # → "aircraft:simple:A8AE7F"

# Get the key for the active rules configuration
rules_key = config_rules_key()         # → "config:rules"
```
