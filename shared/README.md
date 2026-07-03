# shared

The `shared` package contains the Pydantic data models and Redis key name
functions used by all SkyFollower components. Keeping these in one place
ensures that the message envelope format and Redis key naming stay consistent
across the receiver, processor, archive processor, data runners, and UI
backend. Components that run in their own containers install the package at
build time via a relative path reference in their `requirements.txt`.

## Contents

- **`models.py`** — Pydantic models for the RabbitMQ message envelope
  (`InboundMessage`), in-flight telemetry (`Position`, `Velocity`), Redis
  enrichment records (`AircraftRecord`, `OperatorRecord`, `AirportRecord`,
  `FlightEnrichment`), and the completed flight record published to the archive
  queue (`CompletedFlight`). Also exports `generate_flight_id()` which returns
  a UUID-v7 string used as the `_id` of each archived flight.

- **`redis_keys.py`** — Functions (not string constants) for every Redis key
  used in the system. Using functions makes key parameters explicit and allows
  the type checker to catch typos.

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
