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

- **`config.py`** — `load_config()`, which builds a component's configuration
  from environment variables and returns the nested dictionary shape every
  component already consumed. A component names the blocks it reads
  (`load_config("redis", "mqtt", "runner")`), so no container is handed
  credentials it never uses, and a startup failure reports every missing or
  malformed variable at once rather than one per restart. Per-block helpers
  (`mqtt_config()`, `redis_config()`, `rabbitmq_config()`, `s3_config()`, …)
  define each block's shape once. `DATA_DIR` lives here too, as a constant:
  every compose file fixes the data directory at `/app/data` via its bind
  mount, so it was never a per-deployment value.

- **`index_cache.py`** — `INDEX_CACHE_DIR`, `local_index_path()`,
  `write_local_index()`, and `delete_local_index()`: the local mirror of a
  per-flight Parquet index row, shared between `archive-processor`
  (writer) and `archive-compaction` (reader) via a bind mount both
  containers point at the same host directory (see
  `docker-compose.archive.yaml`). Lets `archive-compaction` read a row
  back from local disk instead of re-downloading it from S3 days later.
  See each component's own README for how it's wired in.

- **`redis_keys.py`** — Functions (not string constants) for every Redis key
  used in the system. Using functions makes key parameters explicit and allows
  the type checker to catch typos.

- **`rabbitmq_topology.py`** — The ADS-B exchange's name, type and arguments,
  plus the helpers that declare it and bind a message processor's queue to it.
  The receiver and the message processor both declare this topology on every
  connect and must agree on it exactly, since RabbitMQ answers a
  redeclaration with differing arguments with a channel-level error. Defining
  it once turns that agreement into a guarantee.

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
  README (Fault Tolerance section) for how it's wired in. `drain()` runs
  the whole backlog in one pass; `drain_one()` exposes the same per-row
  step (retry accounting, dead-lettering, cooldown, oldest-first) one row
  at a time, for a caller that needs to interleave higher-priority work
  between rows; `drain_batch(process_fn, max_batch)` is `drain_one()`
  extended to a bounded batch — same semantics, one `DELETE`/`commit` for
  the whole succeeded prefix, stopping at the first failure or
  cooling-down row. The receiver's publisher thread uses `drain_batch()`
  to keep live traffic ahead of backlog drain (re-checking the live queue
  between batches) while catching a large post-outage backlog up far
  faster than one commit per row. `put_many()` inserts a whole list under
  a single commit, for a caller batching a burst of queued writes (the
  receiver's overflow-writer thread) rather than paying a commit per
  message.

  The connection runs `journal_mode=WAL` with `synchronous=NORMAL`: a
  commit no longer fsyncs on every call, so `put()`/`put_many()` are cheap
  enough to sit on a hot path during an outage. A plain process crash
  (SIGKILL, OOM, `docker stop`) still loses nothing — the WAL replays
  intact on reopen; only a host power loss or kernel panic can drop the
  last few committed rows, the same trade the message processor's WAL
  active store already makes.

  A third category sits between "recoverable outage" and "poison": a
  dependency deliberately absent in this deployment (e.g. a message
  processor publishing completed flights with `mandatory=True` against an
  `archive` queue no operator declared because this environment runs no
  archiver). A caller passes the exception type(s) that signal this via
  `non_poison_exceptions` — `FallbackQueue` stays broker-agnostic and
  never imports the caller's library. A row failing only with those types
  is retried forever and never dead-lettered: losing it would discard
  legitimate primary data, and it drains automatically if the dependency
  later appears. To keep the retryable table from growing without bound in
  that case, a caller can also opt into `retryable_max_bytes`, a
  ring-buffer cap on the plain `queue` table with the same oldest-first
  eviction the dead-letter directory uses (logged as a capacity eviction,
  not a poison classification).

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
