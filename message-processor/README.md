# Message Processor

The message processor consumes raw ADS-B and UAT messages from its own RabbitMQ
queue, maintains per-aircraft flight state in a file-backed (WAL-mode) SQLite
database so it survives a process restart, enriches each flight with
registration and operator data from Redis, evaluates
the configured rules engine, publishes MQTT notifications when rules match, and
routes completed flights to the `skyfollower-archive` queue (or a local SQLite fallback when
RabbitMQ is unavailable). One container equals one message processor instance;
scale horizontally by adding message processor containers, whether on the
same host or on separate hosts -- see `MESSAGE_PROCESSOR_ID` below for how
`scripts/install.sh` generates each instance's service block in
`docker-compose.message-processor.yaml`.

![Message Processor architecture](./message-processor.svg)

## Configuration

Reads its configuration from environment variables via `shared/config.py`'s
`load_config("rabbitmq", "redis", "mqtt", "telemetry", "message_processor")`,
interpolated by Compose from this host's `.env` (written by
`scripts/install.sh`).

| Variable | Required | Default | Description |
|---|---|---|---|
| `RABBITMQ_HOST` | ✅ | — | |
| `RABBITMQ_PORT` | ❌ | `5672` | |
| `RABBITMQ_USERNAME` | ✅ | — | |
| `RABBITMQ_PASSWORD` | ✅ | — | |
| `REDIS_HOST` | ✅ | — | |
| `REDIS_PORT` | ❌ | `6379` | |
| `REDIS_PASSWORD` | ✅ | — | Redis requires authentication; see `shared/redis_client.py`'s `build_redis_client()` |
| `MQTT_HOST` | ❌ | — | Leave unset to disable MQTT entirely |
| `MQTT_PORT` | ❌ | `1883` | |
| `MQTT_USERNAME` | ❌ | — | Optional MQTT auth; leave unset for an anonymous broker |
| `MQTT_PASSWORD` | ❌ | — | |
| `LATITUDE` | ✅ | — | Receiver location latitude (decimal degrees), used for single-message CPR airborne position decoding |
| `LONGITUDE` | ✅ | — | Receiver location longitude (decimal degrees) |
| `LOG_LEVEL` | ❌ | `info` | `"debug"` for verbose output |

Timing values -- the MQTT publish cadence, the Redis heartbeat refresh and
TTL, the config-poll interval, the rule-notification max lag, and reconnect
backoffs -- are not environment variables. They are fixed constants in
`shared/timing.py`; see [Timing and cadences](https://github.com/BrentIO/SkyFollower/blob/main/docs/architecture/timing.md).
`flight_ttl_seconds` remains adjustable via the `config:flight_ttl_seconds`
Redis key (see [Redis Key Dependencies](#redis-key-dependencies)).

`active_flights.db` (the durable active flight store) and
`completed_flights.db` (the RabbitMQ offline fallback) are always written
to `/app/data`, a fixed, non-configurable bind mount -- see
`docker-compose.message-processor.yaml`.

### `MESSAGE_PROCESSOR_ID`

`MESSAGE_PROCESSOR_ID` is a single flat, fleet-wide sequential number -- there
is exactly one ID per processor across the whole deployment, not a per-node
prefix plus a local index. It's set per-service in
`docker-compose.message-processor.yaml` as a literal (not read from `.env`
via interpolation), because `scripts/install.sh` decides it at
compose-generation time, not the Python process at first boot: the compose
service name and container name are static values resolved at `docker
compose up` time, so they can't depend on something a container only
discovers after it's already running.

That one ID is used verbatim -- with no local/global translation -- for
every artifact belonging to that processor:

| What | Format |
|---|---|
| Compose service name | `skyfollower-message-processor-{id}` |
| Docker container name | `skyfollower-message-processor-{id}` |
| RabbitMQ queue name | `skyfollower-message-processor-{id}` |
| Redis heartbeat/claim key | `skyfollower-message-processor-{id}` |
| Data directory | `./data/skyfollower-message-processor-{id}` |

The message processor declares and consumes from
`skyfollower-message-processor-{MESSAGE_PROCESSOR_ID}`, binding that queue to
the `skyfollower-adsb` consistent-hash exchange with a weight of `1` (see
[Routing](https://github.com/BrentIO/SkyFollower/blob/main/receiver/README.md#routing)
in the receiver's README for the exchange's shape and its operational
consequences). Because the ID is embedded in the queue name, an abandoned
queue is identifiable by name alone: the RabbitMQ management UI shows
`skyfollower-message-processor-7` with a consumer count of zero once that
message processor is gone.

On startup the message processor attempts to claim a Redis key
`skyfollower-message-processor-{MESSAGE_PROCESSOR_ID}` using `SET NX`. If the
key already exists (i.e., another instance with the same ID is running), the
process exits immediately to prevent duplicate-ID conflicts.

The ID is sequential (operator-supplied count), not random, deliberately:
scale-down safety depends on operators being able to reason about creation
order -- remove the highest-numbered/most-recently-bound instance to avoid a
large RabbitMQ consistent-hash reshuffle (see
[Routing](https://github.com/BrentIO/SkyFollower/blob/main/receiver/README.md#routing)'s
"Remove from the end, never the middle" guidance). A random ID would solve
cross-node uniqueness just as well but would destroy that property.

`docker-compose.message-processor.yaml`, as fetched from the repo, holds only
the shared `x-message-processor`/`x-message-processor-environment` anchors --
no services. `scripts/install.sh`'s `collect_message_processor_env()` asks
whether this run is replacing an existing processor (adopts and confirms one
specific ID) or adding new ones (asks how many are currently implemented
fleet-wide and how many this host will add, then computes the new range as
`existing_count+1` through `existing_count+num_new`), and appends one
concrete service block per ID -- referencing this file's own anchors, since
YAML anchors only resolve within the file that defines them -- to this node's
copy of the file. Re-running it later to add more processors to the same
node appends new blocks without touching already-running ones; the compose
file is no-clobber fetched (only written the first time) for exactly this
reason. See `docker-compose.message-processor.yaml`'s own comments for why
`deploy.replicas` can't substitute for any of this (each replica would need
its own volume and its own derivable ID, and Compose doesn't provide
either).

## Decoding

Raw Mode-S/ADS-B frames are decoded via pyModeS 3.x's single unified
`decode()` call, which returns every decodable field for a message in one
dict. The message processor extracts fields purely by presence — if a field is in
the result, it's used; there's no downlink-format or typecode dispatch, and
no downlink-format allowlist. Message types that don't populate any field
the message processor cares about (e.g. ACAS RA broadcasts) simply produce nothing
and are dropped, with nothing to explicitly filter.

Any message pyModeS flags as CRC-invalid is rejected outright — but this
only provides real protection for DF17/18 (extended squitter), where
`crc_valid` reflects an actual CRC-remainder-equals-zero check. For
DF0/4/5/11/16/20/21 (including squawk, DF5/21), pyModeS hardcodes
`crc_valid=True` unconditionally, since their CRC field encodes the ICAO
address itself rather than providing an independent integrity check —
there's no single-message corruption signal available for those message
types at all. A squawk value is trusted once decoded; there's nothing
further to verify it against outside of multi-message/pipe-mode decoding,
which this message processor doesn't use.

`wake_turbulence_category` is receiver-decode-only — registry/Mictronics
enrichment is never allowed to seed it (`_enrich_aircraft` strips the key
from the Redis-merged aircraft document before it reaches flight state),
and each new live reading simply replaces the last one (no first-wins
protection; there's only one writer). A single shared mapping table
(`_WAKE_TURBULENCE_MAP`) collapses both decode paths' possible source
values down to three canonical strings — `light`, `medium`, `heavy`:

- 1090 (`_decode_1090`): pyModeS's `wake_vortex` field (TC=4 aircraft
  category only — pyModeS is aware of which identification sub-type a
  category code came from, so TC=3 gliders/UAVs and TC=2 surface vehicles
  never produce a `wake_vortex` value in the first place).
- 978 (`_decode_978`): [`pyModeS978`](https://github.com/BrentIO/pyModeS978)'s
  `category` field (an `EmitterCategory` enum member), via the same table.

Both libraries expose the same 7 DO-260B wake/emitter categories (Light,
Medium 1, Medium 2, High Vortex Aircraft, Heavy, High Performance,
Rotorcraft). `Super` has no code point in this data at all — an A380 and a
767 broadcast the identical `Heavy` value — so it's not a reachable output.
`Rotorcraft`/`High Performance` describe emitter type, not wake-turbulence
weight class, so they're intentionally left unset rather than mapped to
anything; the remaining 5 collapse to `light`/`medium`/`heavy` by weight
band.

## Redis Key Dependencies

### Keys read

| Key pattern | Purpose |
|-------------|---------|
| `EVALSHA` → `shared/lua/merge_aircraft.lua` | Aircraft registration and type enrichment (read once per new flight). Not a direct key read: the message processor calls this script (`SCRIPT LOAD`ed once at startup) with `icao_hex` as its sole argument and has no visibility into what it reads. The script itself performs the underlying `JSON.GET`s against `aircraft:mictronics:{icao_hex}`, `aircraft:registry:{icao_hex}`, and `aircraft:livery:{icao_hex}` server-side and returns the deep-merged result (each later source winning on any field overlap — livery over registry over mictronics) in a single round-trip. |
| `operator:{DESIGNATOR}` | Airline operator enrichment (read once per flight when ident is first seen) |
| `EVALSHA` → `shared/lua/route_airports.lua` | Resolves `route:{ident}` into its full ordered array of `airport:{code}` records. Read at most once per flight, the moment ident/position/altitude/heading are all known (not on every message, and not at archive time). See "Route Leg Resolution" below. |
| `config:rules:version` | SHA-256 hash polled every `CONFIG_POLL_INTERVAL_SECONDS` (30 s); a change triggers a rule reload. Used only as a fast-path "unchanged" signal — a missing or body-skewed version key falls through to hashing `config:rules` itself (see below), so it can't leave a processor silently ruleless |
| `config:rules` | JSON rules array; loaded when the version changes, when its own SHA-256 doesn't match the recorded version, or whenever no rules are currently loaded |
| `config:areas:version` | SHA-256 hash polled every 30 s; same fast-path / fall-through behaviour as `config:rules:version` |
| `config:areas` | GeoJSON FeatureCollection; loaded on the same conditions as `config:rules` |
| `config:flight_ttl_seconds` | Plain scalar, read once at startup and cached (not hot-reloaded — restart to pick up a changed value); defaults to `300` if unset. Shared with the archive processor, which uses the same value to detect flights split by a processor-count resize. |

### Keys written

| Key pattern | Purpose |
|-------------|---------|
| `skyfollower-message-processor-{ID}` | Liveness key; claimed with `NX` on startup, refreshed every `HEARTBEAT_INTERVAL_SECONDS` with a `HEARTBEAT_TTL_SECONDS` TTL (see `shared/timing.py`) |
| `registration:{REGISTRATION}` | Reverse-lookup index (registration → ICAO hex); written `NX` when aircraft enrichment is found and a registration exists |
| `metrics:message_processor:{ID}:registration_misses:{hour\|today\|lifetime}` | Incremented each time an `icao_hex` aircraft enrichment lookup (`_enrich_aircraft()`) returns no result. Operator-lookup misses are **not** counted here — they have their own dedicated `operator_misses` key below. |
| `metrics:message_processor:{ID}:operator_misses:{today\|lifetime}` | Incremented each time an `operator:{designator}` lookup (`_enrich_operator()`) returns no result. No `hour` period — operator misses are lower-volume and only tracked today/lifetime. |
| `metrics:message_processor:{ID}:total_messages_processed:{hour\|today\|lifetime}` | Incremented for every message an attempt was made to decode (including CRC-corrupt/no-content messages) — the same point `messages_per_second`'s own rate tracker records at, in `_on_message`. Bucketed by wall-clock processing time, not `received_at` — a backlog replay counts entirely toward the current hour/day, not the messages' original timestamps. |
| `rule_triggers:{identifier}:lifetime` and `rule_triggers:{identifier}:{YYYY-MM-DD}` | How many times each rule has fired — counted **once per flight** the first time a rule matches it (the per-matched-rule loop in `_update_flight` runs once per `(flight, rule)`, since `RulesEngine.evaluate()` skips a rule already in `flight.matched_rules`), not once per message. Keyed by rule identifier, so a deleted-then-recreated identifier starts clean. Same non-blocking pattern as the three counters above — `_KeyedCounterAccumulator` on the hot path, flushed by the telemetry thread via `_flush_rule_trigger_counts()` in one pipelined round trip: `INCRBY` the never-expiring lifetime key, `INCRBY` + `EXPIRE` (`RULE_TRIGGER_DAY_TTL_SECONDS`, 31 days) today's UTC day key. The management-ui backend reads and sums these on demand for the Rules editor's "Triggered since created" / "Triggered in last 30 days" display; management-ui's `DELETE /api/rules/{identifier}` removes a deleted rule's keys. |

All three period counters above share the same reset mechanism and are **write-only**
from this component's own perspective:

- **Real clock-boundary resets**: `hour`/`today` periods are written via
  `EVALSHA` against `shared/lua/incr_period_counter.lua` (`SCRIPT LOAD`ed once
  at startup into `self._incr_period_counter_sha`, matching the
  `merge_aircraft.lua`/`route_airports.lua` pattern above), which sets
  `EXPIREAT` to the real next UTC hour/midnight boundary
  (`shared/metrics.py`'s `next_period_boundary()`) only the instant a call
  creates the key — never on a later increment within the same period, so the
  window can't slide forward. Redis's own TTL expiry deletes the key at the
  boundary; the next increment after that recreates it fresh, a genuine
  reset with no external scheduler involved.
- **`lifetime` periods** have no TTL and instead are explicitly `DELETE`d
  once, in `start()`, before any message is processed — "lifetime" means
  "since this process instance started," not "forever." Redis is a separate,
  persistent service, so a container restart alone does not clear these keys.
- **No per-message Redis round trip**: `_on_message`/`_enrich_aircraft`/
  `_enrich_operator` only ever touch a small in-memory accumulator
  (`_CounterAccumulator`, a lock-protected pending count — `record()`/
  `flush_and_reset()`). The existing telemetry thread (already running on its
  own cadence, already off the message-consuming path) is the only place
  that ever flushes an accumulated delta into Redis, via
  `_flush_period_counters()`.
- **Not self-published over MQTT/Home Assistant.** [`core-health`](../core-health/README.md)
  reads these three Redis key families and publishes them on this
  component's behalf, using the exact topic paths/`unique_id`/device block
  this component's own `_publish_telemetry()`/`_publish_ha_autodiscovery()`
  would otherwise have used — nothing on the wire distinguishes the two. See
  `core-health/README.md`'s "Counter mimicry" section.

![Period counter reset mechanism](./period-counter-sequence.svg)

## Route Leg Resolution

`origin`/`destination` are resolved at most once per flight — not at archive
time, and not on every message. `_maybe_resolve_route()` is called from
`_update_flight()` after each message is applied to the flight's state, and
runs the actual Redis lookup and resolution logic the moment **all** of the
following have been seen for the flight (in any order across messages):

- a route-bearing **ident** — present, not `"00000000"` (both already
  enforced before `flight.ident` is ever set at all — see `_update_flight`),
  and not just the aircraft's own tail number re-broadcast as the callsign
  (`_ident_matches_registration()`, dash-insensitive — the same check
  `_enrich_operator()` uses to decide whether to look up an airline operator,
  matching SkyFollower-legacy's `setIdent()`/`_getOperator()` precedent)
- a **position** (latitude/longitude)
- an **altitude**
- a **heading**

Once resolution runs, `flight.route_resolution_attempted` is set the moment
the outcome is *final* — resolved, or confidently ruled out — so a valid
ident with no known route (or a settled ambiguous/rejected leg) is never
re-queried against Redis on every subsequent message for the rest of the
flight. There's one deliberate exception: a multi-leg route whose recent
headings haven't stabilized yet (see "Heading stability" below) is
re-evaluated on later messages instead of being settled prematurely — but
the airport records fetched from Redis are cached on
`flight.route_candidate_airports` the first time, so every retry re-runs
only the local (no-I/O) resolution logic, never a second `EVALSHA`. The
resolution and sanity-check logic itself lives in
`message_processor/route_resolver.py` as a set of pure functions,
independent of Redis.

![Route leg resolution workflow](./route-leg-resolution-workflow.svg)

`route:{ident}` (written by the `vrs-standing-data` runner) is a raw,
dash-delimited string of ICAO airport codes — a simple point-to-point route
has 2 codes, but a same-day out-and-back reusing one callsign (e.g.
`KMIA-KJFK-KMIA`) or a multi-stop "milk run" can have 3 or more. `EVALSHA
route_airports.lua` resolves the whole string into an ordered array of full
`airport:{code}` records in one round trip (see `shared/lua/route_airports.lua`);
it returns an empty array if the ident has no known route, or if even one
code in the route has no matching airport record.

Because resolution runs as soon as the four conditions above are met — not
at the end of the flight — the position/velocity history it reasons over is
whatever the flight has accumulated *so far*, not necessarily its eventual
full track. This is deliberate: it's what makes `origin`/`destination`
available early enough for the same flight's own rules-engine evaluation or
MQTT notifications to see them, at the cost of the sanity check below only
being as strong as the track recorded up to that point.

- **2 airports**: no ambiguity — they're the origin and destination directly.
- **3+ airports**: the flight is only actually flying one adjacent pair, so it's
  resolved low-altitude-first, falling back to a cruise heuristic:
  - **Proximity + climb/descent** — if the flight's *earliest* position is
    below 10,000ft and near exactly one waypoint (or near a waypoint that
    appears more than once, e.g. the round-trip case, disambiguated by
    which occurrence's climb/descent direction is structurally possible —
    climbing away only makes sense for an occurrence with a next hop,
    descending toward only for one with a previous hop), that's a
    near-dispositive signal for whether this is the leg's origin (climbing
    away) or destination (descending toward).
  - **Heading vs. bearing** — otherwise (cruise altitude, or the proximity
    check was inconclusive), the flight's most recent observed heading is
    compared against each candidate leg's great-circle initial bearing. The
    closest candidate must be within 30° of the observed heading *and* at
    least 15° clearer than the second-closest candidate to count as
    resolved — this is what makes the common out-and-back case tractable
    (the two legs' bearings are roughly 180° apart), while still refusing
    to guess among several similarly-plausible legs on a genuine multi-city
    route. This heuristic only ever runs once heading data has *stabilized*
    (see below) — never against a single instantaneous reading.
  - Either heuristic returning nothing resolved (ambiguous proximity with no
    valid vertical-trend match, or heading inconclusive) leaves both fields
    `None` rather than guessing.

**Heading stability**: a single instantaneous heading reading is not
trustworthy on its own — an aircraft circling in a holding pattern (e.g.
diverted around weather) sweeps its heading through a full circle, and can
momentarily point in almost any direction, including one that happens to
align with a completely different leg's bearing. `heading_is_stable()`
requires at least 3 recent heading samples to agree within 20° of each
other before the heading-vs-bearing heuristic is trusted at all; ordinary
cruise flight naturally produces consistent consecutive headings, while
circling does not. When headings aren't yet stable, resolution defers
rather than settling — see "one deliberate exception" above.

**Sanity check**: whatever pair is selected — a direct 2-airport pass-through
or a resolved multi-leg pair — is checked against the flight's actual
observed track before being trusted. `route:{ident}` is community-maintained
VRS standing data; a callsign can carry a stale or mismatched route with no
way to detect that from the string alone. Two independent checks run against
every recorded position:

- **Cross-track distance** — the perpendicular distance from the position to
  the great-circle line between the candidate origin and destination; if any
  position exceeds `max(150nm, 30% × route_distance)`, the pair is rejected.
  The threshold scales with route length rather than using one flat number:
  a flat 500nm window would barely constrain a short hop like KJFK-KATL
  (~660nm) but would reject perfectly normal long-haul routing variance (jet
  stream, ATC, weather) on a route like MMMX-EGLL (~5,500nm).
- **Along-track bound** — cross-track distance alone only measures
  perpendicular distance to the *infinite* line through both airports; it
  says nothing about whether the position actually falls *between* the two
  endpoints. A position must project onto that line within `[0,
  route_distance]` (plus a fixed 50nm slack for ordinary terminal-area
  maneuvering) or the pair is rejected. This specifically catches a holding
  pattern whose (now-stabilized) heading matches a *different*, wrong leg's
  bearing closely enough, and happens to sit within that wrong leg's
  cross-track tolerance too, while actually being well beyond its
  destination — real example: an aircraft actually flying KJFK→KMIA,
  holding at 22,000ft east of Jacksonville on a `KJFK-KMIA-KMCO-KJFK`
  routing, whose stabilized northbound holding heading (~350°) is a closer
  match to the wrong `KMIA→KMCO` leg's bearing (~341°) than to the correct
  `KJFK→KMIA` leg (~202°) and passes that wrong leg's cross-track check
  (~52nm, under its ~150nm threshold) — but projects ~100nm *past* KMCO
  along that leg's line, which the along-track bound catches and rejects.

Either check failing rejects the pair — treated the same as an unresolvable
case, never a partial guess.

**All-or-nothing**: `origin`/`destination` are set only when exactly one
unambiguous, sanity-checked pair was resolved. Any failure along the way —
no route data, an unresolvable leg, or a rejected sanity check — leaves
*both* fields `None`, never a partial or best-guess value.

Whenever a finalized attempt leaves `origin`/`destination` unset — no known
route, an unresolvable/ambiguous leg, or a rejected sanity check — a `DEBUG`
log line records the ident, `icao_hex`, the exact `route_airports.lua`
response that was rejected, and (where applicable) the specific reason (e.g.
which sanity check failed, at which position, and by how much). Nothing is
logged for the "not final yet" case (heading still stabilizing) — there's
nothing to report until an attempt actually settles.

## MQTT Topics Published

All topics use the root `SkyFollower`.

| Topic | Payload | Retained |
|-------|---------|----------|
| `SkyFollower/message-processor/{ID}/status` | `ONLINE` or `OFFLINE` | Yes |
| `SkyFollower/message-processor/{ID}/statistic/{name}` | One retained topic per stat (see fields below) | Yes |
| `SkyFollower/rule/{IDENTIFIER}` | JSON flight snapshot (no positions/velocities) with `rule` key | No |

**Statistic topic suffixes (`{name}`):**

| Field | Format | Description |
|-------|--------|-------------|
| `started_at` | UTC ISO-8601 timestamp | Process start time |
| `messages_per_second` | Float as string | Rolling 30-second average message rate |
| `processing_time_hwm_ms` | Float as string | End-to-end processing time high-water mark since last publish; resets on publish |
| `message_latency_hwm_ms` | Float as string | Receipt-through-processed latency high-water mark since last publish, including RabbitMQ queue wait time (a superset of `processing_time_hwm_ms`); resets on publish. Wall-clock based (`time.time() - msg.received_at`), unlike `processing_time_hwm_ms`'s monotonic clock, since `received_at` is stamped on the receiver host and crosses the RabbitMQ hop -- sensitive to NTP drift between hosts |
| `rules_engine_hwm_ns` | Float as string | Rules engine duration high-water mark since last publish, in nanoseconds; resets on publish |
| `local_archive_queue_depth` | Integer as string | Completed flights queued in `completed_flights.db` fallback |
| `dead_letter_queue_depth` | Integer as string | Completed flights dead-lettered after repeatedly failing to publish (see [Dead-Lettering Poison Flights](#dead-lettering-poison-flights)) |
| `active_flights` | Integer as string | Flights currently tracked in the active store |
| `rules_version` | String | Last 8 characters of the SHA-256 hash of the rules config **this instance has actually loaded** (`"unknown"` until the first successful load). Lags the canonical value published by `core-health` if a bad payload was pushed and rejected, or if this instance hasn't polled since the last save. Truncated only at publish; the engine keeps the full hash for its own comparison |
| `areas_version` | String | Last 8 characters of the SHA-256 hash of the areas config this instance has actually loaded (`"unknown"` until the first successful load). Same semantics as `rules_version` |

`registration_misses_{hour,today,lifetime}`, `operator_misses_{today,lifetime}`,
and `total_messages_processed_{hour,today,lifetime}` are **not** in the table
above — this component is write-only for those three counters (accumulates in
memory, flushes to Redis on the telemetry cadence) and does not publish their
MQTT statistic topics or Home Assistant discovery entries itself.
[`core-health`](../core-health/README.md) publishes them on this component's
behalf instead — see the "Keys written" Redis table above for the mechanism.
`aircraft_type_misses_{hour,today}` no longer exists at all (it was a dead
metric, never incremented anywhere).

Each stat is published as its own retained topic (not a combined JSON
payload) every `MQTT_PUBLISH_INTERVAL_SECONDS`. Home Assistant autodiscovery
payloads are published to
`homeassistant/sensor/SkyFollower_message_processor_{ID}_{field}/config` on MQTT
connect; each sensor's `state_topic` points directly at its own
`SkyFollower/message-processor/{ID}/statistic/{field}` topic — no `value_template`
needed.

## Fault Tolerance

When RabbitMQ is unavailable at startup or during operation, completed flights
are written to `completed_flights.db` (SQLite WAL mode) in `data_dir`. On the next
successful RabbitMQ reconnect, the fallback queue is drained oldest-first
before new messages are consumed. Redis and MQTT failures are handled
gracefully and logged; enrichment lookups that fail leave the flight partially
enriched rather than dropping it.

Draining is also attempted independently every `MQTT_PUBLISH_INTERVAL_SECONDS`,
not just on a detected reconnect. `_archive()` queues a completed flight to
the fallback on any publish exception without necessarily affecting
`_rmq_connected` or the consume side at all — so a run of publish-only
failures (e.g. a broker-side rejection on the `skyfollower-archive` routing key, with the
consumer connection itself unaffected) would otherwise never trigger a drain
again, since that's only ever spawned from the consumer's own reconnect
path. The periodic check is a cheap no-op when the queue is already empty.

Both triggers go through the same `drain_in_background()`: it spawns the
actual drain on a background thread and returns immediately, so a slow
drain (e.g. a large backlog) never delays that telemetry cycle's publish.
A single-flight guard (a non-blocking lock) ensures only one drain is ever
in progress at a time regardless of which trigger started it — if the
periodic tick fires while the reconnect-triggered drain is still running,
it's a no-op rather than a second overlapping drain, which could otherwise
select the same queued row twice and publish it twice.

The fallback queue is the shared `FallbackQueue` (see
[shared/README.md](../shared/README.md)) rather than a component-local
class.

### Dead-Lettering Poison Flights

A completed flight that fails to publish on every drain attempt — not
because RabbitMQ is down, but because something about that specific record
causes a deterministic failure — would otherwise retry forever, and since
`drain()` always re-selects the oldest row first, it would also block every
other queued flight behind it indefinitely. `FallbackQueue` tracks a
per-row retry count: below the threshold (5, hardcoded), a failure behaves
exactly as before — stop the drain pass, retry from the top next time. At
the threshold, the row is judged permanently poison: it's written out as a
standalone JSON file under `dead_letters/queue/` in `data_dir` (capped at
100MB total, oldest file evicted first) for manual inspection, and the
drain pass continues to whatever's queued behind it instead of stopping.
There's no automated replay path — a dead-lettered file is purely something
an operator inspects or discards out-of-band (`data_dir` is already a
host-mounted volume, same as `completed_flights.db` itself).

A raw attempt count alone isn't safe: `_drain_fallback()` is called on
every successful RabbitMQ reconnect (not just on the `MQTT_PUBLISH_INTERVAL_SECONDS`
tick), so a flapping connection reconnecting every few seconds could
otherwise burn through the retry threshold within seconds — dead-lettering
a flight that was never actually poison, just unlucky enough to be at the
head of the queue during a brief instability. `FallbackQueue` also enforces
a minimum time between attempts on the same row (30 seconds, hardcoded,
independent of how often `_drain_fallback()` itself gets called), so
reaching the threshold always takes a real, bounded amount of elapsed
time — not just a burst of rapid reconnect attempts.

**A missing archive queue is not poison.** `_archive()` and
`_drain_fallback()` publish completed flights with `mandatory=True`
against the well-known `skyfollower-archive` queue, which only exists once an
archive-processor has connected and declared it. A deployment that
deliberately runs no archiver never has that queue, so every publish
returns `pika.exceptions.UnroutableError` — a healthy-connection
condition, not a per-flight fault and not a recoverable outage. The
fallback queue is constructed with that exception type classified as
non-poison: those rows retry forever and are **never** dead-lettered, so
turning on an archive-processor later drains the entire accumulated
backlog automatically. Disk growth for the archiver-less case is instead
bounded by a ring-buffer cap on the retryable table itself (100MB, the
same ceiling the dead-letter directory uses); once an archiver-less
deployment accumulates past it, the oldest completed flights are evicted
first, logged as a capacity eviction. Any *other* publish failure (a real
connection loss, or an archive-processor rejecting a specific payload for
content reasons) still dead-letters at the threshold exactly as above.

### Active flight store durability & crash recovery

`active_flights.db` (SQLite, WAL mode, `data_dir`) holds every currently
tracked flight and is committed to disk after every message. A process end —
whether a deliberate stop (`SIGTERM`/`SIGINT`) or an ungraceful one (OOM-kill,
`docker kill`, host crash) — is recovered identically on the next startup;
there is no special "flush everything" shutdown path for either of those,
since nothing needs force-archiving when the store already survives on its
own. That assumes the store itself survives, though — see "Decommissioning
a Message Processor" below for the one case where it deliberately won't
(permanently destroying the container and its volume together).

On startup, the message processor reopens `active_flights.db` and recovers whatever
flights were still tracked. Recovery is driven by message timestamps, not by
how long the container was down: `message_clock` (`_message_clock` in
`main.py`) is floored at the most recent `last_message` among recovered
flights (not wall-clock "now"), and only advances as messages are actually
consumed — `_update_flight()` bumps it to `max(message_clock,
msg.received_at)` on every message. `_evict_stale()` (which drives both
eviction from the active store and, as a consequence, archiving) compares
each flight's `last_message` against `message_clock - flight_ttl_seconds`,
never against wall-clock time.

This gating matters in two situations, both of which produce the same
symptom — active flights sitting unevicted, `local_archive_queue_depth`
flat — for a reason that is correct, not a stuck or broken pipeline:

- **Recovering after a restart.** A recovered flight is **not** archived
  just because real time passed while the container was stopped — if a
  continuation message for that aircraft is sitting in the RabbitMQ
  backlog, it resumes the same flight once the message processor
  reconnects and drains the backlog. A genuine gap longer than
  `flight_ttl_seconds` — whether it happens live or is discovered while
  replaying a backlog — still correctly splits the flight into two records.
- **Offline replay with nothing live behind it** (see
  `tools/traffic-replayer`). If the only traffic a message processor ever
  sees is a finite replayed capture and no live receiver is attached,
  `message_clock` advances only as far as the capture's last message and
  then simply stops — there is nothing left to bump it further. Flights
  still active when the replay ends stay in the active store indefinitely,
  neither refreshing nor expiring, until either more messages arrive (a
  further replay, or live traffic) or the process is stopped. This can look
  identical to a stuck archive path from the outside; it isn't one.

MQTT rule notifications for messages older than
`rule_notification_max_lag_seconds` are suppressed during backlog replay
(logged at debug) to avoid flooding MQTT the instant a message processor
reconnects after downtime; the rule still fires and is still recorded in
`matched_rules`/the eventual archived flight.

Because `active_flights.db` only depends on `MESSAGE_PROCESSOR_ID` (which
determines the RabbitMQ queue consumed,
`skyfollower-message-processor-{MESSAGE_PROCESSOR_ID}`) and not on container
identity, moving the file to a replacement container with the same
`MESSAGE_PROCESSOR_ID` resumes tracking the same way a restart does. This is
exactly `scripts/install.sh`'s "replacing an existing message processor?"
flow -- see `MESSAGE_PROCESSOR_ID` above. One caveat: the Redis heartbeat key
(`skyfollower-message-processor-{MESSAGE_PROCESSOR_ID}`, `SET NX` with a
`HEARTBEAT_TTL_SECONDS` TTL) must expire — or be deleted manually — before
a replacement container can claim the same ID.

### Decommissioning a Message Processor

Everything above assumes `active_flights.db` survives the process ending —
true for a restart, a crash, or moving the file to a replacement container.
It is **not** true if the intent is to permanently remove a message
processor: destroy its container *and* its volume together (e.g. shrinking
the fleet — see the "treat processors as a stack" operating rule elsewhere
in this repo). Anything still active in `active_flights.db` at that moment
is gone for good unless it's forced through eviction/archival first.

`SIGUSR1` triggers exactly that: a decommission sequence that force-evicts
every active flight regardless of `message_clock`/TTL, waits for the local
archive fallback queue to drain, and only then runs the same graceful
shutdown `SIGTERM`/`SIGINT` already use. `SIGTERM`/`SIGINT` themselves are
untouched by this — they keep meaning "restart, resume from disk," exactly
as described above.

**Procedure:**

1. `docker kill -s SIGUSR1 <container>`
2. Wait for the container to exit on its own. No further action is needed —
   the process forces every active flight through the normal archive path,
   waits for the local fallback queue to drain, and shuts itself down.
3. Check the container's logs for a dead-letter warning (see below) before
   continuing.
4. Only once the container has exited: `docker compose down -v`, or
   otherwise delete the data directory.

**The wait in step 2 is indefinite, by design.** The process polls the local
archive fallback queue (`local_archive_queue_depth`) until it reaches zero
and does not give up on its own — if RabbitMQ is down, the correct
assumption is that it eventually comes back, and a decommission racing an
unrelated broker outage should not exit while flights are sitting only in
local fallback storage, not yet durable anywhere else. If an operator needs
to abandon a stuck wait (e.g. RabbitMQ is gone for good and the data loss is
accepted), the deliberate escape hatch is a manual `docker kill -9`
(`SIGKILL`) on the container — a conscious operator decision, not a timeout
this tool applies automatically.

**Dead-lettered flights are never waited on.** The dead-letter queue
(`dead_letter_queue_depth`) exists specifically for items expected to never
successfully drain via retry — waiting on it too would let a single poison
message hang a decommission forever. If it's nonzero once the retryable
queue has drained, the process logs the count at high severity and
continues shutting down regardless. Those flights' contents need manual
attention (see "Dead-Lettering Poison Flights" above) before the volume is
destroyed, if they matter.

## Rules Engine

Rules and areas are loaded from Redis (`config:rules` / `config:areas`) and
hot-reloaded every 30 seconds. The `config:*:version` SHA-256 keys are a
fast-path "nothing changed" signal, not the sole gate: if a version key is
missing (a deployment from before it existed, a partially-restored volume,
a manual seed), was written out of step with its body, or the engine is
holding no rules while `config:rules` has content, the reload falls
through to hashing the body itself. So a processor started against a Redis
that has `config:rules` but no `config:rules:version` still loads those
rules on its next poll — no UI save required. The rules engine is
implemented in `message-processor/rules_engine.py`.

Each rule must have a unique `identifier`, an `enabled` boolean, and a
non-empty `conditions` array. All conditions within a rule are ANDed together.
A rule fires at most once per flight per identifier (the identifier is added to
`flight.matched_rules` on first match, and subsequent evaluations skip it).

Conditions are sorted by evaluation cost before each evaluation pass — cheap
field comparisons run before expensive geographic checks. See
[Rules & Areas](https://brentio.github.io/SkyFollower/rules-and-areas/) for
the full condition type reference.

A rule may also carry an optional `force_archive` boolean (default `false`).
It isn't a condition — it's an ordinary property alongside `identifier`,
so any number of independent rules can each set it. When a matched rule has
`force_archive: true`, `flight.force_archive` becomes (and stays) `true` for
that flight's lifetime; the archive processor reads this to override its
external-only archive skip (see [archive-processor/README.md](../archive-processor/README.md))
for external-only flights the user does care about — a specific aircraft, an
area, etc. — without archiving every external contact indiscriminately. A
matched `force_archive` rule still publishes its normal MQTT rule
notification like any other rule; nothing about the flag suppresses that.
