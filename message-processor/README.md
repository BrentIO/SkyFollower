# Message Processor

The message processor consumes raw ADS-B and UAT messages from its own RabbitMQ
queue, maintains per-aircraft flight state in a file-backed (WAL-mode) SQLite
database so it survives a process restart, enriches each flight with
registration and operator data from Redis, evaluates
the configured rules engine, publishes MQTT notifications when rules match, and
routes completed flights to the archive queue (or a local SQLite fallback when
RabbitMQ is unavailable). One container equals one message processor instance;
scale horizontally by adding message processor containers, whether on the
same host (see `docker-compose.message-processor.yaml`'s profile-gated
`message-processor-2`..`message-processor-8` service definitions, and
`MESSAGE_PROCESSOR_ID` below) or on separate hosts.

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
| `RULE_NOTIFICATION_MAX_LAG_SECONDS` | ❌ | `30` | Maximum age (seconds, message `received_at` vs. wall-clock time) of a message whose rule match still gets published to MQTT. Older matches (replayed from a RabbitMQ backlog after a restart) still fire and are recorded in `matched_rules`, just not pushed to MQTT — prevents flooding MQTT with backlogged notifications the instant a message processor reconnects. |
| `TELEMETRY_INTERVAL_SECONDS` | ❌ | `30` | How often the message processor publishes MQTT statistic messages and refreshes its Redis heartbeat key |
| `LATITUDE` | ✅ | — | Receiver location latitude (decimal degrees), used for single-message CPR airborne position decoding |
| `LONGITUDE` | ✅ | — | Receiver location longitude (decimal degrees) |
| `LOG_LEVEL` | ❌ | `info` | `"debug"` for verbose output |

`active_flights.db` (the durable active flight store) and
`completed_flights.db` (the RabbitMQ offline fallback) are always written
to `/app/data`, a fixed, non-configurable bind mount -- see
`docker-compose.message-processor.yaml`.

### `MESSAGE_PROCESSOR_ID` and `MESSAGE_PROCESSOR_PREFIX`

`MESSAGE_PROCESSOR_ID` is set per-service in `docker-compose.message-processor.yaml`
as `${MESSAGE_PROCESSOR_PREFIX:-mp}-{n}` (e.g. `turing-node-3-1`) rather than
read directly from `.env` -- it can be any string, it only has to be unique
across the whole deployment, and deriving it from `MESSAGE_PROCESSOR_PREFIX`
(a `.env` value, defaulting to the node's own hostname) plus a fixed
per-service index makes that uniqueness structural rather than something
tracked on paper.

The message processor declares and consumes from `adsb-{MESSAGE_PROCESSOR_ID}`,
binding that queue to the `adsb` consistent-hash exchange with a weight of `1`
(see [Routing](https://github.com/BrentIO/SkyFollower/blob/main/receiver/README.md#routing)
in the receiver's README for the exchange's shape and its operational
consequences). Because the ID is embedded in the queue name, an abandoned
queue is identifiable by name alone: the RabbitMQ management UI shows
`adsb-turing-node-3-1` with a consumer count of zero once that message
processor is gone.

On startup the message processor attempts to claim a Redis key
`message_processor:{MESSAGE_PROCESSOR_ID}:heartbeat` using `SET NX`. If the
key already exists (i.e., another instance with the same ID is running), the
process exits immediately to prevent duplicate-ID conflicts.

Every message processor instance on a host shares the identical `.env` --
only the numeric suffix of `MESSAGE_PROCESSOR_ID` differs, and that comes
from which fixed service definition it is (`message-processor-1` through
`-8`), not a separate value anyone sets. Scaling out on the same host is
just: add the next number to `COMPOSE_PROFILES` in `.env` (e.g.
`COMPOSE_PROFILES=mp-2,mp-3` for three processors total) and bring it up.
No receiver is touched, and none has to be restarted. See
`docker-compose.message-processor.yaml`'s own comments for why
`deploy.replicas` can't substitute for this (each replica would need its
own volume and its own derivable ID, and Compose doesn't provide either).

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
| `config:rules:version` | SHA-256 hash polled every 5 s; triggers rule reload when changed |
| `config:rules` | JSON rules array; loaded when version changes |
| `config:areas:version` | SHA-256 hash polled every 5 s; triggers area reload when changed |
| `config:areas` | GeoJSON FeatureCollection; loaded when version changes |
| `config:flight_ttl_seconds` | Plain scalar, read once at startup and cached (not hot-reloaded — restart to pick up a changed value); defaults to `300` if unset. Shared with the archive processor, which uses the same value to detect flights split by a processor-count resize. |

### Keys written

| Key pattern | Purpose |
|-------------|---------|
| `message_processor:{ID}:heartbeat` | Liveness key; claimed with `NX` on startup, TTL refreshed every `telemetry_interval_seconds × 2` |
| `registration:{REGISTRATION}` | Reverse-lookup index (registration → ICAO hex); written `NX` when aircraft enrichment is found and a registration exists |
| `metrics:message_processor:{ID}:registration_misses:{hour\|today\|lifetime}` | Incremented each time an `icao_hex:` or `operator:` lookup returns no result. The `_hour` key has a 3600 s TTL; `_today` expires at the next UTC midnight. Both are set on first write via `INCR` + `EXPIREAT`/`EXPIRE`. `_lifetime` has no TTL. |
| `metrics:message_processor:{ID}:aircraft_type_misses:{hour\|today\|lifetime}` | Incremented each time an aircraft type lookup returns no result. Same TTL scheme as above. |

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
| `rules_engine_hwm_ms` | Integer as string | Rules engine duration high-water mark since last publish; resets on publish |
| `rabbitmq_input_queue_depth_hwm` | Integer as string | High-water mark of the input queue's depth since the last publish; sampled at most once every 10 seconds, resets on publish (`-1` if no valid sample landed this window) |
| `local_archive_queue_depth` | Integer as string | Completed flights queued in `completed_flights.db` fallback |
| `dead_letter_queue_depth` | Integer as string | Completed flights dead-lettered after repeatedly failing to publish (see [Dead-Lettering Poison Flights](#dead-lettering-poison-flights)) |
| `registration_misses_hour` | Integer as string | Aircraft Redis cache misses this hour |
| `registration_misses_today` | Integer as string | Aircraft Redis cache misses today (UTC) |
| `aircraft_type_misses_hour` | Integer as string | Aircraft type lookup misses this hour |
| `aircraft_type_misses_today` | Integer as string | Aircraft type lookup misses today (UTC) |
| `active_flights` | Integer as string | Flights currently tracked in the active store |

Each stat is published as its own retained topic (not a combined JSON
payload) every `telemetry_interval_seconds`. Home Assistant autodiscovery
payloads are published to
`homeassistant/sensor/SkyFollower_message_processor_{ID}_{field}/config` on MQTT
connect; each sensor's `state_topic` points directly at its own
`SkyFollower/message-processor/{ID}/statistic/{field}` topic — no `value_template`
needed.

`rabbitmq_input_queue_depth_hwm` is sampled by a dedicated background
loop capped at once every 10 seconds, independent of how low
`telemetry_interval_seconds` is configured, and tracked as a high-water
mark that resets each time telemetry is published.

![RabbitMQ queue-depth high-water mark](./rmq-queue-depth-hwm-sequence.svg)

## Fault Tolerance

When RabbitMQ is unavailable at startup or during operation, completed flights
are written to `completed_flights.db` (SQLite WAL mode) in `data_dir`. On the next
successful RabbitMQ reconnect, the fallback queue is drained oldest-first
before new messages are consumed. Redis and MQTT failures are handled
gracefully and logged; enrichment lookups that fail leave the flight partially
enriched rather than dropping it.

Draining is also attempted independently every `telemetry_interval_seconds`,
not just on a detected reconnect. `_archive()` queues a completed flight to
the fallback on any publish exception without necessarily affecting
`_rmq_connected` or the consume side at all — so a run of publish-only
failures (e.g. a broker-side rejection on the `archive` routing key, with the
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
every successful RabbitMQ reconnect (not just on the `telemetry_interval_seconds`
tick), so a flapping connection reconnecting every few seconds could
otherwise burn through the retry threshold within seconds — dead-lettering
a flight that was never actually poison, just unlucky enough to be at the
head of the queue during a brief instability. `FallbackQueue` also enforces
a minimum time between attempts on the same row (30 seconds, hardcoded,
independent of how often `_drain_fallback()` itself gets called), so
reaching the threshold always takes a real, bounded amount of elapsed
time — not just a burst of rapid reconnect attempts.

### Active flight store durability & crash recovery

`active_flights.db` (SQLite, WAL mode, `data_dir`) holds every currently
tracked flight and is committed to disk after every message. A process end —
whether a deliberate stop (`SIGTERM`/`SIGINT`) or an ungraceful one (OOM-kill,
`docker kill`, host crash) — is recovered identically on the next startup;
there is no special "flush everything" shutdown path, since nothing needs
force-archiving when the store already survives on its own.

On startup, the message processor reopens `active_flights.db` and recovers whatever
flights were still tracked. Recovery is driven by message timestamps, not by
how long the container was down: an internal clock is floored at the most
recent `last_message` among recovered flights (not wall-clock "now"), and
only advances as RabbitMQ messages are actually consumed. This means a
recovered flight is **not** archived just because real time passed while the
container was stopped — if a continuation message for that aircraft is
sitting in the RabbitMQ backlog, it resumes the same flight once the
message processor reconnects and drains the backlog. A genuine gap longer than
`flight_ttl_seconds` — whether it happens live or is discovered while
replaying a backlog — still correctly splits the flight into two records.

MQTT rule notifications for messages older than
`rule_notification_max_lag_seconds` are suppressed during backlog replay
(logged at debug) to avoid flooding MQTT the instant a message processor
reconnects after downtime; the rule still fires and is still recorded in
`matched_rules`/the eventual archived flight.

Because `active_flights.db` only depends on `MESSAGE_PROCESSOR_ID` (which
determines the RabbitMQ queue consumed, `adsb-{MESSAGE_PROCESSOR_ID}`) and
not on container identity, moving the file to a replacement container with
the same `MESSAGE_PROCESSOR_ID` resumes tracking the same way a restart
does. One caveat: the Redis heartbeat key
(`message_processor:{MESSAGE_PROCESSOR_ID}:heartbeat`, `SET NX` with a
TTL of `2 × telemetry_interval_seconds`) must expire — or be deleted
manually — before a replacement container can claim the same ID.

## Rules Engine

Rules and areas are loaded from Redis (`config:rules` / `config:areas`) and
hot-reloaded every 30 seconds when the corresponding version hash keys change.
The rules engine is implemented in `message-processor/rules_engine.py`.

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
MLAT-only archive skip (see [archive-processor/README.md](../archive-processor/README.md))
for MLAT-only flights the user does care about — a specific aircraft, an
area, etc. — without archiving every MLAT contact indiscriminately. A
matched `force_archive` rule still publishes its normal MQTT rule
notification like any other rule; nothing about the flag suppresses that.
