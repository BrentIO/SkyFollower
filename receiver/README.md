# Receiver

The receiver connects to one or more readsb TCP streams (raw ADS-B format),
parses each frame to extract the ICAO hex identifier, wraps the message in a
typed `InboundMessage` envelope, and publishes it to RabbitMQ's consistent-hash
exchange keyed by that hex. When RabbitMQ is unavailable the
receiver writes to a local SQLite fallback queue and drains it automatically on
reconnect. One receiver container handles all configured sources concurrently
(one thread per source).

![Receiver architecture](./receiver.svg)

## Configuration

Reads its configuration from environment variables via `shared/config.py`'s
`load_config("receiver", "rabbitmq", "mqtt", "telemetry")`, interpolated by
Compose from this host's `.env` (written by `scripts/install.sh`).

| Variable | Required | Default | Description |
|---|---|---|---|
| `RECEIVER_NAME` | ✅ | — | Friendly label shown in Home Assistant (device name/model) in place of the generic `Receiver {short-id}` fallback. Sensors don't repeat this in their own names -- `has_entity_name: true` has Home Assistant compose each entity's displayed label from the device name plus the sensor's own short name. Purely cosmetic -- has no bearing on MQTT topic addressing or HA entity identity, which stay keyed by the persisted identity below regardless of what this is set to. |
| `RECEIVER_SOURCES` | ✅ | — | Comma-separated `host:port:source` triples (see below). At least one is required. |
| `RABBITMQ_HOST` | ✅ | — | |
| `RABBITMQ_PORT` | ❌ | `5672` | |
| `RABBITMQ_USERNAME` | ✅ | — | |
| `RABBITMQ_PASSWORD` | ✅ | — | |
| `MQTT_HOST` | ❌ | — | Leave unset to disable MQTT entirely |
| `MQTT_PORT` | ❌ | `1883` | |
| `MQTT_USERNAME` | ❌ | — | Optional MQTT auth; leave unset for an anonymous broker |
| `MQTT_PASSWORD` | ❌ | — | |
| `TELEMETRY_INTERVAL_SECONDS` | ❌ | `30` | How often the receiver publishes MQTT statistic messages |
| `LOG_LEVEL` | ❌ | `info` | `"debug"` for verbose output |

`queue.db` (the RabbitMQ offline fallback) is always written to `/app/data`,
a fixed, non-configurable bind mount -- see `docker-compose.receiver.yaml`.

### `RECEIVER_SOURCES`

Comma-separated `host:port:source` triples, parsed by `shared/config.py`'s
`parse_receiver_sources()`. Each triple's `source` is the tag applied to
every message from that stream: one of `1090`, `978`, or `EXTERNAL`
(case-insensitive).

Example, the SDR-hosting receiver (e.g. on the Raspberry Pi):

```
RECEIVER_SOURCES=192.168.1.10:30002:1090,192.168.1.10:30978:978
```

An `EXTERNAL` source does not need to be co-located with the receiver's SDR
hardware — it's a plain TCP connection like any other source, so its host
can point at any other Beast/raw-Mode-S TCP feed. `EXTERNAL` frames use the
same raw Mode S format as `1090`, so no separate parsing is required.
Nothing prevents adding an `EXTERNAL` triple to the same `RECEIVER_SOURCES`
list above; a second, independent receiver instance is also an option (see
[Running Multiple Receiver Instances](#running-multiple-receiver-instances)
below) — message routing is keyed on `icao_hex`, not receiver identity, so
either shape publishes into the exact same pipeline with no special
handling on the message processor side.

## Receiver Identity

Each receiver container needs a stable identifier to distinguish it from any other receiver publishing to the same MQTT broker -- included in every MQTT topic it publishes (`SkyFollower/receiver/{id}/...`) and in its HA `identifiers`/`unique_id`. This used to be a manually-set `RECEIVER_ID` environment variable, which had no way to enforce that an operator actually set it, or set it uniquely.

Instead, the receiver generates a UUID on first startup and persists it to `{data_dir}/receiver_id` (the same host-mounted directory `queue.db` lives in), reusing it on every subsequent restart. No configuration needed, no collision risk, and it's fully decoupled from the optional `name` field above -- renaming a receiver never changes its underlying identity or orphans its MQTT/HA history.

Because identity is generated per instance (keyed off that instance's own `data_dir` volume) rather than assigned centrally, two or more receivers sharing the same MQTT broker or RabbitMQ never risk a collision -- including two running on the same host, below.

## Running Multiple Receiver Instances

Every receiver deployment -- whether it's the only one on a host, or one of several sharing a host -- follows the exact same pattern: its own folder, containing its own `docker-compose.receiver.yaml` and its own `.env`. There's no separate mechanism for "same host" vs. "separate host"; a folder is a folder either way.

`docker-compose.receiver.yaml` sets no project name of its own. It comes from `COMPOSE_PROJECT_NAME` in that folder's `.env`, which `scripts/install.sh` writes: it derives the default from the destination folder's own name, sanitized (lowercased, anything outside `[a-z0-9_-]` replaced with `-`), so two different folders always get two independent Compose project namespaces -- independent container name, independent `./data/receiver` directory -- instead of colliding on a fixed shared name the way a hardcoded project name would. A folder named `receiver` gets project `skyfollower` (container `skyfollower-receiver-1`); a folder named `receiver-2` gets project `skyfollower-receiver-2` (container `skyfollower-receiver-2-receiver-1`). (The `receiver` folder is special-cased to produce no suffix at all -- since the `receiver` service name is always appended by Compose itself, including it in the project name too would otherwise double up into `skyfollower-receiver-receiver-1`.) It's only a default: `.env` is a plain file, so editing `COMPOSE_PROJECT_NAME` renames the project without touching any tracked file.

To run a second receiver on the same host as the first, run the installer again for the `receiver` role -- it prompts for a folder name each time one is selected:

```bash
./scripts/install.sh --role receiver
# Folder name for this receiver instance [receiver]: receiver-2
# ...then RECEIVER_NAME, RECEIVER_SOURCES, RabbitMQ/MQTT credentials for this instance
```

or, without cloning anything first:

```bash
curl -fsSL https://raw.githubusercontent.com/BrentIO/SkyFollower/main/scripts/install.sh | bash
```

Each instance's Compose project is fully independent, so its `./data/receiver` directory, fallback queue, and auto-generated identity (`receiver_id`, see above) never collide with the first instance's -- stopping, restarting, or upgrading one never touches the other. A third (or fourth, ...) instance is just another `--role receiver` run with a different folder name.

Keep in mind each instance is a full copy of the container -- one thread per `RECEIVER_SOURCES` connection, its own RabbitMQ connection, its own MQTT connection -- so host resource limits, not anything in this compose file, become the real ceiling on how many can run on one host.

## Routing

Every message is published to the durable `adsb` exchange with the aircraft's
ICAO hex as the routing key. The exchange is of type `x-consistent-hash`
(RabbitMQ's `rabbitmq_consistent_hash_exchange` plugin), which hashes that key
and delivers the message to exactly one of the queues bound to it — one queue
per message processor, each bound with a weight of `1`. The same hex always
lands on the same queue, so all messages for an aircraft reach the same
message processor and its per-aircraft flight state, with no coordination
between message processors.

The receiver has no idea how many message processors exist and never needs to
be reconfigured or restarted when that number changes. Each message processor
declares and binds its own queue; the exchange starts routing to it the moment
the binding exists. The exchange name, type and arguments are defined once in
[`shared/rabbitmq_topology.py`](https://github.com/BrentIO/SkyFollower/blob/main/shared/rabbitmq_topology.py) and declared
idempotently by both components on every connect.

The exchange carries an `alternate-exchange` argument pointing at the
`adsb-unroutable` fanout exchange, which feeds the durable `adsb-unroutable`
queue. Anything the hash exchange cannot route — which is everything published
while no message processor queue is bound — ends up there. Without it those
messages would be discarded silently, because the receiver publishes without
publisher confirms and without the `mandatory` flag. A non-zero depth on
`adsb-unroutable` means messages arrived with nothing bound to receive them.

### Cutover to consistent-hash routing

An existing deployment's `adsb-0`, `adsb-1`, … queues are bound to the default
exchange and do not migrate themselves. This is a one-time operational
sequence with a single brief ingest gap. (The full operational runbook will
live in the deployment documentation; this is the minimum needed to perform
the cutover.)

1. Stop the receiver(s). If a receiver's `local_queue_depth` is non-zero, let
   it drain to zero before stopping — entries queued under the old routing
   carry an old-format target and are dead-lettered rather than published
   after the cutover.
2. Let the running message processors drain `adsb-0` … `adsb-{n-1}` to zero.
3. Stop the message processors.
4. Add `rabbitmq_consistent_hash_exchange` to `enabled_plugins` and recreate
   the RabbitMQ container.
5. **Start the message processors one at a time, in the intended slot order.**
   Binding order establishes slot order permanently; this is the only moment
   it can be chosen deliberately.
6. Start the receiver(s).
7. Delete the now-empty `adsb-0` … `adsb-{n-1}` queues.

Flights in progress at cutover split and are stitched back together by the
archive processor, as with any resize.

### Restarting, stopping and removing a message processor

**Restarting is free.** A binding is durable state held by RabbitMQ. A
restarting message processor redeclares its queue and rebinds; the binding
already exists, so the call is a no-op, slot order is untouched, and no
aircraft moves. Host reboots, container recreation and rolling upgrades all
cost nothing.

**Stopping is not unbinding.** Stopping a message processor leaves its queue
and binding intact: its share of traffic accumulates in its queue and drains
when it comes back — the existing, desirable behaviour. Unbinding (deleting
the queue) removes its slot and renumbers every slot after it. The two look
like the same action from the operator's side and are not.

**Remove from the end, never the middle.** Slots are positional. Removing the
last-bound message processor moves about 20% of aircraft, redistributed evenly
over the survivors; removing one from the middle moves about 68%, because the
plugin uses Jump Consistent Hash over positional slots. Treat message
processors as a stack: add and remove at the end.

None of that is data loss. Nothing is dropped and every message still routes
and processes. In-progress flights for the reshuffled aircraft split — the old
message processor's partial state ages out after `flight_ttl_seconds` and
archives as one segment, the new one starts another — and the archive
processor's split-flight stitching merges the adjacent segments. The cost is
degraded archive quality for a few minutes.

## RabbitMQ Thread Safety

`pika.BlockingConnection` (and the async transport underneath it) is not
safe to call concurrently from more than one thread. The receiver has
several threads that need to publish -- one per configured `sources[]`
connection, plus the fallback-drain background thread -- but only one
thread, `rabbitmq` (running `_rmq_loop`), actually owns the connection and
drives `process_data_events()`.

Every other thread reaches the connection only through `_pika_invoke()`,
which uses pika's own thread-safe hand-off (`add_callback_threadsafe`) to
run the real `channel.basic_publish()` call on the `rabbitmq` thread,
while still blocking the calling thread for a synchronous success/failure
result -- so `_publish()`'s fallback-on-failure behavior works exactly as
if the call had been made directly. **No code should ever call a pika
channel or connection method directly from any thread other than
`rabbitmq`** -- doing so reintroduces the exact bug this exists to
prevent: two threads simultaneously inside pika's transport internals,
corrupting its buffers and crashing the connection.

## Fault Tolerance

When RabbitMQ is unavailable (at startup or after a disconnect), messages are
written to `queue.db` (SQLite WAL mode) in `data_dir` via the shared
`FallbackQueue` (see [shared/README.md](../shared/README.md)). Since that
class is payload-only, the receiver wraps `{routing_key, payload}` into one
JSON string before queueing it, and unwraps it again on drain — persisting
the routing key alongside the payload keeps the drain path identical to the
live publish path, with no need to re-parse a stored message body to work
out where it was going. When the RabbitMQ connection is
re-established, the fallback queue is drained oldest-first before new
messages are forwarded. If RabbitMQ drops mid-drain, draining stops cleanly
and resumes on the next reconnect.

Draining is also attempted independently every `telemetry_interval_seconds`,
not just on a detected reconnect. A publish failure can leave messages queued
without the underlying connection ever raising an error (a broker-side
rejection, a channel-level error — anything short of the connection itself
dying), in which case the reconnect-triggered drain never fires again on its
own. The periodic check is a cheap no-op when the queue is already empty.

Both triggers go through the same `drain_in_background()`: it spawns the
actual drain on a background thread and returns immediately, so a slow
drain (e.g. a large backlog) never delays that telemetry cycle's publish.
A single-flight guard (a non-blocking lock) ensures only one drain is ever
in progress at a time regardless of which trigger started it — if the
periodic tick fires while the reconnect-triggered drain is still running,
it's a no-op rather than a second overlapping drain, which could otherwise
select the same queued row twice and publish it twice.

### Dead-Lettering Poison Messages

A message that fails to publish on every drain attempt — not because
RabbitMQ is down, but because something about that specific message causes
a deterministic failure — would otherwise retry forever, and since `drain()`
always re-selects the oldest row first, it would also block every other
queued message behind it indefinitely. `FallbackQueue` tracks a per-row
retry count: below the threshold (5, hardcoded), a failure behaves exactly
as before — stop the drain pass, retry from the top next time. At the
threshold, the row is judged permanently poison: it's written out as a
standalone JSON file under `dead_letters/queue/` in `data_dir` (capped at
100MB total, oldest file evicted first) for manual inspection, and the
drain pass continues to whatever's queued behind it instead of stopping.
There's no automated replay path — a dead-lettered file is purely something
an operator inspects or discards out-of-band (`data_dir` is already a
host-mounted volume, same as `queue.db` itself).

A raw attempt count alone isn't safe: `_drain_fallback()` is called on
every successful RabbitMQ reconnect (not just on the `telemetry_interval_seconds`
tick), so a flapping connection reconnecting every few seconds could
otherwise burn through the retry threshold within seconds — dead-lettering
a message that was never actually poison, just unlucky enough to be at the
head of the queue during a brief instability. `FallbackQueue` also enforces
a minimum time between attempts on the same row (30 seconds, hardcoded,
independent of how often `_drain_fallback()` itself gets called), so
reaching the threshold always takes a real, bounded amount of elapsed
time — not just a burst of rapid reconnect attempts.

## MQTT Topics Published

All topics use the root `SkyFollower`.

| Topic | Payload | Retained |
|-------|---------|----------|
| `SkyFollower/receiver/{receiver_id}/status` | `ONLINE` or `OFFLINE` | Yes |
| `SkyFollower/receiver/{receiver_id}/statistic/{name}` | One retained topic per stat (see fields below) | Yes |

**Statistic topic suffixes (`{name}`):**

| Field | Format | Description |
|-------|--------|-------------|
| `messages_{host}_{port}_per_second` | Float as string | Average message rate for one specific `sources[]` connection since last report |
| `{host}_{port}_connected` | `True` or `False` | Whether the TCP connection to that specific `sources[]` entry's readsb instance is currently open |
| `{host}_{port}_reconnect_count` | Integer as string | Number of times that specific `sources[]` connection has dropped and been re-established since process start |
| `{host}_{port}_connected_attributes` | JSON, e.g. `{"last_message_received": "2026-01-15T10:00:00+00:00"}` | Home Assistant `json_attributes_topic` for the sibling `{host}_{port}_connected` sensor; carries when that specific `sources[]` connection last processed a message. Not published at all until the first message on that connection arrives |
| `local_queue_depth` | Integer as string | Messages queued in the local SQLite fallback (`queue.db`) |
| `dead_letter_queue_depth` | Integer as string | Messages dead-lettered after repeatedly failing to publish (see [Dead-Lettering Poison Messages](#dead-lettering-poison-messages)) |
| `rabbitmq_connected` | `True` or `False` | Whether an active RabbitMQ connection is held |
| `started_at` | UTC ISO-8601 timestamp | Process start time |
| `version` | String | Running image version (`VERSION` env var, `"dev"` if unset) |

A `messages_{host}_{port}_per_second`, `{host}_{port}_connected`, `{host}_{port}_reconnect_count`, and (once traffic has been seen) `{host}_{port}_connected_attributes` topic are published for every connection listed in `sources[]` — keyed by connection (`host`/`port`), not by `source` tag, so two connections sharing the same tag (e.g. two EXTERNAL feeds) are tracked independently instead of being conflated. For example, `{ "host": "adsb.lol", "port": 30105, "source": "EXTERNAL" }` publishes to `messages_adsb.lol_30105_per_second`, `adsb.lol_30105_connected`, `adsb.lol_30105_reconnect_count`, and `adsb.lol_30105_connected_attributes`.

Each stat is published as its own retained topic (not a combined JSON payload) every `telemetry_interval_seconds`, except `{host}_{port}_connected_attributes`, which is itself a small JSON payload -- Home Assistant's `json_attributes_topic` mechanism for attaching extra attributes to a sensor doesn't have a plain-value equivalent.

Home Assistant autodiscovery payloads are published to
`homeassistant/sensor/SkyFollower_receiver_{receiver_id}_{field}/config` on MQTT connect,
with `has_entity_name: true` so Home Assistant composes each entity's
displayed label from the device name plus the entity's own short name
rather than the receiver's name being baked into every sensor name.
Each sensor's `state_topic` points directly at its own
`SkyFollower/receiver/{receiver_id}/statistic/{field}` topic — no
`value_template` needed. The `{host}_{port}_connected` sensor additionally
sets `json_attributes_topic` to its sibling `{host}_{port}_connected_attributes`
topic, exposing a `last_message_received` attribute instead of a separate
`_last_message_at` entity.

## Adding or Changing readsb Sources

1. Update `RECEIVER_SOURCES` in `.env` — add, remove, or edit `host:port:source` triples.
2. Restart the receiver container (`docker compose up -d` picks up the new `.env`).

Each source runs in its own thread; adding a source does not affect others.
The `source` tag you choose is stamped on every message and carried through to
the archived flight record.
