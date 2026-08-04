# Receiver

The receiver connects to one or more readsb TCP streams (raw ADS-B format),
parses each frame to extract the ICAO hex identifier, wraps the message in a
typed `InboundMessage` envelope, and routes it to the appropriate RabbitMQ
queue based on a modulo-bucketing scheme. When RabbitMQ is unavailable the
receiver writes to a local SQLite fallback queue and drains it automatically on
reconnect. One receiver container handles all configured sources concurrently
(one thread per source).

![Receiver architecture](./receiver.svg)

## Configuration (`settings.json`)

### Top-level fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | string | — | Optional friendly label shown in Home Assistant (device name/model and every sensor label) in place of the generic `Receiver {short-id}` fallback. Purely cosmetic -- has no bearing on MQTT topic addressing or HA entity identity, which stay keyed by the persisted identity below regardless of what (or whether) this is set. |
| `sources` | array | — | List of readsb source objects (see below). At least one is required. |
| `processor_count` | integer | `1` | Total number of message processor containers. Must match the number of active message processor services. Used to compute `queue_name = adsb-{int(icao_hex, 16) % processor_count}`. Increment this when adding a message processor. |
| `rabbitmq` | object | — | RabbitMQ connection settings (see below). |
| `mqtt` | object | — | MQTT broker settings (see below). Omit the key entirely to disable MQTT. |
| `telemetry_interval_seconds` | integer | `30` | How often (seconds) the receiver publishes MQTT statistic messages. |
| `data_dir` | string | `"/app/data"` | Host-mounted directory where `queue.db` (the RabbitMQ offline fallback) is written. |
| `log_level` | string | `"info"` | Log verbosity. Set to `"debug"` for verbose output. |

### `sources[]` object

| Field | Type | Description |
|-------|------|-------------|
| `host` | string | Hostname or IP of the readsb instance. |
| `port` | integer | TCP port of the readsb raw output (e.g. `30002` for 1090 MHz, `30978` for 978 MHz UAT). |
| `source` | string | Tag applied to every message from this stream. One of `"1090"`, `"978"`, or `"MLAT"`. |

Example, the SDR-hosting receiver (e.g. on the Raspberry Pi):

```json
{
  "sources": [
    { "host": "192.168.1.10", "port": 30002, "source": "1090" },
    { "host": "192.168.1.10", "port": 30978, "source": "978" }
  ]
}
```

An `MLAT` source does not need to be co-located with the receiver's SDR
hardware — it's a plain TCP connection like any other source, so `host` can
point at a remote MLAT-results feed (e.g. a readsb instance receiving results
from an `mlat-client`). MLAT frames use the same raw Mode S format as `1090`,
so no separate parsing is required. Nothing prevents adding an `MLAT` entry
to the same `sources[]` list above, but a **separate receiver container**
is recommended instead — message routing is keyed on
`icao_hex`, not receiver identity, so a second instance publishes into the
exact same pipeline with no special handling on the message processor side, while
keeping internet-facing MLAT ingestion off the resource-constrained device
handling the local RTL-SDR hardware. Any number of MLAT providers can be
configured on that instance — each is its own `sources[]` entry with
`source: "MLAT"`:

```json
{
  "sources": [
    { "host": "mlat-server-a.example.com", "port": 30105, "source": "MLAT" },
    { "host": "mlat-server-b.example.com", "port": 30105, "source": "MLAT" }
  ]
}
```

These two examples match `config/receiver/settings.json.example` (the
SDR-hosting instance) and `config/receiver/mlat-settings.json.example` (a
dedicated MLAT instance) respectively. There's no separate compose file for
the MLAT instance -- it's the exact same `receiver` image and the same
`docker-compose.receiver.yaml`, just deployed a second time on its own host
with a different `settings.json`. Deploying it means cloning the repo on
that host, copying `config/receiver/mlat-settings.json.example` to
`config/receiver/settings.json`, and bringing it up the same way as any
other receiver:

```bash
docker compose -f docker-compose.receiver.yaml up -d
```

It publishes MQTT topics under its own independently-generated receiver
identity (see Receiver Identity below) and drains through its own
`queue.db`, entirely independent of the SDR-hosting instance -- nothing
about it needs to be told it's "the MLAT one" beyond which `sources[]`
entries its `settings.json` lists.

![Receiver MLAT provider topology](./receiver-mlat-topology.svg)

### `rabbitmq` object

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | string | — | RabbitMQ hostname or IP. |
| `port` | integer | `5672` | RabbitMQ AMQP port. |
| `username` | string | — | RabbitMQ username. |
| `password` | string | — | RabbitMQ password. |

### `mqtt` object

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `host` | string | — | MQTT broker hostname or IP. |
| `port` | integer | `1883` | MQTT broker port. |
| `username` | string | — | MQTT username. Optional — omit both `username` and `password` to connect anonymously. |
| `password` | string | — | MQTT password. |

## Receiver Identity

Each receiver container needs a stable identifier to distinguish it from any other receiver publishing to the same MQTT broker -- included in every MQTT topic it publishes (`SkyFollower/receiver/{id}/...`) and in its HA `identifiers`/`unique_id`. This used to be a manually-set `RECEIVER_ID` environment variable, which had no way to enforce that an operator actually set it, or set it uniquely.

Instead, the receiver generates a UUID on first startup and persists it to `{data_dir}/receiver_id` (the same host-mounted directory `queue.db` lives in), reusing it on every subsequent restart. No configuration needed, no collision risk, and it's fully decoupled from the optional `name` field above -- renaming a receiver never changes its underlying identity or orphans its MQTT/HA history.

## Routing

Each incoming message is routed to a durable RabbitMQ queue named
`adsb-{n}` where:

```
n = int(icao_hex, 16) % processor_count
```

This ensures all messages for a given aircraft always go to the same message processor,
preserving per-aircraft flight state without coordination between message processors.
On RabbitMQ connect the receiver pre-declares all queues (`adsb-0` through
`adsb-{processor_count - 1}`).

## Fault Tolerance

When RabbitMQ is unavailable (at startup or after a disconnect), messages are
written to `queue.db` (SQLite WAL mode) in `data_dir` via the shared
`FallbackQueue` (see [shared/README.md](../shared/README.md)). Since that
class is payload-only, the receiver wraps `{queue_name, payload}` into one
JSON string before queueing it, and unwraps it again on drain — `queue_name`
(the target RabbitMQ routing key) is computed once at insert time and has
to survive being persisted alongside the payload, not recomputed against a
possibly-since-changed `processor_count`. When the RabbitMQ connection is
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
| `{host}_{port}_last_message_at` | UTC ISO-8601 timestamp | When that specific `sources[]` connection last processed a message; not published at all until the first one arrives |
| `local_queue_depth` | Integer as string | Messages queued in the local SQLite fallback (`queue.db`) |
| `dead_letter_queue_depth` | Integer as string | Messages dead-lettered after repeatedly failing to publish (see [Dead-Lettering Poison Messages](#dead-lettering-poison-messages)) |
| `rabbitmq_connected` | `True` or `False` | Whether an active RabbitMQ connection is held |
| `started_at` | UTC ISO-8601 timestamp | Process start time |
| `version` | String | Running image version (`VERSION` env var, `"dev"` if unset) |

A `messages_{host}_{port}_per_second`, `{host}_{port}_connected`, `{host}_{port}_reconnect_count`, and (once traffic has been seen) `{host}_{port}_last_message_at` topic are published for every connection listed in `sources[]` — keyed by connection (`host`/`port`), not by `source` tag, so two connections sharing the same tag (e.g. two MLAT feeds) are tracked independently instead of being conflated. For example, `{ "host": "adsb.lol", "port": 30105, "source": "MLAT" }` publishes to `messages_adsb.lol_30105_per_second`, `adsb.lol_30105_connected`, `adsb.lol_30105_reconnect_count`, and `adsb.lol_30105_last_message_at`.

Each stat is published as its own retained topic (not a combined JSON payload) every `telemetry_interval_seconds`.

Home Assistant autodiscovery payloads are published to
`homeassistant/sensor/SkyFollower_receiver_{receiver_id}_{field}/config` on MQTT connect.
Each sensor's `state_topic` points directly at its own
`SkyFollower/receiver/{receiver_id}/statistic/{field}` topic — no
`value_template` needed.

## Adding or Changing readsb Sources

1. Update the `sources` array in `settings.json` — add, remove, or edit entries.
2. Restart the receiver container.

Each source runs in its own thread; adding a source does not affect others.
The `source` tag you choose is stamped on every message and carried through to
the archived flight record.
