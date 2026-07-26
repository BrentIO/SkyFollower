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
to the same `sources[]` list above, but a **separate receiver container and
`RECEIVER_ID`** is recommended instead — message routing is keyed on
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
SDR-hosting instance, wired up in `docker-compose.receiver.yaml`) and
`config/receiver/mlat-settings.json.example` (a dedicated MLAT instance,
wired up in `docker-compose.receiver-mlat.yaml`) respectively. Deploying the
MLAT instance means cloning the repo on its own host, copying
`config/receiver/mlat-settings.json.example` to
`config/receiver/mlat-settings.json`, and bringing it up independently:

```bash
docker compose -f docker-compose.receiver-mlat.yaml up -d
```

It publishes MQTT topics under its own `RECEIVER_ID` (`"1"` by default in
that compose file) and drains through its own `queue.db`, entirely
independent of the SDR-hosting instance's `RECEIVER_ID` (`"0"`).

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

## `RECEIVER_ID` Environment Variable

`RECEIVER_ID` is an optional integer environment variable (default `0`). It distinguishes multiple receiver containers publishing to the same MQTT broker and is included in every MQTT topic published by the receiver.

```yaml
environment:
  RECEIVER_ID: "0"
```

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
written to `queue.db` (SQLite WAL mode) in `data_dir`. Each row stores the
target `queue_name`, the JSON payload, and the `received_at` timestamp. When
the RabbitMQ connection is re-established, the fallback queue is drained
oldest-first before new messages are forwarded. If RabbitMQ drops mid-drain,
draining stops cleanly and resumes on the next reconnect.

Draining is also attempted independently every `telemetry_interval_seconds`,
not just on a detected reconnect. A publish failure can leave messages queued
without the underlying connection ever raising an error (a broker-side
rejection, a channel-level error — anything short of the connection itself
dying), in which case the reconnect-triggered drain never fires again on its
own. The periodic check is a cheap no-op when the queue is already empty.

## MQTT Topics Published

All topics use the root `SkyFollower`.

| Topic | Payload | Retained |
|-------|---------|----------|
| `SkyFollower/receiver/{receiver_id}/status` | `ONLINE` or `OFFLINE` | Yes |
| `SkyFollower/receiver/{receiver_id}/statistic/{name}` | One retained topic per stat (see fields below) | Yes |

**Statistic topic suffixes (`{name}`):**

| Field | Format | Description |
|-------|--------|-------------|
| `messages_1090_per_second` | Float as string | Average 1090 MHz message rate since last report |
| `messages_978_per_second` | Float as string | Average 978 MHz UAT message rate since last report; only present if a `978` source is configured |
| `messages_MLAT_per_second` | Float as string | Average MLAT message rate since last report; only present if an `MLAT` source is configured |
| `local_queue_depth` | Integer as string | Messages queued in the local SQLite fallback (`queue.db`) |
| `rabbitmq_connected` | `True` or `False` | Whether an active RabbitMQ connection is held |
| `started_at` | UTC ISO-8601 timestamp | Process start time |

A `messages_{source}_per_second` topic is published for every source tag present in `sources[]` — the table above lists the currently supported tags, not a fixed schema.

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
