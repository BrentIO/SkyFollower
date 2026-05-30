# SkyFollower Receiver

The receiver connects to one or more readsb TCP streams (raw ADS-B format),
parses each frame to extract the ICAO hex identifier, wraps the message in a
typed `InboundMessage` envelope, and routes it to the appropriate RabbitMQ
queue based on a modulo-bucketing scheme. When RabbitMQ is unavailable the
receiver writes to a local SQLite fallback queue and drains it automatically on
reconnect. One receiver container handles all configured sources concurrently
(one thread per source).

## Configuration (`settings.json`)

### Top-level fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sources` | array | — | List of readsb source objects (see below). At least one is required. |
| `processor_count` | integer | `1` | Total number of processor containers. Must match the number of active processor services. Used to compute `queue_name = adsb-{int(icao_hex, 16) % processor_count}`. Increment this when adding a processor. |
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
| `source` | string | Tag applied to every message from this stream. One of `"1090"`, `"978"`, or `"mlat"`. |

Example with two sources:

```json
{
  "sources": [
    { "host": "192.168.1.10", "port": 30002, "source": "1090" },
    { "host": "192.168.1.10", "port": 30978, "source": "978" }
  ]
}
```

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

## Routing

Each incoming message is routed to a durable RabbitMQ queue named
`adsb-{n}` where:

```
n = int(icao_hex, 16) % processor_count
```

This ensures all messages for a given aircraft always go to the same processor,
preserving per-aircraft flight state without coordination between processors.
On RabbitMQ connect the receiver pre-declares all queues (`adsb-0` through
`adsb-{processor_count - 1}`).

## Fault Tolerance

When RabbitMQ is unavailable (at startup or after a disconnect), messages are
written to `queue.db` (SQLite WAL mode) in `data_dir`. Each row stores the
target `queue_name`, the JSON payload, and the `received_at` timestamp. When
the RabbitMQ connection is re-established, the fallback queue is drained
oldest-first before new messages are forwarded. If RabbitMQ drops mid-drain,
draining stops cleanly and resumes on the next reconnect.

## MQTT Topics Published

All topics use the root `SkyFollower`.

| Topic | Payload | Retained |
|-------|---------|----------|
| `SkyFollower/receiver/status` | `ONLINE` or `OFFLINE` | Yes |
| `SkyFollower/receiver/statistic/started_at` | ISO 8601 UTC timestamp | Yes |
| `SkyFollower/receiver/statistic/messages_{source}_per_second` | float (one per configured source, e.g. `messages_1090_per_second`) | No |
| `SkyFollower/receiver/statistic/local_queue_depth` | integer | No |
| `SkyFollower/receiver/statistic/rabbitmq_connected` | `"true"` or `"false"` | No |

Rates are computed over a 30-second rolling window. Telemetry is published
every `telemetry_interval_seconds`.

Home Assistant autodiscovery payloads are published to
`homeassistant/sensor/SkyFollower_receiver_{name}/config` on MQTT connect.

## Adding or Changing readsb Sources

1. Update the `sources` array in `settings.json` — add, remove, or edit entries.
2. Restart the receiver container.

Each source runs in its own thread; adding a source does not affect others.
The `source` tag you choose is stamped on every message and carried through to
the archived flight record.
