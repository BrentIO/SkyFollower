# core-health

| | |
|---|---|
| **Purpose** | Standalone, always-on RabbitMQ + Redis monitor. Polls RabbitMQ's Management HTTP API and Redis's `INFO`/`MEMORY STATS` on its own connections and publishes curated MQTT/Home Assistant telemetry for both, replacing per-component RabbitMQ queue-depth self-polling |
| **Run frequency** | Always-on, two independent poll loops (RabbitMQ every 10s, Redis every 60s) |
| **Reads/writes** | RabbitMQ Management API (read-only), Redis (`INFO`/`MEMORY STATS` plus plain key reads, both via the same default-user credential every other component uses) — no direct RabbitMQ AMQP connection, no S3 |

## How it works

Two independent background loops, neither blocking the other:

- **RabbitMQ, every 10s**: `GET /api/overview` (broker-wide connection
  count), `GET /api/nodes` (per-node memory/disk alarm flags — `/api/overview`
  itself doesn't carry these despite being the endpoint originally named for
  this data point during design; polling `/api/nodes` too is what actually
  answers it), `GET /api/queues/%2F` (every queue in one call), and
  `GET /api/exchanges/%2F/adsb` (the `adsb` exchange's own aggregate
  publish velocity — `shared/rabbitmq_topology.py`'s `ADSB_EXCHANGE`
  constant, no discovery needed). The queue list is filtered to
  SkyFollower's own queues via `shared/rabbitmq_topology.py`'s
  `SKYFOLLOWER_RABBITMQ_RESOURCE_PATTERN` — the exact same regex
  `scripts/install.sh`'s `provision_rabbitmq_users()` uses to scope the
  application user's permissions, so the two definitions of "what
  SkyFollower owns" can't drift apart (bash can't import a Python
  constant, so the two copies are kept identical by hand — see the comments
  at each site).
- **Redis, every 60s**: `INFO everything` + `MEMORY STATS`, via the same
  default-user credential every other component authenticates with (see
  [Credentials](#credentials) for why this isn't a separate scoped ACL
  user). No keyspace `SCAN`/`--bigkeys` ever — deliberately out of scope
  for this recurring loop (real cost against a large keyspace for a shape
  that doesn't change fast); the two commands above cover every field this
  component publishes.

Every entity core-health publishes for a queue or for Redis uses its own
availability topic (`SkyFollower/core-health/status`), not the topic the
device otherwise belongs to — a message processor being offline doesn't
mean its queue's RabbitMQ-observed stats are unavailable (the broker still
reports them), and vice versa. Queue/broker/Redis entities also carry
`expire_after` (three poll intervals), so a poll failure that leaves a
retained MQTT value in place doesn't read as still-fresh in Home Assistant
indefinitely.

### Where entities land

A Home Assistant device is just whatever entities share one
`device.identifier` — core-health doesn't need to import or call into any
other component's code, only know its device-identifier convention:

| Queue | Device | Identifier |
|---|---|---|
| `skyfollower-message-processor-{id}` | That processor's own existing device | `SkyFollower_message_processor_{id}` (matches `message-processor/main.py`) |
| `archive` | The archive processor's own existing device | `SkyFollower_archive` (matches `archive-processor/main.py`) |
| `adsb-unroutable`, anything else SkyFollower-owned with no natural owner | New `SkyFollower Core` device | `SkyFollower_Core` |

Broker-wide RabbitMQ stats and all of Redis also land on the `SkyFollower
Core` device. Per-queue entity `unique_id`/`object_id`s are suffixed
`_queue_{field}` (e.g. `SkyFollower_message_processor_{id}_queue_consumers`)
so they can never collide with the owning component's own entities on the
same device (e.g. `SkyFollower_message_processor_{id}_processing_time_hwm_ms`).

### Counter mimicry (message-processor and the receiver)

core-health also reads and publishes, on message-processor's and the
receiver's behalf, a handful of their own Redis-backed application
counters — using those components' *exact* existing topic paths,
`unique_id`/`object_id`, and device blocks, so nothing on the wire
distinguishes core-health publishing these from the owning component
publishing them itself:

- **message-processor**: `registration_misses_{hour,today,lifetime}`,
  `operator_misses_{today,lifetime}`,
  `total_messages_processed_{hour,today,lifetime}` — enumerated for free
  from the same `skyfollower-message-processor-{id}` queue list already
  polled above, no separate discovery mechanism.
- **receiver**: `messages_{host}_{port}_total_{hour,today,lifetime}` per
  source connection — discovered via one cheap `SMEMBERS` per RabbitMQ poll
  cycle against a small Redis index set of currently-claimed receiver
  names (never a keyspace scan), then one `GET` per receiver for its
  registration entry (its source list).

A missing period key means the count is genuinely zero, not unavailable —
mirroring message-processor's own `_redis_counter()` precedent. This is
distinct from core-health's own Redis connectivity failing outright, which
is a real "skip this tick, let the entity age out" case (see
`_redis_counter_or_none()` in `main.py`).

### Redis keys

message-processor's counter-*writing* side (`operator_misses`,
`total_messages_processed`, and the reset-mechanism fix for
`registration_misses`) has since landed, and this component's reading side
was reconciled to it: `metrics_registration_misses_key()`/
`metrics_operator_misses_key()`/`metrics_total_messages_processed_key()`
are all real `shared/redis_keys.py` builders now, imported directly rather
than shimmed locally. message-processor is write-only for these three
counters (accumulates in memory, flushes to Redis on its own telemetry
cadence — see `message-processor/README.md`) — this component remains the
only one that ever publishes them over MQTT/Home Assistant.

The receiver's identity/registration mechanism has landed too, and this
component's reading side is reconciled to it in the same way:
`receiver_registry_index_key()`/`receiver_registration_key()`/
`receiver_message_count_key()` are all real `shared/redis_keys.py`
builders now, imported directly rather than shimmed locally. receiver is
write-only for these (accumulates in memory, flushes to Redis on its own
telemetry cadence — see `receiver/README.md`) — this component remains
the only one that ever publishes them over MQTT/Home Assistant. No
provisional local key shims remain in `main.py`.

## Configuration

Reads its configuration from environment variables via `shared/config.py`'s
`load_config("rabbitmq_management", "redis", "mqtt")`, interpolated by
Compose from this host's `.env` (written by `scripts/install.sh`).

| Variable | Required | Default | Description |
|---|---|---|---|
| `RABBITMQ_HOST` | ✅ | — | Same broker every other component connects to |
| `RABBITMQ_MANAGEMENT_PORT` | ❌ | `15672` | RabbitMQ's Management HTTP API port, not the AMQP port |
| `RABBITMQ_MONITORING_USERNAME` | ✅ | — | RabbitMQ's built-in `monitoring`-tagged user (see [Credentials](#credentials)) |
| `RABBITMQ_MONITORING_PASSWORD` | ✅ | — | |
| `REDIS_HOST` | ✅ | — | Same Redis every other component connects to |
| `REDIS_PORT` | ❌ | `6379` | |
| `REDIS_PASSWORD` | ✅ | — | The same default-user credential every other component uses — for both `INFO`/`MEMORY` introspection and plain key reads (application counters); see [Credentials](#credentials) for why this isn't a separate scoped user |
| `MQTT_HOST` | ❌ | — | Leave unset to disable MQTT entirely (core-health still polls, but publishes nothing) |
| `MQTT_PORT` | ❌ | `1883` | |
| `MQTT_USERNAME` | ❌ | — | Optional MQTT auth; leave unset for an anonymous broker |
| `MQTT_PASSWORD` | ❌ | — | |
| `LOG_LEVEL` | ❌ | `info` | `"debug"` for verbose output |

## Credentials

- **RabbitMQ**: a new user tagged `monitoring` — RabbitMQ's built-in tag for
  broker-wide, read-only Management API visibility, provisioned by
  `scripts/install.sh` once the `rabbitmq` container reports healthy (see
  `provision_rabbitmq_users()`). No per-resource permission is granted (or
  needed): the `monitoring` tag alone is what grants visibility, so this
  user's `configure`/`write`/`read` permissions are all set to match
  nothing.
- **Redis**: no new credential — core-health authenticates as the same
  "default" user via `REDIS_PASSWORD` every other component already uses,
  for both `INFO`/`MEMORY` and plain key reads. A separate, scoped
  (`INFO`/`MEMORY`-only) ACL user was designed and considered — it would
  have been the first Redis ACL user in this repo — but dropped: combining
  a new ACL user with the `redis` service's existing `--requirepass`
  mechanism was confirmed live to silently disable password authentication
  on the `default` user entirely (not a startup error — Redis accepts an
  unauthenticated connection with no `AUTH` needed at all), unless the
  ACL file is pre-seeded with the default user's own credentials before
  Redis's very first boot. That bootstrapping complexity and risk wasn't
  worth it for a restriction this component's own code already honors
  by simply never calling a write command — the same trust model every
  other component (message-processor, receiver, archive-processor)
  already operates under today.

## MQTT

Published continuously (all retained) under `SkyFollower/core-health/...`,
plus mimicked topics under the owning component's own root for the counter
passthrough fields described above:

| Topic | Published from |
|---|---|
| `SkyFollower/core-health/status` | `ONLINE`/`OFFLINE`, this component's own availability (LWT) |
| `SkyFollower/core-health/statistic/{started_at,version,rabbitmq_connected,redis_connected}` | General |
| `SkyFollower/core-health/rabbitmq/statistic/{field}` | Broker-wide RabbitMQ stats |
| `SkyFollower/core-health/redis/statistic/{field}` | Redis stats |
| `SkyFollower/core-health/message-processor/{id}/statistic/{field}` | A processor's queue stats |
| `SkyFollower/core-health/archive/statistic/{field}` | The archive queue's stats |
| `SkyFollower/core-health/queue/{queue}/statistic/{field}` | Any other SkyFollower-owned queue's stats (e.g. `adsb-unroutable`) |
| `SkyFollower/message-processor/{id}/statistic/{field}` | Mimicked message-processor counters (exact existing topic) |
| `SkyFollower/receiver/{name}/statistic/{field}` | Mimicked receiver counters (exact existing topic) |

Home Assistant autodiscovery configs are published on every MQTT
(re)connect for the static broker-wide/Redis/general entities, and
opportunistically the first time a given queue/counter/receiver is seen
during this process's lifetime (dynamic entities aren't known in advance —
a message processor or receiver can appear or disappear while core-health
keeps running).

## Deployment

Deployed via `docker-compose.core.yaml`, always-on alongside
`rabbitmq`/`redis`/`ofelia` (`restart: unless-stopped`, no `profiles:`).
No `depends_on` — both clients simply retry on their own poll cadence
until each backend is reachable, and this component keeps no state of its
own (a pure poller/republisher), so it has no data volume, only the same
`tmpfs`-mounted healthcheck heartbeat file every other long-running
component uses.
