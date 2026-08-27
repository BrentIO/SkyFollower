# core-health

| | |
|---|---|
| **Purpose** | Standalone, always-on RabbitMQ + Redis monitor. Polls RabbitMQ's Management HTTP API and Redis's `INFO`/`MEMORY STATS` on its own connections and publishes curated MQTT/Home Assistant telemetry for both, replacing per-component RabbitMQ queue-depth self-polling |
| **Run frequency** | Always-on, two independent poll loops (RabbitMQ every 10s, Redis every 60s) |
| **Reads/writes** | RabbitMQ Management API (read-only), Redis (`INFO`/`MEMORY STATS` via a scoped ACL user, plus plain key reads via the same default-user credential every other component uses) — no direct RabbitMQ AMQP connection, no S3 |

## How it works

Two independent background loops, neither blocking the other:

- **RabbitMQ, every 10s**: `GET /api/overview` (broker-wide connection
  count), `GET /api/nodes` (per-node memory/disk alarm flags — `/api/overview`
  itself doesn't carry these despite being the endpoint originally named for
  this data point during design; polling `/api/nodes` too is what actually
  answers it), and `GET /api/queues/%2F` (every queue in one call). The
  queue list is filtered to SkyFollower's own queues via
  `shared/rabbitmq_topology.py`'s `SKYFOLLOWER_RABBITMQ_RESOURCE_PATTERN` —
  the exact same regex `scripts/install.sh`'s `provision_rabbitmq_users()`
  uses to scope the application user's permissions, so the two definitions
  of "what SkyFollower owns" can't drift apart (bash can't import a Python
  constant, so the two copies are kept identical by hand — see the comments
  at each site).
- **Redis, every 60s**: `INFO everything` + `MEMORY STATS`, via the
  scoped, read-only ACL user described below. No keyspace `SCAN`/`--bigkeys`
  ever — deliberately out of scope for this recurring loop (real cost
  against a large keyspace for a shape that doesn't change fast); the two
  commands above cover every field this component publishes.

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

### Provisional Redis keys

message-processor's/the receiver's own counter-*writing* sides
(`operator_misses`, `total_messages_processed`, and the receiver's identity
redesign + per-connection counters) are separate, not-yet-implemented
changes at the time this component was built. This component's *reading*
side was written defensively against the key-naming conventions described
for them, using the same shape `shared/redis_keys.py`'s existing
`metrics_registration_misses_key()`/`archive_search_index_key()` already
establish, but the exact key strings below are this component's own
provisional choice (not added to `shared/redis_keys.py` itself, to avoid a
duplicate/conflicting definition landing there from two different PRs) and
may need reconciling once those changes land:

| Key | Shape | Written by |
|---|---|---|
| `metrics:message_processor:{id}:operator_misses:{period}` | `period` ∈ `today`, `lifetime` | message-processor (not yet implemented) |
| `metrics:message_processor:{id}:total_messages_processed:{period}` | `period` ∈ `hour`, `today`, `lifetime` | message-processor (not yet implemented) |
| `receiver:index` | `SET` of currently-claimed receiver names | receiver (not yet implemented) |
| `receiver:{name}:registration` | JSON `{"sources": [{"host", "port", "source"}, ...]}`, TTL'd alongside the receiver's own heartbeat | receiver (not yet implemented) |
| `metrics:receiver:{name}:messages_{host}_{port}_total:{period}` | `period` ∈ `hour`, `today`, `lifetime` | receiver (not yet implemented) |

Until those land, core-health simply reads these keys as always-absent and
publishes `0` for every field they'd back — exactly the same behavior as
after they land and the count is genuinely zero so far this period.

## Configuration

Reads its configuration from environment variables via `shared/config.py`'s
`load_config("rabbitmq_management", "redis", "redis_monitoring", "mqtt")`,
interpolated by Compose from this host's `.env` (written by
`scripts/install.sh`).

| Variable | Required | Default | Description |
|---|---|---|---|
| `RABBITMQ_HOST` | ✅ | — | Same broker every other component connects to |
| `RABBITMQ_MANAGEMENT_PORT` | ❌ | `15672` | RabbitMQ's Management HTTP API port, not the AMQP port |
| `RABBITMQ_MONITORING_USERNAME` | ✅ | — | RabbitMQ's built-in `monitoring`-tagged user (see [Credentials](#credentials)) |
| `RABBITMQ_MONITORING_PASSWORD` | ✅ | — | |
| `REDIS_HOST` | ✅ | — | Same Redis every other component connects to |
| `REDIS_PORT` | ❌ | `6379` | |
| `REDIS_PASSWORD` | ✅ | — | The same default-user credential every other component uses — needed here for plain key reads (application counters), not for `INFO`/`MEMORY` |
| `REDIS_MONITORING_USERNAME` | ✅ | — | Scoped Redis ACL user, `INFO`/`MEMORY` only (see [Credentials](#credentials)) |
| `REDIS_MONITORING_PASSWORD` | ✅ | — | |
| `MQTT_HOST` | ❌ | — | Leave unset to disable MQTT entirely (core-health still polls, but publishes nothing) |
| `MQTT_PORT` | ❌ | `1883` | |
| `MQTT_USERNAME` | ❌ | — | Optional MQTT auth; leave unset for an anonymous broker |
| `MQTT_PASSWORD` | ❌ | — | |
| `LOG_LEVEL` | ❌ | `info` | `"debug"` for verbose output |

## Credentials

Two new, least-privilege credentials, both provisioned by
`scripts/install.sh` once their respective container reports healthy (see
`provision_rabbitmq_users()`/`provision_redis_monitoring_user()`):

- **RabbitMQ**: a user tagged `monitoring` — RabbitMQ's built-in tag for
  broker-wide, read-only Management API visibility. No per-resource
  permission is granted (or needed): the `monitoring` tag alone is what
  grants visibility, so this user's `configure`/`write`/`read` permissions
  are all set to match nothing.
- **Redis**: the first Redis ACL user in this repo (every other component
  authenticates as the single "default" user via `REDIS_PASSWORD` alone).
  Scoped via `ACL SETUSER` to `INFO`/`MEMORY` only — no general key access.
  **Known limitation**: the `redis` service isn't configured with an
  `--aclfile`, so this ACL user lives only in the running server's memory
  and does **not** survive a `redis` container restart/recreate. Re-run
  `scripts/install.sh` for the core role afterward to recreate it; until
  then, core-health's Redis-derived entities simply go unavailable, same as
  any other Redis outage.

core-health also uses the same default-user `REDIS_PASSWORD` every other
component already has, for plain key reads (the counter mimicry above) —
that traffic is deliberately kept off the new scoped user, which stays
genuinely `INFO`/`MEMORY`-only as designed.

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
