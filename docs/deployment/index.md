# Deployment

SkyFollower is a single monorepo, but it deploys across several
independent hosts — each one brings up one or more Docker Compose files,
either from a full repo clone or, for a pre-built-image deployment, just
the file(s) it needs (see [Getting Started](/getting-started/) for both
paths). Every role brings up exactly one compose file except the core
host, which brings up two (`docker-compose.core.yaml` and
`docker-compose.management-ui.yaml`), since `management-ui`'s only
dependency is Redis and there's no reason to entangle its lifecycle with
the rabbitmq/redis/runner stack's compose project. See
[Getting Started](/getting-started/) for the commands to actually bring a
host up once you know which compose file(s) it runs.

## Compose Files

Each compose file below is meant to run on its own host, with the
exception of `docker-compose.core.yaml` and
`docker-compose.management-ui.yaml`, which are typically co-located on
the same host but kept as separate compose projects (`management-ui`'s
only dependency is Redis, so it can move to a different host later
without disturbing rabbitmq/redis/runners). The message processor is
designed to scale by adding more hosts, each running the same compose
file. The MLAT receiver is optional, dedicated to MLAT-only `sources[]`,
and deployed separately from the host running the local RTL-SDR
hardware. Archive compaction is also optional (behind the `compaction`
Compose profile) and runs its own `ofelia` instance alongside the
archive processor. Get the relevant file(s) onto each host — clone the
repo, or use `scripts/download-host-files.sh` to fetch just what a given
role needs (see [Getting Started](/getting-started/)) — populate the
relevant `config/` settings files, then bring up the appropriate
file(s):

| File | Role | Services |
|------|------|---------|
| `docker-compose.receiver.yaml` | ADS-B reception (Raspberry Pi); also the optional dedicated MLAT receiver (same file, own `settings.json`, its own auto-generated identity) | `receiver` |
| `docker-compose.core.yaml` | Message bus + enrichment data | `rabbitmq`, `redis`, `ofelia`, all runners |
| `docker-compose.management-ui.yaml` | Rules/areas API (co-located with `docker-compose.core.yaml`) | `management-ui` |
| `docker-compose.message-processor.yaml` | Flight state + rules (scale by adding hosts) | `message-processor-0` (one per host) |
| `docker-compose.archive.yaml` | Long-term storage | `archive-processor`; `archive-compaction` + its own `ofelia` instance (optional, behind the `compaction` Compose profile — see [Archive Compaction](/components/archive-compaction)) |

## Components

| Container | Description | Default port |
|-----------|-------------|--------------|
| `receiver` | Reads raw ADS-B frames from readsb TCP streams; routes to RabbitMQ queues | — |
| `receiver` (MLAT instance, optional) | Same image, its own auto-generated identity, dedicated to MLAT-only `sources[]` on its own host | — |
| `message-processor-0` | Consumes ADS-B messages, maintains flight state, enriches from Redis, runs rules engine | — |
| `archive-processor` | Receives completed flights from RabbitMQ, writes gzipped JSON to S3 | — |
| `archive-compaction` | Daily job (its own `ofelia` instance, alongside the archive processor) consolidating each day's per-flight Parquet index files into one file per partition | — |
| `rabbitmq` | Message broker between receiver, message processors, and archive | 5672, 15672 (mgmt) |
| `redis` | In-memory enrichment store (aircraft, operators, airports, flight O/D, rules, areas) | 6379 |
| `ofelia` | Cron scheduler that runs runner containers on a schedule | — |
| `management-ui` | FastAPI backend + React frontend for rules and areas editing | 80 |
| `mictronics` runner | Imports global aircraft registration data into Redis | — |
| `us-faa-registry` runner | Imports US FAA detailed registration data into Redis | — |
| `ca-transport-canada-registry` runner | Imports Transport Canada detailed registration data into Redis | — |
| `ourairports` runner | Imports airport metadata into Redis | — |

...and 36 more country-specific registration runners — see [Data Runners](/runners/) for the full list.

`redis` in `docker-compose.core.yaml` sets explicit, non-default tuning
(`--save ""`, `--no-appendfsync-on-rewrite yes`, a raised RediSearch fork-GC
interval) — these are deliberate choices for a constrained host (Raspberry
Pi CM4008032, 8GB RAM/32GB eMMC), not an oversight.

## Configuration

Each component reads its settings from `/app/settings.json` inside the
container, bind-mounted read-only from `./config/{component}/settings.json`
on the host. Example files for every component are in `config/`:

| File | Used by |
|------|---------|
| `config/receiver/settings.json.example` | `docker-compose.receiver.yaml` |
| `config/receiver/mlat-settings.json.example` | `docker-compose.receiver.yaml` (deployed a second time, on the MLAT instance's own host) |
| `config/message-processor/settings.json.example` | `docker-compose.message-processor.yaml` |
| `config/archive/settings.json.example` | `docker-compose.archive.yaml` (`archive-processor`) |
| `config/archive/compaction-settings.json.example` | `docker-compose.archive.yaml` (`archive-compaction`) |
| `config/management-ui/settings.json.example` | `docker-compose.management-ui.yaml` |
| `config/runners/settings.json.example` | All runners in `docker-compose.core.yaml` |
| `config/ofelia/config.ini.example` | `ofelia` in `docker-compose.core.yaml` |
| `config/rabbitmq/rabbitmq.conf.example` | `rabbitmq` in `docker-compose.core.yaml` |
| `config/rabbitmq/enabled_plugins.example` | `rabbitmq` in `docker-compose.core.yaml` |

See the component pages for the full list of settings fields:
[Receiver](/components/receiver), [Message Processor](/components/message-processor),
[Archive Processor](/components/archive-processor),
[Archive Compaction](/components/archive-compaction), and
[Data Runners](/runners/) (logging convention, plus one page per
runner).

## Maintenance

Each component has different fault-tolerance characteristics, so the safe
procedure for taking one down — OS patching, a host reboot, a container
image update — depends on what it is and what depends on it.

**Receiver** — no draining needed. It's the origin of the data, not a
consumer of anything upstream, so stopping it is simply a coverage gap in
the ADS-B feed itself; every downstream component (RabbitMQ, message
processors, archive) is unaffected. Stop it, restart it, done. The optional MLAT
receiver instance (same `docker-compose.receiver.yaml`, its own host and
`settings.json`) is maintained identically and independently of the
SDR-hosting instance, with its own independently-generated identity.

**Core** (`rabbitmq`, `redis`, `ofelia`, runners) — stop
`ofelia` first, so a scheduled runner isn't killed mid-write to Redis, and
let any currently-running runner finish (or stop it). Stopping message
processors before taking RabbitMQ/Redis down isn't strictly required —
they retry their connections and, once reconnected, resume exactly where they
left off — but doing so avoids noisy reconnect-retry logging during the
maintenance window. The archive processor is the same story: it also
depends on Redis now, for split-flight stitching, but that dependency fails
soft — a Redis outage doesn't block or fail an archive write, it just means
stitching quietly stops working, and any flight archived during the window
that would have merged onto a recent segment stays as a separate,
un-merged record instead (no data loss, just a permanent miss for that
pair, since there's no later backfill). Stopping the archive processor
first avoids that miss and the log noise, but isn't required for
correctness. Bring everything back in this order: Redis, then RabbitMQ,
then `ofelia`.

**A single message processor** (not a resize — resizing the processor count
up or down changes aircraft-to-message-processor routing and is documented
separately) — stop it. RabbitMQ retains its queue (`adsb-{id}`, durable) and
simply grows while the message processor is down. Restart it and it drains
the backlog automatically: the active flight store is durable, and recovery
is driven by message timestamps rather than wall-clock time, so flights in
progress when the message processor stopped resume correctly instead of
being archived just because time passed while it was down. See the
[Message Processor](/components/message-processor) page's Fault Tolerance
section for the full recovery behavior.

**Archive processor** — stop it. Message processors keep publishing completed
flights to the durable `archive` RabbitMQ queue (or their own local
fallback if RabbitMQ is also unavailable at the time), which simply grows
while the archive processor is down. Restart it and it drains normally —
already fault-tolerant by design, no special handling needed.

**Archive compaction** — no live process to stop; it's a one-shot job its
own `ofelia` instance launches on a daily schedule, not a long-running
container. Taking the host down between scheduled runs just means the next
run picks up wherever the watermark was left — see [Archive
Compaction](/components/archive-compaction)'s own Fault Tolerance
characteristics (parity checks, watermark catch-up).

**Management UI** — stop it. It's a stateless REST API with no queue and no
background work; nothing writes to `config:rules`/`config:areas` while it's
down, and message processors keep running against whatever rules/areas were
last saved. Restart it and it's immediately usable — nothing to drain or
resync.
