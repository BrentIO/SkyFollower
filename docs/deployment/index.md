# Deployment

SkyFollower is a single monorepo, but it deploys as four independent
hosts — each one clones the repo and brings up exactly one Docker Compose
file. See [Getting Started](/getting-started/) for the commands to
actually bring a host up once you know which compose file it runs.

## Host Topology

| Host | Role | Containers |
|------|------|------------|
| Host A — Raspberry Pi | ADS-B reception | `receiver` |
| Host B — Central server | Message bus + enrichment data | `rabbitmq`, `redis`, `ofelia`, data runners |
| Host C — Processor host | Flight state + rules | `processor-0` (one per host; scale by adding hosts) |
| Host D — Archive host | Long-term storage + UI | `archive-processor`, `ui` |

## Compose Files

Each host runs exactly one compose file. Clone the repo on each host, populate
the relevant `config/` settings files, then bring up the appropriate file:

| File | Host | Services |
|------|------|---------|
| `docker-compose.receiver.yaml` | Host A — Raspberry Pi | `receiver` |
| `docker-compose.server.yaml` | Host B — Central server | `rabbitmq`, `redis`, `ofelia`, all data runners |
| `docker-compose.processor.yaml` | Host C — Processor host | `processor-0` |
| `docker-compose.archive.yaml` | Host D — Archive host | `archive-processor`, `ui` |

## Components

| Container | Description | Default port |
|-----------|-------------|--------------|
| `receiver` | Reads raw ADS-B frames from readsb TCP streams; routes to RabbitMQ queues | — |
| `processor-0` | Consumes ADS-B messages, maintains flight state, enriches from Redis, runs rules engine | — |
| `archive-processor` | Receives completed flights from RabbitMQ, writes gzipped JSON to S3 | — |
| `rabbitmq` | Message broker between receiver, processors, and archive | 5672, 15672 (mgmt) |
| `redis` | In-memory enrichment store (aircraft, operators, airports, flight O/D, rules, areas) | 6379 |
| `ofelia` | Cron scheduler that runs data runner containers on a schedule | — |
| `ui` | FastAPI backend + React frontend for rules and areas editing | 8080 |
| `mictronics` runner | Imports global aircraft registration data into Redis | — |
| `us-faa` runner | Imports US FAA detailed registration data into Redis | — |
| `ca-transport-canada` runner | Imports Transport Canada detailed registration data into Redis | — |
| `ourairports` runner | Imports airport metadata into Redis | — |

...and 36 more country-specific registration runners — see [Data Runners](/data-runners/) for the full list.

## Configuration

Each component reads its settings from `/app/settings.json` inside the
container, bind-mounted read-only from `./config/{component}/settings.json`
on the host. Example files for every component are in `config/`:

| File | Used by |
|------|---------|
| `config/receiver/settings.json.example` | `docker-compose.receiver.yaml` |
| `config/processor/settings.json.example` | `docker-compose.processor.yaml` |
| `config/archive/settings.json.example` | `docker-compose.archive.yaml` |
| `config/ui/settings.json.example` | `docker-compose.archive.yaml` |
| `config/runners/settings.json.example` | All runners in `docker-compose.server.yaml` |
| `config/ofelia/config.ini.example` | `ofelia` in `docker-compose.server.yaml` |

See the component pages for the full list of settings fields:
[Receiver](/components/receiver), [Processor](/components/processor),
[Archive Processor](/components/archive-processor), and
[Data Runners](/data-runners/) (logging convention, plus one page per
runner).

## Maintenance

Each component has different fault-tolerance characteristics, so the safe
procedure for taking one down — OS patching, a host reboot, a container
image update — depends on what it is and what depends on it.

**Receiver** — no draining needed. It's the origin of the data, not a
consumer of anything upstream, so stopping it is simply a coverage gap in
the ADS-B feed itself; every downstream component (RabbitMQ, processors,
archive) is unaffected. Stop it, restart it, done.

**Central server** (`rabbitmq`, `redis`, `ofelia`, data runners) — stop
`ofelia` first, so a scheduled runner isn't killed mid-write to Redis, and
let any currently-running runner finish (or stop it). Stopping processors
before taking RabbitMQ/Redis down isn't strictly required — processors
retry their connections and, once reconnected, resume exactly where they
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

**A single processor** (not a resize — resizing the processor count up or
down changes aircraft-to-processor routing and is documented separately) —
stop it. RabbitMQ retains its queue (`adsb-{id}`, durable) and simply grows
while the processor is down. Restart it and it drains the backlog automatically:
the active flight store is durable, and recovery is driven by message
timestamps rather than wall-clock time, so flights in progress when the
processor stopped resume correctly instead of being archived just because
time passed while it was down. See the [Processor](/components/processor)
page's Fault Tolerance section for the full recovery behavior.

**Archive processor** — stop it. Processors keep publishing completed
flights to the durable `archive` RabbitMQ queue (or their own local
fallback if RabbitMQ is also unavailable at the time), which simply grows
while the archive processor is down. Restart it and it drains normally —
already fault-tolerant by design, no special handling needed.
