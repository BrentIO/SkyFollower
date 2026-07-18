# ⚠️ In Development ⚠️

This repo is in active development.  Its full of bugs, incomplete implementation, invalid documentation, and just generally isn't ready for you to fork or test.  Star the repo and check back in a few weeks!

# SkyFollower

SkyFollower is a locally-hosted ADS-B aircraft tracking system. It receives
raw 1090 MHz and 978 MHz UAT messages from one or more readsb decoders, routes
them through RabbitMQ for processing, enriches each flight with registration and
operator data from Redis, evaluates configurable alert rules, archives completed
flights as gzipped JSON to AWS S3, and publishes real-time metrics and rule
notifications over MQTT (with Home Assistant autodiscovery).

**Full documentation: [brentio.github.io/SkyFollower](https://brentio.github.io/SkyFollower/)**

## Architecture

```
RTL-SDR 1090 ──► readsb :30002 ──┐
RTL-SDR 978  ──► readsb :30978 ──┤
                                  ▼
                         [ receiver ]  (Host A — Raspberry Pi)
                         Tags: raw, icao_hex, received_at, source
                         Routes: adsb-{int(icao_hex, 16) % PROCESSOR_COUNT}
                                │                        │
                     RabbitMQ available          RabbitMQ offline
                                │                        ▼
                                ▼                  queue.db (SQLite WAL)
                        RabbitMQ / Redis           drains on reconnect
                         (Host B — server)
                                │
                                ▼
                      [ processor-0 ]  (Host C)
                      SQLite in-memory active store
                      Redis enrichment lookups
                      Rules engine → MQTT notifications
                               │ completed flight
                   RabbitMQ available ──► archive queue ──► [ archive-processor ]
                   RabbitMQ offline   ──► completed_flights.db      │  (Host D)
                                          (SQLite WAL)               ├─► S3 (.json.gz)
                                                                     └─► Parquet index

   Redis ◄──────────────────────────────── data runners (ofelia-scheduled)
    │                                      standing-data, mictronics,
    │                                      us-faa, ca-transport-canada,
    │                                      ourairports, flightaware
    ▼
 [ UI ]  FastAPI backend + React frontend
         Rules editor, Areas editor
```

<!--
  The #region/#endregion comments below are consumed by docs/getting-started/
  and docs/deployment/ via VitePress's `@include` directive, so this stays
  the single source of truth instead of forking into duplicated docs-site
  content. Don't remove a region marker without checking those pages first.
-->
<!-- #region host-topology -->
## Host Topology

| Host | Role | Containers |
|------|------|------------|
| Host A — Raspberry Pi | ADS-B reception | `receiver` |
| Host B — Central server | Message bus + enrichment data | `rabbitmq`, `redis`, `ofelia`, data runners |
| Host C — Processor host | Flight state + rules | `processor-0` (one per host; scale by adding hosts) |
| Host D — Archive host | Long-term storage + UI | `archive-processor`, `ui` |
<!-- #endregion host-topology -->

<!-- #region compose-files -->
## Compose Files

Each host runs exactly one compose file. Clone the repo on each host, populate
the relevant `config/` settings files, then bring up the appropriate file:

| File | Host | Services |
|------|------|---------|
| `docker-compose.receiver.yaml` | Host A — Raspberry Pi | `receiver` |
| `docker-compose.server.yaml` | Host B — Central server | `rabbitmq`, `redis`, `ofelia`, all data runners |
| `docker-compose.processor.yaml` | Host C — Processor host | `processor-0` |
| `docker-compose.archive.yaml` | Host D — Archive host | `archive-processor`, `ui` |
<!-- #endregion compose-files -->

<!-- #region quick-start -->
## Quick Start

```bash
# 1. Copy the example settings for each component on this host and fill in values
#    e.g. for Host B:
cp config/runners/settings.json.example config/runners/settings.json
cp config/ofelia/config.ini.example config/ofelia/config.ini

# Host A — receiver
docker compose -f docker-compose.receiver.yaml up -d

# Host B — central server
docker compose -f docker-compose.server.yaml up -d
# Create runner containers in stopped state so ofelia can schedule them:
docker compose -f docker-compose.server.yaml --profile runners up --no-start

# Host C — processor
docker compose -f docker-compose.processor.yaml up -d

# Host D — archive + UI
docker compose -f docker-compose.archive.yaml up -d
```

To run a single data runner manually (e.g. for a first-time import):
```bash
docker compose -f docker-compose.server.yaml run --rm runner-ourairports
```

To bulk-load *every* runner once (e.g. right after install, so Redis isn't
empty until each runner's first scheduled `ofelia` run — up to a week away
for weekly runners). `mictronics` goes first since most country runners
resolve `icao_hex` against its RediSearch index; the rest follow
alphabetically. The runner list comes from `docker compose config` itself,
not a separate list, so it's always accurate for whatever's actually
declared in `docker-compose.server.yaml`:
```bash
docker compose -f docker-compose.server.yaml run --rm runner-mictronics
for svc in $(docker compose -f docker-compose.server.yaml config --services \
    | grep '^runner-' | grep -v '^runner-mictronics$' | sort); do
  docker compose -f docker-compose.server.yaml run --rm "$svc"
done
```
<!-- #endregion quick-start -->

<!-- #region components -->
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
| `standing-data` runner | Imports VRS SDM aircraft, operator, route, and airport data into Redis | — |
| `mictronics` runner | Imports global aircraft registration data into Redis | — |
| `us-faa` runner | Imports US FAA detailed registration data into Redis | — |
| `ca-transport-canada` runner | Imports Transport Canada detailed registration data into Redis | — |
| `ourairports` runner | Imports airport metadata into Redis | — |
| `flightaware` runner | Imports flight origin/destination data into Redis (paid; optional) | — |
<!-- #endregion components -->

<!-- #region configuration -->
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
<!-- #endregion configuration -->

See the docs site for the full settings reference, scaling instructions,
rules/areas configuration, and Home Assistant integration details:

- [Components](https://brentio.github.io/SkyFollower/components/) and [Data Runners](https://brentio.github.io/SkyFollower/data-runners/) — settings fields for every container
- [Architecture](https://brentio.github.io/SkyFollower/architecture/) — pipeline diagram plus scaling processors horizontally
- [Rules & Areas](https://brentio.github.io/SkyFollower/rules-and-areas/) — condition types, example `rules.json`/`areas.json`
- [MQTT Reference](https://brentio.github.io/SkyFollower/specs/asyncapi) — every topic published, including Home Assistant autodiscovery behavior

## Development

Run the test suite:

```bash
python -m pytest
```

Tests live under `processor/tests/`, `receiver/tests/`, and `shared/tests/`.
