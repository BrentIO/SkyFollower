# SkyFollower

SkyFollower is a locally-hosted ADS-B aircraft tracking system. It receives
raw 1090 MHz and 978 MHz UAT messages from one or more readsb decoders, routes
them through RabbitMQ for processing, enriches each flight with registration and
operator data from Redis, evaluates configurable alert rules, archives completed
flights as gzipped JSON to AWS S3, and publishes real-time metrics and rule
notifications over MQTT (with Home Assistant autodiscovery).

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
                   RabbitMQ offline   ──► archive.db                │  (Host D)
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

## Quick Start

```bash
# 1. Copy and fill in credentials
cp .env.example .env

# 2. Copy the example settings for each component on this host and fill in values
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

To run a data runner manually (e.g. for a first-time import):
```bash
docker compose -f docker-compose.server.yaml run --rm runner-ourairports
```

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

See the component READMEs for the full list of settings fields:

- [receiver/README.md](receiver/README.md)
- [processor/README.md](processor/README.md)
- [data-runners/ourairports/README.md](data-runners/ourairports/README.md)

## Scaling Processors

Each processor handles the subset of aircraft whose ICAO hex modulo
`PROCESSOR_COUNT` equals the processor's ID. To add a second processor:

1. Uncomment the `processor-1` block in `docker-compose.processor.yaml` and its volume entry.
2. Increment `processor_count` in the receiver's `settings.json` (e.g. `"processor_count": 2`).
3. Restart the receiver — it will pre-declare the new queue and begin routing to it.

Each processor must run on a separate host (or at least with a unique
`PROCESSOR_ID`). The processor claims its ID in Redis on startup and exits if
the ID is already claimed by another instance.

## Data Runners

Data runners populate Redis with the enrichment data that processors use to
annotate flights. Each runner downloads a dataset, normalises it, writes it to
Redis with a TTL, and exits. They are scheduled via an `ofelia` cron container
running alongside Redis.

| Runner | Data source | Schedule | Redis TTL |
|--------|-------------|----------|-----------|
| `standing-data` | VRS SDM: aircraft, operators, routes, airports | Weekly (Tue) | 14 days |
| `mictronics` | Global registration | Weekly (Tue) | 14 days |
| `us-faa` | US FAA detailed registration | Weekly (Sat) | 14 days |
| `ca-transport-canada` | Transport Canada detailed registration | Weekly (Sun) | 14 days |
| `ourairports` | Airport metadata | Weekly (Mon) | 14 days |
| `flightaware` | Flight origin/destination (paid; optional) | Every 3 days | 3 days |

Each runner publishes a single MQTT message on completion with `records_imported`,
`last_run_at`, and `last_run_status`.

## Rules

Rules tell SkyFollower which aircraft to alert on. They are stored in Redis
(`config:rules`) and edited through the UI. A rule fires at most once per flight
per rule identifier. All conditions within a rule must match simultaneously (AND
logic).

Example `rules.example.json` entry:

```json
[
  {
    "identifier": "heavy-arrivals",
    "name": "Heavy aircraft arriving",
    "description": "Any heavy aircraft descending below 5000 ft",
    "enabled": true,
    "conditions": [
      { "type": "wake_turbulence_category", "operator": "equals", "value": "heavy" },
      { "type": "altitude", "operator": "maximum", "value": 5000 },
      { "type": "vertical_speed", "operator": "maximum", "value": -100 }
    ]
  }
]
```

Available condition types: `altitude`, `heading`, `velocity`, `vertical_speed`,
`area`, `date`, `ident`, `squawk`, `military`, `operator_airline_designator`,
`aircraft_type_designator`, `aircraft_registration`, `aircraft_icao_hex`,
`aircraft_powerplant_count`, `wake_turbulence_category`, `matched_rules`.

See `processor/README.md` for operator and constraint details.

## Areas

Named geographic polygons used with the `area` condition type. Stored in Redis
(`config:areas`) as a GeoJSON FeatureCollection and edited through the UI's map
editor.

Example `areas.example.json`:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": { "name": "Airport Approach" },
      "geometry": {
        "type": "Polygon",
        "coordinates": [[
          [-84.45, 33.60],
          [-84.35, 33.60],
          [-84.35, 33.70],
          [-84.45, 33.70],
          [-84.45, 33.60]
        ]]
      }
    }
  ]
}
```

## Home Assistant

When MQTT is configured, all components publish Home Assistant autodiscovery
payloads on connect. Each processor, receiver, archive processor, and data
runner appears as a device in Home Assistant with sensor entities for its key
metrics.

Rule notifications are published to `SkyFollower/rule/{identifier}` as JSON
payloads containing the flight's current state. These can be consumed by Home
Assistant automations directly.

Full MQTT topic documentation is in `specs/asyncapi.yaml`.

## Development

Run the test suite:

```bash
python -m pytest
```

Tests live under `processor/tests/`, `receiver/tests/`, and `shared/tests/`.
