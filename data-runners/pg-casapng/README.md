# pg-casapng

| | |
|---|---|
| **Country** | Papua New Guinea |
| **Registration prefix** | `P2-` |
| **Data source** | https://casapng.gov.pg/safety-regulatory/airworthiness/Aircraft-Registers/ |
| **Format** | PDF (date-stamped filename, discovered via index page) |
| **Run frequency** | Monthly (day 1, 11:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The CASA PNG index page is scraped with BeautifulSoup for the first link ending in `.pdf` — that is always the current register. The PDF has no header row, so `pdfplumber`'s `extract_table()` result is read by fixed column position (registration, manufacturer, model, operator, address) rather than by name; only rows whose registration starts with `P2-` are kept. Registrations are then looked up in batches of 100 against the Redis Mictronics search index (RediSearch `TagField` query) to resolve `icao_hex` — any registration not yet present in Mictronics is silently skipped for this run. The address field is comma-delimited with embedded newlines (collapsed to spaces); the last comma-separated segment is treated as the country (`PNG` is expanded to `Papua New Guinea`) and the remainder as the street.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Registration (col 0) | ✅ | `P2-` filter; used as the Mictronics lookup key |
| Manufacturer (col 1) | ✅ | → `aircraft.manufacturer` |
| Model (col 2) | ✅ | → `aircraft.model` |
| Operator (col 3) | ✅ | → `registrant.names[0]` |
| Address (col 4) | ✅ | Comma-delimited; last segment → `registrant.country` (PNG expanded), remainder → `registrant.street` |

See `specs/data-dictionary.yaml` (`pg-casapng` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 898104 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "CESSNA",
        "manufacturer_model": "CESSNA 208 Caravan",
        "model": "208",
        "type_designator": "C208",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "898104",
    "military": false,
    "registrant": {
        "country": "MT HAGEN WESTERN HIGHLANDS PROVINCE",
        "names": [
            "MISSION AVIATION FELLOWSHIP"
        ],
        "street": "PO BOX 273"
    },
    "registration": "P2-MAI",
    "source": "pg-casapng"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 898174 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "BOEING",
        "manufacturer_model": "BOEING 737-800",
        "model": "737",
        "type_designator": "B738",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "898174",
    "military": false,
    "registrant": {
        "country": "Papua New Guinea",
        "names": [
            "AIR NIUGINI"
        ],
        "street": "P.O.BOX 7186, BOROKO NCD"
    },
    "registration": "P2-PXB",
    "source": "pg-casapng"
}
```

Note: the small example (`P2-MAI`, Cessna 208 Caravan) is a single-engine turboprop rather than a 150/172 — this registry has no lighter GA singles, so a turboprop stands in as the "small" example.

## Configuration

Reads `settings.json` (mounted at `/app/settings.json`):

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `redis.host` | ✅ | — | Redis connection host |
| `redis.port` | ❌ | `6379` | |
| `mqtt.host` | ❌ | — | Omit the whole `mqtt` block to skip completion-stats publishing entirely |
| `mqtt.port` | ❌ | `1883` | |
| `mqtt.username` | ❌ | — | Optional MQTT auth (added in #328); omit for an anonymous broker |
| `mqtt.password` | ❌ | — | |
| `redis_ttl_days` | ❌ | `14` | TTL applied to each `aircraft:registry:{icao_hex}` key written by this runner |

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/pg-casapng/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_pg_casapng_{name}/config` for each of the three stats above.
