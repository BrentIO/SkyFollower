# 🇧🇿 bz-bdca

| | |
|---|---|
| **Country** | Belize |
| **Registration prefix** | `V3-` |
| **Data source** | https://www.civilaviation.gov.bz/index.php/bdca-civil-aircraft-register |
| **Format** | HTML table (single static page) |
| **Run frequency** | Weekly (Wednesday, 11:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — the Belize register does not publish ICAO hex (Mode S) addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The register page is fetched directly (no index/discovery step — the URL is fixed). The page contains several navigation tables before the actual register, so the runner scans all `<table>` elements on the page and picks the first whose header row contains "Registration Number". Header names are read from that row and zipped with each subsequent row's cells to build one dict per aircraft. Registrations are then resolved to ICAO hex in batches of 100 via a RediSearch query against the Mictronics index; only rows that resolve are written. Every written record explicitly sets `military: false` — this register is exclusively civil, and the explicit value ensures a stale `military: true` flag (from Mictronics or a prior record on a reused hex) is corrected on re-registration.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Registration Number | ✅ | V3-prefix; used as the Mictronics lookup key and stored as `registration` |
| Manufacturer, Model | ✅ | → `aircraft.model` |
| Serial Number | ✅ | → `aircraft.serial_number`; placeholder values (`–`, `-`, empty) are treated as null and not stored |
| Owner | ✅ | → `registrant.names[0]`; trailing `(Charterer by Demise)` qualifier is stripped |
| Address | ✅ | Split on the first comma into `registrant.street[0]` / `registrant.city`; stored as `registrant.city` only when no comma is present |

See specs/data-dictionary.yaml (`bz-bdca` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 0AB014 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "CESSNA",
        "manufacturer_model": "CESSNA 208 Caravan",
        "model": "Cessna Aircraft Co.,C208B",
        "serial_number": "208B5144",
        "type_designator": "C208",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "0AB014",
    "military": false,
    "registrant": {
        "city": "San Pedro, Belize District",
        "names": [
            "Tropic Air Ltd"
        ],
        "street": [
            "Manta Ray Street"
        ]
    },
    "registration": "V3-HHV",
    "source": "bz-bdca"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 0AB036 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "BELL",
        "manufacturer_model": "BELL 407",
        "model": "Bell Helicopter Textron,Bell 407",
        "serial_number": "53048",
        "type_designator": "B407",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "0AB036",
    "military": false,
    "registrant": {
        "city": "Belize City",
        "names": [
            "Astrum TravelInternational Limited"
        ],
        "street": [
            "Miles 3.5 GeorgePrice Highway"
        ]
    },
    "registration": "V3-AHE",
    "source": "bz-bdca"
}
```

## Configuration

Reads `settings.json` (mounted at `/app/settings.json`):

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `redis.host` | ✅ | — | Redis connection host |
| `redis.port` | ❌ | `6379` | |
| `mqtt.host` | ❌ | — | Omit the whole `mqtt` block to skip completion-stats publishing entirely |
| `mqtt.port` | ❌ | `1883` | |
| `mqtt.username` | ❌ | — | Optional MQTT auth; omit for an anonymous broker |
| `mqtt.password` | ❌ | — | |
| `redis_ttl_days` | ❌ | `14` | TTL applied to each `aircraft:registry:{icao_hex}` key written by this runner |

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/bz-bdca/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_bz_bdca_{name}/config` for each of the three stats above.
