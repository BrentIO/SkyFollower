# br-anac

| | |
|---|---|
| **Country** | Brazil |
| **Registration prefix** | `PP-` / `PR-` / `PT-` / `PS-` / `PU-` |
| **Data source** | https://sistemas.anac.gov.br/dadosabertos/Aeronaves/RAB/dados_aeronaves.json |
| **Format** | JSON API (fixed URL, full register dump) |
| **Run frequency** | Weekly (Wednesday, 08:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; registrations (formatted from `MARCA`) are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The full Registro Aeronáutico Brasileiro (RAB) JSON dump is downloaded from a fixed URL (`utf-8-sig` decoded). Records are filtered to active registrations only — rows with a cancellation date (`DTCANC`) set, or whose `CDINTERDICAO` starts with `R` or `M`, are excluded. The `MARCA` field (e.g. `PPAJH`) is reformatted into a hyphenated registration (`PP-AJH`). Registrations are resolved to `icao_hex` in batches of 100 via RediSearch against the Mictronics index, then a type sanity check compares tokens from `DSMODELO` against the existing Mictronics record, rejecting the match only when both sides have tokens and none overlap. `CDCLS` is a compact 3-character code (`{landing}{engine count}{propulsion}`, or the literal `RPA` for drones) decoded into `aircraft.type`, `aircraft.category`, and the powerplant count/type. The embedded `PROPRIETARIOSJSON` owner list uses a non-standard `/""` escape sequence for embedded quotes that is un-escaped before `json.loads`; only the first owner's name is kept, and placeholder values like `Indisponível` are treated as absent. Unlike the other runners in this batch, writes to Redis here are per-record (not pipelined).

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `MARCA` | ✅ | Reformatted to hyphenated registration (e.g. `PPAJH` → `PP-AJH`); used as the Mictronics lookup key |
| `DTCANC` | ✅ | Used only as a filter (non-empty = inactive, row skipped); not stored |
| `CDINTERDICAO` | ✅ | Used only as a filter (`R`/`M` prefix = inactive, row skipped); not stored |
| `CDCLS` | ✅ | Decoded into `aircraft.type`, `aircraft.category`, `aircraft.powerplant.count`, `aircraft.powerplant.type` |
| `NMFABRICANTE` | ✅ | → `aircraft.manufacturer` |
| `DSMODELO` | ✅ | → `aircraft.model`; also used for the type sanity check against Mictronics |
| `CDTIPOICAO` | ✅ | → `aircraft.type_designator` |
| `NRSERIE` | ✅ | → `aircraft.serial_number` |
| `NRASSENTOS` | ✅ | → `aircraft.seats` |
| `NRANOFABRICACAO` | ✅ | → `aircraft.manufactured_date` (4-digit year → `YYYY-01-01T00:00:00Z`) |
| `PROPRIETARIOSJSON` | ✅ | → `registrant.names[0]`; embedded JSON with a non-standard quote escape; placeholder names (e.g. `Indisponível`) are filtered |

See `specs/data-dictionary.yaml` (`br-anac` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 A4A09D | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "category": "Land",
        "manufactured_date": "2014-01-01T00:00:00Z",
        "manufacturer": "HAWKER BEECHCRAFT",
        "manufacturer_model": "BEECH 58 Baron",
        "model": "G58",
        "powerplant": {
            "count": 2,
            "type": "Piston"
        },
        "seats": 6,
        "serial_number": "TH-2398",
        "type": "Airplane",
        "type_designator": "BE58",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "A4A09D",
    "military": false,
    "registrant": {
        "names": [
            "CEILA SILVA LEMOS"
        ]
    },
    "registration": "PR-JOK",
    "source": "br-anac"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 E47F21 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "category": "Land",
        "manufactured_date": "2005-01-01T00:00:00Z",
        "manufacturer": "BOEING COMPANY",
        "manufacturer_model": "BOEING 767-300",
        "model": "767-316F",
        "powerplant": {
            "count": 2,
            "type": "Turbo-jet"
        },
        "seats": 6,
        "serial_number": "34245",
        "type": "Airplane",
        "type_designator": "B763",
        "wake_turbulence_category": "Heavy"
    },
    "icao_hex": "E47F21",
    "military": false,
    "registrant": {
        "names": [
            "LAN CARGO S.A."
        ]
    },
    "registration": "PR-ABD",
    "source": "br-anac"
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

Published once, at the end of a run, to `SkyFollower/runner/br-anac/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_br_anac_{name}/config` for each of the three stats above.
