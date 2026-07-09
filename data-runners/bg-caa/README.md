# bg-caa

| | |
|---|---|
| **Country** | Bulgaria |
| **Registration prefix** | `LZ-` |
| **Data source** | https://www.caa.bg/bg/category/300/17238 |
| **Format** | Xlsx (date-encoded filename, discovered via index page) |
| **Run frequency** | Weekly (Wednesday, 00:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; `LZ-` registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The Bulgaria CAA index page is scraped for a link matching `Aircraft_Register_\d+.xlsx`, then that xlsx is downloaded and parsed with `openpyxl` in read-only mode. The first two rows (an info line and the header row) are skipped; data starts at row 2. Columns are addressed by fixed 0-based position rather than by header name, and only rows whose registration column starts with `LZ-` are kept. Registrations are resolved to `icao_hex` in batches of 100 via RediSearch against the Mictronics index — unlike the other runners in this batch, `bg-caa` does not run a model/type sanity check against the existing Mictronics record before writing.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Рег. № (col 0) | ❌ | Parsed row but column not read; registration comes from col 4 instead |
| Дата (col 1, Excel serial date) | ❌ | Not read |
| Модел (col 2) | ✅ | → `aircraft.model` |
| Сериен № (col 3) | ✅ | → `aircraft.serial_number` |
| Рег. знак (col 4) | ✅ | `LZ-`-prefix; used as the Mictronics lookup key |
| Категория ВС (col 5) | ✅ | → `aircraft.type`, decoded via a category map (e.g. `sailplane` → `Glider`); unmapped values are dropped, not passed through |
| Основание (col 6) | ❌ | Not read |

See `specs/data-dictionary.yaml` (`bg-caa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 451C2B | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "CIRRUS",
        "manufacturer_model": "CIRRUS SR-22",
        "model": "Cirrus SR 22",
        "serial_number": "00488",
        "type": "Airplane",
        "type_designator": "SR22",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "451C2B",
    "military": false,
    "registration": "LZ-ALA",
    "source": "bg-caa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 451E92 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "AIRBUS",
        "manufacturer_model": "AIRBUS A-319",
        "model": "Airbus A319-112",
        "serial_number": "03188",
        "type": "Airplane",
        "type_designator": "A319",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "451E92",
    "military": true,
    "registration": "LZ-AOB",
    "source": "bg-caa"
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

Published once, at the end of a run, to `SkyFollower/runner/bg-caa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_bg_caa_{name}/config` for each of the three stats above.
