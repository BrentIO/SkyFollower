# nl-ilt

| | |
|---|---|
| **Country** | Netherlands |
| **Registration prefix** | `PH-` |
| **Data source** | https://www.ilent.nl/documenten/lijsten/luchtvaart/databestanden/luchtvaartregister-data |
| **Format** | ODS spreadsheet (date-stamped filename, changes monthly, discovered via index page) |
| **Run frequency** | Weekly (Thursday, 05:10 UTC) |
| **Depends on Mictronics for ICAO hex** | No — the ILT register publishes the ICAO hex directly in its `X-Ponder` column, so records can be written to `aircraft:registry:{icao_hex}` without a RediSearch reverse lookup, and without `mictronics` having run first. |

## How it works

The ILT index page is scraped with a regex for a URL (absolute or relative)
containing the known filename stem
`luchtvaartuigregister-ilt-datas2...*.ods`; the file is downloaded to a temp
path and parsed with `odfpy`. Row 0 holds annotated headers (bracket suffixes
like `` [details=n][kolom=j] `` are stripped off), row 1 is an informational
text row that is skipped, and row 2 onward is data. Cells that carry
OpenDocument's `numbercolumnsrepeated` attribute are expanded so that
merged/repeated blank cells don't shift later columns out of alignment (with
a guard against implausibly large repeat counts on trailing empty cells,
which are collapsed back to one). A row is only written if `X-Ponder` is a
valid 6-hex ICAO code and `Registration` is non-empty.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| X-Ponder | ✅ | Validated as a 6-hex ICAO code; used directly as `icao_hex` (no Mictronics lookup needed) |
| Registration | ✅ | → top-level `registration` |
| Manufacturer | ✅ | → `aircraft.manufacturer` |
| Model | ✅ | → `aircraft.model` |
| Serial | ✅ | → `aircraft.serial_number` |
| Built | ✅ | → `aircraft.manufactured_date` (`YYYY-01-01T00:00:00Z`), only if a 4-digit year |
| Group | ✅ | → `aircraft.type`, mapped through a fixed table (e.g. `Small aeroplane` → `Airplane`, `Rotorcraft` → `Helicopter`); unmapped values pass through unchanged |
| ICAO-code | ✅ | → `aircraft.type_designator` |
| Engines | ✅ | → `aircraft.powerplant.count`, only if numeric |
| EngKind | ✅ | → `aircraft.powerplant.type`, mapped through a fixed table; `"Engine - not defined"` is dropped rather than stored |
| EngManufacturer | ✅ | → `aircraft.powerplant.manufacturer`; the literal value `"Unknown"` is filtered out |
| EngModel | ✅ | → `aircraft.powerplant.model`; values containing `"not further defined"` are filtered out |

The runner only reads the columns listed above; any other columns present in
the ILT ODS file are not referenced by the parser. See
`specs/data-dictionary.yaml` (`nl-ilt` entry) for full column semantics and
cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 484674 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2007-01-01T00:00:00Z",
        "manufacturer": "Piper Aircraft, Inc.",
        "manufacturer_model": "PIPER PA-28-140/150/160/180",
        "model": "PA-28-181",
        "powerplant": {
            "count": 1,
            "manufacturer": "AVCO Corporation, Lycoming Division",
            "model": "O-360-A4M",
            "type": "Piston"
        },
        "serial_number": "2843664",
        "type": "Airplane",
        "type_designator": "P28A",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "484674",
    "military": false,
    "registration": "PH-WKB",
    "source": "nl-ilt"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 484F73 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2012-01-01T00:00:00Z",
        "manufacturer": "Airbus S.A.S. (Société par Actions Simplifiée)",
        "manufacturer_model": "AIRBUS A-330-300",
        "model": "A330-303",
        "powerplant": {
            "count": 2,
            "manufacturer": "General Electric Company, Aircraft Engine Group",
            "model": "CF6-80E1A3",
            "type": "Turbo-fan"
        },
        "serial_number": "1300",
        "type": "Airplane",
        "type_designator": "A333",
        "wake_turbulence_category": "Heavy"
    },
    "icao_hex": "484F73",
    "military": false,
    "registration": "PH-AKD",
    "source": "nl-ilt"
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

Published once, at the end of a run, to `SkyFollower/runner/nl-ilt/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_nl_ilt_{name}/config` for each of the three stats above.
