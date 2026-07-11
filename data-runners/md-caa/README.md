# md-caa

| | |
|---|---|
| **Country** | Moldova |
| **Registration prefix** | `ER-` |
| **Data source** | https://www.caa.md/modules/filemanager/files/documentum/Registrul_Aerian_al_Republicii_Moldova.pdf |
| **Format** | PDF (static URL, no header row) |
| **Run frequency** | Weekly (Tuesday, 14:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — the Moldova CAA register does not publish ICAO hex (Mode S) addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The register PDF is downloaded from a static URL and parsed with
`pdfplumber.extract_table()`, which works reliably here. The table has no
header row, so columns are addressed by fixed 0-based position
(`Nr.`, `Type of aircraft`, `Registration`, `Serial No.`, `Operator`) rather
than by name. Rows whose registration column does not start with `ER-` are
discarded during parsing.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Nr. (position 0) | ❌ | Sequence number; parsed but not stored |
| Type of aircraft (position 1) | ✅ | Whitespace-collapsed → `aircraft.model` |
| Registration (position 2) | ✅ | ER-prefix filter; used as the Mictronics lookup key |
| Serial No. (position 3) | ✅ | → `aircraft.serial_number` |
| Operator (position 4) | ❌ | 3-letter operator code only; parsed but not stored |

See `specs/data-dictionary.yaml` (`md-caa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 504E62 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "CESSNA",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "Cessna 172H",
        "serial_number": "345",
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "504E62",
    "military": false,
    "registration": "ER-COA",
    "source": "md-caa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 504E60 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "BOEING",
        "manufacturer_model": "BOEING 747-200",
        "model": "Boeing 747-243F",
        "serial_number": "22545",
        "type_designator": "B742",
        "wake_turbulence_category": "Heavy"
    },
    "icao_hex": "504E60",
    "military": false,
    "registration": "ER-BAR",
    "source": "md-caa"
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

Published once, at the end of a run, to `SkyFollower/runner/md-caa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_md_caa_{name}/config` for each of the three stats above.
