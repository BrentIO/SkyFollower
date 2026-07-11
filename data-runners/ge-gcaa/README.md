# ge-gcaa

| | |
|---|---|
| **Country** | Georgia |
| **Registration prefix** | `4L-` |
| **Data source** | https://gcaa.ge/civil-aircraft-register/ |
| **Format** | HTML (two pre-rendered tables on a fixed page) |
| **Run frequency** | Weekly (Tuesday, 19:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The Georgia Civil Aviation Agency register page is fetched and two identically
laid-out HTML tables are parsed by element id: `table_1` (operator data) and
`table_2` (owner data). Rows from both tables are merged by registration mark:
aircraft fields (type, serial number, year of manufacture) are taken from
whichever table supplies them first, and `registrant.names` is built from
`table_1`'s operator followed by `table_2`'s owner — the owner is omitted if
it's identical to the operator, so single-name entries don't duplicate.

## Columns

Both tables share the same column layout.

| Source column | Imported | Notes |
|---|---|---|
| Operator (table_1 col 0) / Owner (table_2 col 0) | ✅ | → `registrant.names[0]` (operator) and `registrant.names[1]` (owner, omitted if same as operator) |
| Aircraft type (col 1) | ✅ | → `aircraft.model`; merged from whichever table provides it |
| Registration (col 2) | ✅ | 4L-prefix; used as the Mictronics lookup key |
| Registration date (col 3) | ❌ | Parsed but not stored |
| Serial number (col 4) | ✅ | → `aircraft.serial_number`; merged from whichever table provides it |
| Year of manufacture (col 5) | ✅ | → `aircraft.manufactured_date` (as `YYYY-01-01`); merged from whichever table provides it |

See `specs/data-dictionary.yaml` (`ge-gcaa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 51404B | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2000-01-01",
        "manufacturer": "BOMBARDIER",
        "manufacturer_model": "BOMBARDIER Regional Jet CRJ-200",
        "model": "CL-600-2B19 (CRJ)",
        "serial_number": "7442",
        "type_designator": "CRJ2",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "51404B",
    "military": false,
    "registrant": {
        "names": [
            "შპს აირზენა (Airzena)"
        ]
    },
    "registration": "4L-TGB",
    "source": "ge-gcaa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 514D03 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2002-01-01",
        "manufacturer": "BOEING",
        "manufacturer_model": "BOEING 767-300",
        "model": "Boeing 767-300",
        "serial_number": "29390",
        "type_designator": "B763",
        "wake_turbulence_category": "Heavy"
    },
    "icao_hex": "514D03",
    "military": false,
    "registrant": {
        "names": [
            "შპს ჯორჯიან ეარვეისი (Georgian Airways)"
        ]
    },
    "registration": "4L-GTR",
    "source": "ge-gcaa"
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

Published once, at the end of a run, to `SkyFollower/runner/ge-gcaa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_ge_gcaa_{name}/config` for each of the three stats above.
