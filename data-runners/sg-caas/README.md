# sg-caas

| | |
|---|---|
| **Country** | Singapore |
| **Registration prefix** | `9V-` |
| **Data source** | https://www.caas.gov.sg/industry/aircraft-operators/certificate-of-registration/ |
| **Format** | XLSX (UUID-bearing filename, discovered via index page) |
| **Run frequency** | Monthly (day 1, 10:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — the CAAS register does not publish ICAO hex (Mode S) addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The register xlsx's filename includes a UUID component that changes every month, so the download URL is discovered fresh each run by scraping the CAAS certificate-of-registration page for a link on the `isomer-user-content.by.gov.sg` CDN ending in `.xlsx`. The CDN rejects requests without a browser `User-Agent` and a `Referer` header pointing back to the index page. The workbook is opened read-only with `openpyxl`; the first non-blank row is treated as the header row, and subsequent rows are kept only if their first cell starts with `9V-`.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Aircraft Registration | ✅ | 9V-prefix; used as the Mictronics lookup key |
| Aircraft Manufacturer | ✅ | → `aircraft.manufacturer` |
| Aircraft Model | ✅ | → `aircraft.model` |
| Aircraft S/N | ✅ | → `aircraft.serial_number` |
| Engine Manufacturer | ✅ | → `aircraft.powerplant.manufacturer` |
| Engine Model | ✅ | → `aircraft.powerplant.model` |
| Operator | ✅ | → `registrant.names[0]` |

See `specs/data-dictionary.yaml` (`sg-caas` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

Note: the Singapore register is almost entirely widebody airliners; the small example below (a Diamond DA40) is one of the few GA singles on the register.

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 76E4C7 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "Diamond",
        "manufacturer_model": "DIAMOND DA-40 Club Star",
        "model": "DA40",
        "powerplant": {
            "manufacturer": "Lycoming",
            "model": "Lycoming IO-360-M1A"
        },
        "serial_number": "40.1089",
        "type_designator": "DA40",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "76E4C7",
    "military": false,
    "registrant": {
        "names": [
            "Singapore Youth Flying Club"
        ]
    },
    "registration": "9V-YFG",
    "source": "sg-caas"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 76CD76 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "Airbus",
        "manufacturer_model": "AIRBUS A-380-800",
        "model": "A380-841",
        "powerplant": {
            "manufacturer": "Rolls-Royce",
            "model": "RB211-TRENT 970"
        },
        "serial_number": "247",
        "type_designator": "A388",
        "wake_turbulence_category": "Heavy"
    },
    "icao_hex": "76CD76",
    "military": false,
    "registrant": {
        "names": [
            "Singapore Airlines Limited"
        ]
    },
    "registration": "9V-SKV",
    "source": "sg-caas"
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
| `mqtt.username` | ❌ | — | Optional MQTT auth (added in #328); omit for an anonymous broker |
| `mqtt.password` | ❌ | — | |
| `redis_ttl_days` | ❌ | `14` | TTL applied to each `aircraft:registry:{icao_hex}` key written by this runner |

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/sg-caas/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_sg_caas_{name}/config` for each of the three stats above.
