# tc-caa

| | |
|---|---|
| **Country** | Turks and Caicos Islands |
| **Registration prefix** | `VQ-` |
| **Data source** | https://tcicaa.tc/operations-safety/aircrafts/aircraft-register |
| **Format** | HTML table (single static page, fixed URL) |
| **Run frequency** | Weekly (Thursday, 09:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — the Turks & Caicos register does not publish ICAO hex (Mode S) addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

Unlike most runners, there is no index page or file to discover — the register lives directly on a fixed static HTML page. The page is parsed with BeautifulSoup: every `<table>` on the page is inspected, and the first one whose first row's text contains `REG` is taken as the register table. Header cells become dict keys and each subsequent row is zipped into a `{header: value}` record.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| REG | ✅ | VQ-prefix; used as the Mictronics lookup key |
| MANUFACT. | ✅ | → `aircraft.manufacturer` |
| MODEL | ✅ | → `aircraft.model` |
| S/N | ✅ | → `aircraft.serial_number` |
| OWNER /OPERATOR | ✅ | → `registrant.names[0]` |

See `specs/data-dictionary.yaml` (`tc-caa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

Note: the Turks & Caicos register has no Boeing or Airbus aircraft at all; the "big" example below is an ATR 72-212A, and the "small" example is a Cessna 402 twin (not a 150/172).

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 40038D | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "Cessna",
        "manufacturer_model": "CESSNA 402 Businessliner",
        "model": "402C",
        "serial_number": "402C-0074",
        "type_designator": "C402",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "40038D",
    "military": false,
    "registrant": {
        "names": [
            "Caicos Express Airways"
        ]
    },
    "registration": "VQ-TCE",
    "source": "tc-caa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 40039F | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "ATR-GIE",
        "manufacturer_model": "ATR-72-202",
        "model": "ATR 72-212A",
        "serial_number": "702",
        "type_designator": "AT72",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "40039F",
    "military": false,
    "registrant": {
        "names": [
            "InterCaribbean Airways"
        ]
    },
    "registration": "VQ-THW",
    "source": "tc-caa"
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

Published once, at the end of a run, to `SkyFollower/runner/tc-caa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_tc_caa_{name}/config` for each of the three stats above.
