# ee-transpordiamet

| | |
|---|---|
| **Country** | Estonia |
| **Registration prefix** | `ES-` |
| **Data source** | https://transpordiamet.ee/ohusoidukite-register |
| **Format** | HTML table (single fixed page) |
| **Run frequency** | Weekly (Tuesday, 20:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The Estonian Transport Administration civil aircraft register page is fetched
directly (no index-page discovery needed — the URL is fixed) and its single
HTML table is parsed with BeautifulSoup. The first two rows are header rows
and are skipped. Each data row's registration mark has internal whitespace
stripped (e.g. `ES- FCC` → `ES-FCC`) before it's used as the RediSearch lookup
key against Mictronics.

## Columns

The table has 9 positional columns (0-based); most are blank spacers.

| Source column | Imported | Notes |
|---|---|---|
| Registration mark (col 1) | ✅ | ES-prefix; whitespace normalized; used as the Mictronics lookup key |
| Type of Aircraft (col 4) | ✅ | → `aircraft.model` |
| Serial number (col 5) | ✅ | → `aircraft.serial_number` |
| Owner (col 6) | ✅ | → `registrant.names[0]` |
| Operator (col 7) | ❌ | Parsed but not stored |
| Blank spacer columns (0, 2, 3, 8) | ❌ | No data present |

See `specs/data-dictionary.yaml` (`ee-transpordiamet` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 51105A | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "CESSNA",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "Cessna 172M",
        "serial_number": "17262412",
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "51105A",
    "military": false,
    "registrant": {
        "names": [
            "AS Tackmer Air"
        ]
    },
    "registration": "ES-FCC",
    "source": "ee-transpordiamet"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 511153 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "AIRBUS",
        "manufacturer_model": "AIRBUS A-320",
        "model": "Airbus A320",
        "serial_number": "2689",
        "type_designator": "A320",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "511153",
    "military": false,
    "registrant": {
        "names": [
            "Bank of America, N.A."
        ]
    },
    "registration": "ES-SAY",
    "source": "ee-transpordiamet"
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

Published once, at the end of a run, to `SkyFollower/runner/ee-transpordiamet/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_ee_transpordiamet_{name}/config` for each of the three stats above.
