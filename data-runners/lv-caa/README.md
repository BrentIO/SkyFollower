# lv-caa

| | |
|---|---|
| **Country** | Latvia |
| **Registration prefix** | `YL-` |
| **Data source** | https://data.gov.lv/dati/lv/api/action/datastore_search (resource_id `dbde00e6-8616-449a-8cac-ef748c6793f3`) |
| **Format** | JSON API (data.gov.lv CKAN datastore) |
| **Run frequency** | Weekly (Tuesday, 16:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — the Latvia CAA register does not publish ICAO hex (Mode S) addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The register is fetched in one request from the data.gov.lv CKAN
`datastore_search` API against a fixed resource ID, with a page size large
enough (`limit=50000`) to return the whole dataset in a single call. Records
whose `Registration_Mark` does not start with `YL-` are discarded during
parsing. Free-text fields (`Model`, `Aircraft_Model_Category`) have internal
whitespace collapsed to single spaces. `Construction_Year` is a bare year, so
it is stored as `manufactured_date` with a synthetic `-01-01` day/month
(only if it parses as an integer in the range 1900–2100).

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Registration_Mark | ✅ | YL-prefix filter; used as the Mictronics lookup key |
| Model | ✅ | Whitespace-collapsed → `aircraft.model` |
| Serial_No | ✅ | → `aircraft.serial_number` |
| Construction_Year | ✅ | Bare year → `aircraft.manufactured_date` as `{year}-01-01`; ignored if outside 1900–2100 |
| Aircraft_Model_Category | ✅ | Whitespace-collapsed → `aircraft.type` |

See `specs/data-dictionary.yaml` (`lv-caa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 502C8D | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2005-01-01",
        "manufacturer": "CESSNA",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "Cessna 172S",
        "serial_number": "172S9795",
        "type": "Aircraft",
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "502C8D",
    "military": false,
    "registration": "YL-MBL",
    "source": "lv-caa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 502CCC | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2004-01-01",
        "manufacturer": "AIRBUS",
        "manufacturer_model": "AIRBUS A-321",
        "model": "A321-231",
        "serial_number": "2211",
        "type": "Aircraft",
        "type_designator": "A321",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "502CCC",
    "military": false,
    "registration": "YL-LCQ",
    "source": "lv-caa"
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

Published once, at the end of a run, to `SkyFollower/runner/lv-caa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_lv_caa_{name}/config` for each of the three stats above.
