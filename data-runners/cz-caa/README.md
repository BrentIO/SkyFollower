# 🇨🇿 cz-caa

| | |
|---|---|
| **Country** | Czech Republic |
| **Registration prefix** | `OK-` |
| **Data source** | https://lr.caa.gov.cz/api/avreg/filtered (list) + https://lr.caa.gov.cz/api/avreg/{id} (detail) |
| **Format** | JSON API, two-step (list endpoint, then one detail request per active record) |
| **Run frequency** | Weekly (Tuesday, 21:10 UTC) |
| **Depends on Mictronics for ICAO hex** | No — the detail record's `transponder` field is the Mode S hex address directly. |

## How it works

The list endpoint (`.../avreg/filtered?start=0&length=10000`) returns all records, which are filtered client-side to those with `deletion_date == null` (i.e. still active). Each active record's `id` is then used to fetch a full detail record from a per-aircraft endpoint, with a 0.25s delay between requests and retry-with-backoff (up to 3 attempts) on `403`/`429`/5xx responses and connection errors. Records without a non-empty `transponder` value are skipped, since that field becomes the Redis key. The detail record's `owners` list is used in full (every `display_name` present, not just the first) to populate `registrant.names`; the detail record also carries a distinct `operators` field (e.g. an aeroclub that owns a glider operated by a separate flying school) but it is intentionally not read — only `owners` is tracked for registrant identity. Every written record explicitly sets `military: false` — this register is exclusively civil, and the explicit value ensures a stale `military: true` flag (from Mictronics or a prior record on a reused hex) is corrected on re-registration.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| id (list endpoint) | ❌ | Used only to build the per-record detail request URL; not stored |
| deletion_date | ❌ | Used to filter to active (non-deleted) records only; value itself is not stored |
| transponder | ✅ | → `icao_hex` (skipped if null/empty) |
| registration_number | ✅ | `OK-` prefix prepended → `registration` |
| category | ✅ | Decoded via a category map (e.g. `AVREG_DATA.CATEGORIES.AIRPLANE` → `Airplane`) → `aircraft.type` |
| type (detail endpoint) | ❌ | Free-text type/model designation (e.g. `SZD-45A`) distinct from the coded `category` field; present in source, not read by this runner |
| manufacturer | ✅ | → `aircraft.manufacturer` |
| model | ✅ | → `aircraft.model` |
| serial_number | ✅ | → `aircraft.serial_number` |
| manufacture_year | ✅ | Integer year (1900–2100) → `aircraft.manufactured_date` (`YYYY-01-01`) |
| registration_date | ❌ | Present in source; not read by this runner |
| mtow | ❌ | Present in source; not read by this runner |
| color | ❌ | Present in source; not read by this runner |
| pledge | ❌ | Present in source; not read by this runner |
| pledge_text | ❌ | Present in source; not read by this runner |
| sanctions | ❌ | Present in source; not read by this runner |
| sanctions_text | ❌ | Present in source; not read by this runner |
| transfer_of_rights | ❌ | Present in source; not read by this runner |
| transfer_of_rights_text | ❌ | Present in source; not read by this runner |
| engine_type | ✅ | Decoded via an engine-type map; `NO_ENGINE` omitted entirely → `aircraft.powerplant.type` |
| engine_count | ✅ | → `aircraft.powerplant.count` |
| max_on_board | ✅ | → `aircraft.seats` |
| owners[].display_name | ✅ | All non-empty display names → `registrant.names[]` |
| owners[].legal_id | ❌ | Present in source; not read by this runner |
| operators[].display_name | ❌ | Distinct from owners on many records; present in source, intentionally not read |
| operators[].legal_id | ❌ | Present in source; not read by this runner |

See specs/data-dictionary.yaml (`cz-caa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 49D160 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "1974-01-01",
        "manufacturer": "REIMS AVIATION S.A.",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "FR 172 J",
        "powerplant": {
            "count": 1,
            "type": "Piston"
        },
        "seats": 4,
        "serial_number": "0471",
        "type": "Airplane",
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "49D160",
    "military": false,
    "registrant": {
        "names": [
            "Carnovia Aero s.r.o."
        ]
    },
    "registration": "OK-EKM",
    "source": "cz-caa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 49D166 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "1976-01-01",
        "manufacturer": "PZL MIELEC",
        "manufacturer_model": "ANTONOV An-2",
        "model": "An-2",
        "powerplant": {
            "count": 1,
            "type": "Piston"
        },
        "seats": 14,
        "serial_number": "1G16801",
        "type": "Airplane",
        "type_designator": "AN2",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "49D166",
    "military": false,
    "registrant": {
        "names": [
            "Aeroklub Nové Město nad Metují, z. s."
        ]
    },
    "registration": "OK-GIB",
    "source": "cz-caa"
}
```

Note: `OK-GIB` (an Antonov An-2) is used here in place of the more typical "big example" because Mictronics currently mislabels the widebody-adjacent `OK-OJL` registration as an A319, when it is actually a Zlin Z-37 Čmelák — this is a known upstream data-quality issue, not a bug in this runner.

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

Published once, at the end of a run, to `SkyFollower/runner/cz-caa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_cz_caa_{name}/config` for each of the three stats above.
