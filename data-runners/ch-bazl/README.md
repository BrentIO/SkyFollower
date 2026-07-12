# ch-bazl

| | |
|---|---|
| **Country** | Switzerland |
| **Registration prefix** | `HB-` |
| **Data source** | https://app02.bazl.admin.ch/web/bazl-backend/lfr/csv |
| **Format** | JSON API (`POST`, fixed endpoint) returning a UTF-16 BE, semicolon-delimited CSV body (~3,100 records) |
| **Run frequency** | Weekly (Wednesday, 13:10 UTC) |
| **Depends on Mictronics for ICAO hex** | No — the FOCA/BAZL register publishes the Mode S hex address directly (`Aircraft Address HEX` column). |

## How it works

A single `POST` request to the fixed FOCA/BAZL backend endpoint (`page_result_limit: 10000`, `current_page_number: 1`, filtered to `aircraftStatus: ["Registered"]`) returns the entire register as one UTF-16 encoded, semicolon-delimited CSV — no authentication or page discovery needed. Rows are additionally filtered to `Status == "Registered"` and to those with a valid 6-hex-digit `Aircraft Address HEX`. Note that the CSV column headers all carry a leading space (e.g. `" Registration"`, `" Aircraft Type"`) — this is preserved verbatim in the source and must be matched exactly when reading `row.get(...)`. The `Main Owner` field is a single unstructured address string (`Name[, Canton]?, Street, PostalCode City, Switzerland`) that is best-effort parsed by popping known trailing/canton tokens off a comma-split list, since there's no per-field structure to rely on. Every written record explicitly sets `military: false` — this register is exclusively civil, and the explicit value ensures a stale `military: true` flag (from Mictronics or a prior record on a reused hex) is corrected on re-registration.

Whenever a record has an `aircraft.type_designator`, `aircraft:type:{type_designator}` is looked up in Redis (populated by the `mictronics` runner) and, if found, its `manufacturer_model` and `wake_turbulence_category` are each set directly on this record (independently — a type entry with only one of the two still sets that one) — unconditionally, regardless of whether Mictronics also has values for the same hex. This runner's own `type_designator` is sourced directly from FOCA/BAZL and is authoritative; `merge_aircraft.lua`'s "registry wins over mictronics" precedence rule guarantees these values take priority at read time either way. The lookup is not a hard dependency — a missing reference table entry, or the table not existing yet, leaves the record exactly as it would have been without this step.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| LfrID | ❌ | Present in source; not read by this runner |
| Registration | ✅ | HB-prefix → `registration` |
| Status | ❌ | Used to filter to `Registered` rows only; value itself is not stored |
| Date of Registration | ❌ | Present in source; not read by this runner |
| Date of Deregistration | ❌ | Present in source; not read by this runner |
| Manufacturer | ✅ | → `aircraft.manufacturer` |
| Aicraft Model (sic, source typo) | ✅ | → `aircraft.model` |
| ICAO Aircraft Type | ✅ | → `aircraft.type_designator`; also used to look up `aircraft:type:{type_designator}` in Redis, setting `aircraft.manufacturer_model` and `aircraft.wake_turbulence_category` when found |
| Marketing Designation | ❌ | Present in source; not read by this runner |
| Aircraft Type | ✅ | Decoded via a type map (e.g. `Homebuilt Airplane` → `Airplane`) → `aircraft.type` |
| Certification Basis | ❌ | Present in source; not read by this runner |
| Airworthiness Category | ❌ | Present in source; not read by this runner |
| Legal Basis | ❌ | Present in source; not read by this runner |
| TCDS | ❌ | Present in source; not read by this runner |
| ELA | ❌ | Present in source; not read by this runner |
| Aircraft Address DEC | ❌ | Present in source; not read by this runner |
| Aircraft Address HEX | ✅ | → `icao_hex` |
| Aircraft Address OCT | ❌ | Present in source; not read by this runner |
| Aircraft Address BIN | ❌ | Present in source; not read by this runner |
| ELT Code | ❌ | Present in source; not read by this runner |
| Year of Manufacture | ✅ | 4-digit year → `aircraft.manufactured_date` (`YYYY-01-01`) |
| Serial Number | ✅ | → `aircraft.serial_number` |
| BRS | ❌ | Present in source; not read by this runner |
| MOPSC | ✅ | Summed with Minimum Crew → `aircraft.seats` |
| Minimum Crew | ✅ | Summed with MOPSC → `aircraft.seats` |
| MTOM | ❌ | Present in source; not read by this runner |
| Engine manufacturer | ✅ | → `aircraft.powerplant.manufacturer` |
| Engine | ✅ | First comma-separated value → `aircraft.powerplant.model` |
| Engine Category | ✅ | First comma-separated value decoded via engine map → `aircraft.powerplant.type` |
| Propeller manufacturer | ❌ | Present in source; not read by this runner |
| Propeller | ❌ | Present in source; not read by this runner |
| Noise Standard | ❌ | Present in source; not read by this runner |
| Noise Level | ❌ | Present in source; not read by this runner |
| Noise Class | ❌ | Present in source; not read by this runner |
| Main Owner | ✅ | Best-effort parsed into `registrant.names`/`street`/`city`/`postal_code`; `registrant.country` is hardcoded `CH` |
| Main Operator | ❌ | Present in source; not read by this runner |
| Part Owners | ❌ | Present in source; not read by this runner |
| Part Operators | ❌ | Present in source; not read by this runner |
| Billing Address | ❌ | Present in source; not read by this runner |

See specs/data-dictionary.yaml (`ch-bazl` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 4B012D | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "1944-01-01",
        "manufacturer": "PIPER AIRCRAFT CORPORATION",
        "manufacturer_model": "PIPER J-3 Cub",
        "model": "J3C-65/L-4.",
        "powerplant": {
            "manufacturer": "ROLLS-ROYCE MOTORS LTD.",
            "model": "RR O-200-A",
            "type": "Piston"
        },
        "seats": 2,
        "serial_number": "12026",
        "type": "Airplane",
        "type_designator": "J3",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "4B012D",
    "military": false,
    "registrant": {
        "city": "Wilihof",
        "country": "CH",
        "names": [
            "Kaufmann, Patrick"
        ],
        "postal_code": "6236",
        "street": [
            "Dorfstrasse 22"
        ]
    },
    "registration": "HB-ALP",
    "source": "ch-bazl"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 4B0280 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2021-01-01",
        "manufacturer": "AIRBUS S.A.S.",
        "manufacturer_model": "AIRBUS A-320neo",
        "model": "A320-251N",
        "powerplant": {
            "manufacturer": "SAFRAN AIRCRAFT ENGINES",
            "model": "LEAP-1A26",
            "type": "Turbo-jet"
        },
        "seats": 197,
        "serial_number": "10186",
        "type": "Airplane",
        "type_designator": "A20N",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "4B0280",
    "military": false,
    "registrant": {
        "country": "CH",
        "names": [
            "easyJet Airline Company Limited, London Luton Airport LU2 9PF Luton"
        ],
        "street": [
            "Bedfordshire"
        ]
    },
    "registration": "HB-AYQ",
    "source": "ch-bazl"
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

Published once, at the end of a run, to `SkyFollower/runner/ch-bazl/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_ch_bazl_{name}/config` for each of the three stats above.
