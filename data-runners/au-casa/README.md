# au-casa

| | |
|---|---|
| **Country** | Australia |
| **Registration prefix** | `VH-` |
| **Data source** | https://services.casa.gov.au/CSV/acrftreg.csv |
| **Format** | CSV (fixed URL) |
| **Run frequency** | Weekly (Tuesday, 07:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; `VH-{Mark}` registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The CASA register CSV is downloaded from a fixed URL and parsed with `csv.DictReader` (BOM-tolerant `utf-8-sig` decoding). Rows whose `suspendstatus` is `suspended` are filtered out before lookup. Registrations (`VH-` + `Mark`) are resolved to `icao_hex` in batches of 100 via RediSearch against the Mictronics index, then a type sanity check compares tokens extracted from the CASA `Model` column against the existing Mictronics `type_designator`/`manufacturer_model` fields, rejecting the match only when both sides have tokens and none overlap. `Airframe` and `Engtype` are decoded from CASA's plain-English categories (e.g. `Power Driven Aeroplane` → `Airplane`, `Turbofan` → `Turbo-fan`) and `regholdCountry` full country names are mapped to ISO 3166-1 alpha-2 codes, with unmapped values passed through as-is.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `Mark` | ✅ | Combined with `VH-` prefix; used as the Mictronics lookup key |
| `suspendstatus` | ✅ | Used only as a filter (`suspended` rows are dropped); not stored |
| `Airframe` | ✅ | → `aircraft.type`, decoded from CASA's category names |
| `Manu` | ✅ | → `aircraft.manufacturer` |
| `Model` | ✅ | → `aircraft.model`; also used for the type sanity check against Mictronics |
| `Serial` | ✅ | → `aircraft.serial_number` |
| `Yearmanu` | ✅ | → `aircraft.manufactured_date` (4-digit year → `YYYY-01-01T00:00:00Z`) |
| `ICAOtypedesig` | ✅ | → `aircraft.type_designator` |
| `engnum` | ✅ | → `aircraft.powerplant.count` |
| `Engmanu` | ✅ | → `aircraft.powerplant.manufacturer` |
| `Engtype` | ✅ | → `aircraft.powerplant.type`, decoded (e.g. `Turboprop` → `Turbo-prop`); `Not Applicable` omits the field |
| `Engmodel` | ✅ | → `aircraft.powerplant.model` |
| `regholdname` | ✅ | → `registrant.names[0]` |
| `regholdadd1` / `regholdadd2` | ✅ | → `registrant.street[]` |
| `regholdSuburb` | ✅ | → `registrant.city` |
| `regholdState` | ✅ | → `registrant.administrative_area` |
| `regholdPostcode` | ✅ | → `registrant.postal_code` |
| `regholdCountry` | ✅ | → `registrant.country`, mapped to ISO 3166-1 alpha-2 |

See `specs/data-dictionary.yaml` (`au-casa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 7C4F42 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2008-01-01T00:00:00Z",
        "manufacturer": "CESSNA AIRCRAFT COMPANY",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "172S",
        "powerplant": {
            "count": 1,
            "manufacturer": "TEXTRON LYCOMING",
            "model": "IO-360-L2A",
            "type": "Piston"
        },
        "serial_number": "172S10673",
        "type": "Airplane",
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "7C4F42",
    "military": false,
    "registrant": {
        "administrative_area": "NSW",
        "city": "PARRAMATTA",
        "country": "AU",
        "names": [
            "WESTPAC BANKING CORPORATION"
        ],
        "postal_code": "2150",
        "street": [
            "Level 34 Tower 8 Parramatta Square",
            "10 Darcy St"
        ]
    },
    "registration": "VH-PXW",
    "source": "au-casa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 7C4920 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2008-01-01T00:00:00Z",
        "manufacturer": "AIRBUS INDUSTRIE",
        "manufacturer_model": "AIRBUS A-380-800",
        "model": "A380-842",
        "powerplant": {
            "count": 4,
            "manufacturer": "ROLLS ROYCE LTD",
            "model": "RB211 Trent 972-84",
            "type": "Turbo-fan"
        },
        "serial_number": "0014",
        "type": "Airplane",
        "type_designator": "A388",
        "wake_turbulence_category": "Heavy"
    },
    "icao_hex": "7C4920",
    "military": false,
    "registrant": {
        "administrative_area": "NSW",
        "city": "MASCOT",
        "country": "AU",
        "names": [
            "QF BOC 2008-1 PTY LIMITED"
        ],
        "postal_code": "2020",
        "street": [
            "10 Bourke Rd"
        ]
    },
    "registration": "VH-OQA",
    "source": "au-casa"
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

Published once, at the end of a run, to `SkyFollower/runner/au-casa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_au_casa_{name}/config` for each of the three stats above.
