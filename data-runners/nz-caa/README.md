# nz-caa

| | |
|---|---|
| **Country** | New Zealand |
| **Registration prefix** | `ZK-` |
| **Data source** | https://www.aviation.govt.nz/assets/aircraft/aircraft-register/Aircraft-Register-for-website-.csv |
| **Format** | CSV (fixed URL) |
| **Run frequency** | Weekly (Wednesday, 05:10 UTC) |
| **Depends on Mictronics for ICAO hex** | No — the CSV publishes its own `Mode S Code HEX` column, so `icao_hex` is read directly from the source data. |

## How it works

The CSV is downloaded whole from a fixed URL and decoded as `utf-8-sig` (the file carries a BOM), then parsed with `csv.DictReader`. The genuinely tricky part is `Owner Address`: it's a single free-text field shaped like `"Street 1[, Street 2, ...], City PostalCode, Country"`. `_parse_address` splits on `", "`, treats the last segment as the country and the second-to-last as a combined "City PostalCode" pair (matched with a regex), and falls back to treating everything but the last segment as street lines when the city/postal pattern doesn't match or the candidate city looks like a PO box.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `Mode S Code HEX` | ✅ | ICAO hex; record is dropped entirely if blank |
| `Registration Mark` | ✅ | → `registration` |
| `Model Category` | ✅ | → `aircraft.type`, decoded via lookup table (e.g. `Aeroplane (Aircraft)` → `Airplane`) |
| `Manufacturer` | ✅ | → `aircraft.manufacturer` |
| `Model` | ✅ | → `aircraft.model` |
| `Serial No.` | ✅ | → `aircraft.serial_number` |
| `Owner Name` | ✅ | → `registrant.names[0]` |
| `Owner Address` | ✅ | Free-text parsed into `registrant.street[]` / `city` / `postal_code` / `country` |

See `specs/data-dictionary.yaml` (`nz-caa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 C804C7 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "Cessna Aircraft Company",
        "manufacturer_model": "CESSNA 152",
        "model": "A152",
        "serial_number": "A1520919",
        "type": "Airplane",
        "type_designator": "C152",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "C804C7",
    "military": false,
    "registrant": {
        "city": "Wellington",
        "country": "NZ",
        "names": [
            "Roc On Aviation Limited"
        ],
        "postal_code": "6022",
        "street": [
            "15 Bowes Crescent",
            "Strathmore Park"
        ]
    },
    "registration": "ZK-NPH",
    "source": "nz-caa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 C81E2C | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "Airbus",
        "manufacturer_model": "AIRBUS A-320",
        "model": "A320-232",
        "serial_number": "4926",
        "type": "Airplane",
        "type_designator": "A320",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "C81E2C",
    "military": false,
    "registrant": {
        "city": "Auckland",
        "country": "NZ",
        "names": [
            "Air New Zealand Limited"
        ],
        "postal_code": "1142",
        "street": [
            "C/- Internal Address AKL 43/G",
            "Private Bag 92007",
            "Victoria Street West"
        ]
    },
    "registration": "ZK-OJS",
    "source": "nz-caa"
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

Published once, at the end of a run, to `SkyFollower/runner/nz-caa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_nz_caa_{name}/config` for each of the three stats above.
