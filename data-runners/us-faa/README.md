# us-faa

| | |
|---|---|
| **Country** | United States |
| **Registration prefix** | `N` (no dash; N-number, e.g. `N62770`) |
| **Data source** | https://registry.faa.gov/database/ReleasableAircraft.zip |
| **Format** | ZIP of pipe-free CSV files (fixed URL; no discovery step) |
| **Run frequency** | Weekly (Saturday, 05:10 UTC) |
| **Depends on Mictronics for ICAO hex** | No — `master.txt` (the registration file) publishes the Mode S code hex directly. |

## How it works

The FAA Releasable Aircraft Database ZIP is downloaded from a fixed URL and extracted entirely in memory; filenames are lowercased, and a `.txt.txt` double-extension bug the FAA has occasionally shipped is normalized away. Three files are staged into a local SQLite database rather than written straight to Redis, so that per-tail registrations can be joined against reference tables cheaply:

- `engine.txt` — engine make/model/type/power reference, keyed by engine code
- `acftref.txt` — aircraft make/model/seats/category reference, keyed by aircraft code
- `master.txt` — one row per registered tail, keyed by `icao_hex` (only rows with a well-formed 6-character Mode S hex are kept)

This runner supports the 2017+ column layout only. Numeric FAA type codes (engine type, aircraft type, aircraft class, registrant type) are decoded via lookup tables into human-readable strings. Powerplant power is reported as thrust for jet/fan/ramjet engine types and as horsepower for piston/turboprop/turboshaft/2-4-cycle/rotary types. The final Redis write is driven by a single `LEFT JOIN` query across the staged tables and flushed to Redis in batches of 10,000.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `engine.txt`: CODE | ✅ | Join key to `master.txt`'s engine-code column |
| `engine.txt`: MFR | ✅ | → `aircraft.powerplant.manufacturer` |
| `engine.txt`: MODEL | ✅ | → `aircraft.powerplant.model` |
| `engine.txt`: TYPE | ✅ | → `aircraft.powerplant.type` (decoded) |
| `engine.txt`: HORSEPOWER | ✅ | → `aircraft.powerplant.power_value` (piston/turboprop/turboshaft/2-4-cycle/rotary types) |
| `engine.txt`: THRUST | ✅ | → `aircraft.powerplant.power_value` (turbo-jet/turbo-fan/ramjet types) |
| `acftref.txt`: CODE | ✅ | Join key to `master.txt`'s aircraft-code column |
| `acftref.txt`: MFG | ✅ | → `aircraft.manufacturer` |
| `acftref.txt`: MODEL | ✅ | → `aircraft.model` |
| `acftref.txt`: TYPE-ACFT | ✅ | → `aircraft.type` (decoded) |
| `acftref.txt`: TYPE-ENG | ✅ | → `aircraft.powerplant.type` (decoded; used if no matching `engine.txt` row) |
| `acftref.txt`: CLASS | ✅ | → `aircraft.category` (decoded) |
| `acftref.txt`: RULES | ❌ | Parsed but not stored |
| `acftref.txt`: NO-ENG | ✅ | → `aircraft.powerplant.count` |
| `acftref.txt`: NO-SEATS | ✅ | → `aircraft.seats` |
| `acftref.txt`: AC-WEIGHT | ❌ | Parsed but not stored |
| `acftref.txt`: SPEED | ❌ | Parsed but not stored |
| `master.txt`: N-NUMBER | ✅ | → `registration` (prefixed with `N`) |
| `master.txt`: SERIAL NUMBER | ✅ | → `aircraft.serial_number` |
| `master.txt`: MFR MDL CODE | ✅ | Join key to `acftref.txt` |
| `master.txt`: ENG MFR MDL CODE | ✅ | Join key to `engine.txt` |
| `master.txt`: YEAR MFR | ✅ | → `aircraft.manufactured_date` |
| `master.txt`: TYPE REGISTRANT | ✅ | → `registrant.type` (decoded) |
| `master.txt`: NAME | ✅ | → `registrant.names[0]` |
| `master.txt`: STREET / STREET2 | ✅ | → `registrant.street` |
| `master.txt`: CITY | ✅ | → `registrant.city` |
| `master.txt`: STATE | ✅ | → `registrant.administrative_area` |
| `master.txt`: ZIP CODE | ✅ | → `registrant.postal_code` |
| `master.txt`: COUNTRY | ✅ | → `registrant.country` (defaults to `US`) |
| `master.txt`: OTHER NAMES (1-5) | ✅ | → `registrant.names[1:]` |
| `master.txt`: MODE S CODE HEX | ✅ | → `icao_hex`; rows without a well-formed 6-character hex are discarded |

See `specs/data-dictionary.yaml` (`us-faa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 A833A4 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "category": "Land",
        "manufactured_date": "2008-01-01T00:00:00Z",
        "manufacturer": "CESSNA",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "172S",
        "powerplant": {
            "count": 1,
            "manufacturer": "LYCOMING",
            "model": "IO-360-L2A",
            "power_type": "Horsepower",
            "power_value": 180,
            "type": "Piston"
        },
        "seats": 4,
        "serial_number": "172S10747",
        "type": "Airplane",
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "A833A4",
    "military": false,
    "registrant": {
        "administrative_area": "FL",
        "city": "WINTER PARK",
        "country": "US",
        "names": [
            "HOG ISLAND FLYING LLC"
        ],
        "postal_code": "327893726",
        "street": [
            "807 W MORSE BLVD STE 101"
        ],
        "type": "LLC"
    },
    "registration": "N62770",
    "source": "us-faa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 A8AE7F | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "category": "Land",
        "manufactured_date": "1990-01-01T00:00:00Z",
        "manufacturer": "BOEING",
        "manufacturer_model": "BOEING 757-200",
        "model": "757-232",
        "powerplant": {
            "count": 2,
            "manufacturer": null,
            "model": null,
            "power_type": null,
            "power_value": null,
            "type": "Turbo-fan"
        },
        "seats": 178,
        "serial_number": "24421",
        "type": "Airplane",
        "type_designator": "B752",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "A8AE7F",
    "military": false,
    "registrant": {
        "administrative_area": "GA",
        "city": "ATLANTA",
        "country": "US",
        "names": [
            "DELTA AIR LINES INC"
        ],
        "postal_code": "303543743",
        "street": [
            "1775 M H JACKSON SERVICE RD",
            "DEPT 595 AIRCRAFT REGISTRATIONS"
        ],
        "type": "Corporation"
    },
    "registration": "N659DL",
    "source": "us-faa"
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

Published once, at the end of a run, to `SkyFollower/runner/us-faa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_us_faa_{name}/config` for each of the three stats above.
