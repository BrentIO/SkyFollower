# im-ardis

| | |
|---|---|
| **Country** | Isle of Man |
| **Registration prefix** | `M-` |
| **Data source** | https://ardis.iomaircraftregistry.com/register/search |
| **Format** | HTML table (POSTed search form with CSRF token) |
| **Run frequency** | Weekly (Thursday, 07:10 UTC) |
| **Depends on Mictronics for ICAO hex** | No — this runner supplies its own ICAO hex directly from the register's "Mode S Number" column, so no RediSearch lookup against Mictronics is needed. |

## How it works

The ARDIS (Aircraft Registry Data Information System) search page is first GET'd to extract a `__RequestVerificationToken`, then a full-register search is POSTed with that token and a fixed set of "match any" filter parameters requesting up to 50,000 results. The resulting HTML results table is parsed with BeautifulSoup; header cells contain a redundant sort-link label (e.g. `"Sort column by Registration MarkRegistration Mark"`), which a backreference regex strips down to just the column name so it can be used as a dict key per row. Rows whose `Aircraft Status` is `Deregistered` are filtered out, and any row missing a Mode S Number or registration mark is skipped. This registry is business-jet-heavy, which is reflected in the example output below.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Aircraft Status | ✅ | Used only to filter out `Deregistered` rows; not stored |
| Mode S Number | ✅ | → `icao_hex` (uppercased); this runner's own ICAO hex source |
| Registration Mark | ✅ | → `registration` |
| Aircraft Manufacturer | ✅ | → `aircraft.manufacturer` |
| Aircraft Type | ✅ | → `aircraft.model` |
| Serial Number | ✅ | → `aircraft.serial_number` |
| Registered Owners | ✅ | Split on first comma into `registrant.names[0]` (name) and `registrant.street[0]` (remainder); stored as `names[0]` only if no comma present |

See `specs/data-dictionary.yaml` (`im-ardis` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 43E76C | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "Cessna Aircraft Company",
        "manufacturer_model": "CESSNA T206 Turbo Stationair",
        "model": "T206H",
        "serial_number": "T20608513",
        "type_designator": "T206",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "43E76C",
    "military": false,
    "registrant": {
        "names": [
            "MICHAEL PETER SELBY"
        ],
        "street": [
            "1 Drumclog Avenue Milngavie Glasgow United Kingdom G62 8NA"
        ]
    },
    "registration": "M-AXIM",
    "source": "im-ardis"
}
```

Note: this registry is business-jet-heavy with very few GA singles, so the "small aircraft" example above is a Cessna T206 Turbo Stationair rather than a 150/172.

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 424CB8 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "AIRBUS S.A.S.",
        "manufacturer_model": "AIRBUS A-320",
        "model": "A320-216",
        "serial_number": "5824",
        "type_designator": "A320",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "424CB8",
    "military": false,
    "registrant": {
        "names": [
            "WILMINGTON TRUST SP SERVICES (DUBLIN) LIMITED"
        ],
        "street": [
            "Fourth Floor, 3 George's Dock I.F.S.C. Dublin 1 Ireland D01X5X0"
        ]
    },
    "registration": "M-ABTS",
    "source": "im-ardis"
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

Published once, at the end of a run, to `SkyFollower/runner/im-ardis/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_im_ardis_{name}/config` for each of the three stats above.
