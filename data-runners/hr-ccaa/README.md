# hr-ccaa

| | |
|---|---|
| **Country** | Croatia |
| **Registration prefix** | `9A-` |
| **Data source** | https://www.ccaa.hr/en/list-of-registered-aircraft-94674 |
| **Format** | PDF (URL discovered by scraping the index page for the first `/file/` link) |
| **Run frequency** | Weekly (Tuesday, 23:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The Croatia CCAA index page is scraped for the first anchor tag whose `href` contains `/file/`, which is followed to download the current register PDF. Each page is parsed with `pdfplumber`'s built-in table extraction (`extract_table()`), which works reliably on this PDF. Rows are matched against a strict suffix pattern (`^[A-Z0-9]{2,4}$`) before the `9A-` prefix is prepended, filtering out header/footer noise rows that don't contain a real registration suffix.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| REG. OZNAKA | ✅ | Suffix only (e.g. `ABC`); `9A-` prefix prepended → used as the Mictronics lookup key |
| REDNI BROJ | ❌ | Parsed but not stored (row-number column) |
| PROIZVOĐAČ | ✅ | → `aircraft.manufacturer` |
| OZNAKA ZRAKOPLOVA | ✅ | → `aircraft.model` |
| SERIJSKI BROJ | ✅ | → `aircraft.serial_number` |
| VLASNIK | ✅ | → `registrant.names[0]` |
| ADRESA | ✅ | → `registrant.street` (split on commas into parts) |

See `specs/data-dictionary.yaml` (`hr-ccaa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 501C2E | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "Reims Aviation Industries",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "Cessna F172N",
        "serial_number": "F17201978",
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "501C2E",
    "military": false,
    "registrant": {
        "names": [
            "Vektra d.o.o."
        ],
        "street": [
            "Branka Vodinka 4b",
            "42 000 Varaždin"
        ]
    },
    "registration": "9A-DFK",
    "source": "hr-ccaa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 501C65 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "The Boeing Company",
        "manufacturer_model": "BOEING 737-800",
        "model": "737-800",
        "serial_number": "29659",
        "type_designator": "B738",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "501C65",
    "military": false,
    "registrant": {
        "names": [
            "Horizon Aviation 4 Limited"
        ],
        "street": [
            "32 Molesworth Street",
            "2 Dublin"
        ]
    },
    "registration": "9A-ICF",
    "source": "hr-ccaa"
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

Published once, at the end of a run, to `SkyFollower/runner/hr-ccaa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_hr_ccaa_{name}/config` for each of the three stats above.
