# mv-caa

| | |
|---|---|
| **Country** | Maldives |
| **Registration prefix** | `8Q-` |
| **Data source** | https://www.caa.gov.mv/operations/registration-of-aircraft-and-mortgages |
| **Format** | PDF (opaque hash filename, discovered via index page) |
| **Run frequency** | Weekly (Tuesday, 15:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; `8Q-` registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The CAA index page is scraped for the first link ending in `.pdf` (its
filename is an opaque hash rather than a predictable pattern, so any PDF href
found is taken). Each page of the PDF is parsed with `pdfplumber`'s
`extract_table()`; only rows whose registration column starts with `8Q-` and
whose status column reads exactly `"Valid"` are kept. The combined
Manufacturer+Designation column has embedded newlines collapsed to single
spaces. The Owner Name+Address column is multi-line: the first line is taken
as the owner name, the last line as the country, and any lines in between as
the street address (joined as a list).

## Columns

| Source column | Imported | Notes |
|---|---|---|
| N/S (sequence) | ❌ | Parsed but not stored |
| Certificate# | ❌ | Parsed but not stored |
| Registration | ✅ | 8Q-prefix; used as the Mictronics lookup key |
| Manufacturer+Designation | ✅ | → `aircraft.model`; embedded newlines collapsed to spaces |
| MTOW | ❌ | Parsed but not stored |
| Serial Number | ✅ | → `aircraft.serial_number` |
| Year Built | ✅ | → `aircraft.manufactured_date` (`YYYY-01-01`), only if a 4-digit year |
| Owner Name+Address | ✅ | Multi-line: first line → `registrant.names[0]`, last line → `registrant.country`, middle lines → `registrant.street` (list) |
| Legal Owner | ❌ | Parsed but not stored |
| Registration Basis | ❌ | Parsed but not stored |
| Basis of Registration, Other Specifics | ❌ | Present in source; not read by this runner |
| Mortgage | ❌ | Parsed but not stored |
| IDERA | ❌ | Present in source; not read by this runner |
| Original Issue Date | ❌ | Parsed but not stored |
| Last Revision Date | ❌ | Parsed but not stored |
| Status | ❌ | Used as a filter — only rows with status `"Valid"` are kept; the status value itself is not stored |

See `specs/data-dictionary.yaml` (`mv-caa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 05A097 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "1977-01-01",
        "manufacturer": "CESSNA",
        "manufacturer_model": "CESSNA 152",
        "model": "Textron Aviation Inc., 152",
        "serial_number": "152-80719",
        "type_designator": "C152",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "05A097",
    "military": false,
    "registrant": {
        "country": "Republic of Maldives",
        "names": [
            "Zenith Aviation Academy Pvt. Ltd."
        ],
        "street": [
            "Gan International Airport",
            "S. Gan, 19020"
        ]
    },
    "registration": "8Q-MFB",
    "source": "mv-caa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 05A0AF | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2010-01-01",
        "manufacturer": "AIRBUS",
        "manufacturer_model": "AIRBUS A-330-200",
        "model": "Airbus, Airbus A330-203",
        "serial_number": "1161",
        "type_designator": "A332",
        "wake_turbulence_category": "Heavy"
    },
    "icao_hex": "05A0AF",
    "military": false,
    "registrant": {
        "country": "Republic of Maldives",
        "names": [
            "Island Aviation Services Limited"
        ],
        "street": [
            "M. Dar Al-Eiman Building, Majeedhee Magu",
            "Male’ 20345"
        ]
    },
    "registration": "8Q-IAB",
    "source": "mv-caa"
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

Published once, at the end of a run, to `SkyFollower/runner/mv-caa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_mv_caa_{name}/config` for each of the three stats above.
