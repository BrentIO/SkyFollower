# mk-caa

| | |
|---|---|
| **Country** | North Macedonia |
| **Registration prefix** | `Z3-` |
| **Data source** | https://www.caa.gov.mk/en/safety/airworthiness-and-aircraft-registration/ |
| **Format** | PDF (date-stamped filename, discovered via index page) |
| **Run frequency** | Weekly (Tuesday, 11:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; `Z3-` registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The CAA index page is scraped for the first link whose href contains
`AIRCRAFT-REGISTER` (case-insensitive) and ends in `.pdf`. The PDF has no
table header row at all, so `pdfplumber`'s `extract_table()` output is read
purely by fixed column position rather than by column name: registration is
at index 2, model at 3, manufacturer at 4, serial number at 5, owner name at
6, and owner address at 7. Only rows whose registration starts with `Z3-` are
kept. Model, manufacturer, serial number, owner name, and owner address are
run through the same whitespace-collapsing transform, since `pdfplumber`
sometimes represents a single cell's text with an embedded newline rather
than a space (e.g. a wrapped manufacturer name).

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Certifi. No. (position 0) | ❌ | Certificate/registration-record number; not read by the parser |
| Date of Registration (position 1) | ❌ | Not read by the parser |
| Registration (position 2) | ✅ | Z3-prefix; used as the Mictronics lookup key |
| Model (position 3) | ✅ | → `aircraft.model`; embedded newlines collapsed to a single space |
| Manufacturer (position 4) | ✅ | → `aircraft.manufacturer`; embedded newlines collapsed to a single space |
| Serial Number (position 5) | ✅ | → `aircraft.serial_number`; embedded newlines collapsed to a single space |
| Owner Name (position 6) | ✅ | → `registrant.names[0]`; embedded newlines collapsed to a single space |
| Owner Address (position 7) | ✅ | → `registrant.street`; embedded newlines collapsed to a single space |

See `specs/data-dictionary.yaml` (`mk-caa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 512031 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "The New Piper Aircraft INC",
        "manufacturer_model": "PIPER PA-28R-180/200/201",
        "model": "PIPER PA-28R-201",
        "serial_number": "28R-787120",
        "type_designator": "P28R",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "512031",
    "military": false,
    "registrant": {
        "names": [
            "TRANS INTER DOO / Aeroklub “Skopje” DOO Chucher Sandevo"
        ],
        "street": "Sportski Aerodrom Stenkovec No. 151a, Chucher Sandevo"
    },
    "registration": "Z3-DAJ",
    "source": "mk-caa"
}
```

North Macedonia's registry has no jets or widebodies, so the larger example
below is an Air Tractor AT-802 — the largest type in the registry:

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 512029 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "Air Tractor Inc",
        "manufacturer_model": "AIR TRACTOR AT-802",
        "model": "AT-802A",
        "serial_number": "802A-0313",
        "type_designator": "AT8T",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "512029",
    "military": false,
    "registrant": {
        "names": [
            "Protection and Rescue Directorate"
        ],
        "street": "“Vasko Karangeleski“ bb Skopje"
    },
    "registration": "Z3-BGU",
    "source": "mk-caa"
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

Published once, at the end of a run, to `SkyFollower/runner/mk-caa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_mk_caa_{name}/config` for each of the three stats above.
