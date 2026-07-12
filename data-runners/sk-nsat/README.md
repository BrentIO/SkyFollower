# sk-nsat

| | |
|---|---|
| **Country** | Slovakia |
| **Registration prefix** | `OM-` |
| **Data source** | https://letectvo.nsat.sk/letova-sposobilost/register-lietadiel-slovenskej-republiky/zoznam-registra/ |
| **Format** | PDF (date-stamped filename, discovered via index page) |
| **Run frequency** | Weekly (Tuesday, 09:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — the NSAT (Národný bezpečnostný úrad pre letectvo) register does not publish ICAO hex (Mode S) addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The register PDF's URL is date-stamped and not hardcoded, so the NSAT index page is scraped for the first `.pdf` link found. Tables are extracted with `pdfplumber`; a row is treated as a data row when its second cell, after stripping all whitespace, starts with `OM-` (registration marks appear in the PDF in a spaced format, e.g. `OM - 0101`, and are normalized to `OM-0101`). Once a header row has been seen, the four Slovak column values are re-assigned positionally onto internal column-name constants rather than trusting the header dict directly, since `pdfplumber` can introduce newline/Unicode artifacts into the extracted header text that would otherwise mismatch the constants. Model, serial number, and owner are all run through a whitespace-collapsing transform, since `pdfplumber` can also introduce embedded newlines into a wrapped cell's text. Every written record explicitly sets `military: false` — this register is exclusively civil, and the explicit value ensures a stale `military: true` flag (from Mictronics or a prior record on a reused hex) is corrected on re-registration.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Poznávacia značka (Registration) | ✅ | OM-prefix; used as the Mictronics lookup key |
| Typ lietadla (Aircraft Type) | ✅ | → `aircraft.model`; embedded newlines collapsed to a single space |
| Výrobné číslo (Serial Number) | ✅ | → `aircraft.serial_number`; embedded newlines collapsed to a single space |
| Vlastník (Owner) | ✅ | → `registrant.names[0]`; embedded newlines collapsed to a single space |
| Prevádzkovateľ (Operator) | ❌ | Present in source; not read by this runner |
| Záložné právo (Lien) | ❌ | Present in source; not read by this runner |

See `specs/data-dictionary.yaml` (`sk-nsat` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 505C72 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "PIPER",
        "manufacturer_model": "PIPER PA-28RT-201T Turbo",
        "model": "PA-28RT-201T (Turbo Arrow IV)",
        "serial_number": "28R-8031166",
        "type_designator": "P28U",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "505C72",
    "military": false,
    "registrant": {
        "names": [
            "Žilinská univerzita v Žiline"
        ]
    },
    "registration": "OM-DCR",
    "source": "sk-nsat"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 505A88 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "BOEING",
        "manufacturer_model": "BOEING 737-800",
        "model": "Boeing 737-8Q8",
        "serial_number": "35275",
        "type_designator": "B738",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "505A88",
    "military": false,
    "registrant": {
        "names": [
            "AVIATOR IV 30666, DESIGNATED ACTIVITY COMPANY"
        ]
    },
    "registration": "OM-TVH",
    "source": "sk-nsat"
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

Published once, at the end of a run, to `SkyFollower/runner/sk-nsat/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_sk_nsat_{name}/config` for each of the three stats above.
