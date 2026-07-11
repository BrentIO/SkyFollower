# bs-caa

| | |
|---|---|
| **Country** | Bahamas |
| **Registration prefix** | `C6-` |
| **Data source** | https://caabahamas.com/registers/ |
| **Format** | PDF (date-stamped filename, discovered via index page) |
| **Run frequency** | Weekly (Friday, 05:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The Bahamas CAA registers index page is scraped for a link matching `Updated-THE-BAHAMAS-CIVIL-AIRCRAFT-REGISTER*.pdf` (tried first as an absolute URL, then as a relative `href`), and that PDF is downloaded to a temp file. Unlike the other PDF-based runners in this project, this one relies on `pdfplumber`'s built-in `extract_table()` rather than manual x-position grouping — the Bahamas PDF is Excel-generated with clean table borders, so `pdfplumber` detects columns reliably. Because the header row repeats on every page, any row that matches the captured header exactly is skipped. Registrations are resolved to `icao_hex` in batches of 100 via RediSearch against the Mictronics index, then a type sanity check compares tokens from the combined make/model column against the existing Mictronics record, rejecting the match only when both sides have tokens and none overlap. The Bahamas register carries only owner name, registration, a single combined make/model string, and serial number — no manufacturer, type designator, or powerplant fields are available at the source.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `AIRCRAFT REGISTRATION NUMBER` | ✅ | `C6-`-prefix; used as the Mictronics lookup key |
| `AIRCRAFT TYPE - MAKE/MODEL` | ✅ | → `aircraft.model` (combined make/model, not split); also used for the type sanity check against Mictronics |
| `SERIAL #` | ✅ | → `aircraft.serial_number` |
| `REGISTERED OWNER OF AIRCRAFT` | ✅ | → `registrant.names[0]` |

See `specs/data-dictionary.yaml` (`bs-caa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 0A80AD | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "PIPER",
        "manufacturer_model": "PIPER PA-23-250 Aztec",
        "model": "PA23-250",
        "serial_number": "27-7405407",
        "type_designator": "PA27",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "0A80AD",
    "military": false,
    "registrant": {
        "names": [
            "FYLDEN RUSSELL"
        ]
    },
    "registration": "C6-FJR",
    "source": "bs-caa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 0A80C2 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "BOEING",
        "manufacturer_model": "BOEING 737-700",
        "model": "B737-752",
        "serial_number": "30038",
        "type_designator": "B737",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "0A80C2",
    "military": false,
    "registrant": {
        "names": [
            "BAHAMASAIR HOLDINGS LTD."
        ]
    },
    "registration": "C6-BFZ",
    "source": "bs-caa"
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

Published once, at the end of a run, to `SkyFollower/runner/bs-caa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_bs_caa_{name}/config` for each of the three stats above.
