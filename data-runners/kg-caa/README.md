# kg-caa

| | |
|---|---|
| **Country** | Kyrgyzstan |
| **Registration prefix** | `EX-` |
| **Data source** | https://caa.kg/en/node/46 |
| **Format** | Static HTML table |
| **Run frequency** | Weekly (Tuesday, 17:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The register is a single static HTML page containing one table with bilingual Russian/English headers and rowspan-merged cells for the Operator/Owner and Type columns (a value spans multiple registration rows). The table is walked row-by-row, expanding rowspans into a uniform grid so every row carries a full set of column values regardless of which rows the source PDF/HTML visually merged. Registration and serial number cells are stripped of whitespace and passed through a homoglyph translation table, since some cells use Cyrillic look-alike characters (`Е`, `Х`) in place of Latin `E`/`X` in the `EX-` prefix. Manufacture dates appear either as `DD.MM.YYYY` or as a Russian/English month name plus year (e.g. `Март 2010`), both normalized to ISO date strings.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| № п/п | ❌ | Always empty in data rows; a visual row-number placeholder, not stored |
| Эксплуатант/Operator (Собственник/Owner) | ✅ | Rowspan-expanded; → `registrant.names[0]` |
| Тип ВС/Type of Aircraft | ✅ | Rowspan-expanded; → `aircraft.model` |
| Регистрац. номер/Registration | ✅ | `EX-` prefix; Cyrillic homoglyphs normalized; used as the Mictronics lookup key |
| Дата регистрации/Date of Registration | ❌ | Parsed but not stored |
| Серийный №/Serial Number | ✅ | → `aircraft.serial_number` |
| Дата производства/Date of Manufacture | ✅ | → `aircraft.manufactured_date` (`DD.MM.YYYY` → `YYYY-MM-DD`; `Month YYYY` → `YYYY-01-01`) |

See `specs/data-dictionary.yaml` (`kg-caa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 6010AF | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2010-03-17",
        "manufacturer": "DIAMOND",
        "manufacturer_model": "DIAMOND DA-42 Guardian",
        "model": "Diamond DA-42M-NG",
        "serial_number": "42MN003",
        "type_designator": "DA42",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "6010AF",
    "military": false,
    "registrant": {
        "names": [
            "Sky KG Airlines"
        ]
    },
    "registration": "EX-11001",
    "source": "kg-caa"
}
```

Note: this registry has no GA singles or Pipers, so the "small aircraft" example above is a Diamond DA42M-NG — a twin-engine trainer/utility aircraft — rather than a 150/172.

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 6010E2 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "1987-04-02",
        "manufacturer": "BOEING",
        "manufacturer_model": "BOEING 747-200",
        "model": "Boeing 747-222B",
        "serial_number": "23737",
        "type_designator": "B742",
        "wake_turbulence_category": "Heavy"
    },
    "icao_hex": "6010E2",
    "military": false,
    "registrant": {
        "names": [
            "Aerostan"
        ]
    },
    "registration": "EX-47001",
    "source": "kg-caa"
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

Published once, at the end of a run, to `SkyFollower/runner/kg-caa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_kg_caa_{name}/config` for each of the three stats above.
