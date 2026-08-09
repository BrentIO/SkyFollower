# Suriname 🇸🇷 CASAS Registry

| | |
|---|---|
| **Name** | `sr-casas-registry` |
| **Country** | Suriname |
| **Registration prefix** | `PZ-` |
| **Data source** | https://www.casas.sr/registry/ |
| **Format** | Xlsx (discovered via index page) |
| **Run frequency** | Weekly (pick a day/time — see `docker-compose.core.yaml`) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; `PZ-` registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The CASAS registry page links two xlsx files — the Civil Aircraft Register and a separate UAS/drone registry — so the index page is scraped for a link matching `REGISTER-{MM}-{YYYY}.xlsx` specifically (the UAS file is named `CASAS-UAS-REGISTRY-{Mon}-{YYYY}.xlsx` and doesn't match). The xlsx has a clean named header row, so columns are read by name via `dict(zip(headers, row))` rather than by fixed position. The full registration mark isn't a single column — it's `NATIONALITY MARK OR COMMON MARK` (always `PZ` in the source, but read dynamically rather than hardcoded) concatenated with `REGISTRATION MARK` (e.g. `PZ` + `UBD` → `PZ-UBD`).

The source has both a `MAKE` and a `MANUFACTURER` column that differ on about half of all rows (e.g. `MAKE=GRUMMAN`, `MANUFACTURER=SCHWEIZER` for Grumman Ag-Cat airframes later built under license by Schweizer) — `shared/models.py`'s `AircraftRecord` has only one `manufacturer` field, so `MAKE` (the type's brand) is used and `MANUFACTURER` is dropped. The source also has a separate `OPERATOR (*)` column alongside `OWNER_NAME` — matching the established convention across every other runner in this repo with both an owner and an operator column (e.g. `lu-dac-registry`, `sk-nsat-registry`), only owner becomes `registrant.names`; operator is present in source but intentionally not read, since no runner in this codebase writes `AircraftRecord`'s top-level `operator` field. `MODEL` and `SERIES` are concatenated when both are present (e.g. `G164` + `B` → `G164B`); about 14% of rows have no `SERIES` value. Every written record explicitly sets `military: false` — this register is exclusively civil.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `#` | ❌ | Row sequence number |
| `MAKE` | ✅ | → `aircraft.manufacturer` |
| `MODEL` | ✅ | → `aircraft.model` (trailing `*` stripped — one row in the source has `G164*`, apparently a stray marker); `SERIES` appended when present |
| `SERIES` | ✅ | Appended to `aircraft.model`; absent on ~14% of rows |
| `MANUFACTURER` | ❌ | Differs from `MAKE` on ~half of rows (licensed-builder detail); `MAKE` wins since there's only one manufacturer field |
| `SERIAL_NUMBER` | ✅ | → `aircraft.serial_number`; source has mixed string/numeric cell types, coerced to string |
| `NATIONALITY MARK OR COMMON MARK` | ✅ | Always `PZ` in the source; used as the registration prefix |
| `REGISTRATION MARK` | ✅ | Registration suffix; concatenated with the nationality mark as the lookup key |
| `OWNER_NAME` | ✅ | → `registrant.names` |
| `OPERATOR (*)` | ❌ | Present in source but not read — see "How it works" above |

See `specs/data-dictionary.yaml` (`sr-casas-registry` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 3A1234 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "GRUMMAN",
        "manufacturer_model": "Grumman G-164 Ag Cat",
        "model": "G164B",
        "serial_number": "185B",
        "type_designator": "AGCT"
    },
    "data_sources": [
        "mictronics",
        "sr-casas-registry"
    ],
    "icao_hex": "3A1234",
    "military": false,
    "registrant": {
        "names": [
            "SURINAM SKY FARMERS"
        ]
    },
    "registration": "PZ-UBD"
}
```

(`type_designator` and `manufacturer_model` come from a Mictronics record on the same hex; `manufacturer`, `model`, `serial_number`, and `registrant.names` above are this runner's contribution — note `manufacturer` (`GRUMMAN`, this runner's `MAKE` value) wins over Mictronics' own `manufacturer` field on conflict, per `merge_aircraft.lua`'s mictronics → registry → livery priority order.)

## Configuration

See [Data Runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md#configuration) for the full list of environment variables every runner reads. `REDIS_TTL_DAYS` applies to each `aircraft:registry:{icao_hex}` key written by this runner.

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/sr-casas-registry/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `100` | Integer as string |
| `last_run_at` | e.g. `2026-08-08T21:52:58.718000+00:00` | ISO 8601 UTC |
| `last_run_status` | `Success` or `Failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_sr_casas_registry_{name}/config` for each of the three stats above.
