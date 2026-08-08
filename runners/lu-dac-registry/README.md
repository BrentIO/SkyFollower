# Luxembourg 🇱🇺 DAC Registry

| | |
|---|---|
| **Name** | `lu-dac-registry` |
| **Country** | Luxembourg |
| **Registration prefix** | `LX-` |
| **Data source** | https://dac.gouvernement.lu/en/administration/departements/navigabilite/immatriculation-aeronefs/releve-immatriculations.html |
| **Format** | PDF (index page scraped to discover the current register PDF URL) |
| **Run frequency** | Weekly (Wednesday, 10:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — the Luxembourg DAC register does not publish ICAO hex (Mode S) addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The index page is scraped for a link matching `dam-assets`/`relev*.pdf`
(handling both URL-encoded and plain forms of the accented French/Luxembourgish
path) to discover the current register PDF. The PDF is parsed by grouping
`pdfplumber.extract_words()` output into rows by `top` coordinate (5-point
tolerance) and then into named columns by fixed x0 boundaries, since
`pdfplumber.extract_table()` does not correctly detect all columns in this
PDF. Rows whose `immat` column starts with `LX-` begin a new record;
subsequent rows without an `LX-` value are treated as continuation lines and
appended to the current record's fields (handling multi-line cells).
Only `proprietaire` (owner) values are imported into `registrant.names`;
`exploitant` (operator) is present in source but not read. Owner values
matching known privacy-placeholder strings (`PROPRIÉTAIRE PRIVÉ`,
`COPROPRIÉTÉ`) are omitted from the names list. Every
written record explicitly sets `military: false` — this register is
exclusively civil, and the explicit value ensures a stale `military: true`
flag (from Mictronics or a prior record on a reused hex) is corrected on
re-registration.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| immat (Registration) | ✅ | LX-prefix; used as the Mictronics lookup key |
| constructeur (Manufacturer) | ✅ | → `aircraft.manufacturer` |
| type (Type) | ✅ | → `aircraft.model` |
| sn (Serial Number) | ✅ | → `aircraft.serial_number` |
| exploitant (Operator) | ❌ | Present in source; not read by this runner |
| proprietaire (Owner) | ✅ | → `registrant.names[]`; privacy placeholders (e.g. `PROPRIÉTAIRE PRIVÉ`, `COPROPRIÉTÉ`) are filtered, not stored |

See `specs/data-dictionary.yaml` (`lu-dac-registry` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 4D0310 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "CESSNA AIRCRAFT COMPANY",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "172S Skyhawk SP 172S10739",
        "serial_number": "AÉRO-SPORT DE LUXEMBOURG",
        "type_designator": "C172"
    },
    "data_sources": [
        "mictronics",
        "lu-dac-registry"
    ],
    "icao_hex": "4D0310",
    "military": false,
    "registrant": {
        "names": [
            "DU GRAND-DUCHÉ AÉRO-SPORT A.S.B.L. LUXEMBOURG"
        ]
    },
    "registration": "LX-AIE"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 4D0114 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "BOEING COMPANY, THE",
        "manufacturer_model": "BOEING 747-8",
        "model": "B747-8R7F 38078",
        "serial_number": "COPROPRIÉTÉ",
        "type_designator": "B748"
    },
    "data_sources": [
        "mictronics",
        "lu-dac-registry"
    ],
    "icao_hex": "4D0114",
    "military": false,
    "registrant": {
        "names": [
            "CARGOLUX"
        ]
    },
    "registration": "LX-VCK"
}
```

## Configuration

See [Data Runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md#configuration) for the full list of environment variables every runner reads. `REDIS_TTL_DAYS` applies to each `aircraft:registry:{icao_hex}` key written by this runner.

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/lu-dac-registry/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `Success` or `Failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_lu_dac_registry_{name}/config` for each of the three stats above.
