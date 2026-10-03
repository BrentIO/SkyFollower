# Luxembourg 🇱🇺 DAC Registry

| | |
|---|---|
| **Name** | `lu-dac-registry` |
| **Country** | Luxembourg |
| **Registration prefix** | `LX-` |
| **Data source** | https://data.public.lu/en/datasets/releve-luxembourgeois-des-immatriculations/ (Luxembourg open-data portal, published by the Direction de l'Aviation Civile) |
| **Licence** | [Open Data Commons Attribution (`odc-by`)](https://opendatacommons.org/licenses/by/1-0/) — attribution to the Direction de l'Aviation Civile / data.public.lu is required |
| **Format** | PDF (downloaded through the portal's stable resource permalink, which redirects to the current monthly file) |
| **Run frequency** | Weekly (Wednesday, 10:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — the Luxembourg DAC register does not publish ICAO hex (Mode S) addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The register PDF is downloaded from the open-data portal's resource
permalink (`https://data.public.lu/fr/datasets/r/78d14c57-dee5-4903-9216-c33a0da06647`),
which answers with a redirect to the current file; the dated filename (e.g.
`releve-aeronefs-10-09-2026.pdf`) changes with each monthly update, but the
permalink does not. Any non-200 response fails the run with a clear error.
The PDF is parsed by grouping
`pdfplumber.extract_words()` output into rows by `top` coordinate (5-point
tolerance) and then into named columns by x0 boundaries derived from each page's header row
(each column starts just left of its header word; fixed fallback bounds are
used only if a page has no header row), since
`pdfplumber.extract_table()` does not correctly detect all columns in this
PDF. Header and page-footer lines are discarded, and the run fails rather
than writing wrong data if the parsed rows look mis-columned (fewer than half
of the serial numbers contain a numeral) or no rows are parsed. Rows whose `immat` column starts with `LX-` begin a new record;
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
        "model": "172S Skyhawk SP",
        "serial_number": "172S10739",
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
            "AÉRO-SPORT DU GRAND-DUCHÉ DE LUXEMBOURG A.S.B.L."
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
        "model": "B747-8R7F",
        "serial_number": "38078",
        "type_designator": "B748"
    },
    "data_sources": [
        "mictronics",
        "lu-dac-registry"
    ],
    "icao_hex": "4D0114",
    "military": false,
    "registration": "LX-VCK"
}
```

## Configuration

See [Data Runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md#configuration) for the full list of environment variables every runner reads. This runner writes `aircraft:registry:{icao_hex}` with a fixed 14-day TTL (`ENRICHMENT_TTL_SECONDS` in `shared/timing.py`).

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/lu-dac-registry/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `Success` or `Failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_lu_dac_registry_{name}/config` for each of the three stats above.
