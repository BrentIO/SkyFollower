# lu-dac

| | |
|---|---|
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
Registrant/operator values matching known privacy-placeholder strings
(`EXPLOITANT PRIVÉ`, `PROPRIÉTAIRE PRIVÉ`, `COPROPRIÉTÉ`) are omitted from the
names list, and a duplicate operator/owner value is not repeated.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| immat (Registration) | ✅ | LX-prefix; used as the Mictronics lookup key |
| constructeur (Manufacturer) | ✅ | → `aircraft.manufacturer` |
| type (Type) | ✅ | → `aircraft.model` |
| sn (Serial Number) | ✅ | → `aircraft.serial_number` |
| exploitant (Operator) | ✅ | → `registrant.names[]`; privacy placeholders (e.g. `EXPLOITANT PRIVÉ`) are filtered, not stored |
| proprietaire (Owner) | ✅ | → `registrant.names[]`; privacy placeholders (e.g. `PROPRIÉTAIRE PRIVÉ`, `COPROPRIÉTÉ`) are filtered, not stored; omitted if identical to `exploitant` |

See `specs/data-dictionary.yaml` (`lu-dac` entry) for full column semantics and cross-source schema notes.

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
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "4D0310",
    "military": false,
    "registrant": {
        "names": [
            "AÉRO-SPORT DU GRAND-DUCHÉ DE LUXEMBOURG A.S.B.L."
        ]
    },
    "registration": "LX-AIE",
    "source": "lu-dac"
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
        "type_designator": "B748",
        "wake_turbulence_category": "Heavy"
    },
    "icao_hex": "4D0114",
    "military": false,
    "registrant": {
        "names": [
            "CARGOLUX AIRLINES INTERNATIONAL S.A."
        ]
    },
    "registration": "LX-VCK",
    "source": "lu-dac"
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

Published once, at the end of a run, to `SkyFollower/runner/lu-dac/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_lu_dac_{name}/config` for each of the three stats above.
