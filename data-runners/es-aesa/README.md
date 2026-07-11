# es-aesa

| | |
|---|---|
| **Country** | Spain |
| **Registration prefix** | `EC-` |
| **Data source** | https://www.seguridadaerea.gob.es/sites/default/files/aeronaves_inscritas.pdf |
| **Format** | PDF (fixed URL) |
| **Run frequency** | Weekly (Thursday, 08:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex (Mode S) addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The AESA (Agencia Estatal de Seguridad Aérea) register PDF is downloaded from a
fixed URL and parsed with `pdfplumber`'s `extract_tables()` — unlike some other
PDF-based runners in this repo, table detection works reliably here, so no
manual x-position column grouping is needed. The first row of each detected
table is treated as the header; rows are zipped into dicts keyed by header
text, and only rows whose first cell starts with `EC-` are kept. A quirk of
this PDF: `pdfplumber` sometimes extracts Unicode hyphen variants (en-dash,
em-dash, non-breaking hyphen, etc.) instead of ASCII `-`, so every cell value
is translated through a Unicode-hyphen-to-ASCII table before use — this
matters both for the `EC-` prefix check and for the RediSearch lookup key.
Every written record explicitly sets `military: false` — this register is
exclusively civil, and the explicit value ensures a stale `military: true`
flag (from Mictronics or a prior record on a reused hex) is corrected on
re-registration.
The Spanish `Clase` (aircraft class) column is decoded through a fixed
Spanish→English map (e.g. `AVION` → `Airplane`). No registrant data is
available in this register at all.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Matrícula | ✅ | EC-prefix; used as the Mictronics lookup key |
| Fecha matric. | ❌ | Present in source; not read by this runner |
| Fabricante | ✅ | → `aircraft.manufacturer` |
| Modelo | ✅ | → `aircraft.model` |
| Nº serie | ✅ | → `aircraft.serial_number`; `NO DISPONIBLE` placeholder is filtered, not stored |
| Año cons. | ✅ | → `aircraft.manufactured_date` (as `YYYY-01-01`); sentinel year `1900` is filtered, not stored |
| Marca Motor | ✅ | → `aircraft.powerplant.manufacturer` |
| Modelo Motor | ✅ | → `aircraft.powerplant.model` |
| Nº mot. | ✅ | → `aircraft.powerplant.count` |
| Clase | ✅ | Decoded via a Spanish→English map → `aircraft.type` |

See `specs/data-dictionary.yaml` (`es-aesa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 3471CD | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "1979-01-01",
        "manufacturer": "CESSNA AIRCRAFT COMPANY",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "172N",
        "powerplant": {
            "count": 1,
            "manufacturer": "LYCOMING",
            "model": "O-320-H2AD"
        },
        "serial_number": "17273683",
        "type": "Airplane",
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "3471CD",
    "military": false,
    "registration": "EC-GNS",
    "source": "es-aesa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 342107 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2004-01-01",
        "manufacturer": "AIRBUS S.A.S.",
        "manufacturer_model": "AIRBUS A-321",
        "model": "A321-213",
        "powerplant": {
            "count": 2,
            "manufacturer": "C.F.M",
            "model": "CFM56-5B2/P"
        },
        "serial_number": "2357",
        "type": "Airplane",
        "type_designator": "A321",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "342107",
    "military": false,
    "registration": "EC-JDM",
    "source": "es-aesa"
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

Published once, at the end of a run, to `SkyFollower/runner/es-aesa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_es_aesa_{name}/config` for each of the three stats above.
