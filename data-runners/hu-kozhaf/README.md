# hu-kozhaf

| | |
|---|---|
| **Country** | Hungary |
| **Registration prefix** | `HA-` |
| **Data source** | https://kozlekedesihatosag.kormany.hu/hu/dokumentum/104604 |
| **Format** | PDF (date-stamped filename with rotating query parameters, discovered via index page) |
| **Run frequency** | Weekly (Tuesday, 12:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The KoZHAF (National Transport Authority) index page is scraped with a browser `User-Agent` for a link containing the stable path fragment `/documents/66238/342548/` and `download=true`, since the actual filename and query string change on every publish. The PDF is parsed page-by-page with `pdfplumber`'s `extract_table()`; because the table header is bilingual (Hungarian/English) and unreliable, columns are identified by fixed position instead. Registration marks in the source PDF have a stray space after the hyphen (e.g. `HA- GZQ`), which is normalized to `HA-GZQ` before the Mictronics lookup. Serial number is whitespace-collapsed like model, owner name, and owner address already are, since `pdfplumber` can represent a wrapped cell's text with an embedded newline rather than a space. Every written record explicitly sets `military: false` — this register is exclusively civil, and the explicit value ensures a stale `military: true` flag (from Mictronics or a prior record on a reused hex) is corrected on re-registration.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Registration | ✅ | `HA-` prefix; stray space after hyphen stripped; used as the Mictronics lookup key |
| Type/Model | ✅ | → `aircraft.model` |
| Serial Number | ✅ | → `aircraft.serial_number`; embedded newlines collapsed to a single space |
| Year | ✅ | → `aircraft.manufactured_date` (stored as `YYYY-01-01` when a valid 4-digit year) |
| Owner Name | ✅ | → `registrant.names[0]` |
| Owner Address | ✅ | → `registrant.street` |
| Operator Name | ❌ | Present in source; not read by this runner |
| Operator Address (Üzembentaró címe) | ❌ | Present in source; not read by this runner |
| Date of Registry (Lajstromba vétel) | ❌ | Present in source; not read by this runner |
| ARC Date of Expiry (ARC érv.vége) | ❌ | Present in source; not read by this runner |
| ARC Date of Issue (ARC érv.kezd.) | ❌ | Present in source; not read by this runner |

See `specs/data-dictionary.yaml` (`hu-kozhaf` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 470411 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "1977-01-01",
        "manufacturer": "CESSNA",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "CESSNA 172 K",
        "serial_number": "R1722578",
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "470411",
    "military": false,
    "registrant": {
        "names": [
            "KOVÁCS RÓBERT ÁKOS 50% / KOVÁCS RÓBERT ZOLTÁN 50 %"
        ],
        "street": "H-9025 GYŐR, RÁBA U. 28. / H-9025 GYŐR, RÁBA U. 20-22."
    },
    "registration": "HA-BOB",
    "source": "hu-kozhaf"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 471F44 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2021-01-01",
        "manufacturer": "AIRBUS",
        "manufacturer_model": "AIRBUS A-321neo",
        "model": "AIRBUS A321- 271NX",
        "serial_number": "10509",
        "type_designator": "A21N",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "471F44",
    "military": false,
    "registrant": {
        "names": [
            "INTERTRUST (SINGAPORE) LIMITED, as owner trustee"
        ],
        "street": "77 Robinson Road, #13-00, Robinson 77, SINGAPORE 068896, SINGAPORE"
    },
    "registration": "HA-LVW",
    "source": "hu-kozhaf"
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

Published once, at the end of a run, to `SkyFollower/runner/hu-kozhaf/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_hu_kozhaf_{name}/config` for each of the three stats above.
