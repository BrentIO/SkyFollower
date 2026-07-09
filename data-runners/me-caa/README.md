# me-caa

| | |
|---|---|
| **Country** | Montenegro |
| **Registration prefix** | `4O-` |
| **Data source** | https://www.caa.me/en/registri?field_ispisan_iz_registra_tid=157 |
| **Format** | HTML table (paginated list, pre-filtered to active registrations only) + per-aircraft HTML detail pages, both at fixed URLs |
| **Run frequency** | Weekly (Tuesday, 13:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; `4O-` registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The registry list URL already includes `field_ispisan_iz_registra_tid=157`, which
restricts results to active registrations, so no deregistration filtering is
needed in code — pages are simply paginated until an empty page is returned.
For each `4O-` registration found, a separate detail page is fetched (by
lower-casing the registration into a URL slug) to collect enrichment fields
not present on the list page. Detail pages are retried up to twice with
exponential backoff on connection errors, timeouts, `403`/`429`, and `5xx`
responses. Detail-page field extraction tries three strategies in order —
Drupal `field-label`/`field-item` div pairs, then generic `<table>` rows, then
`<dl>` definition lists — falling back to the next only if the previous finds
nothing. The `Category` field (e.g. `"Transport – Airplane"`) is decoded by
splitting on the first em-dash or hyphen to derive `aircraft.type`. The
combined `Zip code, town` field is split via regex into `postal_code` (leading
digits) and `city` (remainder).

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Registarska oznaka (Registration) | ✅ | 4O-prefix; used as the Mictronics lookup key |
| Redni broj u registru (sequence) | ❌ | Parsed but not stored |
| Ime (operator name, list page) | ❌ | Parsed but not stored; superseded by the detail page's `Name` field |
| Tip (short type code, list page) | ✅ | → `aircraft.model`, only if the detail page's `Aircraft model/type` is empty |
| Aircraft model/type (detail) | ✅ | → `aircraft.model`; preferred over the list page's `Tip` |
| Manufacturer (detail) | ✅ | → `aircraft.manufacturer` |
| Category (detail) | ✅ | → `aircraft.type`; decoded from `"<category> – <type>"` |
| S/N (detail) | ✅ | → `aircraft.serial_number` |
| Year Built (detail) | ✅ | → `aircraft.manufactured_date` (`YYYY-01-01`), only if a 4-digit year |
| ARC expiry date (detail) | ❌ | Parsed but not stored |
| Name (operator, detail) | ✅ | → `registrant.names[0]` |
| Address (operator, detail) | ✅ | → `registrant.street` |
| Zip code, town (operator, detail) | ✅ | Split into `registrant.postal_code` and `registrant.city` |
| Country (operator, detail) | ✅ | → `registrant.country` |

See `specs/data-dictionary.yaml` (`me-caa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 516038 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "1980-01-01",
        "manufacturer": "Textron Aviation Inc.",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "Cessna 172N",
        "serial_number": "70050",
        "type": "Airplane",
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "516038",
    "military": false,
    "registrant": {
        "city": "Podgorica",
        "country": "Montenegro",
        "names": [
            "Airways Scenic & Charter"
        ],
        "postal_code": "81000",
        "street": "Sportski aerodrom Ćemovsko Polje"
    },
    "registration": "4O-VUK",
    "source": "me-caa"
}
```

Montenegro's registry has no Boeing or Airbus airframes, so the larger example
below is an Embraer E195 instead of a typical widebody airliner:

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 516097 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2009-01-01",
        "manufacturer": "Embraer",
        "manufacturer_model": "EMBRAER ERJ-190-200",
        "model": "ERJ 190-200 LR",
        "serial_number": "19000283",
        "type": "Airplane",
        "type_designator": "E195",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "516097",
    "military": false,
    "registrant": {
        "city": "Podgorica",
        "country": "Montenegro",
        "names": [
            "ToMontenegro DOO"
        ],
        "postal_code": "81000",
        "street": "Bulevar Džordža Vašingtona broj 98"
    },
    "registration": "4O-AOB",
    "source": "me-caa"
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

Published once, at the end of a run, to `SkyFollower/runner/me-caa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_me_caa_{name}/config` for each of the three stats above.
