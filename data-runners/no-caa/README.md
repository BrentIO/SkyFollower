# no-caa

| | |
|---|---|
| **Country** | Norway |
| **Registration prefix** | `LN-` |
| **Data source** | https://data.caa.no/nlr/norgesluftfartoyregister.json |
| **Format** | JSON API (fixed URL) |
| **Run frequency** | Weekly (Wednesday, 06:10 UTC) |
| **Depends on Mictronics for ICAO hex** | No — the register publishes its own ICAO 24-bit address (`Heksadesimal`) for every aircraft, so `icao_hex` is read directly from the source data. |

## How it works

The Norway CAA register is downloaded whole from a fixed URL as a single JSON payload (`{headers, data: [...]}`); no index page or scraping is involved. Each record's ICAO hex is pulled from the `Heksadesimal` field inside the `ICAO 24-bits adresse` array. Aircraft category (`Kategori`) and owner country (`Land`) are decoded via lookup tables — Norwegian names are used for Norway/Sweden/Denmark and English names for everything else — with unmapped values passed through as-is rather than dropped. Owners (`Eier(e)`) is an array that may hold multiple entries; the one flagged `Eier/Kontakt` supplies the registrant's address, while names are collected from every owner entry (contact first, then any others, de-duplicated).

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `ICAO 24-bits adresse` (`Heksadesimal`) | ✅ | ICAO hex; record is dropped entirely if missing |
| `ICAO 24-bits adresse` (`Desimal`) | ❌ | Present in source; not read by this runner |
| `ICAO 24-bits adresse` (`Binær`) | ❌ | Present in source; not read by this runner |
| `ICAO 24-bits adresse` (`Oktal`) | ❌ | Present in source; not read by this runner |
| `Registreringsmerke` | ✅ | → `registration` |
| `Kategori` | ✅ | → `aircraft.type`, decoded (Fly/Helikopter/Seilfly/Ballong → English) |
| `Produsent` | ✅ | → `aircraft.manufacturer` |
| `Type` | ✅ | → `aircraft.model` |
| `Serienummer` | ✅ | → `aircraft.serial_number` |
| `Byggeår` | ✅ | → `aircraft.manufactured_date` (4-digit year converted to ISO 8601 UTC) |
| `Registreringsdato` | ❌ | Present in source; not read by this runner |
| `Luftdyktighetskategori` | ❌ | Present in source; not read by this runner |
| `Startmasse MTOM` | ❌ | Present in source; not read by this runner |
| `Eier/Kontakt` (top-level string) | ❌ | Present in source; not read by this runner — distinct from the `Eier(e)` array's `Eier/Kontakt`-flagged entry, which is used |
| `Organisasjonsnummer eier/kontakt` | ❌ | Present in source; not read by this runner |
| `Operatør` | ❌ | Present in source; not read by this runner |
| `Organisasjonsnummer operatør` | ❌ | Present in source; not read by this runner |
| `Eier(e)` → `Eier type` | ✅ | Used to select the `Eier/Kontakt` entry for address fields |
| `Eier(e)` → `Eier siden` | ❌ | Present in source; not read by this runner |
| `Eier(e)` → `Navn` | ✅ | → `registrant.names[]` (contact first, then other owners, de-duplicated) |
| `Eier(e)` → `Gateadresse` | ✅ | → `registrant.street[]` |
| `Eier(e)` → `Poststed` | ✅ | → `registrant.city` |
| `Eier(e)` → `Postnummer` | ✅ | → `registrant.postal_code` |
| `Eier(e)` → `Land` | ✅ | → `registrant.country`, decoded to ISO 3166-1 alpha-2 where known |
| `Eier(e)` → `Organisasjonsnummer` | ❌ | Present in source; not read by this runner |

See `specs/data-dictionary.yaml` (`no-caa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 478E11 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2019-01-01T00:00:00Z",
        "manufacturer": "Textron Aviation Inc.",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "172S",
        "serial_number": "172S12244",
        "type": "Airplane",
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "478E11",
    "military": false,
    "registrant": {
        "city": "ARENDAL",
        "country": "NO",
        "names": [
            "OSM AVIATION AIRTECH AS"
        ],
        "postal_code": "4836",
        "street": [
            "Peder Thomassons gate 12"
        ]
    },
    "registration": "LN-AZE",
    "source": "no-caa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 4791AC | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2011-01-01T00:00:00Z",
        "manufacturer": "The Boeing Company",
        "manufacturer_model": "BOEING 737-800",
        "model": "737-86N",
        "serial_number": "39403",
        "type": "Airplane",
        "type_designator": "B738",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "4791AC",
    "military": false,
    "registrant": {
        "city": "Co. Clare",
        "country": "IE",
        "names": [
            "Celestial Ex-Im Trading 5 Limited"
        ],
        "postal_code": "V14 AN29",
        "street": [
            "Aviation House, Shannon"
        ]
    },
    "registration": "LN-NIQ",
    "source": "no-caa"
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

Published once, at the end of a run, to `SkyFollower/runner/no-caa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_no_caa_{name}/config` for each of the three stats above.
