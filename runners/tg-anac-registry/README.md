# Togo 🇹🇬 ANAC Registry

| | |
|---|---|
| **Name** | `tg-anac-registry` |
| **Country** | Togo |
| **Registration prefix** | `5V-` |
| **Data source** | http://www.anac-togo.tg/espace-professionnel/aeronefs/consultation-du-registre-dimmatriculation/ |
| **Format** | HTML table (single request, no file to download) |
| **Run frequency** | Weekly (pick a day/time — see `docker-compose.core.yaml`) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; `5V-` registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The register is embedded as a single HTML `<table>` on the page, so there's no discovery step — one request, parsed with `BeautifulSoup`. Despite its header, the `type` column holds model designations (e.g. `DC8 62`, `PA 31T`), not the `aircraft.type` category vocabulary (Airplane/Rotorcraft/etc.) — the source's own column naming is misleading, so this column maps to `aircraft.model`.

The table's `Radiation` column (`OUI`/`NON`, "deregistered?") is a real, majority filter, not an edge case: at check time only 13 of 46 rows (28%) were `Radiation = NON` (active); the other 32 (70%) were `OUI` (deregistered) and are dropped. Only `NON` rows are written. One row in the live data has `(Radié)` appended to every cell including the registration mark itself (a data-entry artifact, not the normal `OUI`/`NON` encoding) — this gets rejected by the registration-mark regex before the `Radiation` check even runs, so it's excluded regardless. Registration marks are validated against `^5V-[A-Z0-9]{2,6}$`, verified against every real row in the live table (both active and deregistered) before choosing that width. Every written record explicitly sets `military: false` — this register is exclusively civil.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `N° Ordre` | ❌ | Sequence number |
| `type` | ✅ | → `aircraft.model` — despite the header name, holds model designations, not the `aircraft.type` category |
| `Immatriculation` | ✅ | `5V-`-prefix; used as the Mictronics lookup key |
| `Constructeur` | ✅ | → `aircraft.manufacturer` |
| `N° de serie` | ✅ | → `aircraft.serial_number` |
| `Radiation` | filter only | `OUI` = deregistered (majority of rows), `NON` = active (kept) |
| `Nom propriétaire` | ✅ | → `registrant.names`; blank on many rows |
| `Adresse propriétaire` | ✅ | → `registrant.street`, comma-split; blank on many rows; a few rows have a phone number here instead of an address (source data quirk, stored as-is) |

See `specs/data-dictionary.yaml` (`tg-anac-registry` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 500A1B | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "PIPER AIRCRAFT",
        "manufacturer_model": "Piper PA-31T Cheyenne",
        "model": "PA 31T",
        "serial_number": "7820013",
        "type_designator": "PA31"
    },
    "data_sources": [
        "mictronics",
        "tg-anac-registry"
    ],
    "icao_hex": "500A1B",
    "military": false,
    "registrant": {
        "names": [
            "Mr SITTERLIN"
        ],
        "street": [
            "BP 10019 Lomé TOGO"
        ]
    },
    "registration": "5V-TPT"
}
```

(`type_designator` and `manufacturer_model` come from a Mictronics record on the same hex; `manufacturer`, `model`, `serial_number`, and `registrant` above are this runner's contribution — note `manufacturer` (`PIPER AIRCRAFT`, this runner's value) wins over Mictronics' own `manufacturer` field on conflict, per `merge_aircraft.lua`'s mictronics → registry → livery priority order.)

## Configuration

See [Data Runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md#configuration) for the full list of environment variables every runner reads. `REDIS_TTL_DAYS` applies to each `aircraft:registry:{icao_hex}` key written by this runner.

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/tg-anac-registry/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `13` | Integer as string |
| `last_run_at` | e.g. `2026-08-08T22:21:47.388000+00:00` | ISO 8601 UTC |
| `last_run_status` | `Success` or `Failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_tg_anac_registry_{name}/config` for each of the three stats above.
