# Bosnia and Herzegovina 🇧🇦 BHDCA Registry

| | |
|---|---|
| **Name** | `ba-bhdca-registry` |
| **Country** | Bosnia and Herzegovina |
| **Registration prefix** | `E7-` |
| **Data source** | http://www.bhdca.gov.ba/index.php/en/regulations-and-areas/airworthiness |
| **Format** | PDF (discovered via index page) |
| **Run frequency** | Weekly (Sunday, 06:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; `E7-` registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The BHDCA airworthiness page is scraped for a link matching `Aircraft%20Register` (case-insensitive), then that PDF — "BiH Aircraft Register - extract" — is downloaded and every page's table parsed with `pdfplumber`. The register has no header row that survives extraction cleanly on every page, so rows are validated instead by matching the registration column against `^E7-[A-Z0-9]{2,6}$`; anything that doesn't match (the header row, blank rows) is dropped. A handful of rows in the source PDF have a stray space after the hyphen (e.g. `E7- NEL` instead of `E7-NEL`) — this is normalized before validation so those rows aren't silently lost. Registrations are resolved to `icao_hex` in batches of 100 via RediSearch against the Mictronics index. Every written record explicitly sets `military: false` — this register is exclusively civil.

The source PDF's own header row has a typo — "Manifacturer" — which shows up unmodified in `specs/data-dictionary.yaml`'s documentation of this source; it is not a transcription error here.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| (unlabeled row number) | ❌ | Not read |
| Registrаtion mark | ✅ | `E7-`-prefix; used as the Mictronics lookup key |
| Designation | ✅ | → `aircraft.model` |
| Manifacturer (sic) | ✅ | → `aircraft.manufacturer` |
| Serial number | ✅ | → `aircraft.serial_number` |
| Owner | ✅ | → `registrant.names` |
| Registration date | ❌ | Mixed date formats seen (`16.05.16`, `26.06.00.` with trailing period); not worth normalizing for a field we don't store |

See `specs/data-dictionary.yaml` (`ba-bhdca-registry` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 4A1234 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "AutoGyro GmbH",
        "manufacturer_model": "AutoGyro MTO-Sport",
        "model": "MTO-Sport",
        "serial_number": "M01333",
        "type_designator": "GYRO"
    },
    "data_sources": [
        "mictronics",
        "ba-bhdca-registry"
    ],
    "icao_hex": "4A1234",
    "military": false,
    "registrant": {
        "names": [
            "Auto Gyro Adriatic d.o.o."
        ]
    },
    "registration": "E7-D119"
}
```

(`type_designator` and the pre-registry `manufacturer`/`manufacturer_model` values come from a Mictronics record on the same hex; `manufacturer`, `model`, `serial_number`, and `registrant.names` above are this runner's contribution, and win over Mictronics' `manufacturer` on a field conflict per `merge_aircraft.lua`'s mictronics → registry → livery priority order.)

## Configuration

See [Data Runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md#configuration) for the full list of environment variables every runner reads. This runner writes `aircraft:registry:{icao_hex}` with a fixed 14-day TTL (`ENRICHMENT_TTL_SECONDS` in `shared/timing.py`).

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/ba-bhdca-registry/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `103` | Integer as string |
| `last_run_at` | e.g. `2026-08-08T17:49:51.398000+00:00` | ISO 8601 UTC |
| `last_run_status` | `Success` or `Failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_ba_bhdca_registry_{name}/config` for each of the three stats above.
