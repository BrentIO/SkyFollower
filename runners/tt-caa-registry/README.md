# Trinidad and Tobago 🇹🇹 CAA Registry

| | |
|---|---|
| **Name** | `tt-caa-registry` |
| **Country** | Trinidad and Tobago |
| **Registration prefix** | `9Y-` |
| **Data source** | http://caa.gov.tt/aircraft-on-ttcaa-register/ |
| **Format** | PDF (discovered via index page) |
| **Run frequency** | Weekly (pick a day/time — see `docker-compose.core.yaml`) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; `9Y-` registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The TTCAA index page is scraped for a link matching `AIRCRAFT-ON-TTCAA-REGISTER*.pdf`, then that PDF is downloaded and every page's table parsed with `pdfplumber`. The header row repeats on every page (an Excel-generated PDF artifact), so rows matching the header exactly are skipped rather than assuming the header only appears once. Registration marks are validated against `^9Y-[A-Z0-9]{2,6}$` — a naive `{2,4}` suffix width would silently drop the register's government helicopters, which have a 5-character suffix (`9Y-AG311` through `9Y-AG314`).

This register has no separate owner column — only `NAME OF OPERATOR` — so that column fills the registrant-identity role directly, the same approach `sg-caas-registry` takes for its own operator-only source. `MAKE & MODEL` is a single combined free-text field with no reliable delimiter to split manufacturer from model (e.g. is "Augusta Westland" in `Augusta Westland AW139` the make, or part of the model string?), so it's stored whole as `aircraft.model`, leaving `aircraft.manufacturer` unset — matching `bs-caa-registry`'s approach to the same situation. The address column is comma-split into `registrant.street` parts. Every written record explicitly sets `military: false` — this register is exclusively civil.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `REGISTRATION` | ✅ | `9Y-`-prefix; used as the Mictronics lookup key |
| `MAKE & MODEL` | ✅ | → `aircraft.model` (combined string, not split) |
| `NAME OF OPERATOR` | ✅ | → `registrant.names` — no separate owner column exists in this source |
| `ADDRESS OF OPERATOR` | ✅ | → `registrant.street`, comma-split; may contain embedded newlines, collapsed to single space before splitting |

See `specs/data-dictionary.yaml` (`tt-caa-registry` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 C6A1B2 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "Leonardo",
        "manufacturer_model": "Leonardo AW139",
        "model": "Augusta Westland AW139",
        "type_designator": "AW139"
    },
    "data_sources": [
        "mictronics",
        "tt-caa-registry"
    ],
    "icao_hex": "C6A1B2",
    "military": false,
    "registrant": {
        "names": [
            "Bristow Caribbean Limited"
        ],
        "street": [
            "Hangar #4",
            "Piarco Int’nal Airport",
            "Trinidad"
        ]
    },
    "registration": "9Y-ENT"
}
```

(`type_designator`, `manufacturer`, and `manufacturer_model` come from a Mictronics record on the same hex; `model` and `registrant` above are this runner's contribution — note `aircraft.model` keeps the combined "Augusta Westland AW139" string, distinct from Mictronics' own cleaner `manufacturer`/`manufacturer_model` fields.)

## Configuration

See [Data Runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md#configuration) for the full list of environment variables every runner reads. `REDIS_TTL_DAYS` applies to each `aircraft:registry:{icao_hex}` key written by this runner.

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/tt-caa-registry/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `50` | Integer as string |
| `last_run_at` | e.g. `2026-08-08T22:08:56.301000+00:00` | ISO 8601 UTC |
| `last_run_status` | `Success` or `Failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_tt_caa_registry_{name}/config` for each of the three stats above.
