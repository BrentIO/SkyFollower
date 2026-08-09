# Sri Lanka 🇱🇰 CAA Registry

| | |
|---|---|
| **Name** | `lk-caa-registry` |
| **Country** | Sri Lanka |
| **Registration prefix** | `4R-` |
| **Data source** | https://www.caa.lk/en/downloads/sl-aircraft-register |
| **Format** | PDF (URL discovered by scraping the index page for the register link) |
| **Run frequency** | Weekly (Monday, 13:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The Sri Lanka CAA downloads index page is scraped for an anchor tag whose href ends in `.pdf` and whose link text contains "Civil Aircraft Registered", which is followed to download the current register PDF. The PDF's path and filename are both date-encoded (e.g. `2026_March/1_Civil_aircraft_registered_in_sri_lanka_as_at_11032026.pdf`) and change on every republish, so matching by link text rather than URL pattern is what survives the source's own churn. TLS verification is disabled — the server serves only its leaf certificate, without the intermediate needed to build a trust chain from a standard root store, so every compliant TLS client fails to verify it as-is (same situation as `cy-dca-registry`/`hu-kozhaf-registry`/`rs-cad-registry`).

Each page is parsed with `pdfplumber`'s built-in table extraction (`extract_table()`). A handful of "Hot Air Balloon" entries have a two-line `Make` cell (e.g. `CAMERON - Hot Air\nBalloon`); pdfplumber's table extraction splits the wrapped second line into its own otherwise-empty row (`Ref.No`, `Model No.`, `Registration`, `Operator` all blank). These are filtered out by requiring a non-empty, pattern-matching `Registration` cell — they are a table-extraction artifact, not additional aircraft. A few registration marks also carry a stray space after the hyphen in the source PDF (e.g. `4R- MDA`); internal whitespace is stripped from the registration value before it's regex-validated. Every written record explicitly sets `military: false` — this register is exclusively civil, and the explicit value ensures a stale `military: true` flag (from Mictronics or a prior record on a reused hex) is corrected on re-registration.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Ref.No | ❌ | Parsed but not stored (sequence number) |
| Make | ✅ | → `aircraft.manufacturer` |
| Model No. | ✅ | → `aircraft.model` |
| Registration | ✅ | `4R-`-prefixed → used as the Mictronics lookup key |
| Operator | ✅ | → `registrant.names[0]` — this register has no distinct owner column, only the operator, so that name fills the registrant-identity role directly, the same approach `jo-carc-registry`/`tt-caa-registry` take for their own operator-only sources |

No serial number or registration date columns exist in this source.

See `specs/data-dictionary.yaml` (`lk-caa-registry` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 780A6B | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "AIRBUS",
        "manufacturer_model": "AIRBUS A320-232",
        "model": "A320-232",
        "type_designator": "A320"
    },
    "data_sources": [
        "mictronics",
        "lk-caa-registry"
    ],
    "icao_hex": "780A6B",
    "military": false,
    "registrant": {
        "names": [
            "SriLankan Airlines Ltd."
        ]
    },
    "registration": "4R-ABL"
}
```

## Configuration

See [Data Runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md#configuration) for the full list of environment variables every runner reads. `REDIS_TTL_DAYS` applies to each `aircraft:registry:{icao_hex}` key written by this runner.

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/lk-caa-registry/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `83` | Integer as string |
| `last_run_at` | e.g. `2026-08-09T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `Success` or `Failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_lk_caa_registry_{name}/config` for each of the three stats above.
