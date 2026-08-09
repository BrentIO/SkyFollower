# Jordan 🇯🇴 CARC Registry

| | |
|---|---|
| **Name** | `jo-carc-registry` |
| **Country** | Jordan |
| **Registration prefix** | `JY-` |
| **Data source** | https://www.carc.gov.jo/en/node/684 |
| **Format** | HTML table (single request, no file to download) |
| **Run frequency** | Weekly (pick a day/time — see `docker-compose.core.yaml`) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; `JY-` registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

⚠️ **Coverage is partial by design, not a scraping bug.** CARC's own page text states this only covers "Transport Category aircraft ... registered in the Jordan Civil Aircraft Register and operating for compensation or hire" — general aviation and private aircraft are excluded. If a small privately-owned `JY-` aircraft never shows up here, that's expected, not a bug.

## How it works

The original `carc.jo` domain listed on avcodes.co.uk is dead (DNS failure); the site has moved to `carc.gov.jo`. The register looks like 4 separate tables (one per operator — Royal Jordanian Airlines, Jordan Aviation, Arab Wings, Airlines Solitaire Air Ltd. Co. at check time) but is actually a **single HTML `<table>`**: 1-cell rows are operator-section headers, each followed by a 7-cell column-header row and that operator's data rows. A stray 1-cell row reading "Jordanian Registered Aircraft" (a page-title artifact, not an operator name) appears once, immediately after the first operator's header — it's skipped rather than treated as a new operator, so it doesn't overwrite the real current-operator tracking. The operator list itself is discovered from the page, not hardcoded, since CARC could add or remove operators over time.

This register has no owner column at all — only the operator section an aircraft's row falls under — so that section heading fills the registrant-identity role directly, the same approach `sg-caas-registry`/`tt-caa-registry` take for their own operator-only sources. The `Category` column is always `Transport` (a regulatory category, not the `aircraft.type` vocabulary) and isn't stored. Every written record explicitly sets `military: false` — this register is exclusively civil.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `Manufacturer` | ✅ | → `aircraft.manufacturer` |
| `Model` | ✅ | → `aircraft.model` |
| `Category` | ❌ | Always `Transport`; not the `aircraft.type` vocabulary |
| `MSN` | ✅ | → `aircraft.serial_number` |
| `Reg Mark` | ✅ | `JY-`-prefix; used as the Mictronics lookup key |
| `Reg. No.` | ❌ | Internal CARC registry number, distinct from the registration mark |
| `Reg. Date` | ❌ | Inconsistent date formats across operator sections (`11/12/07` vs. `29/12/2016`) |
| (operator section heading) | ✅ | → `registrant.names` — not a table column, comes from which section the row was scraped from |

See `specs/data-dictionary.yaml` (`jo-carc-registry` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 740A1B | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "Airbus",
        "manufacturer_model": "Airbus A310",
        "model": "A310-304",
        "serial_number": "445",
        "type_designator": "A310"
    },
    "data_sources": [
        "mictronics",
        "jo-carc-registry"
    ],
    "icao_hex": "740A1B",
    "military": false,
    "registrant": {
        "names": [
            "Royal Jordanian Airlines"
        ]
    },
    "registration": "JY-AGQ"
}
```

(`type_designator` and `manufacturer_model` come from a Mictronics record on the same hex; `serial_number` and `registrant.names` above are this runner's contribution.)

## Configuration

See [Data Runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md#configuration) for the full list of environment variables every runner reads. `REDIS_TTL_DAYS` applies to each `aircraft:registry:{icao_hex}` key written by this runner.

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/jo-carc-registry/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `48` | Integer as string |
| `last_run_at` | e.g. `2026-08-08T22:38:57.042000+00:00` | ISO 8601 UTC |
| `last_run_status` | `Success` or `Failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_jo_carc_registry_{name}/config` for each of the three stats above.
