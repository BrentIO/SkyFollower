# Macau 🇲🇴 AACM Registry

| | |
|---|---|
| **Name** | `mo-aacm-registry` |
| **Country** | Macau |
| **Registration prefix** | `B-M` (Macau's slice of China's `B-` namespace) |
| **Data source** | https://www.aacm.gov.mo/zh-hant/industry-page/RegisteredAircraft |
| **Format** | HTML table (single request, no file to download) |
| **Run frequency** | Weekly (pick a day/time — see `docker-compose.core.yaml`) — a tiny register (25 aircraft at check time across 2 operators), so a weekly poll is very cheap |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; `B-M` registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

Only the Traditional Chinese page (`zh-hant`) actually renders a table on a plain HTTP fetch — the `/en-us/` path returns the same page shell with unrendered Vue.js template placeholders and no table content, so the `Operator` column is genuine Chinese text (e.g. `澳門航空股份有限公司`), stored as-is rather than transliterated. This is the first runner in the repo to handle CJK text end-to-end; verified fetch → parse → Redis write → `merge_aircraft.lua` read-back round-trips it unmangled.

The `Operator` column uses `rowspan` to span every aircraft belonging to the same operator (`rowspan="23"` for the first operator at check time) rather than repeating the name on every row, so the table is expanded into a uniform grid before parsing — the same `_expand_table()` approach `kg-caa-registry` uses for its own rowspan-merged operator column. At least one registration cell has the mark split across multiple text nodes in the source HTML (renders as `B-MB U` instead of `B-MBU`) — internal whitespace is stripped from the registration value before validating, not just collapsed, to recover it.

This register has no separate owner column — only `Operator` — so that name fills the registrant-identity role directly, the same approach `sg-caas-registry`/`tt-caa-registry`/`jo-carc-registry` take for their own operator-only sources. `Aircraft Type` is a single combined manufacturer+model string (e.g. `空中巴士 A321-231` / "Airbus A321-231") with no reliable delimiter to split manufacturer out, so it's stored whole as `aircraft.model`, matching `bs-caa-registry`/`tt-caa-registry`'s approach to the same situation. No serial number or address columns exist in this source. Every written record explicitly sets `military: false` — this register is exclusively civil.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `Operator` (經營人) | ✅ | → `registrant.names`; rowspan-merged across each operator's aircraft |
| `Registration Number` (註冊編號) | ✅ | `B-M`-prefix; used as the Mictronics lookup key; internal whitespace stripped before validation |
| `Aircraft Type` (型號) | ✅ | → `aircraft.model` (combined string, not split) |

See `specs/data-dictionary.yaml` (`mo-aacm-registry` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 7C1234 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "Leonardo",
        "manufacturer_model": "Leonardo AW139",
        "model": "阿古斯塔AW139",
        "type_designator": "AW139"
    },
    "data_sources": [
        "mictronics",
        "mo-aacm-registry"
    ],
    "icao_hex": "7C1234",
    "military": false,
    "registrant": {
        "names": [
            "亞太航空有限公司"
        ]
    },
    "registration": "B-MHI"
}
```

(`type_designator`, `manufacturer`, and `manufacturer_model` come from a Mictronics record on the same hex; `model` and `registrant.names` above are this runner's contribution.)

## Configuration

See [Data Runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md#configuration) for the full list of environment variables every runner reads. This runner writes `aircraft:registry:{icao_hex}` with a fixed 14-day TTL (`ENRICHMENT_TTL_SECONDS` in `shared/timing.py`).

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/mo-aacm-registry/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `25` | Integer as string |
| `last_run_at` | e.g. `2026-08-09T07:42:19.821000+00:00` | ISO 8601 UTC |
| `last_run_status` | `Success` or `Failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_mo_aacm_registry_{name}/config` for each of the three stats above.
