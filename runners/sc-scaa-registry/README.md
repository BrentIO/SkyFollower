# Seychelles 🇸🇨 SCAA Registry

| | |
|---|---|
| **Name** | `sc-scaa-registry` |
| **Country** | Seychelles |
| **Registration prefix** | `S7-` |
| **Data source** | https://www.scaa.sc/index.php/regulatory/e-registers/aircraft-civil-register |
| **Format** | HTML table (single request, no file to download) |
| **Run frequency** | Weekly (pick a day/time — see `docker-compose.core.yaml`) — a small register (19 aircraft at check time), a weekly poll is negligible load |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; `S7-` registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

⚠️ **Coverage is deliberately small, not a scraping bug.** SCAA's own page states: "aircraft with a principle place of business outside Seychelles and/or those not operating within or to Seychelles are not registered on the Seychelles Civil Register." 19 aircraft across 3 operator groupings plus a few individually-owned aircraft is the expected full result. The page also states it was last updated 20 January 2025 — this source may update infrequently/irregularly rather than on a predictable schedule.

## How it works

The register is rendered as **three separate `<table>` elements** on the page, one per operator grouping, each with its own repeated header row — every table is parsed, not just the first.

This runner uses a real browser-style User-Agent rather than the `"Mozilla/5.0 (compatible; P5Software SkyFollower)"` string every other runner in this repo shares. Confirmed by direct testing: SCAA's WAF blocks that exact legacy `"Mozilla/5.0 (compatible; ...)"` bot-signature format specifically — not the words "SkyFollower" or "P5Software" (a substitute bot name in the same format was blocked identically; a plain `Mozilla/5.0` alone was not). This is scoped to this runner only; every other runner in the repo currently works fine with the shared UA.

There's no separate owner column beyond `Registered Owner` — that maps directly to `registrant.names`. `Aircraft Type` is a combined manufacturer+model-ish string (e.g. `DHC6-400`, `EC120B`) with no manufacturer name present at all, so it's stored as `aircraft.model` only. One row (`S7-IDC`) has owner `Seychelles Government` — a state aircraft, not military; `military` stays `false` regardless, since it's civil-registered. Every written record explicitly sets `military: false` — this register is exclusively civil.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `Aircraft Registration` | ✅ | `S7-`-prefix; used as the Mictronics lookup key |
| `Aircraft Type` | ✅ | → `aircraft.model` (combined string, no manufacturer present) |
| `Registered Owner` | ✅ | → `registrant.names` |

See `specs/data-dictionary.yaml` (`sc-scaa-registry` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 230A1B | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "De Havilland Canada",
        "manufacturer_model": "DHC-6-400 Twin Otter",
        "model": "DHC6-400",
        "type_designator": "DH6T"
    },
    "data_sources": [
        "mictronics",
        "sc-scaa-registry"
    ],
    "icao_hex": "230A1B",
    "military": false,
    "registrant": {
        "names": [
            "Air Seychelles Ltd."
        ]
    },
    "registration": "S7-LDI"
}
```

(`type_designator`, `manufacturer`, and `manufacturer_model` come from a Mictronics record on the same hex; `model` and `registrant.names` above are this runner's contribution.)

## Configuration

See [Data Runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md#configuration) for the full list of environment variables every runner reads. `REDIS_TTL_DAYS` applies to each `aircraft:registry:{icao_hex}` key written by this runner.

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/sc-scaa-registry/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `19` | Integer as string |
| `last_run_at` | e.g. `2026-08-09T08:00:29.791000+00:00` | ISO 8601 UTC |
| `last_run_status` | `Success` or `Failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_sc_scaa_registry_{name}/config` for each of the three stats above.
