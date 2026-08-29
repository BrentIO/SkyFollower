# Virtual Radar Server Standing Data

Virtual Radar Server Standing Data

| | |
|---|---|
| **Name** | `vrs-standing-data` |
| **Coverage** | Global — flight routes only (not aircraft or airports; see Scope below) |
| **Data source** | https://github.com/vradarserver/standing-data (`routes/schema-01/**/*.csv`) |
| **Format** | GitHub repository tarball (fixed URL; no discovery step) |
| **Run frequency** | Daily, 04:50 UTC |
| **Depends on Mictronics for ICAO hex** | N/A — this runner writes `route:{ident}` records keyed by flight ident/callsign, not `icao_hex`. |

## Scope

The upstream VRS Standing Data Management (SDM) repository also publishes
aircraft, airline, and airport CSVs, but this runner imports **routes
only**:

| Section | Status |
|---|---|
| Aircraft | Covered by Mictronics + all national registry runners — redundant |
| Airlines | Mictronics already downloads `operators.json` and writes `operator:{code}` — redundant |
| Routes | Unique to this source — **imported** |
| Airports | Covered by the `ourairports` runner — redundant |

## How it works

The whole repository is downloaded as a `.tar.gz` from GitHub's codeload
endpoint (`.../vradarserver/standing-data/tar.gz/refs/heads/main`) and
extracted in memory. Only files under `routes/schema-01/` are kept (~1,600
CSVs, one per airline-code prefix, e.g. `routes/schema-01/A/AAL-all.csv`);
everything else in the tarball (aircraft, airline, airport data) is
discarded. Rows are staged into a local SQLite database before being
bulk-written to Redis in batches of 10,000.

Each CSV row maps `Callsign` → `ident` (the Redis key) and `AirportCodes` →
`route` (the Redis value), unchanged. `AirportCodes` is a hyphen-delimited
sequence of ICAO airport codes, e.g. `KJFK-KLAX` for a simple point-to-point
route, or `KDFW-MYNN-KDFW` for a same-day out-and-back that reuses one
callsign across two legs. This runner does **not** split, filter, or
interpret that sequence in any way — whatever the source provides is stored
as-is.

## Columns

CSV columns (verbatim from the source's header row): `Callsign, Code,
Number, AirlineCode, AirportCodes`.

| Source column | Imported | Notes |
|---|---|---|
| `Callsign` | ✅ | → `ident` (the Redis key, uppercased) |
| `Code` | ❌ | Present in source; not read by this runner |
| `Number` | ❌ | Present in source; not read by this runner |
| `AirlineCode` | ❌ | Present in source; not read by this runner |
| `AirportCodes` | ✅ | → `route` (the Redis value); passed through unchanged, including 3+ airport sequences |

See `specs/data-dictionary.yaml` (`route` record) for full field semantics.

## Example Output

This runner writes a plain Redis string, not a JSON document — read it back with `GET`, not `JSON.GET`:

```bash
docker run --rm --network host redis:latest redis-cli GET route:AAL1
```

```
"KJFK-KLAX"
```

```bash
docker run --rm --network host redis:latest redis-cli GET route:AAL1005
```

```
"KDFW-MYNN-KDFW"
```

TTL: fixed at 3 days (`ROUTE_TTL_SECONDS` in `shared/timing.py`), not the 14-day `ENRICHMENT_TTL_SECONDS` every other runner uses — the upstream repository updates daily, so a shorter TTL keeps stale routes from lingering if a run is missed. Neither TTL is operator-configurable.

## Configuration

See [Data Runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md#configuration) for the full list of environment variables every runner reads. This runner writes `route:{ident}` with a fixed 3-day TTL (`ROUTE_TTL_SECONDS` in `shared/timing.py`) -- see the TTL note above.

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/vrs-standing-data/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `620954` | Integer as string |
| `last_run_at` | e.g. `2026-07-25T04:50:12.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `Success` or `Failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_vrs_standing_data_{name}/config` for each of the three stats above.
