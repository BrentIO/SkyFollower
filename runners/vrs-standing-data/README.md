# Virtual Radar Server Standing Data

Virtual Radar Server Standing Data

| | |
|---|---|
| **Name** | `vrs-standing-data` |
| **Coverage** | Global — flight routes, plus the ICAO 24-bit hex-range country-of-registration lookup table (not aircraft or airports; see Scope below) |
| **Data source** | https://github.com/vradarserver/standing-data (`routes/schema-01/**/*.csv`, `code-blocks/schema-01/code-blocks.csv`, `countries/schema-01/countries.csv`) |
| **Format** | GitHub repository tarball (fixed URL; no discovery step) |
| **Run frequency** | Daily, 04:50 UTC |
| **Depends on Mictronics for ICAO hex** | N/A — `route:{ident}` is keyed by flight ident/callsign, not `icao_hex`; the code-blocks/countries lookup tables are global, not per-hex. |

## Scope

The upstream VRS Standing Data Management (SDM) repository also publishes
aircraft, airline, and airport CSVs, but this runner does not import those:

| Section | Status |
|---|---|
| Aircraft | Covered by Mictronics + all national registry runners — redundant |
| Airlines | Mictronics already downloads `operators.json` and writes `operator:{code}` — redundant |
| Routes | Unique to this source — **imported** |
| Airports | Covered by the `ourairports` runner — redundant |
| Code blocks (ICAO hex-range → country) | Unique to this source — **imported** (#1848) |
| Countries (ISO2 → country name) | Unique to this source — **imported** (#1848) |

Code-blocks/countries were a distinct oversight from this runner's original
routes-only scoping, not a deliberate exclusion — see #1848. They're the
standard ICAO 24-bit (Mode S) address allocation table the dump1090/VRS
family of tools uses to resolve an aircraft's country of
registration for every hex, not just the ~50 countries a national registry
runner scrapes a CAA for. The actual per-hex resolution happens server-side
in `shared/lua/merge_aircraft.lua` at read time, not here — this runner only
stages the two lookup tables it reads.

## How it works

The whole repository is downloaded once as a `.tar.gz` from GitHub's
codeload endpoint (`.../vradarserver/standing-data/tar.gz/refs/heads/main`)
and extracted in memory three times against those same bytes, once per
prefix: `routes/schema-01/` (~1,600 CSVs, one per airline-code prefix, e.g.
`routes/schema-01/A/AAL-all.csv`), `code-blocks/schema-01/` (one CSV), and
`countries/schema-01/` (one CSV). Everything else in the tarball (aircraft,
airline, airport data) is discarded. Rows are staged into a local SQLite
database before being written to Redis.

Each route CSV row maps `Callsign` → `ident` (the Redis key) and
`AirportCodes` → `route` (the Redis value), unchanged. `AirportCodes` is a
hyphen-delimited sequence of ICAO airport codes, e.g. `KJFK-KLAX` for a
simple point-to-point route, or `KDFW-MYNN-KDFW` for a same-day out-and-back
that reuses one callsign across two legs. This runner does **not** split,
filter, or interpret that sequence in any way — whatever the source
provides is stored as-is.

`code-blocks.csv`'s `Bitmask`/`SignificantBitmask` hex-string columns are
converted to integers at import time; `Start`/`Finish`/`Count`/`IsMilitary`
are not carried into Redis at all (`Start` always equals `Bitmask` in the
source data; `Finish`/`Count` are derivable from the mask; `IsMilitary` is a
documented non-goal — see #1848, `AircraftRecord.military` stays sourced
only from Mictronics). Rows are written sorted descending by
`SignificantBitmask`, matching the standard VRS-style
longest-prefix-bitmask matching order. The source's `CountryISO2` "ZZ"
rows — two entries that together cover the entire 24-bit address space at
the lowest possible `SignificantBitmask` — are the source's synthetic
"unknown/unassigned" catch-all, not a real country; importing them would
mean `merge_aircraft.lua`'s linear scan always finds a match, hiding the
genuine no-match case behind a fake country, so they're dropped here.

`countries.csv` is imported unmodified (ISO2 → English name), including its
own "ZZ"/"XA"/"XB"/"XC" special-block rows — this table is a general-purpose
name lookup, unrelated to which hex-ranges are eligible to match.

## Columns

Route CSV columns (verbatim from the source's header row): `Callsign, Code,
Number, AirlineCode, AirportCodes`.

| Source column | Imported | Notes |
|---|---|---|
| `Callsign` | ✅ | → `ident` (the Redis key, uppercased) |
| `Code` | ❌ | Present in source; not read by this runner |
| `Number` | ❌ | Present in source; not read by this runner |
| `AirlineCode` | ❌ | Present in source; not read by this runner |
| `AirportCodes` | ✅ | → `route` (the Redis value); passed through unchanged, including 3+ airport sequences |

`code-blocks.csv` columns: `Start, Finish, Count, Bitmask, SignificantBitmask, IsMilitary, CountryISO2`.

| Source column | Imported | Notes |
|---|---|---|
| `Start` | ❌ | Always equals `Bitmask`; not carried into Redis |
| `Finish` | ❌ | Derivable from the mask; not carried into Redis |
| `Count` | ❌ | Derivable from the mask; not carried into Redis |
| `Bitmask` | ✅ | Hex string converted to integer |
| `SignificantBitmask` | ✅ | Hex string converted to integer; array is sorted descending by this value |
| `IsMilitary` | ❌ | Per-range flag, a different granularity from `AircraftRecord.military` (an aircraft-level fact) — see #1848 |
| `CountryISO2` | ✅ | → `country_code`; the two `ZZ` unknown/unassigned catch-all rows are excluded entirely |

`countries.csv` columns: `ISO, Name` — both imported unmodified into a flat `{iso2: name}` object.

See `specs/data-dictionary.yaml` (`route` record) for full route field semantics.

## Example Output

`route:{ident}` is a plain Redis string, not a JSON document — read it back with `GET`, not `JSON.GET`:

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

`lookup:icao-code-blocks` and `lookup:icao-countries` are RedisJSON documents:

```bash
docker run --rm --network host redis/redis-stack-server:latest redis-cli JSON.GET lookup:icao-code-blocks
```

```
[{"bitmask": 11010048, "significant_bitmask": 15728640, "country_code": "US"}, ...]
```

```bash
docker run --rm --network host redis/redis-stack-server:latest redis-cli JSON.GET lookup:icao-countries
```

```
{"US": "United States", "GB": "United Kingdom", ...}
```

TTL: `route:{ident}` is fixed at 3 days (`ROUTE_TTL_SECONDS` in `shared/timing.py`) — the upstream repository's route data updates daily, so a shorter TTL keeps stale routes from lingering if a run is missed. `lookup:icao-code-blocks`/`lookup:icao-countries` use the standard 14-day `ENRICHMENT_TTL_SECONDS` every other runner's weekly-ish data uses — the source table itself is stable (last changed well over a year before #1848 was filed). None of these TTLs are operator-configurable.

## Configuration

See [Data Runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md#configuration) for the full list of environment variables every runner reads. This runner writes `route:{ident}` with a fixed 3-day TTL and `lookup:icao-code-blocks`/`lookup:icao-countries` with the standard 14-day TTL -- see the TTL note above.

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/vrs-standing-data/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `620954` | Integer as string; combined total across routes + code-blocks + countries |
| `last_run_at` | e.g. `2026-07-25T04:50:12.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `Success` or `Failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_vrs_standing_data_{name}/config` for each of the three stats above.
