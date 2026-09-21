# US 🇺🇸 FAA Telephony Designators

| | |
|---|---|
| **Name** | `us-faa-telephony-designators` |
| **Country** | United States (FAA-assigned designators for operators worldwide, plus US-only special telephony) |
| **Data source** | `https://www.faa.gov/air_traffic/publications/atpubs/cnt_html/chap3_section_3.html` and `.../chap3_section_4.html` (FAA Order 7340.2) |
| **Format** | HTML tables (two fixed pages, no discovery step) |
| **Run frequency** | Weekly (Tuesday, 06:10 UTC — one hour after `mictronics`) |
| **Depends on Mictronics** | Must run **after** `mictronics` — see "Additive-only, never overrides" below. |

## What this is

`operator:{designator}` (an operator's name, country, and radio telephony callsign — shown alongside a flight's ident wherever this repo displays operator info) is written only by `runners/mictronics/main.py`, from Mictronics' own `operators.json`. That file has real gaps: as of 2026-09-20, FAA Order 7340.2 assigns **171 ICAO three-letter airline designators** Mictronics' `operators.json` does not have — including currently-operating carriers like KM Malta Airlines (`KMM`), Madagascar Airlines (`MGY`), AJet (`TKJ`), Bermudair (`BMA`), and Porter Airlines' second, Dash-8-specific designator (`PTR`, distinct from its main `POE`, which Mictronics already has).

This runner backfills exactly that gap, from two independent FAA sources:

- **Section 3** — "Three-Letter Designator/Aircraft Company/Telephony Decode": the standard ICAO 3-letter airline company designator table, ~5,700 rows (worldwide, not US-specific).
- **Section 4** — "U.S. Special Telephony/Call Signs": ~20 US government/agency/special-use callsigns (NASA, FEMA, state fire departments, etc.) with a variable-length "Identifier" — not always 3 letters (`ARSIX`, `NASA`, `FEMA`).

## Additive-only, never overrides

Mictronics is the preferred source (better name capitalization, actively maintained). This runner writes `operator:{designator}` **only when that key does not already exist in Redis** — via `shared.redis_json.set_json(..., nx=True)`, Redis `JSON.SET`'s native `NX` option, a single atomic command rather than a check-then-set race. It never updates or removes an existing entry, from any source, ever.

Correctness against a future Mictronics update falls out of **scheduling order, not a merge script**: `mictronics`'s own writer already overwrites `operator:{designator}` unconditionally on every run. If Mictronics later adds a designator this runner already backfilled, Mictronics' next scheduled run simply replaces it — which is exactly why this runner must run *after* `mictronics` in the schedule, the same dependency convention `bz-bdca-registry`/`is-samgongustofa-registry` already use for a different reason (resolving ICAO hex against Mictronics' own data).

## No TTL on the records this runner writes

Every other enrichment key in this repo gets `ENRICHMENT_TTL_SECONDS` (14 days), refreshed on every write. This runner's writes are deliberately different: a given designator is written **at most once, ever** — every subsequent run finds the key already present and the `NX` write is a no-op, so a TTL's clock would never get refreshed after that first write. With a 14-day TTL and this runner's weekly cadence, the record would expire and vanish from Redis between runs, then reappear on the next run — a real, avoidable gap. FAA's published designator table is static reference data, not perishable per-run enrichment, so these records are written without an expiry.

## Section 4 handling

- **`country`** is not a column on this table at all — every Section 4 entry is by definition US-issued, so it's defaulted to `"United States"`.
- **Expiration Date**: a row whose expiration has already passed (checked against the real current date each run, not a fixed cutoff) is skipped entirely — not imported, not retried. `N/A` (permanent) always imports.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Section 3: 3-Ltr | ✅ | → Redis key (`operator:{designator}`) and `airline_designator` |
| Section 3: Company | ✅ | → `name` |
| Section 3: Country | ✅ | → `country` |
| Section 3: Telephony | ✅ (when present) | → `callsign`; ~540 of ~6,240 rows have no assigned callsign — still imported with `name`/`country` only |
| Section 4: Identifier | ✅ | → Redis key and `airline_designator`; **not always 3 letters** |
| Section 4: Telephony/Call Sign | ✅ | → `callsign` |
| Section 4: Company or Operating Agency | ✅ | → `name` |
| Section 4: Expiration Date | ❌ (used only to gate import) | Not stored — a record either imports (not expired) or doesn't; there's no "expired" state to represent in Redis |

The Section 3 page splits its one logical table across ~26 separate `<table>` elements (each with the same repeated header row) rather than one — this runner parses and combines every matching table, not just the first found.

See `specs/data-dictionary.yaml` (`us-faa-telephony-designators` entry) for full column semantics.

## Example Output

A designator this runner backfilled (Mictronics has no `KMM` entry):

```bash
docker run --rm --network host redis:latest redis-cli JSON.GET operator:KMM
```

```json
{
    "airline_designator": "KMM",
    "name": "KM MALTA AIRLINES PLC.",
    "country": "MALTA",
    "callsign": "SKY KNIGHT"
}
```

A Section 4 special-telephony entry:

```bash
docker run --rm --network host redis:latest redis-cli JSON.GET operator:FEMA
```

```json
{
    "airline_designator": "FEMA",
    "name": "Federal Emergency Management Agency",
    "country": "United States",
    "callsign": "FEMA"
}
```

## Related fix in this same change: military aircraft no longer skip operator lookup

`message-processor/main.py`'s `_enrich_operator()` previously returned immediately for any aircraft flagged `military: true`, skipping the `operator:{designator}` lookup entirely regardless of whether a real match existed. Several Section 4 entries (state fire departments, state DOTs, FEMA, the Commemorative Air Force's `TORA TORA TORA`) are plausibly flown by aircraft flagged `military: true` in the registry — exactly the population that blanket skip silently excluded from ever getting an operator name. The skip has been removed; a military aircraft whose ident prefix doesn't match any `operator:{designator}` key now just misses like any other lookup (counted in the existing `_operator_misses` metric), with no change for the common non-matching case.

## Configuration

See [Data Runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md#configuration) for the full list of environment variables every runner reads. Unlike most runners, this one writes `operator:{designator}` with **no TTL** (see above).

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/us-faa-telephony-designators/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `171` | Integer as string — count of designators **newly written** this run, not rows parsed |
| `last_run_at` | e.g. `2026-09-23T06:10:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `Success` or `Failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_us_faa_telephony_designators_{name}/config` for each of the three stats above.
