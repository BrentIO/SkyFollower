# mictronics

| | |
|---|---|
| **Country** | N/A — global source, not country-specific |
| **Registration prefix** | N/A |
| **Data source** | https://github.com/Mictronics/aircraft-database/raw/refs/heads/main/indexedDB.zip |
| **Format** | ZIP archive of JSON files, fixed URL (not scraped/discovered) |
| **Run frequency** | Weekly (Tuesday, 05:10 UTC) |
| **Depends on Mictronics for ICAO hex** | N/A — this *is* the Mictronics runner. It is the foundational ICAO-hex source that every other country runner depends on and resolves registrations against; it does not depend on itself. |

## How it works

This is the foundational data source for the whole enrichment pipeline: every
other runner's example output in this repo includes fields that Mictronics
itself contributed (`manufacturer`, `manufacturer_model`, `type_designator`,
`wake_turbulence_category`) before country-specific data is merged on top, and
country runners resolve `icao_hex` by looking up their own registrations
against the RediSearch index this runner maintains
(`idx:aircraft:mictronics`).

The fixed ZIP URL is downloaded and extracted in memory (no index-page
scraping needed), yielding three JSON files — `aircrafts.json`,
`types.json`, and `operators.json` — which are staged into a local SQLite
database, joined (`aircraft` LEFT JOIN `types` on `type_designator`), and
written to Redis. `types.json`'s `manufacturer_model` string (e.g.
`"Boeing 737-800"`) is split on the first space to derive `aircraft.manufacturer`
(`"Boeing"`); the remainder (`"737-800"`) is only ever visible embedded in the
full `manufacturer_model` string — it is not stored as a separate `model`
field, since Mictronics has no notion of a country-runner-style `model` value.
Because a country runner's `aircraft:registry:{icao_hex}` key is separate from
this runner's `aircraft:mictronics:{icao_hex}` key, and both may be rewritten
in any order across weekly runs, writes here are done via read-modify-write:
each new record is deep-merged into whatever already exists at that key
(rather than overwritten wholesale) so a partial re-run cannot drop fields
written by an earlier one.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `aircrafts.json` key (icao_hex) | ✅ | → `icao_hex`; primary key of `aircraft:mictronics:{icao_hex}` |
| `aircrafts.json` values[0] (registration) | ✅ | → `registration` |
| `aircrafts.json` values[1] (type_designator) | ✅ | → `aircraft.type_designator`; also the join key into `types.json` |
| `aircrafts.json` values[2] flags[0] (military) | ✅ | → `military` boolean |
| `aircrafts.json` values[2] flags[1] (interesting) | ❌ | Staged in SQLite but not written to the Redis record |
| `types.json` key (type_designator) | ✅ | Join key only |
| `types.json` values[0] (manufacturer_model) | ✅ | Split on first space → `aircraft.manufacturer`; full string also kept as `aircraft.manufacturer_model` |
| `types.json` values[1] (description) | ❌ | Parsed but not stored |
| `types.json` values[2] (wtc code) | ✅ | Decoded (`J`/`H`/`M`/`L`/`M/L`/`-` → full name) → `aircraft.wake_turbulence_category` |
| `operators.json` key (airline_designator) | ✅ | → `operator:{designator}` key and `airline_designator` field |
| `operators.json` values[0] (name) | ✅ | → `operator.name` |
| `operators.json` values[1] (country) | ✅ | → `operator.country` |
| `operators.json` values[2] (callsign) | ✅ | → `operator.callsign` |

See `specs/data-dictionary.yaml` (`mictronics` entry) for full column semantics and cross-source schema notes.

## Example Output

`Runner Data Test.md` has no dedicated example section for `mictronics` since
it never writes a fully standalone record a user would look up directly —
its data is only ever seen merged underneath a country runner's own fields.
The example below (from `gg-2reg`'s `43EC60`) illustrates this: `manufacturer`,
`manufacturer_model`, `type_designator`, and `wake_turbulence_category` are
Mictronics's own contributions from `aircraft:mictronics:43EC60` (written by
this runner), present before or independent of any country-specific
enrichment; `model` and `serial_number` are `gg-2reg`'s own additions from a
separate `aircraft:registry:43EC60` key, merged in at read time.

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 43EC60 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "The New Piper Aircraft, Inc",
        "manufacturer_model": "PIPER PA-28-201T/235/236",
        "model": "PA-28-235",
        "serial_number": "28-7210009",
        "type_designator": "P28B",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "43EC60",
    "military": false,
    "registration": "2-GOLD",
    "source": "gg-2reg"
}
```

## Configuration

Reads `settings.json` (mounted at `/app/settings.json`):

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `redis.host` | ✅ | — | Redis connection host |
| `redis.port` | ❌ | `6379` | |
| `mqtt.host` | ❌ | — | Omit the whole `mqtt` block to skip completion-stats publishing entirely |
| `mqtt.port` | ❌ | `1883` | |
| `mqtt.username` | ❌ | — | Optional MQTT auth (added in #328); omit for an anonymous broker |
| `mqtt.password` | ❌ | — | |
| `redis_ttl_days` | ❌ | `14` | TTL applied to each `aircraft:mictronics:{icao_hex}` and `operator:{designator}` key written by this runner |

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/mictronics/statistic/{name}` (all retained). Unlike most runners, this one publishes an extra `operators_imported` stat alongside the standard three:

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `operators_imported` | e.g. `842` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_mictronics_{name}/config` for each of the four stats above.
