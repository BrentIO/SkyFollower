# ourairports

| | |
|---|---|
| **Coverage** | Global — airport metadata, not aircraft (different domain from every other data runner) |
| **Data source** | https://davidmegginson.github.io/ourairports-data/airports.csv |
| **Format** | CSV (fixed URL) |
| **Run frequency** | Weekly (Monday, 05:10 UTC) |
| **Depends on Mictronics for ICAO hex** | N/A — this runner writes `airport:{icao_code}` records, not aircraft enrichment, and has no relationship to the Mictronics-provided `icao_hex` lookup used by the country aircraft-register runners. |

## How it works

The OurAirports CSV is downloaded whole from a fixed URL (no index page or scraping involved) and parsed with `csv.DictReader`. Rows are filtered to those whose `ident` is exactly 4 characters (a valid ICAO airport code — 3-character IATA-only idents and other lengths are dropped) and staged into a fresh local SQLite database (the file is deleted and recreated on every run) before being bulk-written to Redis in batches of 10,000. Each staged row also gets a computed `phonic` field — a voice-friendly spoken name — via `compute_phonic()`, which supports a host-provided JSON override file for exact per-airport overrides (see below).

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `ident` | ✅ | Filtered to 4-character ICAO codes; non-4-char rows dropped; used as `icao_code` and the Redis key |
| `iata_code` | ✅ | → `iata_code`; omitted from the record if blank |
| `name` | ✅ | → `name`; also used as input to phonic computation |
| `municipality` | ✅ | → `city`; also used as input to phonic computation |
| `iso_region` | ✅ | → `region` |
| `iso_country` | ✅ | → `country` |
| `latitude_deg` | ✅ | → `latitude` (float) |
| `longitude_deg` | ✅ | → `longitude` (float) |
| `id` | ❌ | Present in source; not read by this runner |
| `type` | ❌ | Present in source; not read by this runner |
| `elevation_ft` | ❌ | Present in source; not read by this runner |
| `continent` | ❌ | Present in source; not read by this runner |
| `scheduled_service` | ❌ | Present in source; not read by this runner |
| `icao_code` | ❌ | Present in source; not read by this runner — the runner instead derives its own `icao_code` from the (4-character-filtered) `ident` column, not from this field |
| `gps_code` | ❌ | Present in source; not read by this runner |
| `local_code` | ❌ | Present in source; not read by this runner |
| `home_link` | ❌ | Present in source; not read by this runner |
| `wikipedia_link` | ❌ | Present in source; not read by this runner |
| `keywords` | ❌ | Present in source; not read by this runner |

See `specs/data-dictionary.yaml` (`ourairports` entry) for full column semantics and cross-source schema notes.

## Example Output

This runner writes `airport:{icao_code}` records, not aircraft data, so `merge_aircraft.lua` does not apply here. Read a record directly with `JSON.GET`:

```bash
docker run --rm --network host redis:latest redis-cli JSON.GET airport:KATL | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "icao_code": "KATL",
    "name": "Hartsfield-Jackson Atlanta International Airport",
    "city": "Atlanta",
    "region": "US-GA",
    "country": "US",
    "phonic": "Atlanta Hartsfield-Jackson"
}
```

TTL: `redis_ttl_days × 86400` seconds (default 14 days).

## Configuration

Reads `settings.json` (mounted at `/app/settings.json`):

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `redis.host` | ✅ | — | Redis connection host |
| `redis.port` | ❌ | `6379` | |
| `mqtt.host` | ❌ | — | Omit the whole `mqtt` block to skip completion-stats publishing entirely |
| `mqtt.port` | ❌ | `1883` | |
| `mqtt.username` | ❌ | — | Optional MQTT auth; omit for an anonymous broker |
| `mqtt.password` | ❌ | — | |
| `redis_ttl_days` | ❌ | `14` | TTL applied to each `airport:{icao_code}` key written by this runner |

The settings file path defaults to `/app/settings.json` and can be overridden with the `SETTINGS_PATH` environment variable.

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/ourairports/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_ourairports_{name}/config` for each of the three stats above.

## Phonic Names

Each airport record includes a `phonic` field — a voice-friendly spoken name used by downstream systems for overhead announcements. "International" and "Airport" are stripped from every computed phonic.

### Override file

Copy `config/runners/phonic_overrides.json.example` to
`config/runners/phonic_overrides.json` — the same directory `settings.json`
already lives in (relative to `docker-compose.server.yaml`) — and edit it.
That directory is mounted onto `/app/config`; the path defaults to
`/app/config/phonic_overrides.json` and can be overridden with the
`OVERRIDES_PATH` environment variable. Mounting the directory (rather than
the file directly) means the override file can be added, edited, or removed
without needing to recreate the container.

**Format:**
```json
{
    "KXXX": "Spoken name exactly as desired",
    "KYYY": "Another override"
}
```

If an airport's ICAO code has an entry in this file, that value is used
verbatim — no stripping or any other processing is applied. The general
algorithm only runs for airports not in the file.

The file is read once each time the runner starts. Changes take effect on
the next run. A missing file is silently ignored.

### General algorithm (no override)

1. If name starts with "Greater" or contains " of ": use name as-is.
2. If city is not present in the airport name: prepend city.
3. If phonic ends with city: move city to the front.
4. Strip trailing `/` or `-` artifacts (common in names like "City/Town Airport").
5. Normalise `/` and `-` to spaces; collapse extra spaces.
6. Strip "International" and "Airport" from the result.
