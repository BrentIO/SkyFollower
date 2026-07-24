# Data Runners

Each subdirectory is a self-contained data runner: download → normalize →
write to Redis with TTL → exit. Runners are scheduled on their own cadence
via the `ofelia` cron container (see `config/ofelia/config.ini`). See the
[Getting Started](https://brentio.github.io/SkyFollower/getting-started/)
docs for how to run one manually or bulk-load all of them.

## Schedule

Each of the 41 data runners has its own entry in `config/ofelia/config.ini`
(see `config/ofelia/config.ini.example`), staggered across the week so
runners hitting the same country's civil aviation authority don't collide.
That file is the single source of truth for exact schedules.

All runners write registration/airport data to Redis with a 14-day TTL
(`redis_ttl_days` in each runner's `settings.json`, default 14).

Each runner publishes a single MQTT message on completion with `records_imported`,
`last_run_at`, and `last_run_status`.

## Logging

Every runner calls `configure_logging(cfg.get("log_level"))` from
`shared/logging_setup.py` right after loading `settings.json`, wiring the
`log_level` config field (`"debug"` or `"info"`, see
`config/runners/settings.json.example`) to the root logger — `receiver` and
`processor` use the same helper. `configure_logging()` is called once more,
with no argument, if `settings.json` itself can't be loaded, so that failure
still logs formatted instead of falling back to Python's default handler.

### Level convention

| Level | Use for |
|-------|---------|
| `DEBUG` | Per-request detail: the exact URL of every outbound call, retry attempts, per-record skip reasons. Anything you'd otherwise add a temporary `print()` for while debugging a stuck or blocked run. |
| `INFO` | Lifecycle milestones: run started, source URL for the primary download, record counts staged/written, run completed. One line per meaningful step, not per record. |
| `WARNING` | A single record/request failed but the run continues (a detail fetch that exhausted retries, a row that didn't parse) — something worth surfacing without failing the run. |
| `ERROR` / `CRITICAL` | The run cannot produce useful output (settings file missing, primary download failed, no records parsed). Runner exits non-zero. |

**Rule**: any runner that makes more than one HTTP request per run — whether
paginated (a fixed request per page) or per-entity (one request per record,
e.g. a list-then-detail pattern) — logs the exact URL of *every* call at
`DEBUG`, not just the primary one. `cz-caa` and `me-caa` already do this for
their per-record detail fetches:
```python
logger.debug("Fetching Montenegro CAA detail page from %s", url)
```
"More than one HTTP request" means unbounded/high-volume (pagination or
per-entity) — see the audit below for why 2-request runners are exempt.

### Request-count audit

| Category | Runners | URL-at-DEBUG rule |
|----------|---------|--------------------|
| Single request | `airportwebcams-liveries`, `at-austrocontrol`, `au-casa`, `br-anac`, `bz-bdca`, `ca-transport-canada`, `ch-bazl`, `cy-dca`, `ee-transpordiamet`, `es-aesa`, `fr-dgac`, `ge-gcaa`, `is-samgongustofa`, `kg-caa`, `kr-koca`, `ky-caa`, `lv-caa`, `md-caa`, `mictronics`, `no-caa`, `nz-caa`, `ourairports`, `rs-cad`, `tc-caa`, `us-faa` (25) | N/A — one call, already logged at `INFO` |
| Two-step discovery (fetch an index/listing page, then fetch the one file URL found there — fixed at 2 calls, not pagination) | `bg-caa`, `bs-caa`, `gg-2reg`, `hr-ccaa`, `hu-kozhaf`, `im-ardis`, `lu-dac`, `mk-caa`, `mv-caa`, `nl-ilt`, `pg-casapng`, `sg-caas`, `sk-nsat` (13) | N/A — already compliant. All 13 already log both the discovery URL and the resolved file URL at `INFO` (e.g. `logger.info("Downloading Bulgaria CAA index page from %s", _INDEX_URL)`). 2 lines for 2 total requests isn't the per-request noise `DEBUG` exists to declutter, and downgrading them would make this visibility opt-in instead of on by default. |
| Multi-request: pagination or per-entity (unbounded call count) | `cz-caa` (per-ID detail), `me-caa` (paginated list + per-registration detail), `uk-caa` (676 prefix searches + per-aircraft detail) (3) | Compliant — all 3 log the URL of every call at `DEBUG`. |
