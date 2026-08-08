# Data Runners

Each subdirectory is a self-contained data runner: download → normalize →
write to Redis with TTL → exit. Runners are scheduled on their own cadence
via the `ofelia` cron container (see `docker-compose.core.yaml`'s
`ofelia` service labels). See the
[Getting Started](https://brentio.github.io/SkyFollower/getting-started/)
docs for how to run one manually or bulk-load all of them.

## Schedule

Each data runner has its own `ofelia.job-run.*` label block on the `ofelia`
service in `docker-compose.core.yaml`, staggered across the week so runners
hitting the same country's civil aviation authority don't collide. That file
is the single source of truth for exact schedules.

Most runners write registration/airport data to Redis with a 14-day TTL
(`REDIS_TTL_DAYS`, default 14). `vrs-standing-data`
is the one exception: its source updates daily rather than weekly, so it
writes with a fixed 3-day TTL instead of reading `REDIS_TTL_DAYS`.

Each runner publishes a single MQTT message on completion with `records_imported`,
`last_run_at`, and `last_run_status`.

## Configuration

Every runner reads the same three config blocks via `shared/config.py`'s
`load_config("redis", "mqtt", "runner")` — one call, so a runner started
with something missing reports every missing variable together rather than
one per restart. Each runner's own README documents only what's specific
to it (which Redis key(s) `REDIS_TTL_DAYS` applies to, if anything);
the variables themselves are always these:

| Variable | Required | Default | Notes |
|---|---|---|---|
| `REDIS_HOST` | ✅ | — | Redis connection host |
| `REDIS_PORT` | ❌ | `6379` | |
| `REDIS_PASSWORD` | ✅ | — | Redis now requires authentication; every runner authenticates through `shared/redis_client.py`'s `build_redis_client()` |
| `MQTT_HOST` | ❌ | — | Leave unset to skip completion-stats publishing entirely — `MQTT_HOST`/`USERNAME`/`PASSWORD` are optional everywhere, not just for runners |
| `MQTT_PORT` | ❌ | `1883` | |
| `MQTT_USERNAME` | ❌ | — | Optional MQTT auth; leave unset for an anonymous broker |
| `MQTT_PASSWORD` | ❌ | — | |
| `REDIS_TTL_DAYS` | ❌ | `14` | TTL applied to the enrichment key(s) this runner writes — see the runner's own README for exactly which key(s) |
| `LOG_LEVEL` | ❌ | `info` | `"debug"` or `"info"` |

In `docker-compose.core.yaml`, every runner service (and the `ofelia`
job-run labels that schedule them) sources these from the same
`x-runner-environment` anchor, so a host states each value once regardless
of how many runners it runs.

## Logging

Every runner calls `configure_logging(cfg.get("log_level"))` from
`shared/logging_setup.py` right after loading its configuration, wiring
`LOG_LEVEL` to the root logger — `receiver` and
`message-processor` use the same helper. `configure_logging()` is called once more,
with no argument, if configuration loading itself fails, so that failure
still logs formatted instead of falling back to Python's default handler.

### Level convention

| Level | Use for |
|-------|---------|
| `DEBUG` | Per-request detail: the exact URL of every outbound call, retry attempts, per-record skip reasons. Anything you'd otherwise add a temporary `print()` for while debugging a stuck or blocked run. |
| `INFO` | Lifecycle milestones: run started, source URL for the primary download, record counts staged/written, run completed. One line per meaningful step, not per record. |
| `WARNING` | A single record/request failed but the run continues (a detail fetch that exhausted retries, a row that didn't parse) — something worth surfacing without failing the run. |
| `ERROR` / `CRITICAL` | The run cannot produce useful output (required configuration missing, primary download failed, no records parsed). Runner exits non-zero. |

**Rule**: any runner that makes more than one HTTP request per run — whether
paginated (a fixed request per page) or per-entity (one request per record,
e.g. a list-then-detail pattern) — logs the exact URL of *every* call at
`DEBUG`, not just the primary one. `cz-caa-registry` and `me-caa-registry` already do this for
their per-record detail fetches:
```python
logger.debug("Fetching Montenegro CAA detail page from %s", url)
```
"More than one HTTP request" means unbounded/high-volume (pagination or
per-entity) — see the audit below for why 2-request runners are exempt.

### Request-count audit

| Category | Runners | URL-at-DEBUG rule |
|----------|---------|--------------------|
| Single request | `airportwebcams-special-liveries`, `at-austrocontrol-registry`, `au-casa-registry`, `br-anac-registry`, `bz-bdca-registry`, `ca-transport-canada-registry`, `ch-bazl-registry`, `cy-dca-registry`, `ee-transpordiamet-registry`, `es-aesa-registry`, `fr-dgac-registry`, `ge-gcaa-registry`, `is-samgongustofa-registry`, `kg-caa-registry`, `kr-koca-registry`, `ky-caa-registry`, `lv-caa-registry`, `md-caa-registry`, `mictronics`, `no-caa-registry`, `nz-caa-registry`, `ourairports`, `rs-cad-registry`, `tc-caa-registry`, `us-faa-registry`, `vrs-standing-data` (26) | N/A — one call, already logged at `INFO` |
| Two-step discovery (fetch an index/listing page, then fetch the one file URL found there — fixed at 2 calls, not pagination) | `bg-caa-registry`, `bs-caa-registry`, `gg-2reg-registry`, `hr-ccaa-registry`, `hu-kozhaf-registry`, `im-ardis-registry`, `lu-dac-registry`, `mk-caa-registry`, `mv-caa-registry`, `nl-ilt-registry`, `pg-casapng-registry`, `sg-caas-registry`, `sk-nsat-registry` (13) | N/A — already compliant. All 13 already log both the discovery URL and the resolved file URL at `INFO` (e.g. `logger.info("Downloading Bulgaria CAA index page from %s", _INDEX_URL)`). 2 lines for 2 total requests isn't the per-request noise `DEBUG` exists to declutter, and downgrading them would make this visibility opt-in instead of on by default. |
| Multi-request: pagination or per-entity (unbounded call count) | `cz-caa-registry` (per-ID detail), `me-caa-registry` (paginated list + per-registration detail), `uk-caa-registry` (676 prefix searches + per-aircraft detail) (3) | Compliant — all 3 log the URL of every call at `DEBUG`. |
