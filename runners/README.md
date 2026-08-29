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

Most runners write registration/airport data to Redis with a fixed 14-day
TTL (`ENRICHMENT_TTL_SECONDS` in `shared/timing.py`). `vrs-standing-data`
is the one exception: its source updates daily rather than weekly, so it
writes with a fixed 3-day TTL (`ROUTE_TTL_SECONDS`). Neither is
operator-configurable — see [Timing and cadences](https://github.com/BrentIO/SkyFollower/blob/main/docs/architecture/timing.md).

Each runner publishes a single MQTT message on completion with `records_imported`,
`last_run_at`, and `last_run_status`.

## Container image

Every runner's `Dockerfile` bases on `python:3.14-slim`, pinned to an
explicit digest: `FROM python:3.14-slim@sha256:<digest>`. The bare tag alone
is not enough — Docker Hub re-pushes it under the same name on every
upstream patch, and Dependabot only tracks (and opens a rebuild PR for) a
digest that is already present in the `FROM` line. When adding a runner,
copy the digest from an existing runner's `Dockerfile`, or resolve the
current one with `docker buildx imagetools inspect python:3.14-slim`. A CI
guard in `.github/workflows/run-tests.yaml` fails the build if any
`Dockerfile` has an unpinned base image.

## Configuration

Every runner reads the same two config blocks via `shared/config.py`'s
`load_config("redis", "mqtt")` — one call, so a runner started with
something missing reports every missing variable together rather than one
per restart. The enrichment-key TTL is not one of these variables: it is a
fixed constant (`ENRICHMENT_TTL_SECONDS`, or `ROUTE_TTL_SECONDS` for
`vrs-standing-data`) in `shared/timing.py`. Each runner's own README notes
which Redis key(s) it writes.

| Variable | Required | Default | Notes |
|---|---|---|---|
| `REDIS_HOST` | ✅ | — | Redis connection host |
| `REDIS_PORT` | ❌ | `6379` | |
| `REDIS_PASSWORD` | ✅ | — | Redis now requires authentication; every runner authenticates through `shared/redis_client.py`'s `build_redis_client()` |
| `MQTT_HOST` | ❌ | — | Leave unset to skip completion-stats publishing entirely — `MQTT_HOST`/`USERNAME`/`PASSWORD` are optional everywhere, not just for runners |
| `MQTT_PORT` | ❌ | `1883` | |
| `MQTT_USERNAME` | ❌ | — | Optional MQTT auth; leave unset for an anonymous broker |
| `MQTT_PASSWORD` | ❌ | — | |
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
| Single request | `airportwebcams-special-liveries`, `at-austrocontrol-registry`, `au-casa-registry`, `br-anac-registry`, `bz-bdca-registry`, `ca-transport-canada-registry`, `ch-bazl-registry`, `cy-dca-registry`, `ee-transpordiamet-registry`, `es-aesa-registry`, `fr-dgac-registry`, `ge-gcaa-registry`, `is-samgongustofa-registry`, `jo-carc-registry`, `kg-caa-registry`, `kr-koca-registry`, `ky-caa-registry`, `lv-caa-registry`, `md-caa-registry`, `mictronics`, `mo-aacm-registry`, `no-caa-registry`, `nz-caa-registry`, `ourairports`, `rs-cad-registry`, `sc-scaa-registry`, `tc-caa-registry`, `tg-anac-registry`, `us-faa-registry`, `vrs-standing-data` (30) | N/A — one call, already logged at `INFO` |
| Two-step discovery (fetch an index/listing page, then fetch the one file URL found there — fixed at 2 calls, not pagination) | `ba-bhdca-registry`, `bg-caa-registry`, `bs-caa-registry`, `gg-2reg-registry`, `hr-ccaa-registry`, `hu-kozhaf-registry`, `im-ardis-registry`, `lk-caa-registry`, `lu-dac-registry`, `mk-caa-registry`, `mv-caa-registry`, `nl-ilt-registry`, `pg-casapng-registry`, `sg-caas-registry`, `sk-nsat-registry`, `sr-casas-registry`, `tt-caa-registry` (17) | N/A — already compliant. All 17 already log both the discovery URL and the resolved file URL at `INFO` (e.g. `logger.info("Downloading Bulgaria CAA index page from %s", _INDEX_URL)`). 2 lines for 2 total requests isn't the per-request noise `DEBUG` exists to declutter, and downgrading them would make this visibility opt-in instead of on by default. |
| Multi-request: pagination or per-entity (unbounded call count) | `cz-caa-registry` (per-ID detail), `me-caa-registry` (paginated list + per-registration detail), `uk-caa-registry` (676 prefix searches + per-aircraft detail) (3) | Compliant — all 3 log the URL of every call at `DEBUG`. |
