# Data Runners

Each subdirectory is a self-contained data runner: download → normalize →
write to Redis with TTL → exit. Runners are scheduled on their own cadence
via the `ofelia` cron container (see `config/ofelia/config.ini`). See the
root `README.md` for how to run one manually or bulk-load all of them.

## Logging

Every runner calls `configure_logging(cfg.get("log_level"))` from
`shared/logging_setup.py` right after loading `settings.json`, wiring the
`log_level` config field (`"debug"` or `"info"`, see
`config/runners/settings.json.example`) to the root logger — `receiver` and
`processor` use the same helper. `configure_logging()` is called once more,
with no argument, if `settings.json` itself can't be loaded, so that failure
still logs formatted instead of falling back to Python's default handler
(see #331).

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
Bringing the remaining runners identified below up to this rule is
out of scope for this issue — see the follow-up issues instead of deciding
those diffs here.

### Request-count audit

| Category | Runners | URL-at-DEBUG rule |
|----------|---------|--------------------|
| Single request | `at-austrocontrol`, `au-casa`, `br-anac`, `bz-bdca`, `ca-transport-canada`, `ch-bazl`, `cy-dca`, `ee-transpordiamet`, `es-aesa`, `fr-dgac`, `ge-gcaa`, `is-samgongustofa`, `kg-caa`, `kr-koca`, `ky-caa`, `lv-caa`, `md-caa`, `mictronics`, `no-caa`, `nz-caa`, `ourairports`, `rs-cad`, `tc-caa`, `us-faa` (24) | N/A — one call, already logged at `INFO` |
| Two-step discovery (fetch an index/listing page, then fetch the one file URL found there — fixed at 2 calls, not pagination) | `bg-caa`, `bs-caa`, `gg-2reg`, `hr-ccaa`, `hu-kozhaf`, `im-ardis`, `lu-dac`, `mk-caa`, `mv-caa`, `nl-ilt`, `pg-casapng`, `sg-caas`, `sk-nsat` (13) | Lower priority — worth logging both URLs at `DEBUG` for consistency, not urgent |
| Multi-request: pagination or per-entity (unbounded call count) | `cz-caa` (per-ID detail), `me-caa` (paginated list + per-registration detail), `uk-caa` (676 prefix searches + per-aircraft detail) (3) | Primary target. `cz-caa`/`me-caa` already comply; `uk-caa` does not — see follow-up issues |

Follow-up issues for bringing runners up to the URL-at-DEBUG rule:
#440 for `uk-caa`, #441 for the two-step discovery batch.
