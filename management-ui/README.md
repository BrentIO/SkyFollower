# management-ui

| | |
|---|---|
| **Purpose** | REST API and React frontend for rules/areas configuration and archive search history. The backend is the sole write path for `config:rules` / `config:areas` in Redis — every message processor polls the corresponding `:version` key every 30 seconds and hot-reloads on change |
| **Auth** | None (single-instance, trusted-network deployment) |
| **Reads/writes** | Redis only |

Named "management" to leave room for a future, separate UI focused on
viewing live aircraft movement rather than editing configuration.

## Status

The React rules editor, areas editor, and archive search history page
(`frontend/`) are all built.
The Dockerfile is a multi-stage build — a `node` stage produces the static
frontend bundle, and the final stage runs both uvicorn (bound to
`127.0.0.1:8000`, not exposed outside the container) and nginx, started by
`entrypoint.sh`. nginx serves the built frontend at `/` (with a `try_files`
fallback to `index.html` for client-side routing) and proxies `/api/*` to
uvicorn. `docker-compose.management-ui.yaml` maps `80:80`.

## Endpoints

Per-item CRUD, not bulk replace — every rule and area is independently
addressable by `identifier`. `config:rules` / `config:areas` in Redis still
each store one JSON blob (the full array/collection — that's what message
processors poll and hot-reload), so every write below is read-full-array,
splice, validate, write-back, not a partial update in Redis itself.

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/rules` | All rules. `200` with the array (empty if none configured) |
| `GET` | `/api/rules/{identifier}` | One rule. `200`, or `404` if not found |
| `POST` | `/api/rules` | Add one new rule. `201` with the created rule, `409` if `identifier` already exists, `400` on validation failure |
| `PUT` | `/api/rules/{identifier}` | Replace one existing rule. `200` with the updated rule, `404` if `identifier` doesn't exist yet (use `POST` to create), `400` if the body's `identifier` doesn't match the path or on validation failure |
| `DELETE` | `/api/rules/{identifier}` | Remove one rule. `204`, or `404` if not found |
| `GET` | `/api/areas` | All areas, flattened: `[{identifier, name, geometry}, ...]` — not a GeoJSON FeatureCollection; see below |
| `GET` | `/api/areas/{identifier}` | One area. `200`, or `404` if not found |
| `POST` | `/api/areas` | Add one new area. `201`, `409` if `identifier` already exists, `400` on validation failure |
| `PUT` | `/api/areas/{identifier}` | Replace one existing area. `200`, `404` if `identifier` doesn't exist yet, `400` on mismatch/validation failure |
| `DELETE` | `/api/areas/{identifier}` | Remove one area. `204`, or `404` if not found |

Both `Rule` and `Area` require `identifier` (routing key, no spaces —
`message-processor/rules_engine.py` rejects a spaced rule identifier
outright; an area with a spaced or missing identifier is silently dropped
by the engine's existing lenient per-feature area loading, which this
backend turns into an explicit `400` by checking the identifier actually
survived the reload). `name` is a separate, optional free-text display
label that *can* contain spaces.

Aircraft lookup (`GET /api/aircraft/{icao_hex}`, `GET /api/aircraft?registration={reg}`)
and missing-operator reporting (`GET /api/operators/missing`) aren't built yet —
removed for now rather than kept as `501` stubs; see CLAUDE.md's Open Items
("UI expansion").

Every successful write recomputes the full array/collection, computes a
SHA-256 hash of it, and writes both to Redis (`config:rules` /
`config:areas` and their `:version` counterparts) — `:version` is what
message processors actually poll; they never read `config:rules` /
`config:areas` itself unless the hash has changed.

### Areas: flattened API shape vs. GeoJSON storage

The API's `Area` shape (`{identifier, name, geometry}`) is not what's
stored in Redis or handed to `RulesEngine` — `config:areas` is still a
GeoJSON `FeatureCollection`, each `Feature` with `properties: {identifier,
name}` and the area's `geometry`, since that's what `RulesEngine` and the
message processor expect. This backend translates between the two shapes
at the API boundary (`_area_to_feature`/`_feature_to_area` in `main.py`) —
clients of this API never see the `FeatureCollection` wrapper.

An area referenced by a rule's `area` condition is matched by `identifier`,
not `name` (e.g. `{"type": "area", "value": "LI"}` matches an area whose
`identifier` is `"LI"`, regardless of its display `name`). A rule
referencing an area `identifier` that doesn't exist fails with `400` —
areas and rules validate against the same in-process `RulesEngine`
instance, so save the area before a rule that references it.

The full request/response schema is in `specs/openapi.yaml`, exported from
this app's own OpenAPI document (see below). `Rule`/`Condition`/`Area`
shapes there roughly match SkyFollower-legacy's `rules.example.json` /
`areas.example.geojson` conventions (condition values are strings even
for numeric fields, e.g. altitude `"10000"`), and ARE the actual route
parameter types for create/update — a request body that doesn't match
(wrong type, an operator not valid for a condition's `type`, a missing
field) gets a `422` from FastAPI/Pydantic before the route function ever
runs. `Condition` is a `type`-discriminated union of one model per
condition type, so each one's `operator` is restricted to only the values
that type actually supports (e.g. `date` only accepts `minimum`/
`maximum`), rather than a single flat model allowing all 5 operators
everywhere. `RulesEngine` remains a second, independent validation layer
underneath this one — not made redundant by it, since it's the only
validation applied to `config:rules`/`config:areas` written some other way
than through this API (a hand-edited Redis value, a restored backup, a
future integration), and it enforces things a single condition's fields
can't express on their own (e.g. an `area` condition's value must name an
area that actually exists in `config:areas`).

## Frontend (`frontend/`)

React (TypeScript) + Vite + Tailwind CSS + React Router. No other UI
component library, to keep dependencies minimal for ARM builds.

- `Layout.tsx` / `components/SideNav.tsx` — app shell: side nav + routed
  content area. Rules and Areas are both entries in `SideNav.tsx`'s section
  list; a future section adds another entry there without touching
  `Layout.tsx`.
- `hooks/useToast.ts` + `components/ToastContainer.tsx` — shared success/error
  toast notifications.
- `components/ConfirmModal.tsx` — generic confirm dialog (discard unsaved
  changes, delete confirmation).
- `api/client.ts` — thin `fetch` wrapper: JSON parsing and `{"detail": "..."}`
  error surfacing from any 4xx/5xx response.
- `api/rules.ts` — typed client for `/api/rules/*`, plus the `Rule`/`Condition`
  types and the `OPERATORS_BY_TYPE` map used to filter each condition's
  operator dropdown by its type.
- `views/RulesView.tsx` — rule list (each row showing a red "Not Enabled"
  pill when disabled, or a hollow gray "Inactive" pill when enabled but a
  `date` condition isn't currently satisfied) + selected rule's form; owns
  save/discard/delete state and the two confirm flows. Enabling/disabling
  a rule is only done by editing its Enabled checkbox and saving -- no
  inline toggle in the list.
- `components/RuleForm.tsx` / `components/ConditionForm.tsx` — the rule
  editor and its per-condition, type-aware value input (number, hex,
  wake-turbulence dropdown sourced alphabetically and title-cased for
  display, heading min/max pair, `matched_rules` checkbox list with
  includes/excludes operator labels, area dropdown sourced from
  `GET /api/areas`, and the `date` condition's datetime input, which
  converts a local `datetime-local` value to UTC and appends `Z` before
  saving -- the UI always saves the `YYYY-MM-DDTHH:MMZ` form; the
  backend's date-only `YYYY-MM-DD` format is still accepted if written
  some other way, e.g. directly via the API).
- `api/areas.ts` — typed client for `/api/areas/*`.
- `views/AreasView.tsx` — MapLibre GL JS + `@mapbox/mapbox-gl-draw` (works
  against `maplibre-gl`'s Mapbox-GL-compatible API; there is no scoped
  `@maplibre/maplibre-gl-draw` package). Existing areas load as Draw
  features on mount, map bounds auto-fit to them, and each polygon gets a
  centroid label showing its `name`. Drawing a new polygon opens
  `components/AreaNameModal.tsx` to collect `identifier`/`name`, then saves
  immediately via `POST /api/areas` (no separate "Save All" step -- the
  backend is per-item CRUD, not a bulk replace, so each area is created/
  updated/deleted independently, the same as `RulesView.tsx`). Selecting an
  existing area (from the side list or by clicking its polygon) enters
  Draw's `direct_select` mode for vertex editing and shows an inline Name
  field + Save/Discard/Delete in the side list; geometry edits on the map
  and name edits in the list share the same dirty/save/discard state.
- `api/archiveSearch.ts` — typed client for `/api/archive/search/*` and
  `/api/archive/flights/{token}`. `downloadArchiveFlight` fetches via
  `Blob` + a synthetic `<a download>` click rather than a plain
  navigation, specifically so an expired/invalid token's `400` can be
  caught and shown as a toast instead of the browser just rendering a
  raw JSON error page.
- `views/HistoryView.tsx` — search list (left) + selected search's
  results (right on desktop; the same content renders inline as an
  accordion under the selected list entry on mobile instead, since
  there's no persistent side-by-side space below the `md:` breakpoint).
  Polls `GET /api/archive/search` every few seconds while any visible
  search is `RUNNING`, so status pills update to `COMPLETE`/`FAILED`/
  `ABORTED` live. Results (once `COMPLETE`) are fetched and cached
  per-page client-side, keyed by `{uuid}:{page}`, so paging back to an
  already-viewed page never re-fetches. `components/NewSearchModal.tsx`
  collects `name` + a raw SQL `where_clause` (plain `<textarea>`, not a
  syntax-highlighted editor) alongside a persistent column-reference
  legend, since the whole point of that field is writing SQL against
  exact column names from memory.

Client-side validation (`validateRule`/`validateCondition` in `RuleForm.tsx`)
mirrors `message-processor/rules_engine.py`'s per-type checks as a fast-fail
UX nicety — the backend's `400` response is still the source of truth.

```bash
cd management-ui/frontend
npm install
npm run dev    # Vite dev server on :5173, proxying /api to localhost:8000
npm run build  # type-checks (tsc -b) then builds the static bundle to dist/
```

## Configuration (`settings.json`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `redis.host` | string | `"localhost"` | |
| `redis.port` | integer | `6379` | |
| `s3.bucket` | string | — | S3 bucket the archive is written to (see [Archive Search](#archive-search)) |
| `s3.region` | string | `"us-east-1"` | |
| `s3.access_key_id` | string | — | |
| `s3.secret_access_key` | string | — | |
| `athena.workgroup` | string | — | Athena workgroup to run queries against (see [Archive Search](#archive-search)) |
| `athena.database` | string | — | Glue database name |
| `athena.table` | string | — | Glue table name (`archive_flights` by default in `specs/aws/glue-table-definition.json`, but this is operator-configured, not assumed) |
| `log_level` | string | `"info"` | Set to `"debug"` for verbose output |

The settings file path defaults to `/app/settings.json` and can be
overridden with the `SETTINGS_PATH` environment variable.

## Archive Search

Athena/Glue query layer over the archive's Parquet index (see
[Archive Processor](../archive-processor/README.md)'s Parquet Index
section and `specs/data-dictionary.yaml`'s `archive_parquet_index`
record for the 9 underlying columns), with a History page
(`views/HistoryView.tsx`) in the frontend below.

![Archive search: create, background poll, fetch results, fetch a flight, delete](./archive-search-sequence.svg)

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/archive/search` | Start a search: `{name, where_clause}` body. `HTTP 202` with `{uuid}` immediately — the query itself runs in the background (see Background polling below). `HTTP 502` if Athena rejects the query outright (e.g. a permissions mismatch) |
| `GET` | `/api/archive/search` | List all current search records: `{uuid, name, status, submitted_at, expires_at, error}` each (`error` is only ever set for `FAILED`/`ABORTED`) |
| `GET` | `/api/archive/search/{uuid}` | One search record, including its `where_clause`. `HTTP 404` if not found |
| `GET` | `/api/archive/search/{uuid}/results?page={n}` | One page (100 rows) of a `COMPLETE` search's results: `{rows: [...], total_rows}` — `total_rows` is the full match count, not just this page's length, for the frontend's Prev/Next/"Page X of Y" pagination. `HTTP 404` if not found, `HTTP 400` if the search isn't `COMPLETE` yet, `HTTP 502` if Athena/S3 fails to locate or return the result file (e.g. a permissions mismatch, or the file is already gone) |
| `DELETE` | `/api/archive/search/{uuid}` | Delete a search record — best-effort-cancels the Athena query if still `RUNNING`, deletes the Athena result file from S3 if `COMPLETE`, always deletes the Redis record. `HTTP 204`, or `HTTP 404` if not found |
| `GET` | `/api/archive/flights/{token}` | Download one flight's full archived record (gzipped JSON) by its opaque, encrypted fetch token — never a raw S3 key. `HTTP 400` on an invalid/expired token, `HTTP 502` if S3 fails to return the object (e.g. it's already gone, or a permissions mismatch) |

Every `HTTP 502` above shares the same `_AWS_ERROR` response contract: any AWS-side failure (Athena or S3) is caught and surfaced as a single opaque `HTTP 502`, never the raw upstream AWS status code — so a `404`/`NoSuchKey` and a `403`/`AccessDenied` from S3 are indistinguishable to the caller, both just `HTTP 502`.

### Query construction: a raw, user-supplied `WHERE` clause

`where_clause` is genuinely user-authored SQL, not a set of structured
filter fields — a deliberate, informed trade-off given this tool's actual
exposure (single operator, no auth, not internet-facing), not an
oversight. The backend always controls the rest of the statement:

```sql
SELECT icao_hex, registration, type_designator, military, operator_designator,
       ident, first_message, last_message, s3_key
FROM {athena.database}.{athena.table}
WHERE ({where_clause})
```

`where_clause` only ever fills the parenthesized `WHERE` fragment — the
`SELECT` list, `FROM` table, and parentheses are never influenced by user
input, and there's no way to reach a different table. Still bounded by
more than the application layer alone: the querying IAM identity is
already read-only on just this one table (see [AWS Setup](#aws-setup)
below), and Athena executes one statement per `start_query_execution`
call, so no `;`-chained multi-statement injection is possible regardless
of content. On top of that, a cheap early rejection (before ever calling
Athena) 400s a `where_clause` containing `;` or a DDL/DML keyword
(`DROP`/`CREATE`/`ALTER`/`INSERT`/`DELETE`/`UPDATE`/`GRANT`, matched
word-boundary so `ident = 'INSERT1'` doesn't false-positive) — not a real
security boundary given the IAM scoping above, purely so a mistake
produces an instant, clear `400` instead of a slower, more opaque Athena
`AccessDenied`.

`s3_key` **is** selected from Athena (the backend needs it server-side to
mint each row's fetch token and derive its flight UUID — see "Flight
fetch" below) but is never included in any HTTP response body a browser
receives.

### Background polling

One daemon thread per in-flight search, started by the `POST` handler
right after `start_query_execution` returns. Polls Athena's
`GetQueryExecution` on exponential backoff (1s, 2s, 4s, 8s, 16s, then
capped at 30s) for up to 2 minutes wall-clock total. If the deadline is
hit without reaching a terminal state, the thread calls
`stop_query_execution` and marks the search `ABORTED` — deliberately
distinct from `FAILED` (Athena itself reported the query failed) so a
broken Glue table setup doesn't just look like a slow query in the log.
On every process startup, any record still `RUNNING` is immediately
marked `ABORTED` — its polling thread died with the previous process,
nothing is left alive to ever finish that job.

Every write the polling thread makes to Redis is a conditional
`SET ... XX KEEPTTL` (only if the key still exists, and never resets the
fixed 7-day TTL) — this is the thread-resurrection guard: if `DELETE`
removes the record between this thread's last read and its next write,
that write becomes a silent no-op instead of resurrecting a record the
user already deleted.

### Listing active searches

`GET /api/archive/search` and the startup reconciliation sweep above both
need every current `archive_search:{uuid}` record. A Redis `SET`
(`archive_search:index`, holding just each search's `uuid`, no TTL of its
own) is the source of truth for "what searches currently exist" — `SADD`
on create, `SREM` on delete, `SMEMBERS` + one `GET` per uuid to list.

A uuid whose backing `archive_search:{uuid}` key has already expired (the
fixed 7-day TTL) has no way to notify the index that it's gone on its
own, so the index self-heals opportunistically instead: any list/
reconcile call that `GET`s a member and finds it already gone `SREM`s
that stale uuid right there, pruning it the next time anyone asks.

### Result retrieval and pagination

Redis stores a pointer (the Athena `QueryExecutionId`) to the result set
in S3, not the result data itself — the polling thread never fetches or
caches rows at completion time. Rows are only fetched from S3 the first
time a user actually opens a completed search's results: one `GetObject`
downloads the entire result CSV (evaluated and rejected: S3 Select,
byte-range reads, and Athena's own sequential `GetQueryResults`
pagination — the whole archive index is only ~820MB per the original
design's own estimate, so any single query's matching rows are realistically
KB to low-single-digit-MB, well within "download it all and paginate from
memory" territory), parsed once, and cached in a bounded in-process LRU
(`OrderedDict`, move-to-end on access, capped at 10 concurrently-cached
result sets) keyed by search UUID — every subsequent page for that same
search slices the already-parsed list, no repeat S3 call. The cache lives
only in process memory and is wiped on restart: a page request for a
search that was mid-viewing when the container restarted is just a cache
miss (one slower request, cached again from there), not an error or data
loss.

### Flight fetch: encryption, not a raw or encoded key

The browser must never receive a real `s3_key` in any form — a reversible
encoding (e.g. base64) is equivalent to sending the plaintext key. Each
result row's `token` is its `s3_key`, encrypted with `Fernet`
(authenticated symmetric encryption from Python's `cryptography` library)
— a tampered/forged token simply fails to decrypt (`400`) rather than
decrypting to some attacker-chosen key. The Fernet key itself is
**ephemeral**: generated fresh via `Fernet.generate_key()` at every
process startup, held only in memory, never written to
`settings.json`/env/disk. A restart invalidates every token minted by the
previous process — a search still open in a browser tab across a restart
gets a `400` on "view flight" until the page is refreshed (which re-fetches
results from the now-running process, producing freshly-encrypted tokens).
Acceptable for a low-traffic tool with infrequent deploys; every restart
is effectively a free, automatic key rotation. This assumes a single
running instance of this component, matching this project's actual
deployment model (no horizontal scaling anywhere in its design).

### TTLs

- `archive_search:{uuid}` Redis records: **7 days from creation, fixed** —
  never refreshed on access/viewing, or deleted early via `DELETE`.
- `archive_search:index`: **no TTL** — it's a small, actively-maintained
  set of uuids (see [Listing active searches](#listing-active-searches)
  above), not a data record with its own expiry.
- Athena query-result files in S3: **8 days**, one day longer than the
  Redis TTL so Redis's own pointer always drops before the file it
  references actually disappears (an S3 lifecycle rule on the
  query-results prefix — part of [AWS Setup](#aws-setup) below, not
  something this backend enforces itself).

### AWS Setup

**Nothing described here exists yet in a fresh AWS account.** Like
`archive-processor` and `archive-compaction`, this backend never calls a
Glue, IAM, or Athena provisioning API itself — it only prepares a *local
reference file* an operator uses to create its IAM identity by hand. On
every startup, it resolves its own `__BUCKET_NAME__`-templated IAM policy
(baked into its image from `specs/aws/iam-policies/management-ui.json`)
against its configured `s3.bucket` and writes it to
`$DATA_DIR/aws-setup/iam-policy.json`, for pasting directly into the
console's JSON policy editor. Its Athena and Glue permissions are
`Resource: "*"` rather than scoped to a specific table/workgroup ARN — a
properly scoped ARN needs the AWS account ID, which nothing in this
project discovers or is configured with — but its S3 access (the actually
sensitive part) is fully scoped, same as `archive-processor`/
`archive-compaction`'s identities. See
[docs/aws-setup.md](../docs/aws-setup.md) for the full console click-path
setup guide (Glue database/table, Athena workgroup + query-results
location + lifecycle rule, and all three components' IAM identities).

## Backing up `config:rules`/`config:areas`

![Startup backup reconciliation](./config-backup-reconcile-sequence.svg)

`config:rules` and `config:areas` are the only two Redis keys in the whole
schema holding user-authored/curated state with no automatic regeneration
path (every other key is either repopulated by a runner on its next
scheduled run, or transient operational state that naturally rebuilds from
live traffic). Redis's own AOF (`docker-compose.core.yaml`'s
`redis-data` volume) is the only persistence for them otherwise, so this
backend keeps a second, independent copy:

- **On every successful save** (`_save_rules_array`/`_save_areas_array` in
  `main.py`), the same JSON body just written to Redis is also written
  atomically (temp file + `os.replace`, so a crash mid-write can't leave a
  truncated file) to `$DATA_DIR/rules-backup.json` /
  `$DATA_DIR/areas-backup.json`.
- **At startup**, before loading rules/areas into `RulesEngine`, if a
  Redis key is missing but its backup file exists and parses as valid
  JSON, the file's content is restored into Redis (both the key and its
  `:version` hash) and the restore is logged. If the key already has data
  in Redis, the backup file is never consulted — Redis is always the
  source of truth when it has data. A missing or corrupt backup file at
  startup is logged and skipped, not a crash (same empty-array behavior as
  today if nothing can be restored).
- `message-processor/rules_engine.py` needs no changes for this — once a
  restore writes the key back into Redis, the existing 30-second
  `config:*:version` poll picks it up the same way it picks up any other
  change.

`$DATA_DIR` defaults to `/app/data`, matching `docker-compose.management-ui.yaml`'s
`./data/management-ui:/app/data` bind mount (same convention as the message
processor's and archive processor's `/app/data` mounts), and can be
overridden with the `DATA_DIR` environment variable.

## Regenerating `specs/openapi.yaml`

```bash
cd management-ui/backend
python -c "
import yaml
import main
print(yaml.dump(main.app.openapi(), sort_keys=False, allow_unicode=True))
" > ../../specs/openapi.yaml
```

(`app.openapi()` only builds the schema from route declarations — it doesn't
touch Redis or need `settings.json`, so no running Redis or `SETTINGS_PATH`
is required to regenerate the spec.)

`info.version` in the generated document is always `9999.99.99` on `main`
— the release workflow substitutes the real version at build time, matching
`specs/asyncapi.yaml`'s convention.
