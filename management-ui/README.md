# management-ui

| | |
|---|---|
| **Purpose** | REST API and React frontend for rules and areas configuration. The backend is the sole write path for `config:rules` / `config:areas` in Redis — every message processor polls the corresponding `:version` key every 30 seconds and hot-reloads on change |
| **Auth** | None (home lab deployment) |
| **Reads/writes** | Redis only |

Named "management" to leave room for a future, separate UI focused on
viewing live aircraft movement rather than editing configuration.

## Status

The React rules editor (`frontend/`) is built; the areas editor is not yet.
The Dockerfile is a multi-stage build — a `node` stage produces the static
frontend bundle, and the final stage runs both uvicorn (bound to
`127.0.0.1:8000`, not exposed outside the container) and nginx, started by
`entrypoint.sh`. nginx serves the built frontend at `/` (with a `try_files`
fallback to `index.html` for client-side routing) and proxies `/api/*` to
uvicorn. `docker-compose.management-ui.yaml` maps `8080:80`.

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
  content area. Rules is the only section for now; a future areas editor
  adds an entry to `SideNav.tsx`'s section list without touching `Layout.tsx`.
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
| `log_level` | string | `"info"` | Set to `"debug"` for verbose output |

The settings file path defaults to `/app/settings.json` and can be
overridden with the `SETTINGS_PATH` environment variable.

## Backing up `config:rules`/`config:areas`

![Startup backup reconciliation](./config-backup-reconcile-sequence.svg)

`config:rules` and `config:areas` are the only two Redis keys in the whole
schema holding user-authored/curated state with no automatic regeneration
path (every other key is either repopulated by a data-runner on its next
scheduled run, or transient operational state that naturally rebuilds from
live traffic). Redis's own AOF (`docker-compose.server.yaml`'s
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
`management-ui-data:/app/data` volume mount (same convention as
`message-processor-0-archive`'s and `archive-data`'s `/app/data` mounts),
and can be overridden with the `DATA_DIR` environment variable.

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
