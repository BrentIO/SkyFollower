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

Reference-data lookup (aircraft, operator, airport, route — reading current
Redis enrichment state, not the S3/Parquet flight archive) isn't built yet;
see CLAUDE.md's Open Items ("UI expansion"). A previously-considered
missing-operator reporting endpoint (`GET /api/operators/missing`) was
dropped as a legacy carryover rather than planned.

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
shapes there are documentation-only Pydantic models roughly matching
SkyFollower-legacy's `rules.example.json` / `areas.example.geojson`
conventions (condition values are strings even for numeric fields, e.g.
altitude `"10000"`) — they aren't the actual route parameter types (those
stay plain `dict`), so they document the schema without becoming a second
validation layer that could fight `RulesEngine`'s own (more permissive)
rules.

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
- `views/RulesView.tsx` — rule list (with inline enable/disable toggle) +
  selected rule's form; owns save/discard/delete state and the two confirm
  flows.
- `components/RuleForm.tsx` / `components/ConditionForm.tsx` — the rule
  editor and its per-condition, type-aware value input (number, hex,
  wake-turbulence dropdown, heading min/max pair, `matched_rules`
  multi-select, area dropdown sourced from `GET /api/areas`, and the `date`
  condition's Date vs. Date-and-time format selector, which converts a
  local `datetime-local` input to UTC and appends `Z` before saving).

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
