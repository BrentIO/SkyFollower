# management-ui

| | |
|---|---|
| **Purpose** | REST API for rules and areas configuration, backing the React rules/areas editors (#15, #16). The sole write path for `config:rules` / `config:areas` in Redis — every message processor polls the corresponding `:version` key every 30 seconds and hot-reloads on change |
| **Auth** | None (home lab deployment) |
| **Reads/writes** | Redis only |

Named "management" to leave room for a future, separate UI focused on
viewing live aircraft movement rather than editing configuration.

## Status

Backend-only for now. The frontend (React rules/areas editors) doesn't
exist yet — see #15 and #16 — so this image currently runs uvicorn directly
on port 8000 with no nginx in front of it. Once the frontend lands, the
Dockerfile grows a node build stage and nginx starts proxying `/api/*` to
uvicorn while serving the built frontend at `/`; `docker-compose.management-ui.yaml`'s
port mapping moves from `8080:8000` back to `8080:80` at that point.

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
shapes there are documentation-only Pydantic models roughly matching
SkyFollower-legacy's `rules.example.json` / `areas.example.geojson`
conventions (condition values are strings even for numeric fields, e.g.
altitude `"10000"`) — they aren't the actual route parameter types (those
stay plain `dict`), so they document the schema without becoming a second
validation layer that could fight `RulesEngine`'s own (more permissive)
rules.

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
