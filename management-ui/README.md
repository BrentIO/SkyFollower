# management-ui

| | |
|---|---|
| **Purpose** | REST API for rules and areas configuration, backing the React rules/areas editors (#15, #16). The sole write path for `config:rules` / `config:areas` in Redis — every message processor polls the corresponding `:version` key every 5 seconds and hot-reloads on change |
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

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/rules` | Current rules array. `200` with the array, or `204` if none configured yet |
| `PUT` | `/api/rules` | Replace the full rules array. Validated with the same logic the message processor's rules engine uses (`message-processor/rules_engine.py`, imported directly — not duplicated). `200` with the saved array echoed back, `400` with a detail message on validation failure |
| `GET` | `/api/areas` | Current GeoJSON FeatureCollection of named areas. `200`, or `204` if none configured yet |
| `PUT` | `/api/areas` | Replace the areas FeatureCollection. Same validate-then-write pattern as rules |

Aircraft lookup (`GET /api/aircraft/{icao_hex}`, `GET /api/aircraft?registration={reg}`)
and missing-operator reporting (`GET /api/operators/missing`) aren't built yet —
removed for now rather than kept as `501` stubs; see CLAUDE.md's Open Items
("UI expansion").

Every successful `PUT` computes a SHA-256 hash of the saved JSON and writes
it to the matching `:version` key (`config:rules:version` /
`config:areas:version`), which is what message processors actually poll —
they never read `config:rules` / `config:areas` itself unless the hash has
changed.

A GeoJSON area referenced by an `area` condition in a rule must already
exist in the saved areas config, or the `PUT /api/rules` validating that
rule fails with `400` — areas and rules validate against the same
in-process `RulesEngine` instance, so save areas before rules that
reference them.

The full request/response schema is in `specs/openapi.yaml`, exported from
this app's own OpenAPI document (see below). Rule/Condition/Area shapes
there (`Rule`, `Condition`, `AreaFeatureCollection`) are documentation-only
Pydantic models roughly matching SkyFollower-legacy's `rules.example.json` /
`areas.example.geojson` conventions (condition values are strings even for
numeric fields, e.g. altitude `"10000"`) — they aren't the actual route
parameter types, so they document the schema without becoming a second
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
