# Rules & Areas

## Rules

Rules tell SkyFollower which aircraft to alert on. They are stored in Redis
(`config:rules`) and edited through the UI. A rule fires at most once per flight
per rule identifier. All conditions within a rule must match simultaneously (AND
logic).

Example `rules.example.json` entry:

```json
[
  {
    "identifier": "heavy-arrivals",
    "name": "Heavy aircraft arriving",
    "description": "Any heavy aircraft descending below 5000 ft",
    "enabled": true,
    "force_archive": false,
    "conditions": [
      { "type": "wake_turbulence_category", "operator": "equals", "value": "heavy" },
      { "type": "altitude", "operator": "maximum", "value": 5000 },
      { "type": "vertical_speed", "operator": "maximum", "value": -100 }
    ]
  }
]
```

`force_archive` is a boolean, defaulting to `false`. External-only flights
(where the flight's accumulated `receiver_sources` is exactly `["EXTERNAL"]`)
are dropped rather than written to S3 — see the
[Archive Processor docs](/components/archive-processor). Setting
`force_archive: true` on a rule overrides that skip for any flight matching
the rule, so an external-only flight the user cares about still gets archived.

Available condition types: `altitude`, `heading`, `velocity`, `vertical_speed`,
`area`, `date`, `ident`, `squawk`, `military`, `receiver_source`,
`operator_airline_designator`, `aircraft_type_designator`,
`aircraft_registration`, `aircraft_icao_hex`, `aircraft_powerplant_count`,
`wake_turbulence_category`, `matched_rules`.

See the [Message Processor docs](/components/message-processor) for operator and constraint details.

## Rule notifications

When a rule matches a flight, the message processor publishes a JSON
snapshot of that flight to MQTT topic `SkyFollower/rule/{identifier}`,
where `{identifier}` is the matching rule's `identifier`. This is a
one-shot, non-retained publish — nothing is republished if a client
connects late, and a rule fires at most once per flight per identifier, so
each match is announced exactly once.

Two conditions gate the publish:

- The message processor's MQTT connection must be up at the moment of the
  match. If MQTT is down (or not configured), the match is still recorded
  internally (it counts toward `matched_rules` and the rule-trigger
  metrics), but no notification is sent.
- The triggering message must be recent. If the message that caused the
  match is older than `RULE_NOTIFICATION_MAX_LAG_SECONDS` (30 seconds,
  `shared/timing.py`) — which happens when a RabbitMQ backlog is replayed
  after downtime — the notification is suppressed as no longer
  actionable.

### Payload

The payload is a `CompletedFlight` snapshot (the same shape archived to
S3) with `_id`, `positions`, and `velocities` removed, plus a `rule`
object identifying the match:

```json
{
  "aircraft": { "icao_hex": "A1B2C3", "registration": "N12345", "...": "..." },
  "ident": "DAL659",
  "operator": { "airline_designator": "DAL", "name": "Delta Air Lines", "...": "..." },
  "origin": { "icao_code": "KATL", "name": "Hartsfield-Jackson Atlanta International Airport", "...": "..." },
  "destination": { "icao_code": "KJFK", "...": "..." },
  "receiver_sources": ["1090"],
  "first_message": "2026-01-15T10:00:00+00:00",
  "last_message": "2026-01-15T10:45:00+00:00",
  "total_messages": 3842,
  "matched_rules": ["military-aircraft"],
  "rule": {
    "name": "Military Aircraft",
    "description": "Matches any aircraft with military=true",
    "identifier": "military-aircraft"
  }
}
```

See the `FlightRuleNotification` schema on the
[MQTT Reference](/specs/asyncapi) page for the full field list.

### Omit-when-empty fields

A field with nothing to report is left out of the payload entirely rather
than serialized as `null` or `false`:

- `operator`, `registrant`, `origin`, `destination` — omitted whenever
  that lookup didn't resolve for the flight.
- `aircraft.military` — omitted when the aircraft is civilian; present
  and `true` only for military aircraft.
- `force_archive` — omitted when `false`; present (`true`) only when a
  matching rule overrode the external-only archive skip for this flight.
- `matched_rules` — always present, and always contains at least the
  identifier of the rule that triggered this notification (a flight can
  match more than one rule before the notification is sent, in which case
  all of them appear).

### Migrating Home Assistant automations from legacy

The legacy SkyFollower rule payload and this one differ in a few places.
Most are a straight field rename or reshape that only matters if an
automation reads that specific field; `aircraft.military` is the one
change that needs code on the automation side, because it changes from a
value comparison to a presence check.

| Field | Legacy | Next-gen | Action for automations |
|---|---|---|---|
| `aircraft.military` | Always present (`false` when civil) | Omitted when `false`; present and `true` only for military aircraft | Test for presence or `=== true`, not `=== false`. |
| `aircraft.category` | Single field, e.g. `"LandPlane"` | Split into `type` (e.g. `"Airplane"`) and `category` (e.g. `"Land"`) | Re-point any `category == "LandPlane"`-style checks at the new fields. |
| `aircraft.wake_turbulence_category` | e.g. `"Medium 2"` | e.g. `"medium"` (lowercase; `light` / `medium` / `heavy`) | Update string comparisons. |
| `aircraft.powerplant.type` | e.g. `"Jet"` | e.g. `"Turbo-fan"` (richer source data) | Update if matched on. |
| `operator.source` | Present (e.g. `"Mictronics-IndexedDB"`) | Removed | Drop any dependency on it. |
| `origin` / `destination` | Full airport object | Full airport object — unchanged | No action needed. |
| `first_message` / `last_message` | Naive datetime string | ISO-8601 with a UTC offset | Most parsers accept both; a naive string splitter may break. |
| New top-level fields | — | `receiver_sources`, `force_archive` (when `true`), `matched_rules` | Automations should tolerate unknown keys rather than fail on them. |

## Areas

Named geographic polygons used with the `area` condition type. Stored in Redis
(`config:areas`) as a GeoJSON FeatureCollection and edited through the UI's map
editor. Each area has an `identifier` (no spaces — this is what a rule's `area`
condition matches against, e.g. `{ "type": "area", "value": "APPROACH" }`) and
a separate, optional `name` for display, which can contain spaces.

The management UI's `GET`/`POST`/`PUT`/`DELETE /api/areas` endpoints expose a
flattened `{identifier, name, geometry}` shape rather than this GeoJSON
FeatureCollection directly — see `management-ui/README.md`.

Example `areas.example.json`:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": { "identifier": "APPROACH", "name": "Airport Approach" },
      "geometry": {
        "type": "Polygon",
        "coordinates": [[
          [-84.45, 33.60],
          [-84.35, 33.60],
          [-84.35, 33.70],
          [-84.45, 33.70],
          [-84.45, 33.60]
        ]]
      }
    }
  ]
}
```
