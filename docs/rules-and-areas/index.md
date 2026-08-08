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

`force_archive` is a boolean, defaulting to `false`. MLAT-only flights (where
the flight's accumulated `receiver_sources` is exactly `["MLAT"]`) are dropped
rather than written to S3 — see the
[Archive Processor docs](/components/archive-processor). Setting
`force_archive: true` on a rule overrides that skip for any flight matching
the rule, so an MLAT-only flight the user cares about still gets archived.

Available condition types: `altitude`, `heading`, `velocity`, `vertical_speed`,
`area`, `date`, `ident`, `squawk`, `military`, `receiver_source`,
`operator_airline_designator`, `aircraft_type_designator`,
`aircraft_registration`, `aircraft_icao_hex`, `aircraft_powerplant_count`,
`wake_turbulence_category`, `matched_rules`.

See the [Message Processor docs](/components/message-processor) for operator and constraint details.

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
