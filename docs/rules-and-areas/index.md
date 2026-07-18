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
    "conditions": [
      { "type": "wake_turbulence_category", "operator": "equals", "value": "heavy" },
      { "type": "altitude", "operator": "maximum", "value": 5000 },
      { "type": "vertical_speed", "operator": "maximum", "value": -100 }
    ]
  }
]
```

Available condition types: `altitude`, `heading`, `velocity`, `vertical_speed`,
`area`, `date`, `ident`, `squawk`, `military`, `operator_airline_designator`,
`aircraft_type_designator`, `aircraft_registration`, `aircraft_icao_hex`,
`aircraft_powerplant_count`, `wake_turbulence_category`, `matched_rules`.

See the [Processor docs](/components/processor) for operator and constraint details.

## Areas

Named geographic polygons used with the `area` condition type. Stored in Redis
(`config:areas`) as a GeoJSON FeatureCollection and edited through the UI's map
editor.

Example `areas.example.json`:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": { "name": "Airport Approach" },
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
