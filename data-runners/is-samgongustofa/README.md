# is-samgongustofa

| | |
|---|---|
| **Country** | Iceland |
| **Registration prefix** | `TF-` |
| **Data source** | https://island.is/api/graphql (Apollo Persisted Query) |
| **Format** | JSON API (GraphQL persisted query, fixed `sha256Hash`, single page of up to 10,000 results) |
| **Run frequency** | Weekly (Wednesday, 09:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — the Samgöngustofa (Icelandic Transport Authority) register does not publish ICAO hex (Mode S) addresses at all; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The register is fetched in a single request as an Apollo Persisted Query GraphQL call (`GetAllAircrafts`, `pageSize: 10000`) against `island.is/api/graphql` — no scraping or pagination is needed. Each returned aircraft entry's `identifiers` field is treated as the registration mark and looked up in batches against the Mictronics RediSearch index to resolve an ICAO hex, since the API response itself carries no hex/Mode S field. Country names in the operator address (e.g. `"Ísland"`) are normalized to ISO 3166-1 alpha-2 codes via a small lookup table; unrecognized country strings are passed through unchanged.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| identifiers | ✅ | Registration mark; `TF-` prefix; used as the Mictronics lookup key |
| type | ✅ | → `aircraft.model` |
| serialNumber | ✅ | → `aircraft.serial_number` |
| productionYear | ✅ | → `aircraft.manufactured_date` (stored as `YYYY-01-01`; `0`/blank skipped) |
| operator.name | ✅ | → `registrant.names[0]` |
| operator.address | ✅ | → `registrant.street[0]` |
| operator.city | ✅ | → `registrant.city` |
| operator.postcode | ✅ | → `registrant.postal_code` |
| operator.country | ✅ | → `registrant.country`; mapped to ISO 3166-1 alpha-2 via a lookup table (e.g. `Ísland`/`Iceland` → `IS`) |

See `specs/data-dictionary.yaml` (`is-samgongustofa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 4CC289 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "1979-01-01",
        "manufacturer": "CESSNA",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "Textron Aviation Inc. R172K",
        "serial_number": "R-172-3035",
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "4CC289",
    "military": false,
    "registrant": {
        "city": "Reykjavík",
        "country": "IS",
        "names": [
            "Flugklúbbur Íslands ehf."
        ],
        "postal_code": "128",
        "street": [
            "Pósthólf 4040"
        ]
    },
    "registration": "TF-EJG",
    "source": "is-samgongustofa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 4CC581 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2025-01-01",
        "manufacturer": "AIRBUS",
        "manufacturer_model": "AIRBUS A-321neo",
        "model": "Airbus  S.A.S. A321-271NX",
        "serial_number": "12503",
        "type_designator": "A21N",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "4CC581",
    "military": false,
    "registrant": {
        "city": "Hafnarfirði",
        "country": "IS",
        "names": [
            "Icelandair ehf."
        ],
        "postal_code": "221",
        "street": [
            "Flugvöllum 1"
        ]
    },
    "registration": "TF-IAD",
    "source": "is-samgongustofa"
}
```

## Configuration

Reads `settings.json` (mounted at `/app/settings.json`):

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `redis.host` | ✅ | — | Redis connection host |
| `redis.port` | ❌ | `6379` | |
| `mqtt.host` | ❌ | — | Omit the whole `mqtt` block to skip completion-stats publishing entirely |
| `mqtt.port` | ❌ | `1883` | |
| `mqtt.username` | ❌ | — | Optional MQTT auth (added in #328); omit for an anonymous broker |
| `mqtt.password` | ❌ | — | |
| `redis_ttl_days` | ❌ | `14` | TTL applied to each `aircraft:registry:{icao_hex}` key written by this runner |

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/is-samgongustofa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_is_samgongustofa_{name}/config` for each of the three stats above.
