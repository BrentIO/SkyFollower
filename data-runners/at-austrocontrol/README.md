# at-austrocontrol

| | |
|---|---|
| **Country** | Austria |
| **Registration prefix** | `OE-` |
| **Data source** | https://www.austrocontrol.at/lfa-publish-service/v2/oenfl/luftfahrzeuge |
| **Format** | JSON API (single paginated GET, `page=0&size=10000`) |
| **Run frequency** | Weekly (Wednesday, 12:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; `OE-{kennzeichen}` registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The Austrocontrol JSON API is queried directly at a fixed URL (no index-page discovery needed). Deregistered aircraft (`loeschung` not null) are filtered out before lookup. Registrations are resolved to `icao_hex` in batches of 100 via a RediSearch query against the Mictronics index, then a type sanity check compares tokens extracted from the Austrocontrol `baumuster` (model) string against the existing Mictronics `type_designator`/`manufacturer_model` fields, rejecting the match if both sides have tokens but none overlap — this guards against false registration-mark collisions across registries. The `halter` (owner) field is a multi-line string (`Name\r\nPostalCode City, Street\r\nCountry`, repeating per co-owner); only the first owner group is parsed into the registrant sub-object, and the German country name is mapped to its ISO 3166-1 alpha-2 code.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `kennzeichen` | ✅ | Combined with `OE-` prefix; used as the Mictronics lookup key |
| `loeschung` | ✅ | Used only as a filter (non-null = deregistered, row skipped); not stored |
| `luftfahrzeugart` | ✅ | → `aircraft.type`, decoded from German (e.g. `Flugzeug` → `Airplane`) |
| `hersteller` | ✅ | → `aircraft.manufacturer` |
| `baumuster` | ✅ | → `aircraft.model`; also used for the type sanity check against Mictronics |
| `seriennummer` | ✅ | → `aircraft.serial_number` |
| `halter` | ✅ | → `registrant.names[0]`, `street`, `city`, `postal_code`, `country`; only the first owner group of a multi-owner string is parsed |
| `oid` | ❌ | Present in source; not read by this runner |
| `ordnungszahl` | ❌ | Present in source; not read by this runner |
| `luftfahrzeugartOid` | ❌ | Present in source; not read by this runner |
| `mtom` | ❌ | Present in source; not read by this runner |

See `specs/data-dictionary.yaml` (`at-austrocontrol` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 44079C | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "Cessna Aircraft Company",
        "manufacturer_model": "CESSNA 150",
        "model": "150 A",
        "serial_number": "15059176",
        "type": "Airplane",
        "type_designator": "C150",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "44079C",
    "military": false,
    "registrant": {
        "city": "Wien",
        "country": "AT",
        "names": [
            "Tobias Florian Müller"
        ],
        "postal_code": "1030",
        "street": [
            "Döblerhofstraße 10/217"
        ]
    },
    "registration": "OE-AHG",
    "source": "at-austrocontrol"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 440A8D | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "Boeing Commercial Airplane Group",
        "manufacturer_model": "BOEING 747-400",
        "model": "747-400F",
        "serial_number": "36784",
        "type": "Airplane",
        "type_designator": "B744",
        "wake_turbulence_category": "Heavy"
    },
    "icao_hex": "440A8D",
    "military": false,
    "registrant": {
        "city": "Grace-Hollogne",
        "country": "BE",
        "names": [
            "ASL Airlines Belgium SA"
        ],
        "postal_code": "4460",
        "street": [
            "Rue de l´Aéroport 101"
        ]
    },
    "registration": "OE-IFK",
    "source": "at-austrocontrol"
}
```

Note: the second example's registrant is based in Belgium (`BE`), not Austria — the Austrocontrol register lists the operating lessee's address, which can be outside Austria even for an `OE-` registered aircraft.

## Configuration

Reads `settings.json` (mounted at `/app/settings.json`):

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `redis.host` | ✅ | — | Redis connection host |
| `redis.port` | ❌ | `6379` | |
| `mqtt.host` | ❌ | — | Omit the whole `mqtt` block to skip completion-stats publishing entirely |
| `mqtt.port` | ❌ | `1883` | |
| `mqtt.username` | ❌ | — | Optional MQTT auth; omit for an anonymous broker |
| `mqtt.password` | ❌ | — | |
| `redis_ttl_days` | ❌ | `14` | TTL applied to each `aircraft:registry:{icao_hex}` key written by this runner |

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/at-austrocontrol/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_at_austrocontrol_{name}/config` for each of the three stats above.
