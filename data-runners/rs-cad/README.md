# 🇷🇸 rs-cad

| | |
|---|---|
| **Country** | Serbia |
| **Registration prefix** | `YU-` |
| **Data source** | https://apps.cad.gov.rs/ords/dcvws/regvaz/site/listAircraft |
| **Format** | JSON API (Oracle ORDS REST endpoint, fixed URL with query params) |
| **Run frequency** | Weekly (Tuesday, 10:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

A single GET request to the Serbia CAD ORDS REST endpoint (`?...&limit=10000`) returns the entire register in one response's `items[]` array — no pagination or index-page discovery needed. The endpoint's TLS certificate has an untrusted issuer chain, so requests are made with `verify=False` and urllib3's `InsecureRequestWarning` is disabled globally. Only items whose `registarska_oznaka` starts with `YU-` are kept. Aircraft type (`vrsta_vazduhoplova`) is decoded from Serbian via a lookup table, passing through unmapped values. As with `pg-casapng`, registrations are resolved to `icao_hex` in batches of 100 via a RediSearch `TagField` query against the Mictronics index; unmatched registrations are skipped for this run. Every written record explicitly sets `military: false` — this register is exclusively civil, and the explicit value ensures a stale `military: true` flag (from Mictronics or a prior record on a reused hex) is corrected on re-registration.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `registarska_oznaka` | ✅ | `YU-` filter; used as the Mictronics lookup key |
| `vrsta_vazduhoplova` | ✅ | → `aircraft.type`, decoded via lookup table (e.g. `Avion` → `Airplane`) |
| `proizvođač` | ✅ | → `aircraft.manufacturer` |
| `proizvođačka_oznaka` | ✅ | → `aircraft.model` |
| `serijski_broj` | ✅ | → `aircraft.serial_number` |
| `korisnik` | ✅ | → `registrant.names[0]` |

See `specs/data-dictionary.yaml` (`rs-cad` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 4C0E4D | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "TEXTRON AVIATION INC.",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "CESSNA 172S",
        "serial_number": "172S11225",
        "type": "Airplane",
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "4C0E4D",
    "military": false,
    "registrant": {
        "names": [
            "Vazduhoplovna akademija"
        ]
    },
    "registration": "YU-DSN",
    "source": "rs-cad"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 4C01F1 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "The Boeing Company",
        "manufacturer_model": "BOEING 737-700",
        "model": "BOEING 737-700",
        "serial_number": "37592",
        "type": "Airplane",
        "type_designator": "B737",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "4C01F1",
    "military": false,
    "registrant": {
        "names": [
            "Skybridge International Balkan D.O.O."
        ]
    },
    "registration": "YU-APR",
    "source": "rs-cad"
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
| `mqtt.username` | ❌ | — | Optional MQTT auth; omit for an anonymous broker |
| `mqtt.password` | ❌ | — | |
| `redis_ttl_days` | ❌ | `14` | TTL applied to each `aircraft:registry:{icao_hex}` key written by this runner |

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/rs-cad/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_rs_cad_{name}/config` for each of the three stats above.
