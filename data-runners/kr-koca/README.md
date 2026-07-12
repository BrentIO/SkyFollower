# 🇰🇷 kr-koca

| | |
|---|---|
| **Country** | South Korea |
| **Registration prefix** | `HL` |
| **Data source** | http://atis.koca.go.kr/ATIS/aircraft/statListEn01.do?AIR_GUBUN=all |
| **Format** | JSON API (fixed URL; response is double-JSON-encoded) |
| **Run frequency** | Weekly (Tuesday, 08:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — the KOCA register does not publish ICAO hex (Mode S) addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The Korea Office of Civil Aviation (KOCA) ATIS endpoint is fetched with a
fixed URL. The endpoint requires a browser `User-Agent` header — without one
it returns a Korean-language error page instead of JSON. The response body is
itself a JSON string containing the real JSON object, so it is decoded twice
(`json.loads` applied a second time if the first decode yields a `str`). Dates
are given as `YY.MM.DD` with a pivot year of 50 (`>= 50` → 1900s, `< 50` →
2000s). Every written record explicitly sets `military: false` — this
register is exclusively civil, and the explicit value ensures a stale
`military: true` flag (from Mictronics or a prior record on a reused hex) is
corrected on re-registration.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| `REG_SNO` (Registration) | ✅ | HL-prefix; used as the Mictronics lookup key |
| `AIR_TYPE` (Type) | ✅ | → `aircraft.model` |
| `AIR_BUILD_NO` (Serial Number) | ✅ | → `aircraft.serial_number` |
| `AIR_BUILD_DATE` (Build Date) | ✅ | Parsed `YY.MM.DD` (pivot year 50) → `aircraft.manufactured_date` |
| `REG_CUSER` (Registered User/Owner) | ✅ | → `registrant.names[0]` |
| `SEQ` (Sequence Number) | ❌ | Present in source; not read by this runner |
| `AIR_AGE` (Aircraft Age) | ❌ | Present in source; not read by this runner |
| `REG_DATE` (Registration Date) | ❌ | Present in source; not read by this runner |
| `AIR_LIMIT_MAN` (Passenger/Seating Limit) | ❌ | Present in source; not read by this runner |
| `AIR_FLY_WEIGHT` (Max Flying Weight) | ❌ | Present in source; not read by this runner |
| `REG_JANG` (Registration Location) | ❌ | Present in source; not read by this runner |
| `PROC_TYPE` (Acquisition Type, e.g. purchase/lease) | ❌ | Present in source; not read by this runner |
| `ETC` (Miscellaneous flag) | ❌ | Present in source; not read by this runner |
| `SNO` (Internal record number) | ❌ | Present in source; not read by this runner |
| `IMAGE1` (Aircraft photo filename) | ❌ | Present in source; not read by this runner |
| `IMAGE2` (Aircraft photo filename) | ❌ | Present in source; not read by this runner |
| `IMAGE3` (Aircraft photo filename) | ❌ | Present in source; not read by this runner |
| `AWC_VP` (Unclear; empty in observed data) | ❌ | Present in source; not read by this runner |
| `PRJ_GBN` (Business/project classification) | ❌ | Present in source; not read by this runner |

See `specs/data-dictionary.yaml` (`kr-koca` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 718975 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2013-06-25",
        "manufacturer": "CESSNA",
        "manufacturer_model": "CESSNA 172 Skyhawk",
        "model": "C-172S",
        "serial_number": "172S11318",
        "type_designator": "C172",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "718975",
    "military": false,
    "registrant": {
        "names": [
            "한국항공대학교"
        ]
    },
    "registration": "HL1175",
    "source": "kr-koca"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 71BF71 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2018-02-19",
        "manufacturer": "AIRBUS",
        "manufacturer_model": "AIRBUS A-350-900",
        "model": "A350-900",
        "serial_number": "0198",
        "type_designator": "A359",
        "wake_turbulence_category": "Heavy"
    },
    "icao_hex": "71BF71",
    "military": false,
    "registrant": {
        "names": [
            "ASIANA AIRLINES"
        ]
    },
    "registration": "HL7771",
    "source": "kr-koca"
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

Published once, at the end of a run, to `SkyFollower/runner/kr-koca/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_kr_koca_{name}/config` for each of the three stats above.
