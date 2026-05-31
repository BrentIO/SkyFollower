# SkyFollower OurAirports Data Runner

Downloads the [OurAirports airports CSV](https://davidmegginson.github.io/ourairports-data/airports.csv),
filters to 4-character ICAO codes, computes a voice-friendly phonic name for
each airport, stages records in local SQLite, writes enrichment data to Redis,
publishes MQTT completion statistics, and exits. Scheduled via ofelia.

## Configuration (`settings.json`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `redis.host` | string | — | Redis hostname or IP |
| `redis.port` | integer | `6379` | Redis port |
| `redis_ttl_days` | integer | `14` | TTL applied to every `airport:` key written |
| `mqtt.host` | string | — | MQTT broker hostname (omit key to disable MQTT) |
| `mqtt.port` | integer | `1883` | MQTT broker port |
| `log_level` | string | `"info"` | Log verbosity |

The settings file path defaults to `/app/settings.json` and can be overridden
with the `SETTINGS_PATH` environment variable.

## Phonic Names

Each airport record includes a `phonic` field — a voice-friendly spoken name
used by downstream systems for overhead announcements. "International" and
"Airport" are stripped from every computed phonic.

### Override file

**Location:** `phonics_overrides.json`, co-located with `main.py` (baked into
the container image).

**Format:**
```json
{
    "KXXX": "Spoken name exactly as desired",
    "KYYY": "Another override"
}
```

**Behavior:** If an airport's ICAO code has an entry in this file, that value
is used as the phonic verbatim — no stripping or any other processing is
applied. The general algorithm (city deduplication, separator normalisation,
"International"/"Airport" stripping) only runs for airports not in the file.

The file is loaded once at container startup. Adding or changing entries
requires a container restart. A missing file is silently ignored.

### General algorithm (no override)

1. If name starts with "Greater" or contains " of ": use name as-is.
2. If city is not present in the airport name: prepend city.
3. If phonic ends with city: move city to the front.
4. Strip trailing `/` or `-` artifacts (common in names like "City/Town Airport").
5. Normalise `/` and `-` to spaces; collapse extra spaces.
6. Strip "International" and "Airport" from the result.

## Redis Output

Key pattern: `airport:{ICAO_CODE}` (uppercased)

Payload shape (matches legacy AROI):

```json
{
    "icao_code": "KATL",
    "name": "Hartsfield-Jackson Atlanta International Airport",
    "city": "Atlanta",
    "region": "US-GA",
    "country": "US",
    "phonic": "Atlanta Hartsfield-Jackson"
}
```

TTL: `redis_ttl_days × 86400` seconds (default 14 days).

## MQTT Topics

| Topic | Payload | Retained |
|-------|---------|----------|
| `SkyFollower/runner/ourairports/statistics` | JSON (see below) | No |

**Statistics payload:**

| Field | Type | Description |
|-------|------|-------------|
| `records_imported` | integer | Number of airport records written to Redis |
| `last_run_at` | string | UTC ISO-8601 timestamp of run completion |
| `last_run_status` | string | `success` or `failure` |

Home Assistant autodiscovery payloads are published to
`homeassistant/sensor/SkyFollower_runner_ourairports_{name}/config` on connect.
