# cy-dca

| | |
|---|---|
| **Country** | Cyprus |
| **Registration prefix** | `5B-` |
| **Data source** | https://www.mcw.gov.cy/mcw/dca/dca.nsf/All/46E79D3B8B2B752AC2258D8D00384715/$file/Aircraft_Register_Extract.pdf |
| **Format** | PDF (fixed URL) |
| **Run frequency** | Weekly (Tuesday, 22:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — the Cyprus DCA register does not publish ICAO hex addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The register PDF is fetched from a fixed URL (TLS verification is disabled — the server's certificate chain is not fully trusted) and every page is parsed with `pdfplumber`'s built-in `extract_table()`, which works reliably on this PDF's table layout (unlike some other PDF-based runners that must fall back to grouping words by x-position). Rows are filtered to those whose registration mark column starts with `5B-`. Registrations are then resolved to ICAO hex in batches of 100 via a RediSearch query against the Mictronics index; only rows that resolve are written. The owner/operator column can contain multiple `/`-separated entries with `OWNER:`/`TRUSTEE:` header labels and `LESSOR`/`LESSEE` qualifiers, all of which are stripped during cleanup. Manufacturer, model, and serial number all pass through the same whitespace-collapsing helper, since `pdfplumber` sometimes represents a wrapped cell's text with an embedded newline rather than a space. Every written record explicitly sets `military: false` — this register is exclusively civil, and the explicit value ensures a stale `military: true` flag (from Mictronics or a prior record on a reused hex) is corrected on re-registration.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| REGISTRATION MARK | ✅ | 5B-prefix; used as the Mictronics lookup key and stored as `registration` |
| MANUFACTURER | ✅ | → `aircraft.manufacturer` |
| AIRCRAFT TYPE | ✅ | → `aircraft.model` |
| AIRCRAFT SERIAL NO | ✅ | → `aircraft.serial_number`; embedded newlines collapsed to a single space |
| MTOW KGS | ❌ | Parsed but not stored |
| AIRCRAFT OWNER/OPERATOR | ✅ | Split on `/`; `OWNER`/`TRUSTEE` header labels and `LESSOR`/`LESSEE` qualifiers removed → `registrant.names[]` |

See specs/data-dictionary.yaml (`cy-dca` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 4C8038 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "DE HAVILLAND SUPPORT LTD",
        "manufacturer_model": "BEAGLE Pup",
        "model": "BEAGLE B121 SERIES 2",
        "serial_number": "166",
        "type_designator": "PUP",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "4C8038",
    "military": false,
    "registrant": {
        "names": [
            "WALKER AVIATION LTD"
        ]
    },
    "registration": "5B-CKC",
    "source": "cy-dca"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 4C8090 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "AIRBUS SAS",
        "manufacturer_model": "AIRBUS A-320",
        "model": "A320-214",
        "serial_number": "3933 (2009)",
        "type_designator": "A320",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "4C8090",
    "military": false,
    "registrant": {
        "names": [
            "START II IRELAND LEASING 2 LTD",
            "OPERATOR CHARLIE AIRLINES LTD"
        ]
    },
    "registration": "5B-DDR",
    "source": "cy-dca"
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

Published once, at the end of a run, to `SkyFollower/runner/cy-dca/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_cy_dca_{name}/config` for each of the three stats above.
