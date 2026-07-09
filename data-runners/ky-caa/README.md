# ky-caa

| | |
|---|---|
| **Country** | Cayman Islands |
| **Registration prefix** | `VP-C` |
| **Data source** | https://www.caacayman.com/wp-content/uploads/Active-Aircraft-Register.pdf |
| **Format** | PDF (static URL) |
| **Run frequency** | Weekly (Thursday, 06:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — the Cayman Islands register does not publish ICAO hex (Mode S) addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The register PDF is downloaded from a static URL (it does not change with
updates) and parsed with `pdfplumber.extract_table()` — unlike some other PDF
runners, its table detection works reliably here. Column headers that wrap
onto multiple lines are normalised by collapsing embedded newlines to a
single space, and the repeated header row on each page is skipped. After
resolving each registration's `icao_hex` via the Mictronics search index, a
type sanity check compares type-designator-like tokens (e.g. `AW139`)
extracted from the register's "Series Type" column against Mictronics'
`type_designator`/`manufacturer_model` fields; if both sides have recognisable
tokens and they don't overlap, the row is skipped rather than written, to
avoid overwriting a correct Mictronics record with a mismatched one. The
`Nationality` column is mapped to an ISO 3166-1 alpha-2 code via a fixed
lookup table, and the country name is stripped from the end of `Registered
Address` before the remaining address is split into street lines.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Aircraft Registration | ✅ | VP-C-prefix; used as the Mictronics lookup key |
| Registered Owner | ✅ | → `registrant.names[0]` |
| Registered Address | ✅ | Country suffix stripped, then split on commas → `registrant.street[]` |
| Nationality | ✅ | Mapped via fixed table to ISO 3166-1 alpha-2 → `registrant.country`; also used to strip the address suffix |
| Series Type | ✅ | → `aircraft.model`; also cross-checked against Mictronics `type_designator`/`manufacturer_model` as a type sanity check before writing |
| Serial Number | ✅ | → `aircraft.serial_number` |

See `specs/data-dictionary.yaml` (`ky-caa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 4247DF | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "DE",
        "manufacturer_model": "DE HAVILLAND DHC-6 Twin Otter",
        "model": "Viking Air Limited DHC-6 Series 400",
        "serial_number": "967",
        "type_designator": "DHC6",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "4247DF",
    "military": false,
    "registrant": {
        "country": "VG",
        "names": [
            "Sea Aviation 4 Limited"
        ],
        "street": [
            "Floor 4",
            "Banco Popular Building",
            "Road Town",
            "Tortola VG1110"
        ]
    },
    "registration": "VP-CHO",
    "source": "ky-caa"
}
```

Note: the Cayman Islands register contains no GA singles at all, so the "small" example above is a DHC-6 Twin Otter rather than the usual single-engine piston.

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 4247A6 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "BOEING",
        "manufacturer_model": "BOEING 737-700",
        "model": "The Boeing Company 737-7JB",
        "serial_number": "36714",
        "type_designator": "B737",
        "wake_turbulence_category": "Medium"
    },
    "icao_hex": "4247A6",
    "military": false,
    "registrant": {
        "country": "VG",
        "names": [
            "Polywise International Holdings Limited"
        ],
        "street": [
            "Sea Meadow House",
            "Blackburne Highway",
            "P.O. Box 116",
            "Road Town",
            "Tortola"
        ]
    },
    "registration": "VP-CKG",
    "source": "ky-caa"
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

Published once, at the end of a run, to `SkyFollower/runner/ky-caa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_ky_caa_{name}/config` for each of the three stats above.
