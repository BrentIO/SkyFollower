# 🇬🇬 gg-2reg

| | |
|---|---|
| **Country** | Guernsey (Bailiwick of Guernsey) |
| **Registration prefix** | `2-` |
| **Data source** | https://www.2-reg.com/legislation/register/ |
| **Format** | PDF (date-encoded filename, discovered via index page) |
| **Run frequency** | Weekly (Tuesday, 18:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this runner does not publish ICAO hex addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The index page is scraped to discover the current register PDF (matches
`/wp-content/uploads/*/Register_*.pdf`). Pages whose first line matches a known
special-section heading (New Registrations, Deregistrations, Ownership Changes,
Registration Changes, Reserved Marks) are skipped. Remaining pages are parsed by
grouping extracted words into columns by x-position, since `pdfplumber`'s table
detection does not reliably find the column boundaries in this PDF. MSN
(serial number) is whitespace-collapsed like manufacturer, model, and owner
already are, since a word-position-grouped cell can end up with an embedded
newline rather than a space. Every written record explicitly sets `military: false` — this register is
exclusively civil, and the explicit value ensures a stale `military: true`
flag (from Mictronics or a prior record on a reused hex) is corrected on
re-registration.

## Columns

The main register table (all pages before the special sections) has six columns.
The special sections at the end of the PDF (New Registrations, Deregistrations,
Ownership Changes, Registration Changes, Reserved Marks) are skipped entirely by
this runner, but several of them present additional columns not found in the
main table; those are listed below too, since they genuinely exist in the source.

| Source column | Imported | Notes |
|---|---|---|
| Registration | ✅ | 2-prefix; used as the Mictronics lookup key |
| Aircraft Manufacturer | ✅ | → `aircraft.manufacturer` |
| Type | ✅ | → `aircraft.model` |
| MSN | ✅ | → `aircraft.serial_number`; embedded newlines collapsed to a single space |
| Registered Owner | ✅ | → `registrant.names[0]`; privacy placeholders (e.g. `(private)`) are filtered, not stored |
| Date of Registration | ❌ | Parsed but not stored |
| Deregistered on | ❌ | Only present in the "Deregistrations" special section; entire page is skipped by this runner |
| Exported to | ❌ | Only present in the "Deregistrations" special section; entire page is skipped by this runner |
| Previous Owner/Charterer by Demise | ❌ | Only present in the "Registration Ownership Changes" special section; entire page is skipped by this runner |
| Ownership Changed | ❌ | Only present in the "Registration Ownership Changes" special section; entire page is skipped by this runner |
| Previous Marks | ❌ | Only present in the "Registration Changes" special section; entire page is skipped by this runner |
| Effective date | ❌ | Only present in the "Registration Changes" special section; entire page is skipped by this runner |
| Reserved/Unavailable Registration Marks list | ❌ | The "All Current Reserved or Unavailable Registration Marks" page is skipped entirely; it is a plain list of marks with no owner/aircraft data |

See `specs/data-dictionary.yaml` (`gg-2reg` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 43EC60 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "The New Piper Aircraft, Inc",
        "manufacturer_model": "PIPER PA-28-201T/235/236",
        "model": "PA-28-235",
        "serial_number": "28-7210009",
        "type_designator": "P28B",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "43EC60",
    "military": false,
    "registration": "2-GOLD",
    "source": "gg-2reg"
}
```

Note: `registrant` is entirely absent here — this aircraft's owner is recorded as `(private)` in the source PDF, which is filtered at write time rather than stored.

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

Published once, at the end of a run, to `SkyFollower/runner/gg-2reg/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_gg_2reg_{name}/config` for each of the three stats above.
