# 🎨 airportwebcams-liveries

| | |
|---|---|
| **Scope** | Global — not country-specific, like `mictronics` / `ourairports` |
| **Data source** | https://airportwebcams.net/special-liveries/ |
| **Format** | Static server-rendered HTML table (WordPress TablePress, id `tablepress-8`) |
| **Run frequency** | Weekly (Friday, 06:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — this source does not publish ICAO hex addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The special-liveries page is fetched with a single GET (a browser-like `User-Agent` is required — the site's ModSecurity rule blocks the default `curl`/`requests` User-Agent with `HTTP 406`, though an honest identifying UA such as `"Mozilla/5.0 (compatible; P5Software SkyFollower)"` is accepted). The entire ~2,074-row table is server-rendered into the initial HTML response, so no pagination or AJAX crawling is needed; the DataTables JS on the live page only adds client-side search/sort on top of it.

The `Registration` cell (tail number) is resolved to `icao_hex` in batches via a RediSearch query against the Mictronics index, the same pattern used by `at-austrocontrol`, `bz-bdca`, and the other hex-less country runners. Rows whose `Registration` is the literal string `Various` (whole-fleet liveries — e.g. a "10 Years" sticker applied across an entire fleet) are skipped, since they can't be resolved to a single aircraft. Registrations with no Mictronics match are also skipped and logged as a miss, not treated as an error.

The `Description` cell is transformed into `livery_name` rather than stored verbatim, because this field is spoken aloud by Home Assistant TTS on a matched-flight notification and needs to read as a clean phrase:

1. Strip any parenthetical annotation whose contents contain `sticker` or `#new` (case-insensitive) — this covers `(sticker)`, `(stickers)`, `(sticker, underside)`, `(sticker; underside/belly)`, and the site's `(#New at DD-Mon-YY)` freshness marker in whatever exact wording/date format it uses. This step runs **before** the split below, not after — at least one real annotation contains its own `/` (`"(sticker; underside/belly)"`), which would otherwise be misread as an extra compound-description segment.
2. Split what remains on `/` and take the **last** segment — roughly 7% of rows pack more than one livery name into a single cell (e.g. `"50th Anniversary / Air Silk Road / 1 Million Tons, 10 Years"`); the last-listed one is treated as the current/primary livery.
3. Collapse whitespace and trim.

Parenthetical content that's genuinely part of the livery name (e.g. a year like `(2022)`) is left alone — only annotations containing `sticker`/`#new` are stripped. Verified against all 2,074 real rows in the live table: the transform never produces an empty `livery_name`.

Every matched row writes `aircraft:livery:{icao_hex}` = `{"special_livery": true, "livery_name": "<transformed>", "source": "airportwebcams-liveries"}` with the same 14-day TTL used by the other registration-enrichment keys. `shared/lua/merge_aircraft.lua` reads this key in addition to `aircraft:mictronics:{icao_hex}` and `aircraft:registry:{icao_hex}`, deep-merging it last so it takes priority over both — Mictronics on the bottom, country registry in the middle, special livery on top.

## Columns

| Source column | Imported | Notes |
|---|---|---|
| Country | ❌ | Present in source; not read by this runner |
| Airline | ❌ | Present in source; not read by this runner |
| Aircraft Type | ❌ | Present in source; not read by this runner — type/manufacturer data already comes from Mictronics/registry runners |
| Registration | ✅ | Used only as the Mictronics RediSearch lookup key to resolve `icao_hex`; not stored on the record itself. Rows with value `Various` (whole-fleet liveries) are skipped |
| Description | ✅ | → `livery_name`, transformed: strip a `sticker`/`#new` parenthetical annotation, split on `/`, take the last segment |

See `specs/data-dictionary.yaml` (`airportwebcams` entry) for full column semantics and cross-source schema notes.

## Example transform

| `Description` (source) | `livery_name` (stored) |
|---|---|
| `Rugby's Greatest Rivalry / 2026 NZ v SA rugby tour (#New at 17-Jul-26)` | `2026 NZ v SA rugby tour` |
| `50th Anniversary / Air Silk Road / 1 Million Tons, 10 Years (stickers)` | `1 Million Tons, 10 Years` |
| `Amar Bank (sticker) (#New at 17-Jul-26)` | `Amar Bank` |
| `Vets In Blue (2022) / America250 (sticker)` | `America250` |
| `FedEx founder F W Smith (sticker; underside/belly)` | `FedEx founder F W Smith` |
| `SkyTeam` | `SkyTeam` |

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics, any country registry runner, and this runner, whichever have written to the same key). `AA7C64` is JetBlue `N775JB` — verified live against the FAA registry N-number lookup — carrying the `"Vets In Blue (2022) / America250 (sticker)"` livery from the source table:

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 AA7C64 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer_model": "..."
    },
    "icao_hex": "AA7C64",
    "livery_name": "America250",
    "registration": "N775JB",
    "special_livery": true
}
```

(`aircraft.manufacturer_model` above is illustrative only — whatever Mictronics/registry already has on file for this hex; `icao_hex`, `registration`, `livery_name`, and `special_livery` are what this runner actually verifies.)

For comparison, an aircraft with no special livery — e.g. the Delta 757 (`N659DL` / `A8AE7F`) used as an example in `../us-faa/README.md` — merges exactly as it does today, with no `special_livery` or `livery_name` key present at all.
