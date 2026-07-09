# fr-dgac

| | |
|---|---|
| **Country** | France |
| **Registration prefix** | `F-` |
| **Data source** | https://immat.aviation-civile.gouv.fr/immat/servlet/static/upload/export.csv |
| **Format** | CSV (fixed URL) |
| **Run frequency** | Weekly (Wednesday, 07:10 UTC) |
| **Depends on Mictronics for ICAO hex** | Yes — the DGAC register does not publish ICAO hex (Mode S) addresses; registrations are resolved via RediSearch against Mictronics records (`idx:aircraft:mictronics`). Must run after the `mictronics` runner. |

## How it works

The DGAC (Direction Générale de l'Aviation Civile) export CSV is downloaded
from a fixed URL (`;`-delimited, `utf-8-sig` encoded) and parsed with
`csv.DictReader`. Rows are grouped by `IMMATRICULATION` to handle co-ownership
— the same registration can appear on multiple rows with different owners;
owner names are deduplicated in order, and the first non-blank address across
the group is used. `ADRESSE_PROPRIETAIRE` is a single free-text field parsed
into street/city/postal-code/country: known French country names are stripped
from the end (multi-word names like `PAYS BAS` checked before single-word
ones), then a 5-digit postal code is located in the remainder to split
street/city; if no postal code is found, only the decoded country is kept.
Before writing, a **type sanity check** compares tokens extracted from the
DGAC `MODELE` string against the existing Mictronics record's type designator
and model — if both sides yield tokens and they don't intersect, the record is
silently skipped (guards against a reused/reassigned registration mark
pointing at the wrong Mictronics record).

## Columns

| Source column | Imported | Notes |
|---|---|---|
| IMMATRICULATION | ✅ | F-prefix; used as the Mictronics lookup key; groups co-ownership rows |
| CONSTRUCTEUR | ✅ | → `aircraft.manufacturer` |
| MODELE | ✅ | → `aircraft.model`; also used for the pre-write type sanity check against Mictronics |
| NUMERO_SERIE | ✅ | → `aircraft.serial_number` |
| PROPRIETAIRE | ✅ | → `registrant.names[]`; deduplicated across co-ownership rows, order preserved |
| ADRESSE_PROPRIETAIRE | ✅ | Parsed into `registrant.street`/`city`/`postal_code`/`country`; country decoded from French name to ISO 3166-1 alpha-2 |

Other columns present in the DGAC export are not read by this runner.

See `specs/data-dictionary.yaml` (`fr-dgac` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 383433 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "SOCATA",
        "manufacturer_model": "SOCATA Rallye",
        "model": "MS 892 A 150",
        "serial_number": "10557",
        "type_designator": "RALL",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "383433",
    "military": false,
    "registrant": {
        "city": "TOUSSUS-LE-NOBLE",
        "country": "FR",
        "names": [
            "AERO CLUB DE L'OUEST PARISIEN"
        ],
        "postal_code": "78117",
        "street": [
            "AERODROME DE TOUSSUS LE NOBLE BAT. 232"
        ]
    },
    "registration": "F-BNBT",
    "source": "fr-dgac"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 39C429 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufacturer": "THE BOEING COMPANY",
        "manufacturer_model": "BOEING 787-9 Dreamliner",
        "model": "787-9",
        "serial_number": "42497",
        "type_designator": "B789",
        "wake_turbulence_category": "Heavy"
    },
    "icao_hex": "39C429",
    "military": false,
    "registrant": {
        "country": "JP",
        "names": [
            "VENTOUX LEASING CO. LTD"
        ]
    },
    "registration": "F-HRBJ",
    "source": "fr-dgac"
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

Published once, at the end of a run, to `SkyFollower/runner/fr-dgac/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_fr_dgac_{name}/config` for each of the three stats above.
