# ca-transport-canada

| | |
|---|---|
| **Country** | Canada |
| **Registration prefix** | `C-` |
| **Data source** | https://wwwapps.tc.gc.ca/Saf-Sec-Sur/2/CCARCS-RIACC/download/ccarcsdb.zip |
| **Format** | ZIP (fixed URL) containing two CSV files, `carscurr.txt` (aircraft) and `carsownr.txt` (owners) |
| **Run frequency** | Weekly (Sunday, 05:10 UTC) |
| **Depends on Mictronics for ICAO hex** | No — the CCARCS aircraft file publishes the Mode S transponder address directly as a 24-bit binary string, which this runner decodes to hex itself. |

## How it works

The full CCARCS database ZIP is downloaded from a fixed URL and both member files are extracted in memory. `carscurr.txt` (aircraft) and `carsownr.txt` (owners) are ISO-8859-1, comma-delimited, and end with a blank row followed by a record-count row — parsing stops at the first empty row to avoid ingesting that trailer. Rows are staged into a local SQLite database (indexed by registration) rather than being joined in Python, since owner records must be matched back to aircraft records by registration mark. Only aircraft rows with an empty `ineffective_date` (i.e. still on the register) are written to Redis, and only the first `Active`-status owner per registration is used. The Mode S column is a 24-bit binary string (e.g. `"110000000000000000010000"`) converted to 6-character hex via `int(value, 2)` formatted as `06X`.

## Columns

Both files are drawn from the same CCARCS ZIP and joined on the aircraft registration mark (`carscurr.txt` MARK/TRIMMED_MARK ↔ `carsownr.txt` MARK_LINK). Column names and order below match the archive's own `carslayout.txt` data dictionary, verified against the real downloaded files: 47 columns per row in `carscurr.txt`, 20 columns per row in `carsownr.txt`.

### carscurr.txt (aircraft)

| Source column | Imported | Notes |
|---|---|---|
| MARK / TRIMMED_MARK | ✅ | `C-` prefix prepended; `TRIMMED_MARK` (col 46) preferred, falls back to stripped `MARK` (col 0) → `registration`; also the join key to `carsownr.txt` MARK_LINK |
| REGISTRATION_SUB_TYPE_E | ❌ | Present in source; not read by this runner |
| REGISTRATION_SUB_TYPE_F | ❌ | Present in source; not read by this runner |
| COMMON_NAME | ❌ | Present in source; not read by this runner |
| MODEL_NAME | ✅ | → `aircraft.model` |
| MANUFACTURERS_SERIAL_NUMBER | ✅ | → `aircraft.serial_number` |
| MANUFACTURER_SERIAL_COMPRESSED | ❌ | Present in source; not read by this runner |
| ID_PLATE_MANUFACTURERS_NAME | ✅ | → `aircraft.manufacturer` |
| BASIS_FOR_REGISTRATION | ❌ | Present in source; not read by this runner |
| BASIS_FOR_REGISTRATION_F | ❌ | Present in source; not read by this runner |
| AIRCRAFT_CATEGORY_E | ✅ | Decoded (e.g. `Aeroplane` → `Airplane`) → `aircraft.type` |
| AIRCRAFT_CATEGORY_F | ❌ | Present in source; not read by this runner |
| DATE_OF_IMPORT | ❌ | Present in source; not read by this runner |
| ENGINE_MANUF_E | ✅ | → `aircraft.powerplant.manufacturer` |
| POWERGLIDER_FLAG | ❌ | Present in source; not read by this runner |
| ENGINE_CATEGORY_E | ✅ | Decoded (e.g. `Turbo Fan` → `Turbo-fan`) → `aircraft.powerplant.type` |
| ENGINE_CATEGORY_F | ❌ | Present in source; not read by this runner |
| NUMBER_OF_ENGINES | ✅ | → `aircraft.powerplant.count` |
| NUMBER_OF_SEATS | ✅ | → `aircraft.seats` |
| AIR_WEIGHT_KILOS | ❌ | Present in source; not read by this runner |
| SALE_REPORTED | ❌ | Present in source; not read by this runner |
| ISSUE_DATE | ❌ | Present in source; not read by this runner |
| EFFECTIVE_DATE | ❌ | Present in source; not read by this runner |
| INEFFECTIVE_DATE | ❌ | Used to filter to currently-registered aircraft only (`ineffective_date == ''`); value itself is not stored |
| REGISTERED_PURPOSE_E | ❌ | Present in source; not read by this runner |
| REGISTERED_PURPOSE_F | ❌ | Present in source; not read by this runner |
| FLIGHT_AUTHORITY_E | ❌ | Present in source; not read by this runner |
| FLIGHT_AUTHORITY_F | ❌ | Present in source; not read by this runner |
| MANUFACTURE_OR_ASSEMBLY | ❌ | Present in source; not read by this runner |
| COUNTRY_MANUFACTURE_ASS_E | ❌ | Present in source; not read by this runner |
| COUNTRY_MANUFACTURE_ASS_F | ❌ | Present in source; not read by this runner |
| DATE_MANUFACTURE_ASSEMBLY | ✅ | `YYYY/MM/DD` → ISO 8601 UTC → `aircraft.manufactured_date` |
| BASE_OF_OPERATIONS_CTRY_E | ❌ | Present in source; not read by this runner |
| BASE_OF_OPERATIONS_CTRY_F | ❌ | Present in source; not read by this runner |
| BASE_PROVINCE_OR_STATE_E | ❌ | Present in source; not read by this runner |
| BASE_PROVINCE_OR_STATE_F | ❌ | Present in source; not read by this runner |
| CITY_AIRPORT | ❌ | Present in source; not read by this runner |
| TYPE_CERTIFICATE_NUMBER | ❌ | Present in source; not read by this runner |
| REGISTRATION_AUTH_STATUS_E | ❌ | Present in source; not read by this runner |
| REGISTRATION_AUTH_STATUS_F | ❌ | Present in source; not read by this runner |
| MULTIPLE_OWNER_FLAG | ❌ | Present in source; not read by this runner |
| MODIFIED_DATE | ❌ | Present in source; not read by this runner |
| MODE_S_TRANSPONDER_BINARY | ✅ | 24-bit binary string decoded to 6-char hex → `icao_hex` |
| PHYSICAL_FILE_REGION_E | ❌ | Present in source; not read by this runner |
| PHYSICAL_FILE_REGION_F | ❌ | Present in source; not read by this runner |
| EX_MILITARY_MARK | ❌ | Present in source; not read by this runner |

### carsownr.txt (owners)

| Source column | Imported | Notes |
|---|---|---|
| MARK_LINK | ✅ | Join key back to the aircraft record's registration |
| FULL_NAME | ✅ | → `registrant.names[0]` |
| TRADE_NAME | ✅ | → `registrant.names[1]` |
| STREET_NAME | ✅ | → `registrant.street[0]` |
| STREET_NAME2 | ✅ | → `registrant.street[1]` |
| CITY | ✅ | → `registrant.city` |
| PROVINCE_OR_STATE_E | ✅ | → `registrant.administrative_area` |
| PROVINCE_OR_STATE_F | ❌ | Present in source; not read by this runner |
| POSTAL_CODE | ✅ | → `registrant.postal_code` |
| COUNTRY_E | ✅ | Full English country name decoded to ISO 3166-1 alpha-2 (defaults to `CA` if unrecognized) → `registrant.country` |
| COUNTRY_F | ❌ | Present in source; not read by this runner |
| TYPE_OF_OWNER_E | ✅ | → `registrant.type` |
| TYPE_OF_OWNER_F | ❌ | Present in source; not read by this runner |
| ACTIVE_FLAG | ❌ | Used to select the current owner row (`status == 'Active'`, first match only); value itself is not stored |
| CARE_OF | ❌ | Present in source; not read by this runner |
| REGION_E | ❌ | Present in source; not read by this runner |
| REGION_F | ❌ | Present in source; not read by this runner |
| OWNER_NAME_OLD_FORMAT | ❌ | Present in source; not read by this runner |
| MAIL_RECIPIENT | ❌ | Present in source; not read by this runner |
| TRIMMED_MARK | ❌ | Present in source; not read by this runner (distinct from `carscurr.txt`'s own `TRIMMED_MARK` column) |

See specs/data-dictionary.yaml (`ca-tc` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 C00010 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "1966-01-01T00:00:00Z",
        "manufacturer": "Piper Aircraft Corporation",
        "manufacturer_model": "PIPER PA-32",
        "model": "PA-32-260",
        "powerplant": {
            "count": 1,
            "manufacturer": null,
            "type": "Piston"
        },
        "seats": null,
        "serial_number": "32-528",
        "type": "Airplane",
        "type_designator": "PA32",
        "wake_turbulence_category": "Light"
    },
    "icao_hex": "C00010",
    "military": false,
    "registrant": {
        "administrative_area": "Oklahoma",
        "city": "Stillwater",
        "country": "CA",
        "names": [
            "Roger D S Reetz"
        ],
        "postal_code": "74074",
        "street": [
            "2701 S Mar Vista Street"
        ],
        "type": "Individual"
    },
    "registration": "C-FAAP",
    "source": "ca-transport-canada"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 C038AA | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "manufactured_date": "2018-01-01T00:00:00Z",
        "manufacturer": "THE BOEING COMPANY",
        "manufacturer_model": "BOEING 787-9 Dreamliner",
        "model": "787-9",
        "powerplant": {
            "count": 2,
            "manufacturer": null,
            "type": "Turbo-fan"
        },
        "seats": null,
        "serial_number": "38356",
        "type": "Airplane",
        "type_designator": "B789",
        "wake_turbulence_category": "Heavy"
    },
    "icao_hex": "C038AA",
    "military": false,
    "registrant": {
        "administrative_area": "Quebec",
        "city": "Saint-Laurent",
        "country": "CA",
        "names": [
            "Air Canada"
        ],
        "postal_code": "H4S1Z3",
        "street": [
            "7373 de la Côte-Vertu Blvd."
        ],
        "type": "Entity"
    },
    "registration": "C-FVLX",
    "source": "ca-transport-canada"
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

Published once, at the end of a run, to `SkyFollower/runner/ca-transport-canada/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_ca_transport_canada_{name}/config` for each of the three stats above.
