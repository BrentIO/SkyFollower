# uk-caa

| | |
|---|---|
| **Country** | United Kingdom |
| **Registration prefix** | `G-` |
| **Data source** | https://ginfoapi.caa.co.uk (G-INFO REST API) |
| **Format** | JSON API (search + details endpoints; no bulk download) |
| **Run frequency** | Weekly (Monday, 06:10 UTC) |
| **Depends on Mictronics for ICAO hex** | No — the G-INFO details payload includes the aircraft's ICAO 24-bit address directly (`AircraftDetails.ICAO24BitAircraftAddress.Hex`). |

## How it works

G-INFO has no bulk-export endpoint, so this runner enumerates the entire register itself: it POSTs `/api/aircraft/search` once for every 2-letter suffix combination from `AA` to `ZZ` (676 calls total, via `itertools.product`), then, for each result with `RegistrationStatus == "R"`, immediately calls `GET /api/aircraft/details/{id}` and writes the record — there is no intermediate accumulation. Search calls are not rate-limited; details calls sleep for `request_interval_seconds` between requests to be polite to the API. Any call that returns HTTP 403 is queued and retried once at a fixed 500ms interval after the main enumeration pass finishes. Lookup tables decode the API's free-text `AircraftClass` into canonical `aircraft.type`/`aircraft.category` values and full English country names into ISO 3166-1 alpha-2 codes, falling back to the raw source value for anything unmapped. Every written record explicitly sets `military: false` — this register is exclusively civil, and the explicit value ensures a stale `military: true` flag (from Mictronics or a prior record on a reused hex) is corrected on re-registration.

**Notable operational characteristic**: because it makes 676 prefix search calls plus a details fetch per registered aircraft, a full run takes several hours. It is deliberately scheduled as the last runner of the day in `config/ofelia/config.ini.example` (`10 6 * * 1`, after `ourairports` at `10 5 * * 1`) to avoid overlapping with other Monday jobs.

## Columns

Fields below are grouped by the payload they come from: the `POST /api/aircraft/search` response (list of match summaries) and the `GET /api/aircraft/details/{id}` response (full nested record). Field names are otherwise unique except where noted as a duplicate.

### Search response (`POST /api/aircraft/search`)

| Source field | Imported | Notes |
|---|---|---|
| `AircraftID` | ✅ | Used to fetch the details payload; not stored |
| `RegistrationStatus` | ✅ | Filters which `AircraftID`s get a details fetch (`"R"` only); not stored |
| `Mark` | ❌ | Duplicate of `RegistrationDetails.Mark`, which is read from the details payload instead |
| `SerialNo` | ❌ | Duplicate of `AircraftDetails.SerialNumber`, which is read from the details payload instead |
| `MilitarySerialNo` | ❌ | Present in source; never read (no equivalent field exists in the details payload) |
| `AircraftType` | ❌ | Duplicate of `AircraftDetails.Type`, which is read from the details payload instead |

### Details response (`GET /api/aircraft/details/{id}`)

| Source field | Imported | Notes |
|---|---|---|
| `RegistrationDetails.Mark` | ✅ | → `registration` (prefixed with `G-`) |
| `RegistrationDetails.Status` | ✅ | Filtered to `"Registered"` only; not stored |
| `RegistrationDetails.PreviousID` | ❌ | Present in source; not read by this runner |
| `RegistrationDetails.CurrentRegistrationDate` | ❌ | Present in source; not read by this runner |
| `RegistrationDetails.DeRegistrationDate` | ❌ | Present in source; not read by this runner |
| `RegistrationDetails.Reason` | ❌ | Present in source; not read by this runner |
| `RegistrationDetails.To` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.Manufacturer` | ✅ | → `aircraft.manufacturer` |
| `AircraftDetails.Type` | ✅ | → `aircraft.model` |
| `AircraftDetails.SerialNumber` | ✅ | → `aircraft.serial_number` |
| `AircraftDetails.ICAO24BitAircraftAddress.Hex` | ✅ | → `icao_hex`; records without a 6-character hex are discarded |
| `AircraftDetails.ICAO24BitAircraftAddress.Binary` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.ICAO24BitAircraftAddress.Octal` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.PopularName` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.ICAOAircraftTypeDesignator` | ✅ | → `aircraft.type_designator` |
| `AircraftDetails.AircraftClass` | ✅ | → `aircraft.type` / `aircraft.category` (decoded) |
| `AircraftDetails.BuildCategory` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.EasaCategory` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.Engines[0].TotalNumberOfEngines` | ✅ | → `aircraft.powerplant.count` |
| `AircraftDetails.Engines[0].Name` | ✅ | → `aircraft.powerplant.model`; only the first engine entry is used |
| `AircraftDetails.Engines[0].NumberOfEngines` | ❌ | Distinct field from `TotalNumberOfEngines` (which is used); present in source, not read |
| `AircraftDetails.Engines[0].IsPropeller` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.Mtow` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.TotalAirframeHours` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.YearBuild` | ✅ | → `aircraft.manufactured_date` |
| `AircraftDetails.MaintenanceProgramme` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.Expiry` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.OwnershipStatus` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.MaximumPassengers` | ✅ | → `aircraft.seats` (`MaximumPassengers + 1`) |
| `AircraftDetails.MaximumTakeOffMass` | ❌ | Present in source; not read by this runner (numeric duplicate of `Mtow`) |
| `AircraftDetails.CofaDetails.CofaCategory` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.CofaDetails.CofaValidityExpiryLabel` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.CofaDetails.CofaValidityExpiry` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.CofaDetails.CofaValidityReference` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.CofaDetails.CofaOperatingLimitations` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.CofaDetails.ShowOperatingLimitations` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.IsPotentialChangeOfOwnership` | ❌ | Present in source; not read by this runner |
| `AircraftDetails.EmergencyBallisticDeviceFitted` | ❌ | Present in source; not read by this runner |
| `MortgageDetails[].EntryDateAndTime` | ❌ | Present in source; not read by this runner |
| `MortgageDetails[].EntryType` | ❌ | Present in source; not read by this runner |
| `MortgageDetails[].ReferenceType` | ❌ | Present in source; not read by this runner |
| `RegisteredAircraftOwners[0].RegisteredOwner` | ✅ | → `registrant.names[0]`; only the first owner is used |
| `RegisteredAircraftOwners[0].AddressIndicatorID` | ❌ | Present in source; not read by this runner |
| `RegisteredAircraftOwners[0].AddressIndicator` | ❌ | Present in source; not read by this runner |
| `RegisteredAircraftOwners[0].TradingAs` | ❌ | Present in source; not read by this runner |
| `RegisteredAircraftOwners[0].TrusteeOf` | ❌ | Present in source; not read by this runner |
| `RegisteredAircraftOwners[0].Address1` / `Address2` | ✅ | → `registrant.street` |
| `RegisteredAircraftOwners[0].Town` | ✅ | → `registrant.city` |
| `RegisteredAircraftOwners[0].County` | ✅ | → `registrant.administrative_area` |
| `RegisteredAircraftOwners[0].PostCode` | ✅ | → `registrant.postal_code` |
| `RegisteredAircraftOwners[0].Country` | ✅ | → `registrant.country` (decoded to ISO 3166-1 alpha-2 where known) |
| `AircraftOperatedByAocHolder.showSection` | ❌ | Present in source; not read by this runner |
| `AircraftOperatedByAocHolder.OperatorName` | ❌ | Present in source; not read by this runner (operator enrichment for G-registered aircraft is not sourced from here) |
| `AircraftOperatedByAocHolder.OperatorTrandingAs` | ❌ | Present in source (sic — typo in the live API); not read by this runner |
| `InsuranceSummaryDetails.EvidenceVerifiedDate` | ❌ | Present in source; not read by this runner |
| `InsuranceSummaryDetails.NoFlyDate` | ❌ | Present in source; not read by this runner |
| `InsuranceSummaryDetails.AircraftMarkStatusID` | ❌ | Present in source; not read by this runner |
| `InsuranceSummaryDetails.AircraftID` | ❌ | Present in source; not read by this runner (duplicate of the search result's `AircraftID`) |
| `InsuranceSummaryDetails.Mark` | ❌ | Present in source; not read by this runner (duplicate of `RegistrationDetails.Mark`) |
| `NoiseSummaryDetails.NoiseCertificateIssueDate` | ❌ | Present in source; not read by this runner |
| `NoiseSummaryDetails.NoiseCertificateNumber` | ❌ | Present in source; not read by this runner |
| `NoiseSummaryDetails.NoiseChapter` | ❌ | Present in source; not read by this runner |
| `RegistrationMarkExemption.IssueDate` | ❌ | Present in source; not read by this runner |
| `RegistrationMarkExemption.Origin` | ❌ | Present in source; not read by this runner |
| `RegistrationMarkExemption.CodeCarried` | ❌ | Present in source; not read by this runner |
| `RegistrationMarkExemption.SerialCarried` | ❌ | Present in source; not read by this runner |
| `OpenRegistrationCases[].CaseName` | ❌ | Present in source; not read by this runner |
| `OpenRegistrationCases[].OpenDate` | ❌ | Present in source; not read by this runner |
| `OpenRegistrationCases[].ReviewDate` | ❌ | Present in source; not read by this runner |
| `OpenRegistrationCases[].PotentialChangeOfOwnership` | ❌ | Present in source; not read by this runner |
| `OpenApplicationsAndApprovalsCases[].CaseName` | ❌ | Present in source; not read by this runner |
| `OpenApplicationsAndApprovalsCases[].OpenDate` | ❌ | Present in source; not read by this runner |
| `OpenApplicationsAndApprovalsCases[].ReviewDate` | ❌ | Present in source; not read by this runner |
| `OpenApplicationsAndApprovalsCases[].PotentialChangeOfOwnership` | ❌ | Present in source; not read by this runner |
| `ExtractDate` | ❌ | Present in source; not read by this runner |

See `specs/data-dictionary.yaml` (`uk-caa` entry) for full column semantics and cross-source schema notes.

## Example Output

Read back the merged record for a given ICAO hex (combines this runner's data with Mictronics and any other sources that have written to the same key):

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 401A48 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "category": "Land",
        "manufactured_date": "1910-01-01T00:00:00Z",
        "manufacturer": "DEPERDUSSIN CIE",
        "model": "DEPERDUSSIN MONOPLANE",
        "powerplant": {
            "count": 1,
            "model": "ANZANI Y TYPE"
        },
        "seats": 1,
        "serial_number": "43",
        "type": "Airplane",
        "type_designator": "ULAC"
    },
    "icao_hex": "401A48",
    "registrant": {
        "city": "BIGGLESWADE",
        "country": "GB",
        "names": [
            "RICHARD SHUTTLEWORTH TRUSTEES"
        ],
        "postal_code": "SG18 9ER",
        "street": [
            "OLD WARDEN AERODROME",
            "OLD WARDEN"
        ]
    },
    "registration": "G-AANH",
    "source": "uk-caa"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 407F19 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "category": "Land",
        "manufactured_date": "2023-01-01T00:00:00Z",
        "manufacturer": "AIRBUS SAS",
        "manufacturer_model": "AIRBUS A-350-1000",
        "model": "AIRBUS A350-1041",
        "powerplant": {
            "count": 2,
            "model": "ROLLS-ROYCE Trent XWB-97"
        },
        "seats": 336,
        "serial_number": "605",
        "type": "Airplane",
        "type_designator": "A35K",
        "wake_turbulence_category": "Heavy"
    },
    "icao_hex": "407F19",
    "military": false,
    "registrant": {
        "city": "CRAWLEY",
        "country": "GB",
        "names": [
            "VIRGIN ATLANTIC AIRWAYS LTD"
        ],
        "postal_code": "RH10 9DF",
        "street": [
            "THE VHQ",
            "FLEMING WAY"
        ]
    },
    "registration": "G-VBOB",
    "source": "uk-caa"
}
```

## Configuration

Reads `settings.json` (mounted at `/app/settings.json`):

| Parameter | Required | Default | Notes |
|---|---|---|---|
| `redis.host` | ✅ | — | Redis connection host |
| `redis.port` | ❌ | `6379` | |
| `request_interval_seconds` | ❌ | `0.1` | Sleep between G-INFO details calls (seconds); search calls are not rate-limited |
| `mqtt.host` | ❌ | — | Omit the whole `mqtt` block to skip completion-stats publishing entirely |
| `mqtt.port` | ❌ | `1883` | |
| `mqtt.username` | ❌ | — | Optional MQTT auth; omit for an anonymous broker |
| `mqtt.password` | ❌ | — | |
| `redis_ttl_days` | ❌ | `14` | TTL applied to each `aircraft:registry:{icao_hex}` key written by this runner |

## MQTT

Published once, at the end of a run, to `SkyFollower/runner/uk-caa/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `records_imported` | e.g. `271` | Integer as string |
| `last_run_at` | e.g. `2026-07-07T14:32:01.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_uk_caa_{name}/config` for each of the three stats above.
