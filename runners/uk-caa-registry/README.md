# United Kingdom 🇬🇧 CAA Registry

| | |
|---|---|
| **Name** | `uk-caa-registry` |
| **Country** | United Kingdom |
| **Registration prefix** | `G-` |
| **Data source** | https://ginfoapi.caa.co.uk (G-INFO REST API) |
| **Format** | JSON API (search + details endpoints; no bulk download) |
| **Run frequency** | Search: weekly (Monday, 06:10 UTC). Details: daily (19:10 UTC) |
| **Depends on Mictronics for ICAO hex** | No — the G-INFO details payload includes the aircraft's ICAO 24-bit address directly (`AircraftDetails.ICAO24BitAircraftAddress.Hex`). |

## How it works

G-INFO has no bulk-export endpoint, and its rate limit makes fetching the whole register in one run too slow. The runner is therefore two jobs, run from one image:

| Job | Command | Schedule | What it does |
|---|---|---|---|
| Search | `python uk-caa-registry/main.py search` | Weekly | Searches every 2-letter suffix `AA`–`ZZ` (676 `POST /api/aircraft/search` calls) and maintains a queue of registered aircraft in Redis. It never fetches details. |
| Details | `python uk-caa-registry/main.py details` | Daily | Calls `GET /api/aircraft/details/{id}` for the aircraft that are due, up to a limit, and writes `aircraft:registry:{icao_hex}`. It never searches. |

The search response has no ICAO address, so only the details job can write aircraft records. Search is weekly because how often CAA publishes register changes is not known.

Lookup tables decode the API's free-text `AircraftClass` into canonical `aircraft.type`/`aircraft.category` values and full English country names into ISO 3166-1 alpha-2 codes, falling back to the raw source value for anything unmapped. Every written record explicitly sets `military: false` — this register is exclusively civil, and the explicit value ensures a stale `military: true` flag (from Mictronics or a prior record on a reused hex) is corrected on re-registration.

Whenever a record has an `aircraft.type_designator`, `aircraft:type:{type_designator}` is looked up in Redis (populated by the `mictronics` runner) and, if found, its `manufacturer_model` and `description_code` (ICAO Doc 8643 description code, e.g. `L2J`) are set directly on this record — unconditionally, regardless of whether Mictronics also has values for the same hex. This runner's own `type_designator` is sourced directly from the CAA and is authoritative; `merge_aircraft.lua`'s "registry wins over mictronics" precedence rule guarantees these values take priority at read time either way. The lookup is not a hard dependency — a missing reference table entry, or the table not existing yet, leaves the record exactly as it would have been without this step.

## Redis keys

| Key | Contents | TTL | TTL renewed by |
|---|---|---|---|
| `uk_caa:aircraft:{AircraftID}` | Queue record: `aircraft_id`, `mark`, `next_fetch_at` (epoch seconds; `0` until first processed) | 18 days | A search that finds the aircraft registered |
| `uk_caa:search:progress` | Hash of prefix → epoch seconds of its last successful search | None | — |
| `uk_caa:lock` | Held by whichever job is calling CAA | 5 minutes | The running job |
| `aircraft:registry:{icao_hex}` | Aircraft record | 70 days | A successful details fetch |

The details job asks the `idx:uk_caa:aircraft` search index for due records, so Redis does the filtering, sorting and limiting:

```
FT.SEARCH idx:uk_caa:aircraft "@next_fetch_at:[-inf {now}]" SORTBY next_fetch_at ASC LIMIT 0 {batch} NOCONTENT
```

Both TTLs drop data only after two missed refreshes: details are due every 28 days and searches every 7.

### Search job

- Prefixes are searched longest-waiting first, so a blocked or stopped run doesn't starve later ones. A prefix's timestamp changes only when its search succeeds.
- A new registered (`R`) aircraft gets a queue record that is due immediately. A known one has its TTL renewed and keeps its `next_fetch_at`.
- A result with any other status deletes the queue record **only if its mark matches the queued mark**. G-INFO lists one `AircraftID` under every mark the aircraft has held, so a deregistered old mark says nothing about the current one.
- An aircraft missing from the results, or a failed search, deletes nothing.

### Details job

| Outcome | `next_fetch_at` | Aircraft record |
|---|---|---|
| Fetched | 28 days later | Written with a 70-day TTL |
| Request failed | 1 day later | Not renewed |
| Not `Registered`, or no ICAO address or mark | 28 days later | Not written |

Neither job deletes `aircraft:registry:*` keys. A deregistered aircraft's record expires on its own, up to 70 days after its last successful fetch.

## First fill

New queue records are all due at once. At the default 1,000 a day, a register of about 20,000 aircraft (an estimate) takes about 20 days to fill, if CAA accepts that rate. Once filled, a 28-day cycle needs about 700 a day. If searches fail for long enough that queue records expire, those aircraft come back as due immediately.

## Rate limiting

G-INFO refuses clients that send requests too quickly with HTTP 403. Its limit is not published. In live runs, a block often followed 25 accepted requests, and sometimes came after only one.

- **Pacing:** the runner waits before every request, 1 second by default.
- **Refusals:** on HTTP 403 or 429 the runner waits, then retries the same request. The waits are 65, 120, 300, 600 and 900 seconds. A longer `Retry-After` value is honoured, up to one hour. Each refusal is logged with the number of requests accepted since the previous one, the response headers useful for diagnosis, and the start of the response body.
- **Giving up:** if a request is still refused after the last wait, the run stops. The aircraft being fetched stays due.
- **Other errors:** server errors, connection errors and timeouts are retried after 2, 5 and 15 seconds.
- **One job at a time:** both jobs share CAA's limit, so each takes a Redis lock before its first request, waiting if the other holds it.
- **Maximum run time:** 12 hours for search and 10 for details, including any wait for the lock. The schedules leave room for both.

## Run status

Reaching the details limit, or the details job's maximum run time after making progress, is a success.

| Job | `last_run_status` is `Failure` when |
|---|---|
| Search | It stops before searching every prefix, or any prefix search fails |
| Details | It stops on persistent refusal, loses the lock, cannot fetch anything while aircraft are due, or more than 1% of requests fail |

The exit code is non-zero on failure. Work already done is kept.

## Upgrading from the single weekly job

`scripts/install.sh --upgrade` on the core role does the following:

1. Replaces the compose file, and `up -d` recreates `ofelia` with the two new jobs in place of the old one.
2. Stops any UK runner container still running from another version.
3. If no search has completed a prefix yet, starts the search job in the background. The daily details job picks up queue records as they appear.

Existing aircraft records are not migrated: each keeps its TTL until a details fetch rewrites it or it expires.

To upgrade by hand, from the core host's directory after replacing `docker-compose.core.yaml` and setting the new `SKYFOLLOWER_VERSION`:

```bash
docker ps --format '{{.ID}} {{.Image}}' | grep skyfollower-runner-uk-caa-registry   # docker stop any old run
docker compose --profile runners pull
docker compose up -d
docker compose run -d --rm runner-uk-caa-registry-search
```

## Running by hand

```bash
docker compose run --rm runner-uk-caa-registry-search
docker compose run --rm runner-uk-caa-registry-details
```

To follow a run started by `ofelia` or the installer:

```bash
docker logs -f "$(docker ps -q --filter ancestor=ghcr.io/brentio/skyfollower-runner-uk-caa-registry:$(grep '^SKYFOLLOWER_VERSION=' .env | cut -d= -f2))"
```

## Columns

Fields below are grouped by the payload they come from: the `POST /api/aircraft/search` response (list of match summaries) and the `GET /api/aircraft/details/{id}` response (full nested record). Field names are otherwise unique except where noted as a duplicate.

### Search response (`POST /api/aircraft/search`)

| Source field | Imported | Notes |
|---|---|---|
| `AircraftID` | ✅ | → queue record `aircraft_id` and key; used to fetch the details payload |
| `RegistrationStatus` | ✅ | Only `"R"` results are queued; not stored |
| `Mark` | ✅ | → queue record `mark`; the aircraft record's `registration` comes from `RegistrationDetails.Mark` |
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
| `AircraftDetails.ICAOAircraftTypeDesignator` | ✅ | → `aircraft.type_designator`; also used to look up `aircraft:type:{type_designator}` in Redis, setting `aircraft.manufacturer_model` and `aircraft.description_code` when found |
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

See `specs/data-dictionary.yaml` (`uk-caa-registry` entry) for full column semantics and cross-source schema notes.

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
        "manufacturer_model": "DEPERDUSSIN CIE DEPERDUSSIN MONOPLANE",
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
    "data_sources": [
        "uk-caa-registry"
    ],
    "icao_hex": "401A48",
    "military": false,
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
    "registration": "G-AANH"
}
```

```bash
docker run --rm --network host redis:latest redis-cli EVAL "$(cat ./shared/lua/merge_aircraft.lua)" 0 407F19 | python3 -m json.tool --sort-keys --no-ensure-ascii
```

```json
{
    "aircraft": {
        "category": "Land",
        "description_code": "L2J",
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
        "type_designator": "A35K"
    },
    "data_sources": [
        "mictronics",
        "uk-caa-registry"
    ],
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
    "registration": "G-VBOB"
}
```

## Configuration

See [Data Runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md#configuration) for the environment variables every runner reads. Two more are optional, set in the core host's `.env`:

| `.env` variable | In the container | Default | What it does |
|---|---|---|---|
| `UK_CAA_REQUEST_INTERVAL_SECONDS` | `REQUEST_INTERVAL_SECONDS` | `1` | Delay before every request |
| `UK_CAA_DETAILS_LIMIT` | `DETAILS_LIMIT` | `1000` | Most aircraft one details run processes |

After changing either, run `docker compose up -d` so `ofelia` picks it up. TTLs, refresh intervals and maximum run times are the `UK_CAA_*` constants in `shared/timing.py`.

## MQTT

Published once, at the end of each run, to `SkyFollower/runner/uk-caa-registry/{job}/statistic/{name}` (all retained), where `{job}` is `search` or `details`:

| Job | Topic suffix | Value |
|---|---|---|
| search | `prefixes_searched` / `prefixes_failed` | Prefix searches this run |
| search | `aircraft_found` | Registered results seen |
| search | `aircraft_added` / `aircraft_removed` | Queue records created and deleted |
| details | `records_imported` | Aircraft records written this run |
| details | `requests_failed` | Failed details requests this run |
| details | `queue_size` | Aircraft in the queue |
| details | `aircraft_due` | Aircraft due now |
| details | `aircraft_never_fetched` | Queue records not yet processed |
| details | `oldest_overdue_days` | How far overdue the longest-waiting processed aircraft is. A value that keeps rising means the limit or rate is too low; near 42, aircraft records start expiring |
| both | `last_run_at` | ISO 8601 UTC |
| both | `last_run_status` | `Success` or `Failure` |

The four queue counts are omitted if Redis cannot be queried. Each run also publishes empty retained payloads to the single weekly job's old topics and discovery configs, which removes its three Home Assistant sensors.

Home Assistant autodiscovery configs are also published (retained) to `homeassistant/sensor/SkyFollower_runner_uk_caa_registry_{job}_{name}/config` for each statistic above, on one shared device.
