# Third-Party Notices

SkyFollower itself is licensed under `GPL-3.0-or-later` (see
[`LICENSE`](https://github.com/BrentIO/SkyFollower/blob/main/LICENSE)).
It depends on and, in some cases, redistributes third-party components that
carry their own licenses. This file records the ones that are copyleft or
otherwise carry an attribution/notice requirement; it is not an exhaustive
inventory of every transitive dependency (the large majority are permissively
licensed — MIT, BSD, Apache-2.0, ISC — and impose no notice obligation).

## Copyleft components

### pyModeS

- **Project:** https://github.com/junzis/pyModeS
- **License:** GPL-3.0
- **Used by:** `receiver`, `message-processor`, `tools/traffic-recorder`
- **How:** imported as a library dependency (`pyModeS==3.6.0`) for Mode-S / ADS-B
  (1090 MHz) frame decoding. Not modified; not vendored into this repo.

### pyModeS978

- **Project:** https://github.com/BrentIO/pyModeS978
- **License:** GPL-3.0-or-later
- **Used by:** `message-processor`
- **How:** imported as a library dependency (`pyModeS978==2026.9.1` — the first
  GPL-3.0-or-later release) for UAT (978 MHz) frame decoding — a standalone,
  pure-Python decoder with no `pyModeS` dependency. Authored by the same
  maintainer as SkyFollower. Not modified; not vendored into this repo.

### odfpy

- **Project:** https://github.com/eea/odfpy
- **License:** Apache-2.0 OR GPL-2.0-or-later (multi-licensed; the Apache-2.0 and
  GPL-2.0-or-later options are both compatible with `GPL-3.0-or-later`)
- **Used by:** registry data runners that parse OpenDocument spreadsheet source files
- **How:** imported as a library dependency. Not modified; not vendored into this repo.

## Weak-copyleft dependencies

Bundled into component container images. Each is either dual-licensed with a
permissive option or file-scoped copyleft; none impose a source-disclosure
obligation on SkyFollower, and all are compatible with `GPL-3.0-or-later`.

### paho-mqtt

- **Project:** https://github.com/eclipse-paho/paho.mqtt.python
- **License:** EPL-2.0 OR BSD-3-Clause (Eclipse Distribution License)
- **Used by:** nearly every component — it's in `shared/` plus each component and
  data runner that publishes MQTT status/telemetry
- **How:** imported as a library dependency. Not modified; not vendored.

### certifi

- **Project:** https://github.com/certifi/python-certifi
- **License:** MPL-2.0
- **Used by:** transitively, wherever `requests` / `boto3` are present
  (`management-ui` backend, `archive-processor`, `archive-compaction`,
  `core-health`, `aws-setup`, every data runner, `tools/legacy-migration`)
- **How:** a CA-certificate bundle, imported transitively. Not modified; not vendored.

## Basemap data and styles

The `map` and `management-ui` frontends render a MapLibre GL basemap from
[OpenFreeMap](https://openfreemap.org/) (`tiles.openfreemap.org/styles/positron`),
which carries upstream attribution requirements surfaced in the map's own
attribution control:

- **Map data:** © OpenStreetMap contributors, under the
  [Open Database License (ODbL) 1.0](https://opendatacommons.org/licenses/odbl/1-0/).
- **"Positron" style design:** © [CARTO](https://carto.com/), under
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- **Tile hosting / vector schema:** [OpenFreeMap](https://openfreemap.org/)
  (data prepared with Planetiler, Apache-2.0).

No tiles or style JSON are vendored into this repo; they are fetched at runtime
from OpenFreeMap by the browser.

## Runtime data sources

The data runners fetch third-party datasets at run time and write them into
Redis, from where enrichment fields (registration, operator, aircraft type,
route, airport) are embedded into archived flight records. This data is **not**
redistributed by SkyFollower itself — each deployment fetches it directly and
stores archives in its own S3 bucket — but an operator who republishes those
archives inherits the source terms. The notable ones:

- **Mictronics aircraft database** (https://www.mictronics.de/aircraft-database/) —
  distributed with an Open Data Commons license (attribution, and for ODbL,
  share-alike on the database).
- **OurAirports** (https://ourairports.com/data/) — dedicated to the public
  domain; a link back is requested.
- **VRS standing-data** (https://github.com/vradarserver/standing-data) — no
  license stated by upstream; used for route (`route:{ident}`) enrichment.
- **Per-country civil-aviation registries** — each under its own government's
  terms of use; see each runner's page under `runners/`.

## Adding a component

When vendoring third-party material into this repository, add an entry here with
its project URL, license, where it lives in the tree, and the required
attribution. Only licenses compatible with `GPL-3.0-or-later` may be vendored.
