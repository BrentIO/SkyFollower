# Third-Party Notices

SkyFollower itself is licensed under `GPL-3.0-or-later` (see
[`LICENSE`](https://github.com/BrentIO/SkyFollower/blob/main/LICENSE)).
It depends on and, in some cases, redistributes third-party components that
carry their own licenses. This file records the ones that are copyleft or
otherwise noteworthy; it is not an exhaustive inventory of every transitive
dependency (the large majority are permissively licensed — MIT, BSD, Apache-2.0,
ISC).

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

## Adding a component

When vendoring third-party material into this repository, add an entry here with
its project URL, license, where it lives in the tree, and the required
attribution. Only licenses compatible with `GPL-3.0-or-later` may be vendored.
