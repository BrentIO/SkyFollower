// Pure view-model builder for the aircraft list flyout (see
// components/AircraftListPanel.tsx). Same split as lib/aircraftDetail.ts's
// pairing with AircraftDetailPanel.tsx -- the per-column source/omission
// rules are the panel's actual spec, so they're covered by plain unit
// tests rather than only exercisable through a rendered map (this project
// has no jsdom/component-render test setup -- see lib/config.test.ts).

import type { AircraftRecord, AircraftMap } from "./aircraftState";
import { buildDistanceDisplay, isEmergencySquawk } from "./aircraftDetail";
import type { CenterPoint } from "./config";
import { formatAltitude, trendArrow } from "./infoBox";

function nonEmpty(value: string | null | undefined): string | null {
  return value != null && value.trim() !== "" ? value : null;
}

export interface AircraftListRow {
  icaoHex: string;
  /** Plain ident string -- the Military/Special-livery pills the Ident
   * column renders are decoration only and never derived from this. */
  ident: string | null;
  registration: string | null;
  typeDesignator: string | null;
  /** ICAO Doc 8643 description code, e.g. "L2J" -- same field
   * aircraftIconResolver.ts keys off of. */
  descriptionCode: string | null;
  military: boolean;
  specialLivery: string | null;
  /** ISO 3166-1 alpha-2 country-of-registration code (AircraftInfo.country_code)
   * -- drives the flag icon rendered next to Ident (lib/countryFlag.ts).
   * Null when no country has resolved for this aircraft. */
  countryCode: string | null;
  /** Resolved country display name (AircraftInfo.country) -- used as the
   * flag icon's title/tooltip text. */
  country: string | null;
  /** Raw feet, for sorting -- see altitudeDisplay for the rendered text. */
  altitudeFt: number | null;
  /** "39500", "↓ 22750", "↑ 37000" -- trendArrow()'s ↑/↓ glyph prefixed
   * (with a separating space) onto the formatted altitude, matching this
   * column's worked example ("v 39500"). trendArrow() itself is also used
   * by lib/infoBox.ts's on-map info boxes, which *suffix* it instead --
   * that existing usage is untouched; this is a second, independent call. */
  altitudeDisplay: string | null;
  /** Raw nautical miles, for sorting -- see distanceDisplay for the
   * rendered text. Derived from distanceDisplay rather than recomputed,
   * so the sorted value and the displayed value can never disagree. */
  distanceNm: number | null;
  /** Same computation as AircraftDetailPanel's own Distance row --
   * aircraftDetail.ts's buildDistanceDisplay(flight, center), reused
   * verbatim rather than reimplemented. */
  distanceDisplay: string | null;
  /** isEmergencySquawk(flight.squawk) -- drives the row's emergency
   * highlight, overriding normal banding entirely. */
  emergency: boolean;
  /** True when flight.receiver_sources includes "978" (UAT) -- drives the
   * Tags column's blue "U" badge (#1901). */
  isUat: boolean;
  /** True when flight.receiver_sources includes "EXTERNAL" -- drives the
   * Tags column's blue "E" badge (#1901). */
  isExternal: boolean;
}

// One aircraft's row. `flight` is an AircraftRecord (not just MapFlight)
// since the panel's population rule keys off AircraftRecord.hidden -- see
// buildAircraftListRows below, which applies that filter before this runs.
export function buildAircraftListRow(flight: AircraftRecord, center: CenterPoint | null): AircraftListRow {
  const aircraft = flight.aircraft;
  const distanceDisplay = buildDistanceDisplay(flight, center);
  const arrow = trendArrow(flight.vs);
  const altitudeDisplay =
    flight.alt != null ? (arrow ? `${arrow} ${formatAltitude(flight.alt)}` : formatAltitude(flight.alt)) : null;

  return {
    icaoHex: flight.icao_hex,
    ident: nonEmpty(flight.ident),
    registration: nonEmpty(aircraft?.registration),
    typeDesignator: nonEmpty(aircraft?.type_designator),
    descriptionCode: nonEmpty(aircraft?.description_code),
    military: !!aircraft?.military,
    specialLivery: nonEmpty(aircraft?.special_livery),
    countryCode: nonEmpty(aircraft?.country_code),
    country: nonEmpty(aircraft?.country),
    altitudeFt: flight.alt ?? null,
    altitudeDisplay,
    distanceNm: distanceDisplay != null ? Number.parseFloat(distanceDisplay) : null,
    distanceDisplay,
    emergency: isEmergencySquawk(flight.squawk),
    isUat: !!flight.receiver_sources?.includes("978"),
    isExternal: !!flight.receiver_sources?.includes("EXTERNAL"),
  };
}

// Population rule (see the issue's "Panel mechanics" section): every
// aircraft currently tracked that isn't `hidden` -- the same filter
// lib/featureCollections.ts already applies for the map's own icon layer.
// `stale` aircraft are included, not specially marked in this iteration.
export function buildAircraftListRows(aircraft: AircraftMap, center: CenterPoint | null): AircraftListRow[] {
  return Object.values(aircraft)
    .filter((a) => !a.hidden)
    .map((a) => buildAircraftListRow(a, center));
}
