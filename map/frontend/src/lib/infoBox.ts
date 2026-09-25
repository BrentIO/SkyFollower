// Pure formatting/omission logic for the ATC-style info-box overlay (see
// components/InfoBox.tsx). Split out so the field-order and omit-don't-
// placeholder rules -- extensively iterated on in design -- are covered by
// plain unit tests rather than only exercisable through a rendered map.

export interface InfoBoxAircraft {
  ident?: string | null;
  alt?: number | null; // feet MSL
  velocity?: number | null; // knots (groundspeed)
  vs?: number | null; // ft/min; negative = descending
  aircraft?: {
    registration?: string | null;
    type_designator?: string | null;
  } | null;
}

// ±500 ft/min band around zero is "level" -- the arrow is omitted
// entirely rather than shown as a placeholder character.
const VERTICAL_SPEED_LEVEL_THRESHOLD = 500;

export type TrendArrow = "↑" | "↓" | "";

// Derives the climb/descend glyph from vertical_speed. Level (within
// ±500ft/min, inclusive) or unknown vertical_speed both omit the arrow --
// there is no third "level" glyph.
//
// Kept exactly as-is (Unicode glyph, not a direction enum) for its other
// two callers -- lib/aircraftListRow.ts's list panel and
// lib/aircraftDetail.ts's detail panel -- which render it as ordinary text
// and aren't part of issue #2001. See trendDirection() below for the
// info-box-only replacement.
export function trendArrow(verticalSpeed: number | null | undefined): TrendArrow {
  if (verticalSpeed == null) return "";
  if (verticalSpeed > VERTICAL_SPEED_LEVEL_THRESHOLD) return "↑";
  if (verticalSpeed < -VERTICAL_SPEED_LEVEL_THRESHOLD) return "↓";
  return "";
}

export type TrendDirection = "up" | "down" | null;

// Same ±500ft/min threshold as trendArrow(), but returns a direction
// instead of a Unicode character. #2001: the info box renders the trend
// as an inline SVG glyph rather than a font glyph (a specific Unicode
// character isn't guaranteed to be in every platform's font's glyph
// table, and macOS was observed silently substituting a different,
// undersized fallback font for just that character) -- InfoBoxLayer.tsx
// needs a direction value to pick an SVG shape, not a character to embed
// in text. A separate function rather than reusing trendArrow()'s output
// so the two Unicode-text callers above stay untouched.
export function trendDirection(verticalSpeed: number | null | undefined): TrendDirection {
  if (verticalSpeed == null) return null;
  if (verticalSpeed > VERTICAL_SPEED_LEVEL_THRESHOLD) return "up";
  if (verticalSpeed < -VERTICAL_SPEED_LEVEL_THRESHOLD) return "down";
  return null;
}

// "35000" -- full feet, no thousands separator, never flight-level shorthand.
export function formatAltitude(altitudeFt: number): string {
  return String(Math.round(altitudeFt));
}

// "450kt" -- no space before the unit suffix.
export function formatGroundspeed(velocityKt: number): string {
  return `${Math.round(velocityKt)}kt`;
}

export interface AltitudeSpeedLine {
  altitude: string | null;
  trend: TrendDirection;
  groundspeed: string | null;
}

// Line 2: rendered by InfoBoxLayer.tsx as "35000<arrow> 450kt". Returned
// as parts, not one formatted string like line 1/3, because the trend
// renders as an inline SVG glyph there, not a character embedded in text
// (#2001). Either altitude or groundspeed alone is independently omitted
// (null) when its underlying field is unknown -- never a placeholder. The
// whole line is omitted (returns null) only when both are unknown. trend
// is only ever non-null alongside a known altitude (an arrow with nothing
// to its left would be a stray glyph).
export function buildAltitudeSpeedLine(aircraft: InfoBoxAircraft): AltitudeSpeedLine | null {
  const altitude = aircraft.alt != null ? formatAltitude(aircraft.alt) : null;
  const trend = altitude !== null ? trendDirection(aircraft.vs) : null;
  const groundspeed = aircraft.velocity != null ? formatGroundspeed(aircraft.velocity) : null;

  if (altitude === null && groundspeed === null) return null;
  return { altitude, trend, groundspeed };
}

// Line 3: "N988DL B752". A missing registration or type designator is
// simply omitted (not "?"/"N/A"); if both are missing, the whole line is
// omitted (null) rather than rendered empty.
export function formatRegistrationTypeLine(aircraft: InfoBoxAircraft): string | null {
  const parts = [aircraft.aircraft?.registration, aircraft.aircraft?.type_designator].filter(
    (p): p is string => !!p && p.trim() !== "",
  );
  return parts.length > 0 ? parts.join(" ") : null;
}

// Line 1: ident/callsign, blank/omitted (not falling back to icao_hex)
// when not yet resolved.
export function formatIdentLine(aircraft: InfoBoxAircraft): string | null {
  const ident = aircraft.ident?.trim();
  return ident ? ident : null;
}

export interface InfoBoxLines {
  ident: string | null;
  altitudeSpeed: AltitudeSpeedLine | null;
  registrationType: string | null;
}

// All three lines, in the deliberately-chosen order: ident, then
// altitude/trend/groundspeed, then registration/type. Do not reorder --
// see the issue's Info box section for why this order was chosen. A box
// with every line omitted (a freshly-tracked aircraft with no metadata or
// position fields resolved yet) still returns an object with all-null
// lines; callers should skip rendering the box entirely in that case.
export function buildInfoBoxLines(aircraft: InfoBoxAircraft): InfoBoxLines {
  return {
    ident: formatIdentLine(aircraft),
    altitudeSpeed: buildAltitudeSpeedLine(aircraft),
    registrationType: formatRegistrationTypeLine(aircraft),
  };
}
