// Pure formatting/omission logic for the ATC-style info-box overlay (see
// components/InfoBox.tsx). Split out so the field-order and omit-don't-
// placeholder rules -- extensively iterated on in design -- are covered by
// plain unit tests rather than only exercisable through a rendered map.

export interface InfoBoxAircraft {
  ident?: string | null;
  altitude?: number | null; // feet MSL
  velocity?: number | null; // knots (groundspeed)
  vertical_speed?: number | null; // ft/min; negative = descending
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
export function trendArrow(verticalSpeed: number | null | undefined): TrendArrow {
  if (verticalSpeed == null) return "";
  if (verticalSpeed > VERTICAL_SPEED_LEVEL_THRESHOLD) return "↑";
  if (verticalSpeed < -VERTICAL_SPEED_LEVEL_THRESHOLD) return "↓";
  return "";
}

// "35,000" -- full feet, comma-formatted, never flight-level shorthand.
export function formatAltitude(altitudeFt: number): string {
  return Math.round(altitudeFt).toLocaleString("en-US");
}

// "450kt" -- no space before the unit suffix.
export function formatGroundspeed(velocityKt: number): string {
  return `${Math.round(velocityKt)}kt`;
}

// Line 2: "35,000↓ 450kt". Either half (altitude+arrow, or groundspeed)
// is independently omitted when its underlying field is unknown -- never
// shown as a placeholder. The whole line is omitted (null) only when both
// halves are unknown. The trend arrow is only ever attached to a known
// altitude (an arrow with nothing to its left would be a stray glyph).
export function formatAltitudeSpeedLine(aircraft: InfoBoxAircraft): string | null {
  const altitudePart =
    aircraft.altitude != null ? `${formatAltitude(aircraft.altitude)}${trendArrow(aircraft.vertical_speed)}` : null;
  const speedPart = aircraft.velocity != null ? formatGroundspeed(aircraft.velocity) : null;

  const parts = [altitudePart, speedPart].filter((p): p is string => p !== null);
  return parts.length > 0 ? parts.join(" ") : null;
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
  altitudeSpeed: string | null;
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
    altitudeSpeed: formatAltitudeSpeedLine(aircraft),
    registrationType: formatRegistrationTypeLine(aircraft),
  };
}

// How many of the (up to 3) lines actually render -- used to size the box
// for the overlap-placement algorithm (lib/placement.ts) before the DOM
// element exists.
export function infoBoxLineCount(lines: InfoBoxLines): number {
  return [lines.ident, lines.altitudeSpeed, lines.registrationType].filter((l) => l !== null).length;
}
