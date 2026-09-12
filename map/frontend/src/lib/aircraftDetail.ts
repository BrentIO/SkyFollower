// Pure formatting/omission logic for the aircraft detail side panel (see
// components/AircraftDetailPanel.tsx). Same split as lib/infoBox.ts's
// InfoBoxLayer pairing -- the omit-if-unknown rules here are the panel's
// actual spec, so they're covered by plain unit tests rather than only
// exercisable through a rendered map (this project has no jsdom/component-
// render test setup -- see lib/config.test.ts's own note on that).

import type { AircraftInfo, AirportRef, MapFlight, OperatorInfo, RegistrantInfo } from "../api/types";
import type { HomePoint } from "./config";
import { airportLocation, receiverSourceLabel } from "./flightView";
import { greatCircleNm } from "./geo";
import { trendArrow } from "./infoBox";

// Same four emergency squawk codes as management-ui's lib/flightView.ts
// EMERGENCY_SQUAWKS -- own copy, per this project's "separate frontend,
// separate copy" convention (see this file's sibling flightView.ts).
export const EMERGENCY_SQUAWKS = new Set(["7500", "7600", "7700", "7777"]);

// A near-miss (e.g. "7501") is plain text, not colored -- only an exact
// match to one of the four codes above triggers the red+bold treatment.
export function isEmergencySquawk(squawk: string | null | undefined): boolean {
  return !!squawk && EMERGENCY_SQUAWKS.has(squawk);
}

export interface AirportBlockData {
  icaoCode: string;
  iataCode: string | null;
  name: string | null;
  location: string | null;
}

function nonEmpty(value: string | null | undefined): string | null {
  return value != null && value.trim() !== "" ? value : null;
}

function buildAirportBlock(airport: AirportRef | null | undefined): AirportBlockData | null {
  const icaoCode = nonEmpty(airport?.icao_code);
  if (!icaoCode) return null;
  return {
    icaoCode,
    iataCode: nonEmpty(airport?.iata_code),
    name: nonEmpty(airport?.name),
    location: airport ? airportLocation(airport) : null,
  };
}

export interface RouteData {
  origin: AirportBlockData;
  destination: AirportBlockData;
}

// The Route section is an atomic unit: unlike every other section, it
// disappears entirely (bar included) unless *both* endpoints resolve to at
// least an ICAO code -- "half a route" (only an origin, or only a
// destination) isn't meaningful to show on its own.
function buildRoute(flight: MapFlight): RouteData | null {
  const origin = buildAirportBlock(flight.origin);
  const destination = buildAirportBlock(flight.destination);
  if (!origin || !destination) return null;
  return { origin, destination };
}

export interface OperatorData {
  name: string | null;
  callsign: string | null;
  country: string | null;
}

// The whole Operator section is omitted (the VFR/GA case) when there's no
// operator record at all, or when the record is present but empty.
function buildOperator(operator: OperatorInfo | null | undefined): OperatorData | null {
  if (!operator) return null;
  const name = nonEmpty(operator.name);
  const callsign = nonEmpty(operator.callsign);
  const country = nonEmpty(operator.country);
  if (!name && !callsign && !country) return null;
  return { name, callsign, country };
}

export interface ManufacturerModelRow {
  label: "Manufacturer/Model" | "Model";
  value: string;
}

// manufacturer_model is normally synthesized backend-side by
// merge_aircraft.lua from manufacturer+model; the bare model fallback (and
// row relabel) covers whenever that synthesis had nothing to work with.
// Row omitted only when neither field is present.
function buildManufacturerModelRow(aircraft: AircraftInfo | null | undefined): ManufacturerModelRow | null {
  const manufacturerModel = nonEmpty(aircraft?.manufacturer_model);
  if (manufacturerModel) {
    const typeDesignator = nonEmpty(aircraft?.type_designator);
    const value = typeDesignator ? `${manufacturerModel} (${typeDesignator})` : manufacturerModel;
    return { label: "Manufacturer/Model", value };
  }
  const model = nonEmpty(aircraft?.model);
  return model ? { label: "Model", value: model } : null;
}

function buildRegistrantName(registrant: RegistrantInfo | null | undefined): string | null {
  const names = (registrant?.names ?? []).filter((n): n is string => !!n && n.trim() !== "");
  return names.length > 0 ? names.join(", ") : null;
}

// "↑ 800 ft/min" / "↓ 500 ft/min" / "—". Reuses trendArrow()'s own ±500
// ft/min inclusive-threshold boundary exactly -- level (or exactly at the
// threshold) is an em dash, never omitted outright, unlike every other
// Flight row: the row itself only disappears when vertical_speed is
// entirely unknown (null/undefined).
function buildVerticalSpeedDisplay(verticalSpeed: number | null | undefined): string | null {
  if (verticalSpeed == null) return null;
  const arrow = trendArrow(verticalSpeed);
  if (arrow === "") return "—";
  return `${arrow} ${Math.round(Math.abs(verticalSpeed))} ft/min`;
}

// Omitted whenever config.home is null, or the aircraft's own position
// isn't known yet.
function buildDistanceDisplay(flight: MapFlight, home: HomePoint | null): string | null {
  if (!home || flight.lat == null || flight.lon == null) return null;
  const nm = greatCircleNm(home, { latitude: flight.lat, longitude: flight.lon });
  return nm.toFixed(1);
}

export interface AircraftDetailData {
  title: string;
  registration: string | null;
  icaoHex: string | null;
  military: boolean;
  specialLivery: string | null;
  route: RouteData | null;
  operator: OperatorData | null;
  manufacturerModel: ManufacturerModelRow | null;
  registrant: string | null;
  squawk: { value: string; emergency: boolean } | null;
  altitude: string | null;
  speed: string | null;
  verticalSpeed: string | null;
  track: string | null;
  distance: string | null;
  sources: string[];
  matchedRules: string[];
}

// Builds the panel's entire view model in one pass -- the component itself
// stays a thin, near-logic-free rendering layer over this. `home` is
// config.home (HomePoint | null); see lib/geo.ts's greatCircleNm doc for why
// a null home simply omits the Distance row rather than being this
// function's concern to validate further.
export function buildAircraftDetail(flight: MapFlight, home: HomePoint | null): AircraftDetailData {
  const aircraft = flight.aircraft;
  const squawk = nonEmpty(flight.squawk);

  return {
    // Header title falls back to icao_hex when ident isn't resolved yet --
    // same convention as management-ui's FlightViewModal.tsx header
    // (`view.ident ?? view.icao_hex`) -- so the header is never blank.
    title: nonEmpty(flight.ident) ?? flight.icao_hex,
    registration: nonEmpty(aircraft?.registration),
    icaoHex: nonEmpty(flight.icao_hex),
    military: !!aircraft?.military,
    specialLivery: nonEmpty(aircraft?.special_livery),
    route: buildRoute(flight),
    operator: buildOperator(flight.operator),
    manufacturerModel: buildManufacturerModelRow(aircraft),
    registrant: buildRegistrantName(flight.registrant),
    squawk: squawk ? { value: squawk, emergency: isEmergencySquawk(squawk) } : null,
    altitude: flight.alt != null ? Math.round(flight.alt).toLocaleString("en-US") : null,
    speed: flight.velocity != null ? String(Math.round(flight.velocity)) : null,
    verticalSpeed: buildVerticalSpeedDisplay(flight.vs),
    track: flight.hdg != null ? flight.hdg.toFixed(1) : null,
    distance: buildDistanceDisplay(flight, home),
    sources: (flight.receiver_sources ?? []).map(receiverSourceLabel),
    matchedRules: flight.matched_rules ?? [],
  };
}
