// Wire types for the map backend's REST/WebSocket API -- mirrors
// specs/openapi.yaml's MapFlight and specs/asyncapi.yaml's
// MapFlightState/MapWs* schemas (see map/main.py, map/state_store.py for
// the implementation those specs document). Kept intentionally close to
// the spec's field list rather than the broader FlightSnapshot schema --
// only what this frontend actually reads is typed beyond `unknown`.

export interface AircraftInfo {
  icao_hex: string;
  registration?: string;
  type_designator?: string;
  type?: string;
  category?: string;
  manufacturer?: string;
  model?: string;
  manufacturer_model?: string;
  /** ICAO Doc 8643 description code, e.g. "L2J" (sourced from Mictronics
   * types.json, carried through the map metadata payload) -- char 1 =
   * category, digit = engine count, char 3 = engine type. Drives the
   * icon-shape resolver (aircraftIconResolver.ts). */
  description_code?: string;
  seats?: number;
  wake_turbulence_category?: string;
  military?: boolean;
  serial_number?: string;
  manufactured_date?: string;
}

// One aircraft's full merged current-state, as returned by one
// GET /api/flights array element or one WebSocket `metadata` event
// (minus its `type` key) -- specs/asyncapi.yaml's MapFlightState.
// Nothing but icao_hex is guaranteed present; a freshly-tracked aircraft
// may have only position fields, or only metadata fields, so far.
export interface MapFlight {
  icao_hex: string;
  lat?: number;
  lon?: number;
  alt?: number;
  velocity?: number;
  hdg?: number;
  vs?: number;
  ident?: string;
  aircraft?: AircraftInfo;
  squawk?: string;
  first_message?: string;
  last_message?: string;
  total_messages?: number;
  receiver_sources?: string[];
  matched_rules?: string[];
}

// One point of a server-side trail, as returned in GET /api/flights/{icao_hex}'s
// `trail` array (map/main.py's get_flight / map/state_store.py's get_trail).
// Wire field names (`lat`/`lon`/`alt`) match the position event; `alt` is null
// where altitude wasn't known when the point was recorded.
export interface TrailWirePoint {
  lat: number;
  lon: number;
  alt: number | null;
}

// GET /api/flights/{icao_hex}: one aircraft's merged current-state (a MapFlight)
// plus its accumulated server-side trail, oldest first.
export interface MapFlightHistory extends MapFlight {
  trail: TrailWirePoint[];
}

export interface MapWsPositionEvent extends Pick<MapFlight, "lat" | "lon" | "alt" | "velocity" | "hdg" | "vs"> {
  type: "position";
  icao_hex: string;
}

export interface MapWsMetadataEvent extends MapFlight {
  type: "metadata";
}

export interface MapWsStaleEvent {
  type: "stale";
  icao_hex: string;
}

export interface MapWsHideEvent {
  type: "hide";
  icao_hex: string;
}

export interface MapWsRemoveEvent {
  type: "remove";
  icao_hex: string;
}

export type MapWsEvent = MapWsPositionEvent | MapWsMetadataEvent | MapWsStaleEvent | MapWsHideEvent | MapWsRemoveEvent;

export type ProcessorStatusValue = "green" | "amber" | "red";

// One rostered message processor's liveness -- specs/openapi.yaml's
// ProcessorStatus, as returned by one GET /api/processors `processors`
// array element (see map/state_store.py's FlightStateStore.get_processor_statuses).
export interface ProcessorStatus {
  processor_id: string;
  last_seen: number;
  status: ProcessorStatusValue;
}

// GET /api/processors' full response body -- specs/openapi.yaml's
// ProcessorRosterResponse.
export interface ProcessorRoster {
  overall: ProcessorStatusValue;
  processors: ProcessorStatus[];
}
