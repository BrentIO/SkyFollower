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
  latitude?: number;
  longitude?: number;
  altitude?: number;
  velocity?: number;
  heading?: number;
  vertical_speed?: number;
  ident?: string;
  aircraft?: AircraftInfo;
  squawk?: string;
  first_message?: string;
  last_message?: string;
  total_messages?: number;
  receiver_sources?: string[];
  matched_rules?: string[];
}

export interface MapWsPositionEvent extends Pick<MapFlight, "latitude" | "longitude" | "altitude" | "velocity" | "heading" | "vertical_speed"> {
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
