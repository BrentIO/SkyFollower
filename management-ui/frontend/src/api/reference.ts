import { apiClient } from "./client";

// These four endpoints return JSONResponse(content=...) directly in
// management-ui/backend/main.py, which bypasses FastAPI's response_model
// filtering -- the actual payload is whatever's really in Redis (e.g. an
// aircraft doc's full registrant/powerplant/category detail, or an
// airport's city/region/phonic/iata_code), a strict superset of
// shared/models.py's AircraftRecord/OperatorRecord/AirportRecord. Typed
// here as an open record with only the field each endpoint's own Pydantic
// model guarantees is non-optional -- LookupView renders whatever keys are
// actually present rather than assuming a fixed shape.

export type AircraftRecord = Record<string, unknown> & { icao_hex: string };
export type OperatorRecord = Record<string, unknown> & { airline_designator: string };
export type AirportRecord = Record<string, unknown> & { icao_code: string; name?: string };

export interface RouteLookup {
  ident: string;
  origin: AirportRecord;
  destination: AirportRecord;
  stops: AirportRecord[];
  operator: OperatorRecord | null;
}

// Exactly one of icaoHex/registration must be set -- the backend 422s
// otherwise. LookupView's dispatch-by-length logic decides which one to pass.
export function getAircraft(params: { icaoHex?: string; registration?: string }): Promise<AircraftRecord> {
  const query = new URLSearchParams();
  if (params.icaoHex) query.set("icao_hex", params.icaoHex);
  if (params.registration) query.set("registration", params.registration);
  return apiClient.get<AircraftRecord>(`/api/aircraft?${query.toString()}`);
}

export function getOperator(designator: string): Promise<OperatorRecord> {
  return apiClient.get<OperatorRecord>(`/api/operators/${encodeURIComponent(designator)}`);
}

export function getAirport(code: string): Promise<AirportRecord> {
  return apiClient.get<AirportRecord>(`/api/airports/${encodeURIComponent(code)}`);
}

export function getRoute(ident: string): Promise<RouteLookup> {
  return apiClient.get<RouteLookup>(`/api/routes/${encodeURIComponent(ident)}`);
}
