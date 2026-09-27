import { apiClient } from "./client";

// These endpoints return the raw Redis document (a superset of
// shared/models.py's record types), so each is typed here as an open record
// with only the field its Pydantic model guarantees non-optional.

export type AircraftRecord = Record<string, unknown> & { icao_hex: string };
export type OperatorRecord = Record<string, unknown> & { airline_designator: string };
export type AirportRecord = Record<string, unknown> & { icao_code: string; name?: string };

// origin/destination/stops are absent when the route itself is unknown but
// its operator prefix still resolves.
export interface RouteLookup {
  ident: string;
  origin?: AirportRecord;
  destination?: AirportRecord;
  stops?: AirportRecord[];
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
