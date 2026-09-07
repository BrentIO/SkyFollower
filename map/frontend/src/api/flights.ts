import type { MapFlight } from "./types";

export class FlightsApiError extends Error {}

// GET /api/flights -- the initial snapshot. See map/main.py's
// get_flights(): one object per currently-tracked aircraft, the same
// shape a WebSocket `metadata` event carries.
export async function fetchFlights(restFlightsUrl: string): Promise<MapFlight[]> {
  const response = await fetch(restFlightsUrl);
  if (!response.ok) {
    throw new FlightsApiError(`GET ${restFlightsUrl} failed: HTTP ${response.status}`);
  }
  return (await response.json()) as MapFlight[];
}
