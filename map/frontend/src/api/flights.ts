import type { MapFlight, ProcessorRoster } from "./types";

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

// GET /api/processors -- the message-processor liveness roster/status. See
// map/main.py's get_processor_status(). Polled (see
// hooks/useProcessorRoster.ts), not pushed over WS -- a processor's status
// can change purely from time passing (green ageing into amber/red) with
// no new packet to trigger a push.
export async function fetchProcessorRoster(restProcessorsUrl: string): Promise<ProcessorRoster> {
  const response = await fetch(restProcessorsUrl);
  if (!response.ok) {
    throw new FlightsApiError(`GET ${restProcessorsUrl} failed: HTTP ${response.status}`);
  }
  return (await response.json()) as ProcessorRoster;
}
