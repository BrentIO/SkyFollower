import type { MapFlight, MapFlightHistory, ProcessorRoster } from "./types";

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

// GET /api/flights/{icao_hex} -- one aircraft's current state plus its
// accumulated server-side trail (map/main.py's get_flight). Fetched when an
// aircraft is selected so the drawn trail reflects the whole flight, not just
// what this browser saw since it connected. Returns null on HTTP 404 (the
// aircraft is no longer tracked -- evicted, or never seen), which callers
// treat as "no server history to seed, keep the client-accumulated trail".
export async function fetchFlightHistory(
  restFlightsUrl: string,
  icaoHex: string,
): Promise<MapFlightHistory | null> {
  const url = `${restFlightsUrl}/${encodeURIComponent(icaoHex)}`;
  const response = await fetch(url);
  if (response.status === 404) return null;
  if (!response.ok) {
    throw new FlightsApiError(`GET ${url} failed: HTTP ${response.status}`);
  }
  return (await response.json()) as MapFlightHistory;
}

// POST /api/flights/batch -- the batched counterpart to fetchFlightHistory
// above (map/main.py's get_flights_batch). Given a list of icao_hex values,
// returns one MapFlightHistory per hex that's still tracked in a single HTTP
// request/two Redis round trips, instead of one fetchFlightHistory request
// per hex -- see issue #2052. A hex that's no longer tracked is just absent
// from the returned array; there's no per-item 404 to handle here, unlike
// the singular fetchFlightHistory.
export async function fetchFlightHistoryBatch(
  restFlightsUrl: string,
  icaoHexList: string[],
): Promise<MapFlightHistory[]> {
  const url = `${restFlightsUrl}/batch`;
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ icao_hex: icaoHexList }),
  });
  if (!response.ok) {
    throw new FlightsApiError(`POST ${url} failed: HTTP ${response.status}`);
  }
  return (await response.json()) as MapFlightHistory[];
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
