import { useEffect, useState } from "react";
import { fetchProcessorRoster } from "../api/flights";
import type { ProcessorRoster } from "../api/types";

// How often to re-poll GET /api/processors. Matches
// MAP_HEARTBEAT_INTERVAL_SECONDS (shared/timing.py) -- a processor's
// status can change purely from time passing (green ageing into
// amber/red, with no new packet to trigger anything), so this has to be a
// poll, not a one-time fetch or a WS push driven by packet arrival.
const POLL_INTERVAL_MS = 5000;

const EMPTY_ROSTER: ProcessorRoster = { overall: "red", processors: [] };

// Owns the GET /api/processors poll loop for the message-processor
// liveness roster the connection indicator (ControlsPanel) renders. A
// separate poll from useMapFlights' WebSocket, deliberately: the roster's
// per-processor colors decay purely from wall-clock time, which a
// packet-driven WS relay has no natural trigger to push on its own.
export function useProcessorRoster(restProcessorsUrl: string): ProcessorRoster {
  const [roster, setRoster] = useState<ProcessorRoster>(EMPTY_ROSTER);

  useEffect(() => {
    let cancelled = false;

    async function poll(): Promise<void> {
      try {
        const next = await fetchProcessorRoster(restProcessorsUrl);
        if (!cancelled) setRoster(next);
      } catch (err) {
        console.error("Failed to fetch processor roster:", err);
      }
    }

    poll();
    const interval = setInterval(poll, POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [restProcessorsUrl]);

  return roster;
}
