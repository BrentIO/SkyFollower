import { useEffect, useState } from "react";
import { fetchFlights } from "../api/flights";
import type { MapWsEvent } from "../api/types";
import { applySnapshot, applyWsEvents, type AircraftMap } from "../lib/aircraftState";

// Reconnect delay after an unexpected WebSocket close -- fixed, not
// exponential backoff; this view targets a single always-on backend on a
// local network, not a flaky public endpoint.
const RECONNECT_DELAY_MS = 3000;

export interface UseMapFlightsResult {
  aircraft: AircraftMap;
  connected: boolean;
}

// Owns the WebSocket connection + REST snapshot fetch and their
// deliberate sequencing: connect the WebSocket *first* (buffering
// whatever arrives), then call GET /api/flights, then apply every
// buffered WS message on top of that snapshot before the first
// `aircraft` state is ever published -- closing the gap between "snapshot
// fetched" and "WS live" that a snapshot-then-connect order would leave
// open. See map/README.md's WebSocket API section and the issue this
// implements for why this order matters.
export function useMapFlights(wsUrl: string, restFlightsUrl: string): UseMapFlightsResult {
  const [aircraft, setAircraft] = useState<AircraftMap>({});
  const [connected, setConnected] = useState(false);

  useEffect(() => {
    let cancelled = false;
    let socket: WebSocket | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

    function connect(): void {
      if (cancelled) return;

      // Buffers every WS message until the snapshot fetch resolves --
      // see the function docstring above.
      const buffer: MapWsEvent[] = [];
      let hasSnapshot = false;

      const ws = new WebSocket(wsUrl);
      socket = ws;

      ws.onopen = () => {
        if (!cancelled) setConnected(true);
      };

      ws.onmessage = (event) => {
        let batch: MapWsEvent[];
        try {
          batch = JSON.parse(event.data as string) as MapWsEvent[];
        } catch {
          return; // Malformed frame -- drop it rather than crash the view.
        }
        if (!hasSnapshot) {
          buffer.push(...batch);
          return;
        }
        setAircraft((prev) => applyWsEvents(prev, batch));
      };

      ws.onclose = () => {
        if (cancelled) return;
        setConnected(false);
        reconnectTimer = setTimeout(connect, RECONNECT_DELAY_MS);
      };

      ws.onerror = () => {
        // Let onclose (which always follows onerror for a WebSocket)
        // drive reconnect -- nothing extra to do here.
      };

      // Started immediately after opening the socket -- not awaited
      // before it, so the WS connection is already buffering by the time
      // this resolves.
      fetchFlights(restFlightsUrl)
        .then((snapshot) => {
          if (cancelled) return;
          const merged = applyWsEvents(applySnapshot(snapshot), buffer);
          buffer.length = 0;
          hasSnapshot = true;
          setAircraft(merged);
        })
        .catch((err) => {
          console.error("Failed to fetch initial flight snapshot:", err);
        });
    }

    connect();

    return () => {
      cancelled = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, [wsUrl, restFlightsUrl]);

  return { aircraft, connected };
}
