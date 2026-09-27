import { useCallback, useEffect, useRef, useState } from "react";
import { fetchFlights, fetchFlightHistory, fetchFlightHistoryBatch } from "../api/flights";
import type { MapWsEvent } from "../api/types";
import {
  applySnapshot,
  applyTrailSeed,
  applyWsEvents,
  releasePendingRemoval,
  type AircraftMap,
} from "../lib/aircraftState";

// Reconnect delay after an unexpected WebSocket close -- fixed, not
// exponential backoff; this view targets a single always-on backend on a
// local network, not a flaky public endpoint.
const RECONNECT_DELAY_MS = 3000;

export interface UseMapFlightsResult {
  aircraft: AircraftMap;
  connected: boolean;
  /**
   * Fetches the server's accumulated trail for one aircraft and reseeds
   * that aircraft's client trail from it, so a selected aircraft's drawn
   * trail reflects the whole flight. Safe to call repeatedly; a 404 or
   * fetch error is swallowed, leaving the client-accumulated trail in place.
   */
  seedTrailFor: (icaoHex: string) => void;
  /** Batched seedTrailFor: one request for a whole list instead of one per
   * aircraft (used by "Trails: All"). Empty list is a no-op. */
  seedTrailForMany: (icaoHexList: string[]) => void;
  /**
   * Applies a `remove` that was deferred because `protectedIcaoHex`
   * matched at the time it arrived -- call once the aircraft detail panel
   * closes or the selection moves elsewhere. A no-op if nothing was
   * actually deferred.
   */
  releaseHold: (icaoHex: string) => void;
}

// Owns the WebSocket connection + REST snapshot fetch and their
// deliberate sequencing: connect the WebSocket *first* (buffering
// whatever arrives), then call GET /api/flights, then apply every
// buffered WS message on top of that snapshot before the first
// `aircraft` state is ever published -- closing the gap between "snapshot
// fetched" and "WS live" that a snapshot-then-connect order would leave open.
//
// `protectedIcaoHex` is the aircraft detail panel's currently-open/selected
// icao_hex, if any, threaded into every applyWsEvents call as the
// eviction-deferral hold. Kept in a ref rather than the effect's
// dependency array: a selection change must not tear down and reconnect
// the WebSocket, it just needs the next processed batch to see it.
export function useMapFlights(
  wsUrl: string,
  restFlightsUrl: string,
  protectedIcaoHex: string | null = null,
): UseMapFlightsResult {
  const [aircraft, setAircraft] = useState<AircraftMap>({});
  const [connected, setConnected] = useState(false);

  const protectedIcaoHexRef = useRef(protectedIcaoHex);
  protectedIcaoHexRef.current = protectedIcaoHex;

  const releaseHold = useCallback((icaoHex: string) => {
    setAircraft((prev) => releasePendingRemoval(prev, icaoHex));
  }, []);

  const seedTrailFor = useCallback(
    (icaoHex: string) => {
      fetchFlightHistory(restFlightsUrl, icaoHex)
        .then((history) => {
          if (!history) return; // 404 -- aircraft no longer tracked.
          setAircraft((prev) => applyTrailSeed(prev, icaoHex, history.trail));
        })
        .catch((err) => {
          console.error(`Failed to fetch flight history for ${icaoHex}:`, err);
        });
    },
    [restFlightsUrl],
  );

  const seedTrailForMany = useCallback(
    (icaoHexList: string[]) => {
      if (icaoHexList.length === 0) return;
      fetchFlightHistoryBatch(restFlightsUrl, icaoHexList)
        .then((histories) => {
          setAircraft((prev) => {
            let next = prev;
            for (const history of histories) {
              next = applyTrailSeed(next, history.icao_hex, history.trail);
            }
            return next;
          });
        })
        .catch((err) => {
          console.error(`Failed to batch-fetch flight history for ${icaoHexList.length} aircraft:`, err);
        });
    },
    [restFlightsUrl],
  );

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
        setAircraft((prev) => applyWsEvents(prev, batch, { protectedIcaoHex: protectedIcaoHexRef.current }));
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
          const merged = applyWsEvents(applySnapshot(snapshot), buffer, {
            protectedIcaoHex: protectedIcaoHexRef.current,
          });
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

  return { aircraft, connected, seedTrailFor, seedTrailForMany, releaseHold };
}
