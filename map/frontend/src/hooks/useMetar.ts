import { useEffect, useMemo, useState } from "react";
import { fetchMetar } from "../api/metar";
import { currentStations, type MetarResponse, type MetarStation } from "../lib/metar";

const POLL_INTERVAL_MS = 60_000;

/** Polls GET /api/metar while `enabled`. A failed poll keeps the last
 * response, and reports still expire as they age. */
export function useMetar(apiBaseUrl: string, enabled: boolean): { stations: MetarStation[]; now: number } {
  const [response, setResponse] = useState<MetarResponse | null>(null);
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    if (!enabled) return;
    let cancelled = false;

    async function poll(): Promise<void> {
      // Ticks first, so a hanging request can't postpone expiry.
      setNow(Date.now());
      try {
        const next = await fetchMetar(apiBaseUrl);
        if (!cancelled) {
          setResponse(next);
          setNow(Date.now());
        }
      } catch (err) {
        console.error("Failed to fetch METAR reports:", err);
      }
    }

    poll();
    const interval = setInterval(poll, POLL_INTERVAL_MS);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [apiBaseUrl, enabled]);

  const stations = useMemo(() => (enabled ? currentStations(response, now) : []), [enabled, response, now]);
  return { stations, now };
}
