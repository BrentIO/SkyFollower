import { useEffect, useState } from "react";
import { fetchRangeOutline } from "../api/rangeOutline";
import { EMPTY_FEATURE_COLLECTION } from "../lib/featureCollections";
import type { FeatureCollection } from "geojson";

// Matches the backend's own snapshot cadence (MAP_RANGE_OUTLINE_SNAPSHOT_INTERVAL_SECONDS,
// shared/timing.py) -- the outline can't change faster than that, so polling
// more often than this would just re-fetch identical data.
const POLL_INTERVAL_MS = 60_000;

// Owns the GET /api/range-outline poll loop for the toggle-able range
// outline overlay -- polls only while `enabled` is true (no point polling
// an overlay nobody's viewing), and stops/cleans up immediately when it
// flips off or the component unmounts.
export function useRangeOutline(apiBaseUrl: string, enabled: boolean): FeatureCollection {
  const [outline, setOutline] = useState<FeatureCollection>(EMPTY_FEATURE_COLLECTION);

  useEffect(() => {
    if (!enabled) {
      setOutline(EMPTY_FEATURE_COLLECTION);
      return;
    }

    let cancelled = false;

    async function poll(): Promise<void> {
      try {
        const next = await fetchRangeOutline(apiBaseUrl);
        if (!cancelled) setOutline(next);
      } catch (err) {
        console.error("Failed to fetch range outline:", err);
      }
    }

    poll();
    const interval = setInterval(poll, POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [apiBaseUrl, enabled]);

  return outline;
}
