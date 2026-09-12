import type { FeatureCollection } from "geojson";

export class RangeOutlineApiError extends Error {}

// GET /api/range-outline?band=envelope -- the daily reception range
// outline's envelope band only (farthest reception per bearing, across all
// altitude bands). See map/README.md's Range Outline section. Scoped
// server-side to `band=envelope` rather than fetching every band and
// filtering client-side, since v1 only ever renders the flat envelope line.
export async function fetchRangeOutline(apiBaseUrl: string): Promise<FeatureCollection> {
  const url = `${apiBaseUrl}/api/range-outline?band=envelope`;
  const response = await fetch(url);
  if (!response.ok) {
    throw new RangeOutlineApiError(`GET ${url} failed: HTTP ${response.status}`);
  }
  return (await response.json()) as FeatureCollection;
}
