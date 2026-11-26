import type { Map as MapLibreMap } from "maplibre-gl";
import { METAR_LAYER_ID } from "./mapLayerIds";

/** Slack around a click, so a small dot can be tapped with a finger. */
export const METAR_HIT_SLACK_PX = 8;

/** The station whose dot is closest to `point` within the hit slack. */
export function nearestMetarStation(map: MapLibreMap, point: { x: number; y: number }): string | null {
  if (!map.getLayer(METAR_LAYER_ID)) return null;
  const features = map.queryRenderedFeatures(
    [
      [point.x - METAR_HIT_SLACK_PX, point.y - METAR_HIT_SLACK_PX],
      [point.x + METAR_HIT_SLACK_PX, point.y + METAR_HIT_SLACK_PX],
    ],
    { layers: [METAR_LAYER_ID] },
  );
  let nearest: string | null = null;
  let best = Infinity;
  // Features arrive topmost first, so a tie keeps the dot drawn on top.
  for (const feature of features) {
    if (feature.geometry.type !== "Point") continue;
    const dot = map.project(feature.geometry.coordinates as [number, number]);
    const distance = Math.hypot(dot.x - point.x, dot.y - point.y);
    if (distance < best) {
      best = distance;
      nearest = String(feature.properties.station_id);
    }
  }
  return nearest;
}
