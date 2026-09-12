// Pure helper for MapView.tsx's click/hover handling. Extracted so "which
// icao_hex does a click/hover point resolve to, given the rendered
// features MapLibre's queryRenderedFeatures returns for it" is covered by
// a plain unit test rather than only exercisable through a real MapLibre
// map -- see followTarget.ts's own note on this project having no jsdom/
// component-render test setup.
//
// A single query across every SELECTABLE_LAYER_IDS member, then picking
// the first result, is also what fixes MapView's click double-fire bug:
// AIRCRAFT_LAYER_ID and TRAIL_HIT_AREA_LAYER_ID overlap by design (a
// trail's most recent segment terminates exactly at the aircraft's own
// icon), and MapLibre's per-layer delegated `map.on(event, layerId, ...)`
// registration does its own independent hit test per layer -- so a single
// physical click landing on that overlap previously invoked the selection
// handler twice for the same aircraft, toggling it on and immediately back
// off. Querying once, across all selectable layers together, and taking
// the first hit guarantees exactly one decision per physical event,
// regardless of how many layers the point happens to intersect.
export function topIcaoHex(
  features: ReadonlyArray<{ properties?: Record<string, unknown> | null }>,
): string | undefined {
  const icaoHex = features[0]?.properties?.icao_hex;
  return typeof icaoHex === "string" ? icaoHex : undefined;
}
