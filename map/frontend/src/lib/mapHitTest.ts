// Pure helper for MapView.tsx's click/hover handling, extracted so it's
// unit-testable without a real MapLibre map.
//
// A single query across every SELECTABLE_LAYER_IDS member, then picking
// the first result, avoids a click double-fire: AIRCRAFT_LAYER_ID and
// TRAIL_HIT_AREA_LAYER_ID overlap by design (a trail's most recent segment
// terminates exactly at the aircraft's own icon), and MapLibre's per-layer
// delegated registration does its own independent hit test per layer, so a
// click on that overlap would otherwise fire the selection handler twice.
export function topIcaoHex(
  features: ReadonlyArray<{ properties?: Record<string, unknown> | null }>,
): string | undefined {
  const icaoHex = features[0]?.properties?.icao_hex;
  return typeof icaoHex === "string" ? icaoHex : undefined;
}
