// Decides the next selection set for an aircraft click. Pure and
// MapLibre-agnostic so it's covered by plain unit tests, same pattern as
// trailSeeding.ts.
//
// Selection is single-select: clicking an aircraft replaces whatever was
// selected before it, rather than adding to it. Clicking the
// already-selected aircraft again deselects it. `selected` stays a
// `Set<string>` (rather than `string | null`) so every downstream consumer
// (InfoBoxLayer, featureCollections.ts, the trail-seeding effect) needs no
// change -- this helper just guarantees the set never grows past one entry.

export function nextSelection(current: ReadonlySet<string>, icaoHex: string): Set<string> {
  return current.has(icaoHex) ? new Set() : new Set([icaoHex]);
}
