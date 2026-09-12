// Decides which aircraft need their server-side (Redis) trail fetched when
// "History: All" is on. Pure and MapLibre-agnostic so it's covered by plain
// unit tests, same pattern as trailSegments.ts.
//
// `alreadySeeded` only ever grows (never pruned when an aircraft
// disappears), so toggling "History: All" off and back on -- or a WS tick
// for an aircraft already seeded -- doesn't re-fetch it again. Only a
// genuinely new icao_hex, seen for the first time while "History: All" is
// on, is returned.

// Returns the icao_hex values from `aircraftIds` that still need seeding.
// Returns an empty array outright when `historyAll` is false.
export function aircraftNeedingHistorySeed(
  historyAll: boolean,
  aircraftIds: Iterable<string>,
  alreadySeeded: ReadonlySet<string>,
): string[] {
  if (!historyAll) return [];

  const needed: string[] = [];
  for (const icaoHex of aircraftIds) {
    if (!alreadySeeded.has(icaoHex)) needed.push(icaoHex);
  }
  return needed;
}
