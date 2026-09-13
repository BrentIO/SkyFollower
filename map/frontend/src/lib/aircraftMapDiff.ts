// Diffs two AircraftMap snapshots by per-key object *reference*, not deep
// equality, to find which icao_hexes actually changed between one render
// and the next -- MapView.tsx's sync effect uses this to push an
// incremental GeoJSONSource.updateData() diff instead of rebuilding every
// feature on every tick (see that effect's own comment, and #1775).
//
// This is only correct because aircraftState.ts's applyWsEvent(s) (and
// applyTrailSeed/releasePendingRemoval) never touch an untouched
// aircraft's record reference -- every state transition spreads the old
// state object and replaces only the keys an event actually named, so an
// aircraft nobody sent an event for this batch keeps the exact same
// object reference across the transition. aircraftState.test.ts pins that
// contract explicitly so a future change to the merge logic can't quietly
// break this file's own correctness.

import type { AircraftMap } from "./aircraftState";

// Every icao_hex present in either map whose record reference differs
// between them -- added, removed, or updated. A key present in both with
// the identical reference (nothing touched it) is not included.
export function diffAircraftMaps(prev: AircraftMap, next: AircraftMap): Set<string> {
  const changed = new Set<string>();
  for (const hex of Object.keys(next)) {
    if (prev[hex] !== next[hex]) changed.add(hex);
  }
  for (const hex of Object.keys(prev)) {
    if (!(hex in next)) changed.add(hex);
  }
  return changed;
}
