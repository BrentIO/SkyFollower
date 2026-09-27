// Diffs two AircraftMap snapshots by per-key object *reference*, not deep
// equality, to find which icao_hexes actually changed -- MapView.tsx's
// sync effect uses this to push an incremental GeoJSONSource.updateData()
// diff instead of rebuilding every feature on every tick.
//
// This is only correct because aircraftState.ts's applyWsEvent(s) never
// touch an untouched aircraft's record reference -- every state transition
// spreads the old state object and replaces only the keys an event
// actually named. aircraftState.test.ts pins that contract explicitly so a
// future change to the merge logic can't quietly break this file's
// correctness.

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
