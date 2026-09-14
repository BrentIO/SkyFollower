// Coalesces rapid, repeated requests to rebuild MapView's aircraft/trail
// GeoJSON sources into a single call, rather than one full rebuild per
// request. Pure timer logic, MapLibre-agnostic, so it's covered by plain
// unit tests (see the "throttle" test file) -- same pure-logic-extraction
// pattern as trailSegments.ts/trailSeeding.ts.
//
// Leading + trailing semantics (matching a standard UI throttle, not a
// debounce): the first request after an idle period runs immediately --
// so an isolated update is never delayed -- and any further requests
// arriving before `intervalMs` has elapsed since that run are coalesced
// into exactly one trailing run, using only the *latest* request's
// function (earlier ones in the same window are superseded, never run).
// This is what makes a burst of WebSocket batches or rapid user-driven
// state changes (see MapView.tsx's sync effect) cost one rebuild instead
// of one per request, without adding latency to the common case where
// requests already arrive further apart than `intervalMs`.

export interface Throttle {
  /**
   * Requests that `fn` run. Runs synchronously right now if nothing has
   * run within the last `intervalMs`; otherwise `fn` replaces whatever was
   * previously pending (if anything) and is scheduled to run once, at the
   * trailing edge of the current window.
   */
  request(fn: () => void): void;
  /** Cancels any pending trailing-edge run without running it. */
  cancel(): void;
}

// `now` is injectable (default Date.now()) so tests can pin it, matching
// aircraftState.ts's pushTracePoint convention.
export function createTrailingThrottle(intervalMs: number, now: () => number = Date.now): Throttle {
  let lastRunAt: number | null = null;
  let timer: ReturnType<typeof setTimeout> | null = null;
  let pending: (() => void) | null = null;

  function request(fn: () => void): void {
    const current = now();
    if (lastRunAt === null || current - lastRunAt >= intervalMs) {
      lastRunAt = current;
      pending = null;
      fn();
      return;
    }
    // Still within the cooldown window -- replace whatever was pending
    // (coalescing) and arm the trailing timer only if one isn't already
    // scheduled; its delay was computed correctly from lastRunAt on the
    // first request in this window and doesn't need to change.
    pending = fn;
    if (timer === null) {
      const delay = intervalMs - (current - lastRunAt);
      timer = setTimeout(() => {
        timer = null;
        lastRunAt = now();
        const toRun = pending;
        pending = null;
        if (toRun) toRun();
      }, delay);
    }
  }

  function cancel(): void {
    if (timer !== null) {
      clearTimeout(timer);
      timer = null;
    }
    pending = null;
  }

  return { request, cancel };
}

// The coalescing window for MapView's aircraft/trail sync effect.
//
// Raised from 200ms to 500ms in #1787 to cut steady-state GPU/compositor
// load: a DevTools trace showed the map redrawing at ~47fps continuously
// even with the camera stationary, driven by how often this effect marks
// the aircraft/trail sources dirty. This is now above the map service's own
// MAP_WS_BATCH_INTERVAL_SECONDS (250ms, see shared/timing.py), so unlike the
// prior 200ms window, a real isolated position/metadata update can now sit
// in the throttle's trailing edge rather than always firing on the leading
// edge -- a deliberate trade of on-screen update latency for lower render
// frequency (#1787 raised this over #1775's stated preference for faster
// updates; see that issue for the explicit call). A burst of backlogged
// WebSocket frames or several user-driven state changes (selection,
// Isolate, Follow, History: All) landing in the same tick still only pays
// the full-fleet rebuild cost once per window, which is this constant's
// other purpose -- see that effect's own comment on prevVisibilityInputsRef.
export const MAP_SYNC_THROTTLE_MS = 500;

// SCREEN_POSITION_THROTTLE_MS (the coalescing window for MapView's former
// `"move"`-driven screen-position sync, which repositioned InfoBoxLayer's
// DOM boxes during pan/zoom/Follow) was removed in #1808: InfoBoxLayer.tsx
// -- the DOM component that needed those per-frame-projected screen
// positions in the first place -- was replaced by a MapLibre symbol layer,
// which MapLibre itself repositions every frame as part of normal GPU
// rendering. There's no more per-tick `project()`-over-the-fleet JS work
// (or its own throttle) to bound.
