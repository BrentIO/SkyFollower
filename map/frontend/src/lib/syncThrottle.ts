// Coalesces rapid, repeated requests to rebuild MapView's aircraft/trail
// GeoJSON sources into a single call, rather than one full rebuild per
// request. Pure timer logic, MapLibre-agnostic, so it's unit-testable.
//
// Leading + trailing semantics (a standard UI throttle, not a debounce):
// the first request after an idle period runs immediately, and any
// further requests arriving before `intervalMs` has elapsed are coalesced
// into exactly one trailing run using only the *latest* request's
// function. A burst of WebSocket batches or rapid state changes then costs
// one rebuild instead of one per request.

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

// The coalescing window for MapView's aircraft/trail sync effect. Above
// the map service's own MAP_WS_BATCH_INTERVAL_SECONDS (250ms, see
// shared/timing.py), so a real isolated position/metadata update can sit
// in the throttle's trailing edge rather than always firing on the leading
// edge -- a deliberate trade of on-screen update latency for lower render
// frequency. A burst of backlogged WebSocket frames or several
// user-driven state changes landing in the same tick still only pays the
// full-fleet rebuild cost once per window.
export const MAP_SYNC_THROTTLE_MS = 500;

// Deliberately tighter than MAP_SYNC_THROTTLE_MS: unlike the data-source
// sync above (which waits on new WebSocket data), this handler only
// re-projects positions the app already has, so there's no reason to trail
// the same distance behind -- this needs to read as instantaneous during
// continuous camera movement while still bounding the unthrottled
// per-frame `project()`-over-the-fleet cost `"move"` would otherwise pay.
export const SCREEN_POSITION_THROTTLE_MS = 50;
