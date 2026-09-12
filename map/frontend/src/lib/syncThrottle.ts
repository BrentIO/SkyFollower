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

// The coalescing window for MapView's aircraft/trail sync effect. Deliberately
// under the map service's own MAP_WS_BATCH_INTERVAL_SECONDS (250ms, see
// shared/timing.py) so, in the ordinary case, WebSocket batches keep
// arriving further apart than this window and every one of them still
// triggers an immediate (leading-edge) rebuild -- no added latency for a
// real, isolated position/metadata update. This only changes behavior when
// requests cluster closer together than that -- a burst of backlogged
// WebSocket frames (e.g. delivered back-to-back after a background tab's
// timers were throttled) or several user-driven state changes (selection,
// Isolate, Follow, History: All) landing in the same tick -- which is
// exactly the case a full source rebuild shouldn't pay for more than once.
export const MAP_SYNC_THROTTLE_MS = 200;
