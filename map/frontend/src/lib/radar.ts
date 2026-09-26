// Live weather radar overlay (#1896) -- pure tile-URL/zoom-bounds/frame-
// sequence logic, kept separate from MapView.tsx so it's unit-testable
// without a MapLibre mount (this project has no jsdom/component-render
// test setup -- see lib/config.test.ts's own note).
//
// Source: IEM (Iowa Environmental Mesonet) NEXRAD composite mosaic --
// mesonet.agron.iastate.edu, public-domain NOAA data, genuinely open
// (unlike the other lightweight candidate evaluated, RainViewer, which is
// a commercial product's free tier licensed "personal/educational use
// only"). No API key, no CORS/User-Agent concerns -- a plain MapLibre
// raster source, browser-fetched directly like the base map's own tiles.

// Verified empirically against the live service (see #1896): tiles at
// z6/z7/z8 carry real (non-fully-transparent) radar pixel data at a real
// CONUS coordinate; z9 and above return a uniformly transparent
// placeholder regardless of coordinate. 8 is the effective native max
// zoom -- past it MapLibre should over-zoom (upscale) the z8 tile rather
// than request tiles that don't exist.
export const RADAR_MIN_ZOOM = 0;
export const RADAR_MAX_ZOOM = 8;
export const RADAR_TILE_SIZE = 256;

const RADAR_TILE_HOST = "https://mesonet.agron.iastate.edu";

// Archived-frame minute-offsets IEM's tile.py service actually serves
// (nexrad-n0q-m05m .. -m30m, 5-minute steps), oldest to newest, with 0
// (the always-current snapshot, a different path -- see
// radarFrameTileUrl) appended last so a playback loop reads directly as
// "the last 30 minutes, ending on now."
export const RADAR_PLAYBACK_OFFSETS_MINUTES: readonly number[] = [30, 25, 20, 15, 10, 5, 0];

// Time each frame stays on screen during playback. Not specified by the
// issue -- picked to match the common weather-radar-loop pace (TV
// weather, RainViewer's own reference implementation).
export const RADAR_FRAME_INTERVAL_MS = 500;

// #1910: safety ceiling on how long playback waits for one frame's tiles
// to finish loading during the prefetch phase before giving up on that
// frame and moving on anyway. Prefetching exists so the *visible* step
// interval never shows a blank/loading tile (the original bug); this
// timeout exists so a single slow/failed network request can't block
// playback from starting at all -- matches this app's standing
// fault-tolerance convention of degrading gracefully rather than hanging.
export const RADAR_FRAME_LOAD_TIMEOUT_MS = 5000;

// The current-snapshot layer's own auto-refresh cadence while radar is on
// and not playing. Matches IEM's own `Cache-Control: public, max-age=300`
// on the current-tile endpoint (verified against a live response) --
// refreshing more often would just re-request a browser-cached, unchanged
// image; the browser's HTTP cache naturally serves fresh content again
// once this window elapses.
export const RADAR_REFRESH_INTERVAL_MS = 5 * 60 * 1000;

/**
 * Builds the XYZ tile URL template for one radar frame.
 * `offsetMinutes = 0` is the always-current snapshot
 * (`.../nexrad-n0q/{z}/{x}/{y}.png`); any other value in
 * RADAR_PLAYBACK_OFFSETS_MINUTES is that many minutes old
 * (`.../nexrad-n0q-m{NN}m/{z}/{x}/{y}.png`).
 */
export function radarFrameTileUrl(offsetMinutes: number): string {
  if (offsetMinutes === 0) {
    return `${RADAR_TILE_HOST}/cache/tile.py/1.0.0/nexrad-n0q/{z}/{x}/{y}.png`;
  }
  const padded = String(offsetMinutes).padStart(2, "0");
  return `${RADAR_TILE_HOST}/c/tile.py/1.0.0/nexrad-n0q-m${padded}m/{z}/{x}/{y}.png`;
}

// #1910 (2nd attempt): the source/layer id for one playback frame.
// Previously the playback effect reused a single MapLibre source across
// all 7 frames, retargeting it with setTiles() -- verified live (real
// browser, Playwright) that this never actually behaves as "instant once
// prefetched": each retarget makes MapLibre re-request that raster
// source's *entire* zoom pyramid (z0-z8, ~20 tile requests per frame, not
// just the current view's tile), and isSourceLoaded()/'sourcedata' only
// resolves once EVERY one of those requests settles -- individually fast
// (double-digit-to-low-hundreds ms each) but serialized across 7 frames,
// the whole prefetch phase measured 20-30+ real seconds before playback
// ever started, nowhere near instant. Giving each frame its own
// source+layer, all added and loaded in parallel up front, then animated
// by toggling `visibility` (no network, no re-decode, genuinely instant)
// is the standard flip-book pattern for this exact problem and avoids
// the repeated-retarget cost entirely.
export function radarPlaybackFrameId(offsetMinutes: number): string {
  return `sf-radar-playback-${offsetMinutes}`;
}

// #1965: the ambient rolling cache's ring-buffer size. Deliberately equal to
// RADAR_PLAYBACK_OFFSETS_MINUTES.length (7 slots covering the same 30
// minutes / 5-minute cadence the playback loop targets) -- a full cache is
// exactly enough to cover every playback slot, and no more.
export const RADAR_AMBIENT_CACHE_CAPACITY = RADAR_PLAYBACK_OFFSETS_MINUTES.length;

// #1965: id for one ambient-cache ring-buffer slot's source+layer. Distinct
// from radarPlaybackFrameId (keyed by minute-offset, torn down every time
// playback stops) -- an ambient slot is keyed by its position in the ring
// buffer and persists for as long as radar stays on, whether or not
// playback is ever used.
export function radarAmbientFrameId(slot: number): string {
  return `sf-radar-ambient-${slot}`;
}

export interface RadarAmbientCacheEntry {
  /** Ring-buffer slot holding this frame (0..RADAR_AMBIENT_CACHE_CAPACITY-1). */
  slot: number;
  /** When this frame's tiles were fetched, i.e. what moment it depicts. */
  timestampMs: number;
}

// #1965: how close an ambient frame's capture time has to be to one of the
// 7 target playback slots to be reused as-is, instead of fetching that slot
// fresh. Half of RADAR_REFRESH_INTERVAL_MS's 5-minute cadence: with ambient
// captures landing (ideally) exactly every 5 minutes, +/-2.5 minutes is the
// widest tolerance that still maps every possible capture time to exactly
// one target slot with no gaps and no slot eligible for two captures at
// once. Real ambient cadence has some jitter (refresh timers aren't
// millisecond-precise, and a tab backgrounded/foregrounded can skew an
// interval), so this is a real approximation, not a zero-cost one -- see
// #1965's own "trade-offs" section.
export const RADAR_AMBIENT_MATCH_TOLERANCE_MS = 2.5 * 60 * 1000;

// #2015: the tri-state Radar button's animate state no longer tears down a
// fetch-frame source/layer the instant the operator cancels out of it --
// it keeps loading in the background (see MapView.tsx's playback effect),
// cached across animate sessions in a Map keyed by offsetMinutes rather
// than being refetched from scratch every time. RadarFetchCacheEntry is
// that cache's own value shape; radarFetchAction below is the pure
// decision of what to do with a given plan entry against it.
export interface RadarFetchCacheEntry {
  /** When this offset's fetch was last *attempted* (source added, or an
   * existing one retargeted) -- not when/if it resolved. Drives the retry
   * cooldown below. */
  attemptedAtMs: number;
  /** True once MapLibre reports the source fully loaded (isSourceLoaded).
   * A loaded entry is reused outright, regardless of age -- the cooldown
   * only ever gates *unresolved* attempts. */
  loaded: boolean;
}

// Minimum wait since a frame's last fetch *attempt* before it's eligible
// to be fetched again, so a rapid off/on/animate/off/on/animate burst
// can't fire duplicate in-flight requests for the same gap -- the operator
// re-entering animate finds the previous attempt still (or newly) resolved
// and just reuses it. ~15s per the issue's own discussion: long enough to
// cover a realistic "wrong button" burst of clicks, short enough that a
// genuinely new animate session shortly after isn't held back by a stale
// attempt for long.
export const RADAR_FETCH_RETRY_COOLDOWN_MS = 15 * 1000;

/**
 * Decides what MapView's playback effect should do with one plan entry
 * that needs a fresh fetch (`source.kind === "fetch"`), given whatever
 * cache entry already exists for that offset (undefined if never
 * attempted this radarOn session):
 * - "issue": no entry yet, or its attempt is unresolved and past the
 *   cooldown -- (re)issue the fetch and stamp a fresh attemptedAtMs.
 * - "reuse": the entry is already loaded -- use it as-is, no network
 *   activity, unaffected by the cooldown regardless of age.
 * - "wait": the entry is unresolved but still within the cooldown of its
 *   last attempt -- leave it alone; it either finishes on its own or gets
 *   reconsidered next time this is called.
 */
export function radarFetchAction(
  entry: RadarFetchCacheEntry | undefined,
  nowMs: number,
): "issue" | "reuse" | "wait" {
  if (!entry) return "issue";
  if (entry.loaded) return "reuse";
  return nowMs - entry.attemptedAtMs >= RADAR_FETCH_RETRY_COOLDOWN_MS ? "issue" : "wait";
}

// #2015: collapses the old separate radarOn/radarPlaying booleans into one
// tri-state value driving a single icon-column button.
export type RadarState = "off" | "on" | "animate";

/**
 * Advances the tri-state Radar button's state on each click: off -> on ->
 * animate -> off. Pure so the cycle order is unit-testable without
 * mounting MapView.
 */
export function nextRadarState(current: RadarState): RadarState {
  if (current === "off") return "on";
  if (current === "on") return "animate";
  return "off";
}

export type RadarPlaybackFrameSource = { kind: "ambient"; slot: number } | { kind: "fetch" };

export interface RadarPlaybackPlan {
  offsetMinutes: number;
  source: RadarPlaybackFrameSource;
}

/**
 * For each of the 7 playback target slots (RADAR_PLAYBACK_OFFSETS_MINUTES,
 * relative to `nowMs`), decides whether an existing ambient-cache entry is
 * close enough (RADAR_AMBIENT_MATCH_TOLERANCE_MS) to reuse as-is, or whether
 * that slot has to be fetched fresh from IEM's archived endpoint (#1965).
 * Greedily assigns each cache entry to its single closest unclaimed target
 * slot, oldest-target-first, so no entry is reused for two slots at once.
 * An empty/near-empty `cache` degrades to "fetch everything," i.e. today's
 * pre-#1965 behavior -- no regression versus the pre-existing worst case.
 */
export function planRadarPlaybackFrames(
  cache: readonly RadarAmbientCacheEntry[],
  nowMs: number,
): RadarPlaybackPlan[] {
  const unclaimed = [...cache];
  return RADAR_PLAYBACK_OFFSETS_MINUTES.map((offsetMinutes) => {
    const targetMs = nowMs - offsetMinutes * 60 * 1000;
    let bestIndex = -1;
    let bestDelta = RADAR_AMBIENT_MATCH_TOLERANCE_MS;
    unclaimed.forEach((entry, index) => {
      const delta = Math.abs(entry.timestampMs - targetMs);
      if (delta <= bestDelta) {
        bestDelta = delta;
        bestIndex = index;
      }
    });
    if (bestIndex === -1) {
      return { offsetMinutes, source: { kind: "fetch" } };
    }
    const [matched] = unclaimed.splice(bestIndex, 1);
    return { offsetMinutes, source: { kind: "ambient", slot: matched.slot } };
  });
}
