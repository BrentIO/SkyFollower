// Live weather radar overlay -- pure tile-URL/zoom-bounds/frame-sequence
// logic, kept separate from MapView.tsx so it's unit-testable without a
// MapLibre mount (this project has no jsdom/component-render test setup).
//
// Source: IEM (Iowa Environmental Mesonet) NEXRAD composite mosaic --
// public-domain NOAA data, no API key or CORS/User-Agent concerns, a
// plain MapLibre raster source fetched directly like the base map's tiles.

// Verified against the live service: tiles at z6/z7/z8 carry real radar
// pixel data; z9+ returns a uniformly transparent placeholder. 8 is the
// effective native max zoom -- past it MapLibre should over-zoom the z8
// tile rather than request tiles that don't exist.
export const RADAR_MIN_ZOOM = 0;
export const RADAR_MAX_ZOOM = 8;
export const RADAR_TILE_SIZE = 256;

const RADAR_TILE_HOST = "https://mesonet.agron.iastate.edu";

// Archived-frame minute-offsets IEM's tile.py service serves (5-minute
// steps), oldest to newest, with 0 (the always-current snapshot, a
// different path -- see radarFrameTileUrl) appended last so a playback
// loop reads directly as "the last 30 minutes, ending on now."
export const RADAR_PLAYBACK_OFFSETS_MINUTES: readonly number[] = [30, 25, 20, 15, 10, 5, 0];

// Time each frame stays on screen during playback -- picked to match the
// common weather-radar-loop pace (TV weather, RainViewer's reference UI).
export const RADAR_FRAME_INTERVAL_MS = 500;

// Safety ceiling on how long playback waits for one frame's tiles to load
// during prefetch before giving up on that frame and moving on anyway, so
// a single slow/failed request can't block playback from starting at all.
export const RADAR_FRAME_LOAD_TIMEOUT_MS = 5000;

// The current-snapshot layer's auto-refresh cadence while radar is on and
// not playing. Matches IEM's `Cache-Control: public, max-age=300` on the
// current-tile endpoint; refreshing more often would just re-request a
// browser-cached, unchanged image.
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

// The source/layer id for one playback frame. Each frame gets its own
// source+layer, all added and loaded in parallel up front and animated by
// toggling opacity (no network, no re-decode) -- retargeting a single
// shared source via setTiles() instead would make MapLibre re-request
// that raster source's *entire* zoom pyramid on every retarget.
export function radarPlaybackFrameId(offsetMinutes: number): string {
  return `sf-radar-playback-${offsetMinutes}`;
}

// The ambient rolling cache's ring-buffer size. Equal to
// RADAR_PLAYBACK_OFFSETS_MINUTES.length -- a full cache is exactly enough
// to cover every playback slot, and no more.
export const RADAR_AMBIENT_CACHE_CAPACITY = RADAR_PLAYBACK_OFFSETS_MINUTES.length;

// Id for one ambient-cache ring-buffer slot's source+layer. Distinct from
// radarPlaybackFrameId (keyed by minute-offset, torn down when playback
// stops) -- an ambient slot is keyed by its ring-buffer position and
// persists for as long as radar stays on, whether or not playback is used.
export function radarAmbientFrameId(slot: number): string {
  return `sf-radar-ambient-${slot}`;
}

export interface RadarAmbientCacheEntry {
  /** Ring-buffer slot holding this frame (0..RADAR_AMBIENT_CACHE_CAPACITY-1). */
  slot: number;
  /** When this frame's tiles were fetched, i.e. what moment it depicts. */
  timestampMs: number;
}

// How close an ambient frame's capture time has to be to a target playback
// slot to be reused as-is, instead of fetching fresh. Half of
// RADAR_REFRESH_INTERVAL_MS's 5-minute cadence -- the widest tolerance
// that still maps every possible capture time to exactly one target slot
// with no gaps and no slot eligible for two captures at once.
export const RADAR_AMBIENT_MATCH_TOLERANCE_MS = 2.5 * 60 * 1000;

// The tri-state Radar button's animate state doesn't tear down a
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
// to be fetched again, so a rapid off/on/animate burst can't fire
// duplicate in-flight requests for the same gap -- the operator re-entering
// animate finds the previous attempt still (or newly) resolved and reuses
// it. ~15s: long enough to cover a "wrong button" burst of clicks, short
// enough that a genuinely new animate session isn't held back for long.
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

// Collapses the old separate radarOn/radarPlaying booleans into one
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
 * For each playback target slot (RADAR_PLAYBACK_OFFSETS_MINUTES, relative
 * to `nowMs`), decides whether an existing ambient-cache entry is close
 * enough to reuse as-is, or whether that slot must be fetched fresh.
 * Greedily assigns each cache entry to its single closest unclaimed
 * target slot, oldest-target-first, so no entry is reused twice. An
 * empty/near-empty `cache` degrades to "fetch everything."
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
