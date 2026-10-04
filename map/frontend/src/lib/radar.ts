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
 * Builds the XYZ tile URL template for one radar frame. Both branches use
 * IEM's `/cache/` path -- `/c/` (its 14-day stable-timestamp cache) 404s
 * for the `-mXXm` layers, which are relative to "now" rather than a fixed
 * time (#2026). `offsetMinutes = 0` is the always-current snapshot
 * (`nexrad-n0q`); any other value in RADAR_PLAYBACK_OFFSETS_MINUTES is
 * that many minutes old (`nexrad-n0q-m{NN}m`).
 */
export function radarFrameTileUrl(offsetMinutes: number): string {
  if (offsetMinutes === 0) {
    return `${RADAR_TILE_HOST}/cache/tile.py/1.0.0/nexrad-n0q/{z}/{x}/{y}.png`;
  }
  const padded = String(offsetMinutes).padStart(2, "0");
  return `${RADAR_TILE_HOST}/cache/tile.py/1.0.0/nexrad-n0q-m${padded}m/{z}/{x}/{y}.png`;
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
  /** When this frame was captured; orders entries most-recent-first. */
  timestampMs: number;
}

/**
 * URL of the single tile used to detect whether the upstream composite has
 * changed since the last ambient capture: the current snapshot's world-wide
 * z0 tile, which covers the whole mosaic, so any radar update alters its
 * bytes. Fetched by the app itself (rather than only by MapLibre) so the
 * response bytes can be hashed.
 */
export function radarChangeProbeUrl(): string {
  return radarFrameTileUrl(0).replace("{z}", "0").replace("{x}", "0").replace("{y}", "0");
}

/** Hex SHA-256 of a fetched tile's bytes -- the content identity compared between ambient polls. */
export async function hashRadarTileBytes(bytes: ArrayBuffer): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * Whether a freshly-fetched probe hash represents a genuinely new radar
 * image worth advancing the ambient ring buffer for. The first capture
 * (no previous hash) always counts.
 */
export function isNewRadarContent(previousHash: string | null, nextHash: string): boolean {
  return previousHash === null || previousHash !== nextHash;
}

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
 * Builds the playback frame plan. The ambient cache only ever holds
 * distinct images (a slot advances only when the upstream content actually
 * changed), so every held entry is reused, most recent first, filling the
 * newest playback offsets; whatever offsets remain (the oldest ones) are
 * fetched from IEM's archived-frame endpoints. An empty `cache` degrades to
 * "fetch everything." Entries beyond the playback length are ignored, as are
 * entries older than the playback window plus one refresh interval of slack.
 */
export const RADAR_AMBIENT_MAX_AGE_MS =
  Math.max(...RADAR_PLAYBACK_OFFSETS_MINUTES) * 60 * 1000 + RADAR_REFRESH_INTERVAL_MS;

export function planRadarPlaybackFrames(
  cache: readonly RadarAmbientCacheEntry[],
  nowMs: number = Date.now(),
): RadarPlaybackPlan[] {
  const newestFirst = cache
    .filter((entry) => nowMs - entry.timestampMs <= RADAR_AMBIENT_MAX_AGE_MS)
    .sort((a, b) => b.timestampMs - a.timestampMs);
  const offsetsNewestFirst = [...RADAR_PLAYBACK_OFFSETS_MINUTES].reverse();
  return offsetsNewestFirst
    .map((offsetMinutes, index): RadarPlaybackPlan => {
      const entry = newestFirst[index];
      return {
        offsetMinutes,
        source: entry ? { kind: "ambient", slot: entry.slot } : { kind: "fetch" },
      };
    })
    .reverse();
}
