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
