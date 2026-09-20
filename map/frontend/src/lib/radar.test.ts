import { describe, expect, it } from "vitest";
import {
  RADAR_MAX_ZOOM,
  RADAR_MIN_ZOOM,
  RADAR_PLAYBACK_OFFSETS_MINUTES,
  radarFrameTileUrl,
} from "./radar";

describe("radarFrameTileUrl", () => {
  it("returns the always-current snapshot URL for offset 0", () => {
    expect(radarFrameTileUrl(0)).toBe(
      "https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/nexrad-n0q/{z}/{x}/{y}.png",
    );
  });

  it("returns a zero-padded archived-frame URL for a non-zero offset", () => {
    expect(radarFrameTileUrl(5)).toBe(
      "https://mesonet.agron.iastate.edu/c/tile.py/1.0.0/nexrad-n0q-m05m/{z}/{x}/{y}.png",
    );
    expect(radarFrameTileUrl(30)).toBe(
      "https://mesonet.agron.iastate.edu/c/tile.py/1.0.0/nexrad-n0q-m30m/{z}/{x}/{y}.png",
    );
  });

  it("produces a distinct URL for every offset in the playback sequence", () => {
    const urls = new Set(RADAR_PLAYBACK_OFFSETS_MINUTES.map(radarFrameTileUrl));
    expect(urls.size).toBe(RADAR_PLAYBACK_OFFSETS_MINUTES.length);
  });
});

describe("RADAR_PLAYBACK_OFFSETS_MINUTES", () => {
  it("covers the last 30 minutes in 5-minute steps, oldest to newest, ending on the current frame", () => {
    expect(RADAR_PLAYBACK_OFFSETS_MINUTES).toEqual([30, 25, 20, 15, 10, 5, 0]);
  });
});

describe("zoom bounds", () => {
  it("caps at the empirically-verified native max zoom", () => {
    expect(RADAR_MIN_ZOOM).toBe(0);
    expect(RADAR_MAX_ZOOM).toBe(8);
  });
});
