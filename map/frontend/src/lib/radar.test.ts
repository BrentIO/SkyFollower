import { describe, expect, it } from "vitest";
import {
  planRadarPlaybackFrames,
  RADAR_AMBIENT_CACHE_CAPACITY,
  RADAR_AMBIENT_MATCH_TOLERANCE_MS,
  RADAR_MAX_ZOOM,
  RADAR_MIN_ZOOM,
  RADAR_PLAYBACK_OFFSETS_MINUTES,
  radarAmbientFrameId,
  radarFrameTileUrl,
  radarPlaybackFrameId,
  type RadarAmbientCacheEntry,
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

describe("radarPlaybackFrameId", () => {
  it("produces a distinct id for every offset in the playback sequence", () => {
    const ids = new Set(RADAR_PLAYBACK_OFFSETS_MINUTES.map(radarPlaybackFrameId));
    expect(ids.size).toBe(RADAR_PLAYBACK_OFFSETS_MINUTES.length);
  });

  it("is deterministic for the same offset", () => {
    expect(radarPlaybackFrameId(30)).toBe(radarPlaybackFrameId(30));
  });

  it("includes the offset value in the id, for easy debugging against the real MapLibre style", () => {
    expect(radarPlaybackFrameId(15)).toContain("15");
  });
});

describe("radarAmbientFrameId", () => {
  it("produces a distinct id for every ring-buffer slot", () => {
    const ids = new Set(
      Array.from({ length: RADAR_AMBIENT_CACHE_CAPACITY }, (_, slot) => radarAmbientFrameId(slot)),
    );
    expect(ids.size).toBe(RADAR_AMBIENT_CACHE_CAPACITY);
  });

  it("never collides with a playback frame id", () => {
    const ambientIds = new Set(
      Array.from({ length: RADAR_AMBIENT_CACHE_CAPACITY }, (_, slot) => radarAmbientFrameId(slot)),
    );
    RADAR_PLAYBACK_OFFSETS_MINUTES.forEach((offsetMinutes) => {
      expect(ambientIds.has(radarPlaybackFrameId(offsetMinutes))).toBe(false);
    });
  });
});

describe("RADAR_AMBIENT_CACHE_CAPACITY", () => {
  it("matches the number of playback target slots", () => {
    expect(RADAR_AMBIENT_CACHE_CAPACITY).toBe(RADAR_PLAYBACK_OFFSETS_MINUTES.length);
  });
});

describe("planRadarPlaybackFrames", () => {
  const NOW = Date.parse("2026-01-01T00:30:00.000Z");

  it("fetches every slot fresh when the cache is empty", () => {
    const plan = planRadarPlaybackFrames([], NOW);
    expect(plan).toEqual(RADAR_PLAYBACK_OFFSETS_MINUTES.map((offsetMinutes) => ({ offsetMinutes, source: { kind: "fetch" } })));
  });

  it("reuses a cache entry that lands exactly on a target slot", () => {
    // 10 minutes before NOW is one of the 7 target slots.
    const entry: RadarAmbientCacheEntry = { slot: 3, timestampMs: NOW - 10 * 60 * 1000 };
    const plan = planRadarPlaybackFrames([entry], NOW);
    const tenMinuteStep = plan.find((step) => step.offsetMinutes === 10);
    expect(tenMinuteStep?.source).toEqual({ kind: "ambient", slot: 3 });
    // Every other slot still has nothing to reuse.
    plan
      .filter((step) => step.offsetMinutes !== 10)
      .forEach((step) => expect(step.source).toEqual({ kind: "fetch" }));
  });

  it("reuses an entry within tolerance of a target slot, not just an exact match", () => {
    const entry: RadarAmbientCacheEntry = {
      slot: 0,
      timestampMs: NOW - 10 * 60 * 1000 - (RADAR_AMBIENT_MATCH_TOLERANCE_MS - 1),
    };
    const plan = planRadarPlaybackFrames([entry], NOW);
    expect(plan.find((step) => step.offsetMinutes === 10)?.source).toEqual({ kind: "ambient", slot: 0 });
  });

  it("does not reuse an entry that falls outside tolerance of every target slot", () => {
    // Older than the oldest (30-minute) target slot by well more than the
    // tolerance, with no other slot nearby on either side.
    const entry: RadarAmbientCacheEntry = { slot: 0, timestampMs: NOW - 40 * 60 * 1000 };
    const plan = planRadarPlaybackFrames([entry], NOW);
    plan.forEach((step) => expect(step.source).toEqual({ kind: "fetch" }));
  });

  it("assigns each cache entry to at most one target slot", () => {
    // Two entries close enough together that both are within tolerance of
    // the same single target slot -- only one can win it, the other must
    // fall back to a fetch rather than being double-counted.
    const target = NOW - 10 * 60 * 1000;
    const entries: RadarAmbientCacheEntry[] = [
      { slot: 0, timestampMs: target - 60 * 1000 },
      { slot: 1, timestampMs: target + 60 * 1000 },
    ];
    const plan = planRadarPlaybackFrames(entries, NOW);
    const ambientSteps = plan.filter((step) => step.source.kind === "ambient");
    expect(ambientSteps).toHaveLength(1);
  });

  it("reuses a full cache for every slot, needing no fresh fetch", () => {
    const entries: RadarAmbientCacheEntry[] = RADAR_PLAYBACK_OFFSETS_MINUTES.map((offsetMinutes, slot) => ({
      slot,
      timestampMs: NOW - offsetMinutes * 60 * 1000,
    }));
    const plan = planRadarPlaybackFrames(entries, NOW);
    plan.forEach((step) => expect(step.source.kind).toBe("ambient"));
  });
});
