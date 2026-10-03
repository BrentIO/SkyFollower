import { describe, expect, it } from "vitest";
import {
  hashRadarTileBytes,
  isNewRadarContent,
  nextRadarState,
  planRadarPlaybackFrames,
  RADAR_AMBIENT_CACHE_CAPACITY,
  RADAR_FETCH_RETRY_COOLDOWN_MS,
  RADAR_MAX_ZOOM,
  RADAR_MIN_ZOOM,
  RADAR_PLAYBACK_OFFSETS_MINUTES,
  radarAmbientFrameId,
  radarChangeProbeUrl,
  radarFetchAction,
  radarFrameTileUrl,
  radarPlaybackFrameId,
  type RadarAmbientCacheEntry,
  type RadarFetchCacheEntry,
} from "./radar";

describe("radarFrameTileUrl", () => {
  it("returns the always-current snapshot URL for offset 0", () => {
    expect(radarFrameTileUrl(0)).toBe(
      "https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/nexrad-n0q/{z}/{x}/{y}.png",
    );
  });

  it("returns a zero-padded archived-frame URL for a non-zero offset", () => {
    expect(radarFrameTileUrl(5)).toBe(
      "https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/nexrad-n0q-m05m/{z}/{x}/{y}.png",
    );
    expect(radarFrameTileUrl(30)).toBe(
      "https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/nexrad-n0q-m30m/{z}/{x}/{y}.png",
    );
  });

  it("produces a distinct URL for every offset in the playback sequence", () => {
    const urls = new Set(RADAR_PLAYBACK_OFFSETS_MINUTES.map(radarFrameTileUrl));
    expect(urls.size).toBe(RADAR_PLAYBACK_OFFSETS_MINUTES.length);
  });

  it("never requests IEM's /c/ path -- 404s for the -mXXm layers (#2026)", () => {
    for (const offset of RADAR_PLAYBACK_OFFSETS_MINUTES) {
      expect(radarFrameTileUrl(offset)).not.toContain("/c/tile.py");
    }
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
  const fetchAll = RADAR_PLAYBACK_OFFSETS_MINUTES.map((offsetMinutes) => ({ offsetMinutes, source: { kind: "fetch" } }));

  it("fetches every slot fresh when the cache is empty", () => {
    expect(planRadarPlaybackFrames([])).toEqual(fetchAll);
  });

  it("reuses a single held frame for the newest slot and fetches the rest", () => {
    const plan = planRadarPlaybackFrames([{ slot: 3, timestampMs: 1000 }]);
    expect(plan.find((step) => step.offsetMinutes === 0)?.source).toEqual({ kind: "ambient", slot: 3 });
    plan.filter((step) => step.offsetMinutes !== 0).forEach((step) => expect(step.source).toEqual({ kind: "fetch" }));
  });

  it("assigns held frames most-recent-first regardless of how far apart in time they were captured", () => {
    const hour = 60 * 60 * 1000;
    const plan = planRadarPlaybackFrames([
      { slot: 0, timestampMs: 1 * hour },
      { slot: 2, timestampMs: 3 * hour },
      { slot: 1, timestampMs: 2 * hour },
    ]);
    expect(plan.find((step) => step.offsetMinutes === 0)?.source).toEqual({ kind: "ambient", slot: 2 });
    expect(plan.find((step) => step.offsetMinutes === 5)?.source).toEqual({ kind: "ambient", slot: 1 });
    expect(plan.find((step) => step.offsetMinutes === 10)?.source).toEqual({ kind: "ambient", slot: 0 });
    expect(plan.filter((step) => step.source.kind === "fetch").map((step) => step.offsetMinutes)).toEqual([30, 25, 20, 15]);
  });

  it("keeps the plan in oldest-to-newest offset order", () => {
    const plan = planRadarPlaybackFrames([{ slot: 0, timestampMs: 1 }]);
    expect(plan.map((step) => step.offsetMinutes)).toEqual([...RADAR_PLAYBACK_OFFSETS_MINUTES]);
  });

  it("uses each held slot at most once and needs no fetch when the cache is full", () => {
    const entries: RadarAmbientCacheEntry[] = RADAR_PLAYBACK_OFFSETS_MINUTES.map((_, slot) => ({ slot, timestampMs: slot }));
    const plan = planRadarPlaybackFrames(entries);
    plan.forEach((step) => expect(step.source.kind).toBe("ambient"));
    const slots = plan.map((step) => (step.source.kind === "ambient" ? step.source.slot : -1));
    expect(new Set(slots).size).toBe(RADAR_PLAYBACK_OFFSETS_MINUTES.length);
  });
});

describe("radarChangeProbeUrl", () => {
  it("is the current snapshot's world z0 tile with no unresolved placeholders", () => {
    expect(radarChangeProbeUrl()).toBe(
      "https://mesonet.agron.iastate.edu/cache/tile.py/1.0.0/nexrad-n0q/0/0/0.png",
    );
  });
});

describe("hashRadarTileBytes", () => {
  it("is stable for identical bytes and differs for different bytes", async () => {
    const a = new Uint8Array([1, 2, 3]).buffer;
    const b = new Uint8Array([1, 2, 3]).buffer;
    const c = new Uint8Array([1, 2, 4]).buffer;
    expect(await hashRadarTileBytes(a)).toBe(await hashRadarTileBytes(b));
    expect(await hashRadarTileBytes(a)).not.toBe(await hashRadarTileBytes(c));
    expect(await hashRadarTileBytes(a)).toMatch(/^[0-9a-f]{64}$/);
  });
});

describe("isNewRadarContent", () => {
  it("always accepts the first capture", () => {
    expect(isNewRadarContent(null, "abc")).toBe(true);
  });

  it("skips an unchanged hash and accepts a changed one", () => {
    expect(isNewRadarContent("abc", "abc")).toBe(false);
    expect(isNewRadarContent("abc", "def")).toBe(true);
  });
});

// #2015
describe("nextRadarState", () => {
  it("cycles off -> on -> animate -> off", () => {
    expect(nextRadarState("off")).toBe("on");
    expect(nextRadarState("on")).toBe("animate");
    expect(nextRadarState("animate")).toBe("off");
  });

  it("is a closed three-cycle -- three clicks from any state return to it", () => {
    for (const start of ["off", "on", "animate"] as const) {
      const afterThree = nextRadarState(nextRadarState(nextRadarState(start)));
      expect(afterThree).toBe(start);
    }
  });
});

describe("radarFetchAction", () => {
  const NOW = Date.parse("2026-01-01T00:30:00.000Z");

  it("issues a fresh fetch when there's no prior cache entry at all", () => {
    expect(radarFetchAction(undefined, NOW)).toBe("issue");
  });

  it("reuses a loaded entry outright, no matter how old", () => {
    const entry: RadarFetchCacheEntry = { attemptedAtMs: NOW - 999 * 60 * 1000, loaded: true };
    expect(radarFetchAction(entry, NOW)).toBe("reuse");
  });

  it("waits on an unresolved entry still within the retry cooldown", () => {
    const entry: RadarFetchCacheEntry = { attemptedAtMs: NOW - (RADAR_FETCH_RETRY_COOLDOWN_MS - 1), loaded: false };
    expect(radarFetchAction(entry, NOW)).toBe("wait");
  });

  it("issues a retry once an unresolved entry's cooldown has fully elapsed", () => {
    const entry: RadarFetchCacheEntry = { attemptedAtMs: NOW - RADAR_FETCH_RETRY_COOLDOWN_MS, loaded: false };
    expect(radarFetchAction(entry, NOW)).toBe("issue");
  });

  it("issues a retry well past the cooldown, not just at the boundary", () => {
    const entry: RadarFetchCacheEntry = { attemptedAtMs: NOW - RADAR_FETCH_RETRY_COOLDOWN_MS * 10, loaded: false };
    expect(radarFetchAction(entry, NOW)).toBe("issue");
  });

  it("an unresolved entry attempted just now waits", () => {
    const entry: RadarFetchCacheEntry = { attemptedAtMs: NOW, loaded: false };
    expect(radarFetchAction(entry, NOW)).toBe("wait");
  });
});
