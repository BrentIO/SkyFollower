import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  clampPanelWidth,
  DEFAULT_PANEL_WIDTH_PX,
  loadPersistedPanelWidth,
  MAX_PANEL_WIDTH_PX,
  MIN_PANEL_WIDTH_PX,
  savePersistedPanelWidth,
} from "./aircraftListPanelPersistence";

// No jsdom in this project's test setup (vitest's default "node"
// environment, see lib/config.test.ts's own note) -- localStorage isn't a
// global here, so this stubs an in-memory implementation good enough to
// exercise aircraftListPanelPersistence.ts's getItem/setItem calls --
// same helper as controlsPersistence.test.ts's own.
function makeMemoryStorage(): Storage {
  const store = new Map<string, string>();
  return {
    getItem: (key: string) => (store.has(key) ? store.get(key)! : null),
    setItem: (key: string, value: string) => {
      store.set(key, value);
    },
    removeItem: (key: string) => {
      store.delete(key);
    },
    clear: () => {
      store.clear();
    },
    key: (index: number) => Array.from(store.keys())[index] ?? null,
    get length() {
      return store.size;
    },
  } as Storage;
}

const STORAGE_KEY = "skyfollower-map:aircraft-list-panel:v1";

let storage: Storage;

beforeEach(() => {
  storage = makeMemoryStorage();
  vi.stubGlobal("localStorage", storage);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("clampPanelWidth", () => {
  it("passes through a value already within range", () => {
    expect(clampPanelWidth(800)).toBe(800);
  });

  it("clamps below MIN_PANEL_WIDTH_PX up to the minimum", () => {
    expect(clampPanelWidth(100)).toBe(MIN_PANEL_WIDTH_PX);
  });

  it("clamps above MAX_PANEL_WIDTH_PX down to the maximum", () => {
    expect(clampPanelWidth(5000)).toBe(MAX_PANEL_WIDTH_PX);
  });
});

describe("loadPersistedPanelWidth/savePersistedPanelWidth -- round-trip", () => {
  it("returns what was just saved", () => {
    savePersistedPanelWidth(900);

    expect(loadPersistedPanelWidth()).toBe(900);
  });

  it("clamps an out-of-range width on save, so load returns the clamped value", () => {
    savePersistedPanelWidth(50);

    expect(loadPersistedPanelWidth()).toBe(MIN_PANEL_WIDTH_PX);
  });
});

describe("loadPersistedPanelWidth -- missing/empty storage", () => {
  it("falls back to the default when nothing has been stored", () => {
    expect(loadPersistedPanelWidth()).toBe(DEFAULT_PANEL_WIDTH_PX);
  });

  it("falls back to the default when the stored value is an empty string", () => {
    storage.setItem(STORAGE_KEY, "");

    expect(loadPersistedPanelWidth()).toBe(DEFAULT_PANEL_WIDTH_PX);
  });
});

describe("loadPersistedPanelWidth -- malformed/corrupted stored value", () => {
  it("falls back to the default on invalid JSON", () => {
    storage.setItem(STORAGE_KEY, "{not valid json");

    expect(loadPersistedPanelWidth()).toBe(DEFAULT_PANEL_WIDTH_PX);
  });

  it("falls back to the default when widthPx is missing entirely", () => {
    storage.setItem(STORAGE_KEY, JSON.stringify({ version: 1 }));

    expect(loadPersistedPanelWidth()).toBe(DEFAULT_PANEL_WIDTH_PX);
  });

  it("falls back to the default when widthPx has the wrong type", () => {
    storage.setItem(STORAGE_KEY, JSON.stringify({ version: 1, widthPx: "wide" }));

    expect(loadPersistedPanelWidth()).toBe(DEFAULT_PANEL_WIDTH_PX);
  });

  it("falls back to the default when widthPx is NaN/Infinity", () => {
    storage.setItem(STORAGE_KEY, JSON.stringify({ version: 1, widthPx: Infinity }));

    expect(loadPersistedPanelWidth()).toBe(DEFAULT_PANEL_WIDTH_PX);
  });

  it("falls back to the default when the stored value is a JSON scalar, not an object", () => {
    storage.setItem(STORAGE_KEY, JSON.stringify(42));

    expect(loadPersistedPanelWidth()).toBe(DEFAULT_PANEL_WIDTH_PX);
  });
});

describe("loadPersistedPanelWidth -- blocked storage", () => {
  it("falls back to the default without throwing when localStorage.getItem throws", () => {
    vi.stubGlobal("localStorage", {
      getItem: () => {
        throw new Error("blocked");
      },
      setItem: () => {
        throw new Error("blocked");
      },
    });

    expect(() => loadPersistedPanelWidth()).not.toThrow();
    expect(loadPersistedPanelWidth()).toBe(DEFAULT_PANEL_WIDTH_PX);
  });

  it("swallows a write failure without throwing when localStorage.setItem throws", () => {
    vi.stubGlobal("localStorage", {
      getItem: () => null,
      setItem: () => {
        throw new Error("quota exceeded");
      },
    });

    expect(() => savePersistedPanelWidth(900)).not.toThrow();
  });

  it("falls back to the default without throwing when localStorage itself is unavailable", () => {
    vi.stubGlobal("localStorage", undefined);

    expect(() => loadPersistedPanelWidth()).not.toThrow();
    expect(loadPersistedPanelWidth()).toBe(DEFAULT_PANEL_WIDTH_PX);
  });
});

describe("loadPersistedPanelWidth -- version mismatch", () => {
  it("treats a different version as absent and falls back to the default", () => {
    storage.setItem(STORAGE_KEY, JSON.stringify({ version: 2, widthPx: 900 }));

    expect(loadPersistedPanelWidth()).toBe(DEFAULT_PANEL_WIDTH_PX);
  });

  it("treats a missing version as absent and falls back to the default", () => {
    storage.setItem(STORAGE_KEY, JSON.stringify({ widthPx: 900 }));

    expect(loadPersistedPanelWidth()).toBe(DEFAULT_PANEL_WIDTH_PX);
  });
});
