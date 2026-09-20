import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { loadPersistedControls, savePersistedControls } from "./controlsPersistence";

// No jsdom in this project's test setup (vitest's default "node"
// environment, see lib/config.test.ts's own note) -- localStorage isn't a
// global here, so this stubs an in-memory implementation good enough to
// exercise controlsPersistence.ts's getItem/setItem calls.
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

const STORAGE_KEY = "skyfollower-map:controls:v1";

const ALL_ON = {
  historyAll: true,
  labelsAll: true,
  mapLabelsOn: true,
  rangeOutlineVisible: true,
  radarOn: true,
  radarOpacity: 0.8,
};

const DEFAULTS = {
  historyAll: false,
  labelsAll: false,
  mapLabelsOn: false,
  rangeOutlineVisible: false,
  radarOn: false,
  radarOpacity: 0.2,
};

let storage: Storage;

beforeEach(() => {
  storage = makeMemoryStorage();
  vi.stubGlobal("localStorage", storage);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("loadPersistedControls/savePersistedControls -- round-trip", () => {
  it("returns what was just saved", () => {
    savePersistedControls(ALL_ON);

    expect(loadPersistedControls()).toEqual(ALL_ON);
  });

  it("round-trips a mix of true/false values", () => {
    const mixed = {
      historyAll: true,
      labelsAll: false,
      mapLabelsOn: true,
      rangeOutlineVisible: false,
      radarOn: true,
      radarOpacity: 0.5,
    };
    savePersistedControls(mixed);

    expect(loadPersistedControls()).toEqual(mixed);
  });

  it("round-trips radarOpacity at 0 and 1, the slider's own extremes", () => {
    savePersistedControls({ ...ALL_ON, radarOpacity: 0 });
    expect(loadPersistedControls().radarOpacity).toBe(0);

    savePersistedControls({ ...ALL_ON, radarOpacity: 1 });
    expect(loadPersistedControls().radarOpacity).toBe(1);
  });
});

describe("loadPersistedControls -- missing/empty storage", () => {
  it("falls back to today's defaults when nothing has been stored", () => {
    expect(loadPersistedControls()).toEqual(DEFAULTS);
  });

  it("falls back to defaults when the stored value is an empty string", () => {
    storage.setItem(STORAGE_KEY, "");

    expect(loadPersistedControls()).toEqual(DEFAULTS);
  });
});

describe("loadPersistedControls -- malformed/corrupted stored value", () => {
  it("falls back to defaults on invalid JSON", () => {
    storage.setItem(STORAGE_KEY, "{not valid json");

    expect(loadPersistedControls()).toEqual(DEFAULTS);
  });

  it("falls back to defaults when controls is missing entirely", () => {
    storage.setItem(STORAGE_KEY, JSON.stringify({ version: 2 }));

    expect(loadPersistedControls()).toEqual(DEFAULTS);
  });

  it("falls back to defaults when a field has the wrong type", () => {
    storage.setItem(
      STORAGE_KEY,
      JSON.stringify({ version: 2, controls: { ...ALL_ON, historyAll: "yes" } }),
    );

    expect(loadPersistedControls()).toEqual(DEFAULTS);
  });

  it("falls back to defaults when radarOn has the wrong type", () => {
    storage.setItem(
      STORAGE_KEY,
      JSON.stringify({ version: 2, controls: { ...ALL_ON, radarOn: "on" } }),
    );

    expect(loadPersistedControls()).toEqual(DEFAULTS);
  });

  it("falls back to defaults when radarOpacity is not a number", () => {
    for (const badOpacity of ["0.5", null]) {
      storage.setItem(
        STORAGE_KEY,
        JSON.stringify({ version: 2, controls: { ...ALL_ON, radarOpacity: badOpacity } }),
      );

      expect(loadPersistedControls()).toEqual(DEFAULTS);
    }
  });


  it("falls back to defaults when the stored value is a JSON scalar, not an object", () => {
    storage.setItem(STORAGE_KEY, JSON.stringify(42));

    expect(loadPersistedControls()).toEqual(DEFAULTS);
  });
});

describe("loadPersistedControls -- blocked storage", () => {
  it("falls back to defaults without throwing when localStorage.getItem throws", () => {
    vi.stubGlobal("localStorage", {
      getItem: () => {
        throw new Error("blocked");
      },
      setItem: () => {
        throw new Error("blocked");
      },
    });

    expect(() => loadPersistedControls()).not.toThrow();
    expect(loadPersistedControls()).toEqual(DEFAULTS);
  });

  it("swallows a write failure without throwing when localStorage.setItem throws", () => {
    vi.stubGlobal("localStorage", {
      getItem: () => null,
      setItem: () => {
        throw new Error("quota exceeded");
      },
    });

    expect(() => savePersistedControls(ALL_ON)).not.toThrow();
  });

  it("falls back to defaults without throwing when localStorage itself is unavailable", () => {
    vi.stubGlobal("localStorage", undefined);

    expect(() => loadPersistedControls()).not.toThrow();
    expect(loadPersistedControls()).toEqual(DEFAULTS);
  });
});

describe("loadPersistedControls -- version mismatch", () => {
  it("treats a pre-#1896 v1 payload (no radar fields) as absent and falls back to defaults, not a partial merge", () => {
    storage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        version: 1,
        controls: { historyAll: true, labelsAll: true, mapLabelsOn: true, rangeOutlineVisible: true },
      }),
    );

    expect(loadPersistedControls()).toEqual(DEFAULTS);
  });

  it("treats a different version as absent and falls back to defaults", () => {
    storage.setItem(STORAGE_KEY, JSON.stringify({ version: 3, controls: ALL_ON }));

    expect(loadPersistedControls()).toEqual(DEFAULTS);
  });

  it("treats a missing version as absent and falls back to defaults", () => {
    storage.setItem(STORAGE_KEY, JSON.stringify({ controls: ALL_ON }));

    expect(loadPersistedControls()).toEqual(DEFAULTS);
  });
});
