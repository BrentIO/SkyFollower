import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { loadConfig } from "./config";

// No jsdom in this project's test setup (vitest's default "node"
// environment) -- config.ts's resolveWsUrl() falls back to window.location
// only when no VITE_MAP_API_BASE_URL is set, so a minimal stub is enough
// to exercise that branch without pulling in a DOM environment dependency.
function stubWindow(): void {
  vi.stubGlobal("window", { location: { protocol: "http:", host: "localhost:5173" } });
}

beforeEach(() => {
  stubWindow();
  vi.stubGlobal("fetch", vi.fn());
  // Never let a real dev-fallback env value leak in from the runner's own
  // environment between tests.
  vi.unstubAllEnvs();
});

afterEach(() => {
  vi.unstubAllEnvs();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("loadConfig -- center, fetched from GET /api/config", () => {
  it("resolves center from a successful /api/config response", async () => {
    vi.mocked(fetch).mockResolvedValue(
      new Response(JSON.stringify({ center: { latitude: 33.9425, longitude: -118.4081 } }), {
        status: 200,
      }),
    );

    const config = await loadConfig();

    expect(fetch).toHaveBeenCalledWith("/api/config");
    expect(config.center).toEqual({ latitude: 33.9425, longitude: -118.4081 });
  });

  it("resolves center to null when the backend reports no center configured", async () => {
    vi.mocked(fetch).mockResolvedValue(
      new Response(JSON.stringify({ center: null }), { status: 200 }),
    );

    const config = await loadConfig();

    expect(config.center).toBeNull();
  });

  it("resolves center to null (without throwing) on an HTTP error status", async () => {
    vi.mocked(fetch).mockResolvedValue(new Response("boom", { status: 500 }));
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    const config = await loadConfig();

    expect(config.center).toBeNull();
    expect(warnSpy).toHaveBeenCalled();
  });

  it("resolves center to null (without throwing) when the fetch itself rejects", async () => {
    vi.mocked(fetch).mockRejectedValue(new TypeError("network error"));
    vi.spyOn(console, "warn").mockImplementation(() => {});

    const config = await loadConfig();

    expect(config.center).toBeNull();
  });

  it("treats a center with a non-finite coordinate as absent", async () => {
    vi.mocked(fetch).mockResolvedValue(
      new Response(JSON.stringify({ center: { latitude: "not-a-number", longitude: -118.4 } }), {
        status: 200,
      }),
    );
    vi.spyOn(console, "warn").mockImplementation(() => {});

    const config = await loadConfig();

    expect(config.center).toBeNull();
  });
});

describe("loadConfig -- dev-only VITE_CENTER_LATITUDE/LONGITUDE fallback", () => {
  it("prefers the dev env override over the network fetch when both are set", async () => {
    vi.stubEnv("VITE_CENTER_LATITUDE", "1.5");
    vi.stubEnv("VITE_CENTER_LONGITUDE", "2.5");

    const config = await loadConfig();

    expect(config.center).toEqual({ latitude: 1.5, longitude: 2.5 });
    expect(fetch).not.toHaveBeenCalled();
  });
});

describe("loadConfig -- apiBaseUrl/restFlightsUrl/wsUrl", () => {
  beforeEach(() => {
    vi.mocked(fetch).mockResolvedValue(new Response(JSON.stringify({ center: null }), { status: 200 }));
  });

  it("defaults to same-origin (empty apiBaseUrl) when VITE_MAP_API_BASE_URL is unset", async () => {
    const config = await loadConfig();

    expect(config.apiBaseUrl).toBe("");
    expect(config.restFlightsUrl).toBe("/api/flights");
    expect(config.restProcessorsUrl).toBe("/api/processors");
    expect(config.wsUrl).toBe("ws://localhost:5173/ws");
  });

  it("derives REST/WS URLs from VITE_MAP_API_BASE_URL when set", async () => {
    vi.stubEnv("VITE_MAP_API_BASE_URL", "http://map-host:8080/");

    const config = await loadConfig();

    expect(config.apiBaseUrl).toBe("http://map-host:8080");
    expect(config.restFlightsUrl).toBe("http://map-host:8080/api/flights");
    expect(config.restProcessorsUrl).toBe("http://map-host:8080/api/processors");
    expect(config.wsUrl).toBe("ws://map-host:8080/ws");
    expect(fetch).toHaveBeenCalledWith("http://map-host:8080/api/config");
  });
});
