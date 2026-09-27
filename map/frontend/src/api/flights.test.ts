import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fetchFlightHistory, fetchFlightHistoryBatch, fetchFlights, FlightsApiError } from "./flights";

beforeEach(() => {
  vi.stubGlobal("fetch", vi.fn());
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("fetchFlights -- GET /api/flights", () => {
  it("returns the parsed snapshot on a successful response", async () => {
    vi.mocked(fetch).mockResolvedValue(
      new Response(JSON.stringify([{ icao_hex: "A1B2C3" }]), { status: 200 }),
    );

    const flights = await fetchFlights("/api/flights");

    expect(fetch).toHaveBeenCalledWith("/api/flights");
    expect(flights).toEqual([{ icao_hex: "A1B2C3" }]);
  });

  it("throws FlightsApiError on a non-ok response", async () => {
    vi.mocked(fetch).mockResolvedValue(new Response("boom", { status: 500 }));

    await expect(fetchFlights("/api/flights")).rejects.toThrow(FlightsApiError);
  });
});

describe("fetchFlightHistory -- GET /api/flights/{icao_hex}", () => {
  it("returns the parsed history on a successful response", async () => {
    const history = { icao_hex: "A1B2C3", lat: 1, lon: 2, trail: [{ lat: 1, lon: 2, alt: null }] };
    vi.mocked(fetch).mockResolvedValue(new Response(JSON.stringify(history), { status: 200 }));

    const result = await fetchFlightHistory("/api/flights", "A1B2C3");

    expect(fetch).toHaveBeenCalledWith("/api/flights/A1B2C3");
    expect(result).toEqual(history);
  });

  it("returns null on HTTP 404 (aircraft no longer tracked)", async () => {
    vi.mocked(fetch).mockResolvedValue(new Response("not found", { status: 404 }));

    const result = await fetchFlightHistory("/api/flights", "A1B2C3");

    expect(result).toBeNull();
  });

  it("throws FlightsApiError on a non-404 error response", async () => {
    vi.mocked(fetch).mockResolvedValue(new Response("boom", { status: 500 }));

    await expect(fetchFlightHistory("/api/flights", "A1B2C3")).rejects.toThrow(FlightsApiError);
  });

  it("URL-encodes the icao_hex into the path", async () => {
    vi.mocked(fetch).mockResolvedValue(
      new Response(JSON.stringify({ icao_hex: "A1/B2", trail: [] }), { status: 200 }),
    );

    await fetchFlightHistory("/api/flights", "A1/B2");

    expect(fetch).toHaveBeenCalledWith("/api/flights/A1%2FB2");
  });
});

describe("fetchFlightHistoryBatch -- POST /api/flights/batch (#2052)", () => {
  it("POSTs a JSON body with icao_hex and returns the parsed array", async () => {
    const histories = [
      { icao_hex: "A1B2C3", trail: [{ lat: 1, lon: 2, alt: null }] },
      { icao_hex: "AABBCC", trail: [] },
    ];
    vi.mocked(fetch).mockResolvedValue(new Response(JSON.stringify(histories), { status: 200 }));

    const result = await fetchFlightHistoryBatch("/api/flights", ["A1B2C3", "AABBCC"]);

    expect(fetch).toHaveBeenCalledWith("/api/flights/batch", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ icao_hex: ["A1B2C3", "AABBCC"] }),
    });
    expect(result).toEqual(histories);
  });

  it("fires exactly one fetch call regardless of how many hexes are requested", async () => {
    vi.mocked(fetch).mockResolvedValue(new Response(JSON.stringify([]), { status: 200 }));
    const manyHexes = Array.from({ length: 129 }, (_, i) => i.toString(16).padStart(6, "0").toUpperCase());

    await fetchFlightHistoryBatch("/api/flights", manyHexes);

    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("returns an empty array when none of the requested hexes are tracked", async () => {
    vi.mocked(fetch).mockResolvedValue(new Response(JSON.stringify([]), { status: 200 }));

    const result = await fetchFlightHistoryBatch("/api/flights", ["A1B2C3"]);

    expect(result).toEqual([]);
  });

  it("throws FlightsApiError on a non-ok response", async () => {
    vi.mocked(fetch).mockResolvedValue(new Response("boom", { status: 500 }));

    await expect(fetchFlightHistoryBatch("/api/flights", ["A1B2C3"])).rejects.toThrow(FlightsApiError);
  });
});
