import { describe, expect, it } from "vitest";
import {
  currentStations,
  flightCategoryStyle,
  formatReportTime,
  METAR_FULL_DETAIL_ZOOM,
  METAR_MIN_SPACING_PX,
  metarFeatureCollection,
  metarRows,
  thinStations,
  worldPixel,
  type MetarStation,
} from "./metar";

const NOW = Date.UTC(2026, 10, 26, 18, 0, 0);

function station(overrides: Partial<MetarStation> = {}): MetarStation {
  return {
    station_id: "KMCI",
    lat: 39.3,
    lon: -94.7,
    observed_at: NOW / 1000 - 12 * 60,
    flight_category: "VFR",
    raw: "METAR KMCI",
    ...overrides,
  };
}

describe("flightCategoryStyle", () => {
  it.each([
    ["VFR", "#16a34a"],
    ["MVFR", "#2563eb"],
    ["IFR", "#dc2626"],
    ["LIFR", "#d946ef"],
  ])("colours %s", (category, color) => {
    expect(flightCategoryStyle(category).color).toBe(color);
  });

  it.each([undefined, "null", "SVFR", "toString", ""])("shows %s as gray Unknown", (category) => {
    expect(flightCategoryStyle(category)).toMatchObject({ label: "Unknown", color: "#9ca3af" });
  });

  it("keeps the station code readable on every badge colour", () => {
    expect(flightCategoryStyle(undefined).textColor).toBe("#111827");
    for (const category of ["VFR", "MVFR", "IFR", "LIFR"]) {
      expect(flightCategoryStyle(category).textColor).toBe("#ffffff");
    }
  });

  it("ranks worse conditions higher so they draw on top", () => {
    const ranks = [undefined, "VFR", "MVFR", "IFR", "LIFR"].map((c) => flightCategoryStyle(c).priority);
    expect(ranks).toEqual([...ranks].sort((a, b) => a - b));
    expect(new Set(ranks).size).toBe(5);
  });

  it("passes colour and rank to the map layer", () => {
    const [feature] = metarFeatureCollection([station({ flight_category: "IFR" })]).features;
    expect(feature.properties).toEqual({ station_id: "KMCI", color: "#dc2626", priority: 3 });
    expect(feature.geometry.coordinates).toEqual([-94.7, 39.3]);
  });
});

describe("currentStations", () => {
  it("hides a report once it is older than the maximum age", () => {
    const response = {
      max_age_seconds: 5400,
      stations: [
        station({ station_id: "NEW", observed_at: NOW / 1000 - 5400 }),
        station({ station_id: "OLD", observed_at: NOW / 1000 - 5401 }),
      ],
    };
    expect(currentStations(response, NOW).map((s) => s.station_id)).toEqual(["NEW"]);
    expect(currentStations(response, NOW + 1000)).toEqual([]);
    expect(currentStations(null, NOW)).toEqual([]);
  });
});

// Deterministic pseudo-random spread over roughly a 250 nm radius.
function scattered(count: number): MetarStation[] {
  let seed = 7;
  const random = () => (seed = (seed * 48271) % 2147483647) / 2147483647;
  const categories = ["VFR", "VFR", "VFR", "MVFR", "IFR", "LIFR", undefined];
  return Array.from({ length: count }, (_, i) =>
    station({
      station_id: `S${String(i).padStart(3, "0")}`,
      lat: 35 + random() * 8,
      lon: -100 + random() * 10,
      flight_category: categories[Math.floor(random() * categories.length)],
      station_priority: i % 11 === 10 ? undefined : i % 10,
    }),
  );
}

function pixelDistance(a: MetarStation, b: MetarStation, zoom: number): number {
  const [ax, ay] = worldPixel(a.lat, a.lon, zoom);
  const [bx, by] = worldPixel(b.lat, b.lon, zoom);
  return Math.hypot(ax - bx, ay - by);
}

describe("thinStations", () => {
  const all = scattered(300);

  it.each([4, 5, 6, 7, 7.9])("at zoom %s only hides a station beside a visible one of its colour", (zoom) => {
    const kept = thinStations(all, zoom);
    const keptIds = new Set(kept.map((s) => s.station_id));
    expect(kept.length).toBeLessThan(all.length);
    for (const hidden of all.filter((s) => !keptIds.has(s.station_id))) {
      const color = flightCategoryStyle(hidden.flight_category).color;
      expect(
        kept.some(
          (k) =>
            flightCategoryStyle(k.flight_category).color === color &&
            // ...and one at least as important.
            (k.station_priority ?? Infinity) <= (hidden.station_priority ?? Infinity) &&
            pixelDistance(k, hidden, zoom) < METAR_MIN_SPACING_PX,
        ),
      ).toBe(true);
    }
  });

  it("shows fewer stations the further out it is zoomed", () => {
    const counts = [4, 5, 6, 7, METAR_FULL_DETAIL_ZOOM].map((zoom) => thinStations(all, zoom).length);
    expect(counts).toEqual([...counts].sort((a, b) => a - b));
    expect(counts[0]).toBeLessThan(counts[3]);
  });

  it("shows every station from the full-detail zoom, even co-located ones", () => {
    const twins = [station({ station_id: "A" }), station({ station_id: "B" })];
    expect(thinStations(twins, METAR_FULL_DETAIL_ZOOM)).toHaveLength(2);
    expect(thinStations(twins, 12)).toHaveLength(2);
    expect(thinStations(all, METAR_FULL_DETAIL_ZOOM)).toHaveLength(all.length);
  });

  it("never lets a different colour hide a station", () => {
    const together = ["VFR", "MVFR", "IFR", "LIFR", undefined].map((flight_category, i) =>
      station({ station_id: `S${i}`, flight_category }),
    );
    expect(thinStations(together, 3)).toHaveLength(5);
  });

  it.each([
    ["the lower priority number", 6, 1, "ZZZ"],
    ["priority 0 over any other", 1, 0, "ZZZ"],
    ["a priority over none", undefined, 9, "ZZZ"],
    ["the first station ID on a tie", 3, 3, "AAA"],
    ["the first station ID with neither", undefined, undefined, "AAA"],
  ])("keeps %s", (_name, first, second, expected) => {
    const pair = [
      station({ station_id: "AAA", station_priority: first }),
      station({ station_id: "ZZZ", station_priority: second }),
    ];
    expect(thinStations(pair, 5).map((s) => s.station_id)).toEqual([expected]);
  });

  it("chooses the same stations whatever order they arrive in", () => {
    const ids = (list: MetarStation[]) => thinStations(list, 5).map((s) => s.station_id);
    expect(ids([...all].reverse())).toEqual(ids(all));
  });
});

describe("metarRows", () => {
  const rows = (overrides: Partial<MetarStation>) => Object.fromEntries(metarRows(station(overrides), NOW));

  it("formats every field in US aviation units", () => {
    expect(
      rows({
        wind_dir_deg: 60,
        wind_speed_kt: 12,
        wind_gust_kt: 20,
        visibility_sm: "10+",
        weather: "-RA BR",
        clouds: [{ cover: "FEW", base_ft: 7000 }, { cover: "BKN", base_ft: 15000 }],
        temperature_c: 24.4,
        dewpoint_c: -1,
        altimeter_inhg: 30,
      }),
    ).toEqual({
      Report: "12 min ago (17:48 UTC)",
      Wind: "060° at 12 kt, gusting 20 kt",
      Visibility: "10+ SM",
      Weather: "-RA BR",
      Clouds: "FEW 7,000 ft, BKN 15,000 ft",
      Temperature: "24.4 °C",
      "Dew point": "-1 °C",
      Altimeter: "30.00 inHg",
    });
  });

  it("leaves out everything the report does not carry", () => {
    expect(rows({})).toEqual({ Report: "12 min ago (17:48 UTC)" });
  });

  it("keeps zero values", () => {
    expect(rows({ temperature_c: 0, dewpoint_c: 0 })).toMatchObject({ Temperature: "0 °C", "Dew point": "0 °C" });
  });

  it("describes calm and variable wind", () => {
    expect(rows({ wind_dir_deg: 0, wind_speed_kt: 0 }).Wind).toBe("Calm");
    expect(rows({ wind_variable: true, wind_speed_kt: 3 }).Wind).toBe("Variable at 3 kt");
  });

  it("describes obscured and clear skies", () => {
    expect(rows({ clouds: [{ cover: "OVX", base_ft: 700 }] }).Clouds).toBe("Vertical visibility 700 ft");
    expect(rows({ sky_clear: "CLR" }).Clouds).toBe("Clear");
    expect(rows({ sky_clear: "CAVOK" }).Clouds).toBe("CAVOK");
  });

  it("never shows a negative age", () => {
    expect(formatReportTime(NOW / 1000 + 30, NOW)).toBe("0 min ago (18:00 UTC)");
  });
});
