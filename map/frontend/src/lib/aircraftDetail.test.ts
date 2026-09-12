import { describe, expect, it } from "vitest";
import type { MapFlight } from "../api/types";
import { buildAircraftDetail, isEmergencySquawk } from "./aircraftDetail";

describe("isEmergencySquawk", () => {
  it("is true for each of the four emergency codes", () => {
    expect(isEmergencySquawk("7500")).toBe(true);
    expect(isEmergencySquawk("7600")).toBe(true);
    expect(isEmergencySquawk("7700")).toBe(true);
    expect(isEmergencySquawk("7777")).toBe(true);
  });

  it("is false for a near-miss, and every other value", () => {
    expect(isEmergencySquawk("7501")).toBe(false);
    expect(isEmergencySquawk("1200")).toBe(false);
    expect(isEmergencySquawk("0000")).toBe(false);
  });

  it("is false when unknown", () => {
    expect(isEmergencySquawk(null)).toBe(false);
    expect(isEmergencySquawk(undefined)).toBe(false);
    expect(isEmergencySquawk("")).toBe(false);
  });
});

// Minimal fixture -- only icao_hex is guaranteed on MapFlight.
function baseFlight(overrides: Partial<MapFlight> = {}): MapFlight {
  return { icao_hex: "A2C9E4", ...overrides };
}

describe("buildAircraftDetail -- header", () => {
  it("uses ident as the title when resolved", () => {
    expect(buildAircraftDetail(baseFlight({ ident: "DAL659" }), null).title).toBe("DAL659");
  });

  it("falls back to icao_hex when ident is unresolved -- same convention as FlightViewModal.tsx's header", () => {
    expect(buildAircraftDetail(baseFlight({ ident: undefined }), null).title).toBe("A2C9E4");
    expect(buildAircraftDetail(baseFlight({ ident: "" }), null).title).toBe("A2C9E4");
    expect(buildAircraftDetail(baseFlight({ ident: "   " }), null).title).toBe("A2C9E4");
  });

  it("omits registration when unknown, keeps hex", () => {
    const data = buildAircraftDetail(baseFlight(), null);
    expect(data.registration).toBeNull();
    expect(data.icaoHex).toBe("A2C9E4");
  });

  it("includes registration when known", () => {
    const data = buildAircraftDetail(baseFlight({ aircraft: { icao_hex: "A2C9E4", registration: "N727JF" } }), null);
    expect(data.registration).toBe("N727JF");
  });
});

describe("buildAircraftDetail -- badges", () => {
  it("military and special_livery both default to false/null (no badges)", () => {
    const data = buildAircraftDetail(baseFlight(), null);
    expect(data.military).toBe(false);
    expect(data.specialLivery).toBeNull();
  });

  it("military true and special_livery is the livery name, not a boolean", () => {
    const data = buildAircraftDetail(
      baseFlight({ aircraft: { icao_hex: "A2C9E4", military: true, special_livery: "Golden Era Retro" } }),
      null,
    );
    expect(data.military).toBe(true);
    expect(data.specialLivery).toBe("Golden Era Retro");
  });
});

describe("buildAircraftDetail -- route (whole-section omit)", () => {
  const origin = { icao_code: "KOAK", iata_code: "OAK", name: "Oakland International Airport", city: "Oakland", region: "California", country: "United States" };
  const destination = { icao_code: "KLAS", iata_code: "LAS", name: "Harry Reid International Airport", city: "Las Vegas", region: "Nevada", country: "United States" };

  it("renders the route only when both origin and destination are known", () => {
    const data = buildAircraftDetail(baseFlight({ origin, destination }), null);
    expect(data.route).not.toBeNull();
    expect(data.route?.origin.icaoCode).toBe("KOAK");
    expect(data.route?.destination.icaoCode).toBe("KLAS");
  });

  it("omits the whole route when only the origin is known -- half a route isn't meaningful", () => {
    expect(buildAircraftDetail(baseFlight({ origin }), null).route).toBeNull();
  });

  it("omits the whole route when only the destination is known", () => {
    expect(buildAircraftDetail(baseFlight({ destination }), null).route).toBeNull();
  });

  it("omits the whole route when neither is known", () => {
    expect(buildAircraftDetail(baseFlight(), null).route).toBeNull();
  });

  it("omits IATA code and name independently within a known airport block", () => {
    const data = buildAircraftDetail(
      baseFlight({ origin: { icao_code: "KDOV" }, destination }),
      null,
    );
    expect(data.route?.origin.iataCode).toBeNull();
    expect(data.route?.origin.name).toBeNull();
    expect(data.route?.origin.location).toBeNull();
  });

  it("ports airportLocation's dedup rule -- a region equal to its country collapses", () => {
    const data = buildAircraftDetail(
      baseFlight({
        origin: { icao_code: "WSSS", city: "Singapore", region: "Singapore", country: "Singapore" },
        destination,
      }),
      null,
    );
    expect(data.route?.origin.location).toBe("Singapore");
  });
});

describe("buildAircraftDetail -- operator (whole-section omit)", () => {
  it("omits the whole section when there is no operator at all (VFR/GA case)", () => {
    expect(buildAircraftDetail(baseFlight(), null).operator).toBeNull();
  });

  it("omits the whole section when the operator record is present but empty", () => {
    expect(buildAircraftDetail(baseFlight({ operator: {} }), null).operator).toBeNull();
  });

  it("includes name/callsign/country independently when present", () => {
    const data = buildAircraftDetail(
      baseFlight({ operator: { name: "Jet Force Charters", callsign: "JET FORCE", country: "United States" } }),
      null,
    );
    expect(data.operator).toEqual({ name: "Jet Force Charters", callsign: "JET FORCE", country: "United States" });
  });

  it("omits callsign independently when only name/country are known", () => {
    const data = buildAircraftDetail(baseFlight({ operator: { name: "Southwest Airlines" } }), null);
    expect(data.operator).toEqual({ name: "Southwest Airlines", callsign: null, country: null });
  });
});

describe("buildAircraftDetail -- Manufacturer/Model fallback", () => {
  it("uses manufacturer_model + type_designator, labeled Manufacturer/Model", () => {
    const data = buildAircraftDetail(
      baseFlight({ aircraft: { icao_hex: "A2C9E4", manufacturer_model: "Boeing 727-200", type_designator: "B722" } }),
      null,
    );
    expect(data.manufacturerModel).toEqual({ label: "Manufacturer/Model", value: "Boeing 727-200 (B722)" });
  });

  it("omits the parenthesized type designator when it's unknown, keeping manufacturer_model bare", () => {
    const data = buildAircraftDetail(baseFlight({ aircraft: { icao_hex: "A2C9E4", manufacturer_model: "Boeing 727-200" } }), null);
    expect(data.manufacturerModel).toEqual({ label: "Manufacturer/Model", value: "Boeing 727-200" });
  });

  it("falls back to the bare model field, relabeled Model, when manufacturer_model is absent", () => {
    const data = buildAircraftDetail(baseFlight({ aircraft: { icao_hex: "A2C9E4", model: "727-200" } }), null);
    expect(data.manufacturerModel).toEqual({ label: "Model", value: "727-200" });
  });

  it("prefers manufacturer_model over the bare model field when both are present", () => {
    const data = buildAircraftDetail(
      baseFlight({ aircraft: { icao_hex: "A2C9E4", manufacturer_model: "Boeing 727-200", model: "727-200" } }),
      null,
    );
    expect(data.manufacturerModel?.label).toBe("Manufacturer/Model");
  });

  it("omits the row entirely when neither field is present", () => {
    expect(buildAircraftDetail(baseFlight(), null).manufacturerModel).toBeNull();
  });
});

describe("buildAircraftDetail -- Registrant", () => {
  it("omits the row when unknown", () => {
    expect(buildAircraftDetail(baseFlight(), null).registrant).toBeNull();
    expect(buildAircraftDetail(baseFlight({ registrant: { names: [] } }), null).registrant).toBeNull();
  });

  it("renders the registrant's name(s) when known", () => {
    const data = buildAircraftDetail(baseFlight({ registrant: { names: ["Pacific Aviation Holdings LLC"] } }), null);
    expect(data.registrant).toBe("Pacific Aviation Holdings LLC");
  });
});

describe("buildAircraftDetail -- Squawk emergency coloring", () => {
  it("flags each of the four emergency codes", () => {
    for (const code of ["7500", "7600", "7700", "7777"]) {
      expect(buildAircraftDetail(baseFlight({ squawk: code }), null).squawk).toEqual({
        value: code,
        emergency: true,
      });
    }
  });

  it("leaves a near-miss plain", () => {
    expect(buildAircraftDetail(baseFlight({ squawk: "7501" }), null).squawk).toEqual({
      value: "7501",
      emergency: false,
    });
  });

  it("omits the row when unknown", () => {
    expect(buildAircraftDetail(baseFlight(), null).squawk).toBeNull();
  });
});

describe("buildAircraftDetail -- Altitude/Speed", () => {
  it("formats altitude with thousands grouping and rounds", () => {
    expect(buildAircraftDetail(baseFlight({ alt: 33000.4 }), null).altitude).toBe("33,000");
  });

  it("formats speed rounded, no unit suffix baked in", () => {
    expect(buildAircraftDetail(baseFlight({ velocity: 459.6 }), null).speed).toBe("460");
  });

  it("omits both rows when unknown", () => {
    const data = buildAircraftDetail(baseFlight(), null);
    expect(data.altitude).toBeNull();
    expect(data.speed).toBeNull();
  });
});

describe("buildAircraftDetail -- Vertical Speed (trendArrow boundary)", () => {
  it("shows an up arrow with magnitude above the +500 threshold", () => {
    expect(buildAircraftDetail(baseFlight({ vs: 800 }), null).verticalSpeed).toBe("↑ 800 ft/min");
  });

  it("shows a down arrow with magnitude below the -500 threshold", () => {
    expect(buildAircraftDetail(baseFlight({ vs: -500.6 }), null).verticalSpeed).toBe("↓ 501 ft/min");
  });

  it("shows an em dash (not omitted) at and inside the inclusive ±500 threshold, including exactly 0", () => {
    expect(buildAircraftDetail(baseFlight({ vs: 500 }), null).verticalSpeed).toBe("—");
    expect(buildAircraftDetail(baseFlight({ vs: -500 }), null).verticalSpeed).toBe("—");
    expect(buildAircraftDetail(baseFlight({ vs: 0 }), null).verticalSpeed).toBe("—");
    expect(buildAircraftDetail(baseFlight({ vs: 250 }), null).verticalSpeed).toBe("—");
  });

  it("omits the row entirely (not even an em dash) when vertical_speed is unknown", () => {
    expect(buildAircraftDetail(baseFlight(), null).verticalSpeed).toBeNull();
  });
});

describe("buildAircraftDetail -- Track", () => {
  it("formats to one decimal place", () => {
    expect(buildAircraftDetail(baseFlight({ hdg: 108.44 }), null).track).toBe("108.4");
    expect(buildAircraftDetail(baseFlight({ hdg: 0 }), null).track).toBe("0.0");
  });

  it("omits when unknown", () => {
    expect(buildAircraftDetail(baseFlight(), null).track).toBeNull();
  });
});

describe("buildAircraftDetail -- Distance", () => {
  const center = { latitude: 33.9425, longitude: -118.4081 };

  it("omits when config.center is null", () => {
    expect(buildAircraftDetail(baseFlight({ lat: 34, lon: -118 }), null).distance).toBeNull();
  });

  it("omits when the aircraft's own position is unknown", () => {
    expect(buildAircraftDetail(baseFlight(), center).distance).toBeNull();
  });

  it("computes great-circle distance (nmi, one decimal) when both are known", () => {
    const data = buildAircraftDetail(baseFlight({ lat: 33.9425, lon: -118.4081 }), center);
    expect(data.distance).toBe("0.0");
  });
});

describe("buildAircraftDetail -- Sources / Matched Rules", () => {
  it("omits both when empty", () => {
    const data = buildAircraftDetail(baseFlight(), null);
    expect(data.sources).toEqual([]);
    expect(data.matchedRules).toEqual([]);
  });

  it("maps receiver_sources through receiverSourceLabel (EXTERNAL -> External)", () => {
    const data = buildAircraftDetail(baseFlight({ receiver_sources: ["1090", "EXTERNAL"] }), null);
    expect(data.sources).toEqual(["1090", "External"]);
  });

  it("passes matched_rules through unrelabeled", () => {
    const data = buildAircraftDetail(baseFlight({ matched_rules: ["high-altitude-transit", "restricted-zone"] }), null);
    expect(data.matchedRules).toEqual(["high-altitude-transit", "restricted-zone"]);
  });
});

describe("buildAircraftDetail -- representative full-data case", () => {
  it("resolves every field for a complete flight record", () => {
    const center = { latitude: 33.9425, longitude: -118.4081 };
    const flight: MapFlight = {
      icao_hex: "A2C9E4",
      ident: "JFA727",
      lat: 36.08,
      lon: -115.15,
      alt: 33000,
      velocity: 460,
      hdg: 108.4,
      vs: 800,
      squawk: "4521",
      aircraft: {
        icao_hex: "A2C9E4",
        registration: "N727JF",
        manufacturer_model: "Boeing 727-200",
        type_designator: "B722",
        military: false,
        special_livery: "Golden Era Retro",
      },
      operator: { name: "Jet Force Charters", callsign: "JET FORCE", country: "United States" },
      registrant: { names: ["Pacific Aviation Holdings LLC"] },
      origin: { icao_code: "KOAK", iata_code: "OAK", name: "Oakland International Airport", city: "Oakland", region: "California", country: "United States" },
      destination: { icao_code: "KLAS", iata_code: "LAS", name: "Harry Reid International Airport", city: "Las Vegas", region: "Nevada", country: "United States" },
      receiver_sources: ["1090", "EXTERNAL"],
      matched_rules: ["high-altitude-transit", "restricted-zone"],
    };

    const data = buildAircraftDetail(flight, center);

    expect(data.title).toBe("JFA727");
    expect(data.registration).toBe("N727JF");
    expect(data.icaoHex).toBe("A2C9E4");
    expect(data.military).toBe(false);
    expect(data.specialLivery).toBe("Golden Era Retro");
    expect(data.route).not.toBeNull();
    expect(data.operator).toEqual({ name: "Jet Force Charters", callsign: "JET FORCE", country: "United States" });
    expect(data.manufacturerModel).toEqual({ label: "Manufacturer/Model", value: "Boeing 727-200 (B722)" });
    expect(data.registrant).toBe("Pacific Aviation Holdings LLC");
    expect(data.squawk).toEqual({ value: "4521", emergency: false });
    expect(data.altitude).toBe("33,000");
    expect(data.speed).toBe("460");
    expect(data.verticalSpeed).toBe("↑ 800 ft/min");
    expect(data.track).toBe("108.4");
    expect(data.distance).not.toBeNull();
    expect(data.sources).toEqual(["1090", "External"]);
    expect(data.matchedRules).toEqual(["high-altitude-transit", "restricted-zone"]);
  });
});

describe("buildAircraftDetail -- VFR/no-operator representative case", () => {
  it("omits route, operator, badges, registrant, and distance for a minimal GA flight", () => {
    const flight: MapFlight = {
      icao_hex: "A2C9E4",
      ident: "N727JF",
      alt: 4500,
      velocity: 120,
      hdg: 270,
      squawk: "1200",
      aircraft: { icao_hex: "A2C9E4", registration: "N727JF", model: "172S" },
    };

    const data = buildAircraftDetail(flight, null);

    expect(data.title).toBe("N727JF");
    expect(data.military).toBe(false);
    expect(data.specialLivery).toBeNull();
    expect(data.route).toBeNull();
    expect(data.operator).toBeNull();
    expect(data.manufacturerModel).toEqual({ label: "Model", value: "172S" });
    expect(data.registrant).toBeNull();
    expect(data.squawk).toEqual({ value: "1200", emergency: false });
    expect(data.verticalSpeed).toBeNull();
    expect(data.distance).toBeNull();
    expect(data.sources).toEqual([]);
    expect(data.matchedRules).toEqual([]);
  });
});
