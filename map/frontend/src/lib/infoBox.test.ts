import { describe, expect, it } from "vitest";
import {
  buildInfoBoxLines,
  formatAltitude,
  formatAltitudeSpeedLine,
  formatGroundspeed,
  formatIdentLine,
  formatRegistrationTypeLine,
  trendArrow,
} from "./infoBox";

describe("trendArrow", () => {
  it("returns up arrow when climbing above the +500ft/min threshold", () => {
    expect(trendArrow(501)).toBe("↑");
    expect(trendArrow(3000)).toBe("↑");
  });

  it("returns down arrow when descending below the -500ft/min threshold", () => {
    expect(trendArrow(-501)).toBe("↓");
    expect(trendArrow(-3000)).toBe("↓");
  });

  it("omits the arrow (empty string, not a placeholder) when level, inclusive of the threshold", () => {
    expect(trendArrow(0)).toBe("");
    expect(trendArrow(500)).toBe("");
    expect(trendArrow(-500)).toBe("");
    expect(trendArrow(250)).toBe("");
  });

  it("omits the arrow when vertical_speed is unknown", () => {
    expect(trendArrow(null)).toBe("");
    expect(trendArrow(undefined)).toBe("");
  });
});

describe("formatAltitude", () => {
  it("formats full feet with no thousands separator, never flight-level shorthand", () => {
    expect(formatAltitude(35000)).toBe("35000");
    expect(formatAltitude(900)).toBe("900");
    expect(formatAltitude(0)).toBe("0");
  });

  it("rounds to the nearest foot", () => {
    expect(formatAltitude(1250.6)).toBe("1251");
  });
});

describe("formatGroundspeed", () => {
  it("appends kt with no space", () => {
    expect(formatGroundspeed(450)).toBe("450kt");
  });

  it("rounds to the nearest knot", () => {
    expect(formatGroundspeed(449.6)).toBe("450kt");
  });
});

describe("formatAltitudeSpeedLine", () => {
  it("renders the full example from the design spec", () => {
    expect(formatAltitudeSpeedLine({ altitude: 35000, vertical_speed: -1200, velocity: 450 })).toBe("35000↓ 450kt");
  });

  it("omits the arrow entirely when level", () => {
    expect(formatAltitudeSpeedLine({ altitude: 35000, vertical_speed: 0, velocity: 450 })).toBe("35000 450kt");
  });

  it("omits the groundspeed half when velocity is unknown, keeping altitude+arrow", () => {
    expect(formatAltitudeSpeedLine({ altitude: 12000, vertical_speed: 1500, velocity: null })).toBe("12000↑");
  });

  it("omits the altitude+arrow half when altitude is unknown, keeping groundspeed", () => {
    expect(formatAltitudeSpeedLine({ altitude: null, vertical_speed: 1500, velocity: 200 })).toBe("200kt");
  });

  it("never attaches an arrow to a missing altitude even if vertical_speed is known", () => {
    const result = formatAltitudeSpeedLine({ altitude: null, vertical_speed: 1500, velocity: 200 });
    expect(result).not.toContain("↑");
  });

  it("returns null (whole line omitted) when both altitude and velocity are unknown", () => {
    expect(formatAltitudeSpeedLine({ altitude: null, vertical_speed: null, velocity: null })).toBeNull();
  });
});

describe("formatRegistrationTypeLine", () => {
  it("renders the full example from the design spec", () => {
    expect(formatRegistrationTypeLine({ aircraft: { registration: "N988DL", type_designator: "B752" } })).toBe(
      "N988DL B752",
    );
  });

  it("omits a missing registration, keeping the type designator", () => {
    expect(formatRegistrationTypeLine({ aircraft: { registration: null, type_designator: "B752" } })).toBe("B752");
  });

  it("omits a missing type designator, keeping the registration", () => {
    expect(formatRegistrationTypeLine({ aircraft: { registration: "N988DL", type_designator: null } })).toBe(
      "N988DL",
    );
  });

  it("returns null (whole line disappears) when both are missing", () => {
    expect(formatRegistrationTypeLine({ aircraft: { registration: null, type_designator: null } })).toBeNull();
  });

  it("returns null when aircraft enrichment itself is entirely absent", () => {
    expect(formatRegistrationTypeLine({})).toBeNull();
  });

  it("never renders a ?/N/A placeholder for a missing field", () => {
    const result = formatRegistrationTypeLine({ aircraft: { registration: "N988DL", type_designator: null } });
    expect(result).not.toMatch(/\?|N\/A/);
  });
});

describe("formatIdentLine", () => {
  it("returns the ident when resolved", () => {
    expect(formatIdentLine({ ident: "DAL659" })).toBe("DAL659");
  });

  it("returns null (blank, not a fallback) when ident is unresolved", () => {
    expect(formatIdentLine({ ident: null })).toBeNull();
    expect(formatIdentLine({})).toBeNull();
    expect(formatIdentLine({ ident: "" })).toBeNull();
    expect(formatIdentLine({ ident: "   " })).toBeNull();
  });
});

describe("buildInfoBoxLines", () => {
  it("builds all three lines in the fixed ident/altitude-speed/registration-type order", () => {
    const lines = buildInfoBoxLines({
      ident: "DAL659",
      altitude: 35000,
      vertical_speed: -1200,
      velocity: 450,
      aircraft: { registration: "N988DL", type_designator: "B752" },
    });
    expect(lines).toEqual({
      ident: "DAL659",
      altitudeSpeed: "35000↓ 450kt",
      registrationType: "N988DL B752",
    });
  });

  it("drops to a 2-line box when only registration/type is missing", () => {
    const lines = buildInfoBoxLines({
      ident: "DAL659",
      altitude: 35000,
      vertical_speed: 0,
      velocity: 450,
      aircraft: {},
    });
    expect(lines.registrationType).toBeNull();
  });

  it("returns a 0-line box for a freshly-tracked aircraft with nothing resolved yet", () => {
    const lines = buildInfoBoxLines({});
    expect(lines).toEqual({ ident: null, altitudeSpeed: null, registrationType: null });
  });
});
