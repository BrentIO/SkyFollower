import { describe, expect, it } from "vitest";
import {
  buildAltitudeSpeedLine,
  buildInfoBoxLines,
  formatAltitude,
  formatGroundspeed,
  formatIdentLine,
  formatRegistrationTypeLine,
  trendArrow,
  trendDirection,
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

describe("trendDirection -- #2001's direction-value counterpart to trendArrow(), used only by the info box", () => {
  it("returns 'up' when climbing above the +500ft/min threshold", () => {
    expect(trendDirection(501)).toBe("up");
    expect(trendDirection(3000)).toBe("up");
  });

  it("returns 'down' when descending below the -500ft/min threshold", () => {
    expect(trendDirection(-501)).toBe("down");
    expect(trendDirection(-3000)).toBe("down");
  });

  it("returns null (no arrow) when level, inclusive of the threshold", () => {
    expect(trendDirection(0)).toBeNull();
    expect(trendDirection(500)).toBeNull();
    expect(trendDirection(-500)).toBeNull();
    expect(trendDirection(250)).toBeNull();
  });

  it("returns null when vertical_speed is unknown", () => {
    expect(trendDirection(null)).toBeNull();
    expect(trendDirection(undefined)).toBeNull();
  });

  it("agrees with trendArrow()'s direction at every threshold boundary", () => {
    for (const vs of [501, 3000, -501, -3000, 0, 500, -500, 250, null, undefined]) {
      const arrow = trendArrow(vs);
      const direction = trendDirection(vs);
      if (arrow === "↑") expect(direction).toBe("up");
      else if (arrow === "↓") expect(direction).toBe("down");
      else expect(direction).toBeNull();
    }
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

describe("buildAltitudeSpeedLine", () => {
  it("renders the full example from the design spec, as parts", () => {
    expect(buildAltitudeSpeedLine({ alt: 35000, vs: -1200, velocity: 450 })).toEqual({
      altitude: "35000",
      trend: "down",
      groundspeed: "450kt",
    });
  });

  it("omits the trend entirely when level", () => {
    expect(buildAltitudeSpeedLine({ alt: 35000, vs: 0, velocity: 450 })).toEqual({
      altitude: "35000",
      trend: null,
      groundspeed: "450kt",
    });
  });

  it("omits the groundspeed half when velocity is unknown, keeping altitude+trend", () => {
    expect(buildAltitudeSpeedLine({ alt: 12000, vs: 1500, velocity: null })).toEqual({
      altitude: "12000",
      trend: "up",
      groundspeed: null,
    });
  });

  it("omits the altitude+trend half when altitude is unknown, keeping groundspeed", () => {
    expect(buildAltitudeSpeedLine({ alt: null, vs: 1500, velocity: 200 })).toEqual({
      altitude: null,
      trend: null,
      groundspeed: "200kt",
    });
  });

  it("never attaches a trend to a missing altitude even if vertical_speed is known", () => {
    const result = buildAltitudeSpeedLine({ alt: null, vs: 1500, velocity: 200 });
    expect(result?.trend).toBeNull();
  });

  it("returns null (whole line omitted) when both altitude and velocity are unknown", () => {
    expect(buildAltitudeSpeedLine({ alt: null, vs: null, velocity: null })).toBeNull();
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
      alt: 35000,
      vs: -1200,
      velocity: 450,
      aircraft: { registration: "N988DL", type_designator: "B752" },
    });
    expect(lines).toEqual({
      ident: "DAL659",
      altitudeSpeed: { altitude: "35000", trend: "down", groundspeed: "450kt" },
      registrationType: "N988DL B752",
    });
  });

  it("drops to a 2-line box when only registration/type is missing", () => {
    const lines = buildInfoBoxLines({
      ident: "DAL659",
      alt: 35000,
      vs: 0,
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
