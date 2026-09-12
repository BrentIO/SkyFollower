import { describe, expect, it } from "vitest";
import { airportLocation, receiverSourceLabel } from "./flightView";

// Reference values mirrored from
// management-ui/frontend/src/lib/flightView.test.ts's airportLocation
// suite -- this is a verbatim port of the same function, so the same
// input/output pairs must hold here too.
describe("airportLocation", () => {
  it("joins city, region, and country when all are present", () => {
    expect(airportLocation({ icao_code: "KDFW", city: "Dallas", region: "TX", country: "US" })).toBe(
      "Dallas, TX, US",
    );
  });

  it("joins only the parts that are present", () => {
    expect(airportLocation({ icao_code: "ETAR", city: "Ramstein-Miesenbach", country: "DE" })).toBe(
      "Ramstein-Miesenbach, DE",
    );
  });

  it("returns null when none of the location parts are present", () => {
    expect(airportLocation({ icao_code: "KDOV" })).toBeNull();
  });

  it("drops a part equal to the one immediately before it", () => {
    // Real-world case: ELLX (Luxembourg-Findel) has city/region/country
    // all literally "Luxembourg" once region is resolved from a name
    // that duplicates the country.
    expect(
      airportLocation({ icao_code: "ELLX", city: "Luxembourg", region: "Luxembourg", country: "Luxembourg" }),
    ).toBe("Luxembourg");
  });

  it("keeps non-adjacent repeats (only adjacent duplicates are dropped)", () => {
    expect(airportLocation({ icao_code: "KDFW", city: "Dallas", region: "TX", country: "Dallas" })).toBe(
      "Dallas, TX, Dallas",
    );
  });
});

describe("receiverSourceLabel", () => {
  it('renders "EXTERNAL" as "External"', () => {
    expect(receiverSourceLabel("EXTERNAL")).toBe("External");
  });

  it("passes 1090 through unchanged", () => {
    expect(receiverSourceLabel("1090")).toBe("1090");
  });

  it("passes 978 through unchanged", () => {
    expect(receiverSourceLabel("978")).toBe("978");
  });
});
