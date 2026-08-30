import { describe, expect, it } from "vitest";
import {
  categoriesToQuery,
  classifyLookup,
  isAirport,
  isHex,
  isOperator,
  isRegistration,
  isRoute,
} from "./lookupClassifier";

describe("individual predicates", () => {
  it("isHex matches exactly 6 hex digits, any case", () => {
    expect(isHex("A8AE7F")).toBe(true);
    expect(isHex("a8ae7f")).toBe(true);
    expect(isHex("ABC123")).toBe(true);
    expect(isHex("N659DL")).toBe(false); // N is not a hex digit
    expect(isHex("A8AE7")).toBe(false); // too short
    expect(isHex("A8AE7FF")).toBe(false); // too long
    expect(isHex("12345")).toBe(false);
  });

  it("isRegistration matches any hyphenated string (unchanged)", () => {
    expect(isRegistration("2-GOLD")).toBe(true);
    expect(isRegistration("OE-LBA")).toBe(true);
    expect(isRegistration("VP-CKA")).toBe(true);
    expect(isRegistration("G-ABCD")).toBe(true);
  });

  it("isRegistration matches N/HL/JA prefixes only with a digit present (unchanged)", () => {
    expect(isRegistration("N659DL")).toBe(true);
    expect(isRegistration("HL7771")).toBe(true);
    expect(isRegistration("JA8905")).toBe(true);
    expect(isRegistration("n659dl")).toBe(true); // case-insensitive prefix
    expect(isRegistration("JAX")).toBe(false);
    expect(isRegistration("NAV")).toBe(false);
    expect(isRegistration("HL")).toBe(false);
  });

  it("isRegistration rejects non-hyphen strings without an allowlisted prefix (unchanged)", () => {
    expect(isRegistration("A8AE7F")).toBe(false);
    expect(isRegistration("ABC123")).toBe(false);
    expect(isRegistration("DAL2")).toBe(false);
  });

  it("isOperator matches 2-3 alphanumeric chars with at least one letter", () => {
    expect(isOperator("DAL")).toBe(true);
    expect(isOperator("FFT")).toBe(true);
    expect(isOperator("UA")).toBe(true);
    expect(isOperator("5X")).toBe(true); // real IATA codes carry digits
    expect(isOperator("9E")).toBe(true);
    expect(isOperator("A")).toBe(false); // too short
    expect(isOperator("KJFK")).toBe(false); // too long
    expect(isOperator("55")).toBe(false); // no letter
    expect(isOperator("123")).toBe(false); // no letter
  });

  it("isAirport matches 3-4 alphanumeric chars", () => {
    expect(isAirport("JFK")).toBe(true);
    expect(isAirport("FFT")).toBe(true);
    expect(isAirport("KJFK")).toBe(true);
    expect(isAirport("KFFT")).toBe(true);
    expect(isAirport("KX14")).toBe(true); // FAA-LID-derived code with digits
    expect(isAirport("0S9")).toBe(true);
    expect(isAirport("JF")).toBe(false); // too short
    expect(isAirport("KJFKX")).toBe(false); // too long
  });

  it("isRoute matches one-or-more letters, digits, optional trailing letters", () => {
    expect(isRoute("DAL2")).toBe(true);
    expect(isRoute("AA100")).toBe(true); // 2-letter IATA-style prefix
    expect(isRoute("VIR92MC")).toBe(true); // trailing letter suffix
    expect(isRoute("UAL1")).toBe(true);
    expect(isRoute("DAL")).toBe(false); // no digits
    expect(isRoute("2DAL")).toBe(false); // leading digit
    expect(isRoute("92MC")).toBe(false); // no leading letter
  });
});

describe("classifyLookup", () => {
  it("VIR92MC -> route only (was rejected as no-match before)", () => {
    expect(classifyLookup("VIR92MC")).toEqual(["route"]);
  });

  it("KX14 -> airport (and route, from the relaxed shapes) -- resolvable server-side", () => {
    const categories = classifyLookup("KX14");
    expect(categories).toContain("airport");
    expect(categories).toEqual(["airport", "route"]);
  });

  it("5X -> operator (digit-bearing 2-char IATA code)", () => {
    expect(classifyLookup("5X")).toEqual(["operator"]);
  });

  it("FFT -> operator and airport (the documented collision)", () => {
    expect(classifyLookup("FFT")).toEqual(["operator", "airport"]);
  });

  it("ABC123 -> aircraft-hex and route (the documented collision)", () => {
    expect(classifyLookup("ABC123")).toEqual(["aircraft-hex", "route"]);
  });

  it("N659DL -> aircraft-registration only", () => {
    expect(classifyLookup("N659DL")).toEqual(["aircraft-registration"]);
  });

  it("2-GOLD -> aircraft-registration only", () => {
    expect(classifyLookup("2-GOLD")).toEqual(["aircraft-registration"]);
  });

  it("A8AE7F -> aircraft-hex only", () => {
    expect(classifyLookup("A8AE7F")).toEqual(["aircraft-hex"]);
  });

  it("DAL2 -> airport and route (4 alphanumeric now also shape-matches airport)", () => {
    expect(classifyLookup("DAL2")).toEqual(["airport", "route"]);
  });

  it("HL7771 -> aircraft-registration only (not operator/airport)", () => {
    expect(classifyLookup("HL7771")).toEqual(["aircraft-registration"]);
  });

  it("JA8905 -> aircraft-registration only (not operator/airport)", () => {
    expect(classifyLookup("JA8905")).toEqual(["aircraft-registration"]);
  });

  it("DAL -> operator and airport (3 alpha chars)", () => {
    expect(classifyLookup("DAL")).toEqual(["operator", "airport"]);
  });

  it("KJFK -> airport only (4 alpha chars, no digit)", () => {
    expect(classifyLookup("KJFK")).toEqual(["airport"]);
  });

  it("12345 -> no shape match", () => {
    expect(classifyLookup("12345")).toEqual([]);
  });

  it("X -> no shape match", () => {
    expect(classifyLookup("X")).toEqual([]);
  });

  it("trims surrounding whitespace before classifying", () => {
    expect(classifyLookup("  A8AE7F  ")).toEqual(["aircraft-hex"]);
    expect(classifyLookup("   ")).toEqual([]);
  });

  it("empty string -> no shape match", () => {
    expect(classifyLookup("")).toEqual([]);
  });

  it("lowercase hex still classifies as aircraft-hex", () => {
    expect(classifyLookup("a8ae7f")).toEqual(["aircraft-hex"]);
  });

  it("a hyphenated registration that also looks hex-ish stays registration", () => {
    expect(classifyLookup("D-ABCD")).toEqual(["aircraft-registration"]);
  });
});

describe("categoriesToQuery -- LookupView's route fallback", () => {
  it("passes matched categories through unchanged", () => {
    expect(categoriesToQuery("FFT")).toEqual(["operator", "airport"]);
    expect(categoriesToQuery("A8AE7F")).toEqual(["aircraft-hex"]);
  });

  it("falls back to a route-only query when nothing matched", () => {
    expect(categoriesToQuery("12345")).toEqual(["route"]);
    expect(categoriesToQuery("X")).toEqual(["route"]);
  });

  it("returns [] for empty/whitespace input (caller guards, but be safe)", () => {
    expect(categoriesToQuery("")).toEqual([]);
    expect(categoriesToQuery("   ")).toEqual([]);
  });
});
