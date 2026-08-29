import { describe, expect, it } from "vitest";
import {
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

  it("isRegistration matches any hyphenated string", () => {
    expect(isRegistration("2-GOLD")).toBe(true);
    expect(isRegistration("OE-LBA")).toBe(true);
    expect(isRegistration("VP-CKA")).toBe(true);
    expect(isRegistration("G-ABCD")).toBe(true);
  });

  it("isRegistration matches N/HL/JA prefixes only with a digit present", () => {
    expect(isRegistration("N659DL")).toBe(true);
    expect(isRegistration("HL7771")).toBe(true);
    expect(isRegistration("JA8905")).toBe(true);
    expect(isRegistration("n659dl")).toBe(true); // case-insensitive prefix
    // Load-bearing digit clause: bare alpha guesses must NOT classify as
    // registration, or they collide with operator/airport.
    expect(isRegistration("JAX")).toBe(false);
    expect(isRegistration("NAV")).toBe(false);
    expect(isRegistration("HL")).toBe(false);
  });

  it("isRegistration rejects non-hyphen strings without an allowlisted prefix", () => {
    expect(isRegistration("A8AE7F")).toBe(false);
    expect(isRegistration("ABC123")).toBe(false);
    expect(isRegistration("DAL2")).toBe(false);
  });

  it("isOperator matches alpha-only strings of length 2-3", () => {
    expect(isOperator("DAL")).toBe(true);
    expect(isOperator("FFT")).toBe(true);
    expect(isOperator("UA")).toBe(true);
    expect(isOperator("A")).toBe(false);
    expect(isOperator("KJFK")).toBe(false);
    expect(isOperator("DAL2")).toBe(false);
    expect(isOperator("A8A")).toBe(false);
  });

  it("isAirport matches alpha-only strings of length 3 or 4", () => {
    expect(isAirport("JFK")).toBe(true);
    expect(isAirport("FFT")).toBe(true);
    expect(isAirport("KJFK")).toBe(true);
    expect(isAirport("KFFT")).toBe(true);
    expect(isAirport("JF")).toBe(false);
    expect(isAirport("KJFKX")).toBe(false);
    expect(isAirport("DAL2")).toBe(false); // digit -> not an airport code
  });

  it("isRoute matches 3 letters followed by one or more digits", () => {
    expect(isRoute("DAL2")).toBe(true);
    expect(isRoute("ABC123")).toBe(true);
    expect(isRoute("UAL1")).toBe(true);
    expect(isRoute("DAL")).toBe(false); // no digits
    expect(isRoute("DA2")).toBe(false); // only 2 letters
    expect(isRoute("DALX2")).toBe(false); // 4 letters
    expect(isRoute("2DAL")).toBe(false);
  });
});

describe("classifyLookup - every example from the issue", () => {
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

  it("DAL2 -> route only", () => {
    expect(classifyLookup("DAL2")).toEqual(["route"]);
  });

  it("HL7771 -> aircraft-registration only (not operator/airport)", () => {
    expect(classifyLookup("HL7771")).toEqual(["aircraft-registration"]);
  });

  it("JA8905 -> aircraft-registration only (not operator/airport)", () => {
    expect(classifyLookup("JA8905")).toEqual(["aircraft-registration"]);
  });

  it("12345 -> no matches, no network call", () => {
    expect(classifyLookup("12345")).toEqual([]);
  });

  it("X -> no matches, no network call", () => {
    expect(classifyLookup("X")).toEqual([]);
  });
});

describe("classifyLookup - additional shape coverage", () => {
  it("DAL -> operator and airport (3 alpha chars)", () => {
    expect(classifyLookup("DAL")).toEqual(["operator", "airport"]);
  });

  it("KJFK -> airport only (4 alpha chars)", () => {
    expect(classifyLookup("KJFK")).toEqual(["airport"]);
  });

  it("trims surrounding whitespace before classifying", () => {
    expect(classifyLookup("  A8AE7F  ")).toEqual(["aircraft-hex"]);
    expect(classifyLookup("   ")).toEqual([]);
  });

  it("empty string -> no matches", () => {
    expect(classifyLookup("")).toEqual([]);
  });

  it("lowercase hex still classifies as aircraft-hex", () => {
    expect(classifyLookup("a8ae7f")).toEqual(["aircraft-hex"]);
  });

  it("a hyphenated registration that also looks hex-ish stays registration", () => {
    expect(classifyLookup("D-ABCD")).toEqual(["aircraft-registration"]);
  });
});
