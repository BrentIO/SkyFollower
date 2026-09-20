import { describe, expect, it } from "vitest";
import { countryFlag } from "./countryFlag";

describe("countryFlag", () => {
  it("renders the Cayman Islands flag", () => {
    expect(countryFlag("KY")).toBe("\u{1F1F0}\u{1F1FE}");
  });

  it("renders the United Kingdom flag", () => {
    expect(countryFlag("GB")).toBe("\u{1F1EC}\u{1F1E7}");
  });

  it("normalizes lowercase input", () => {
    expect(countryFlag("ky")).toBe(countryFlag("KY"));
  });

  it("returns null for a non-two-letter code", () => {
    expect(countryFlag("USA")).toBeNull();
  });

  it("returns null for a non-alpha code", () => {
    expect(countryFlag("U1")).toBeNull();
  });

  it("returns null for an empty string", () => {
    expect(countryFlag("")).toBeNull();
  });
});
