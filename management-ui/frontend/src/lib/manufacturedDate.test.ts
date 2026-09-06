import { describe, expect, it } from "vitest";
import { formatManufacturedDate } from "./manufacturedDate";

describe("formatManufacturedDate", () => {
  it("returns the year only for a Jan 1 midnight-UTC value", () => {
    expect(formatManufacturedDate("2019-01-01T00:00:00Z")).toBe("2019");
  });

  it("returns the year only for any time of day on Jan 1 UTC", () => {
    expect(formatManufacturedDate("2019-01-01T09:30:00Z")).toBe("2019");
  });

  it("returns the fixed full date for a non-Jan-1 value", () => {
    expect(formatManufacturedDate("2019-03-14T00:00:00Z")).toBe("2019-03-14");
  });

  it("returns undefined for an empty value", () => {
    expect(formatManufacturedDate("")).toBeUndefined();
  });

  it("returns undefined for a garbage value", () => {
    expect(formatManufacturedDate("not-a-date")).toBeUndefined();
  });

  it("classifies Jan 1 in UTC regardless of the viewer's negative local offset", () => {
    // A viewer several hours behind UTC would see this instant as Dec 31
    // locally; the classification must still be based on UTC fields.
    expect(formatManufacturedDate("2019-01-01T02:00:00Z")).toBe("2019");
  });
});
