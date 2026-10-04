import { describe, expect, it } from "vitest";
import { todayUtc } from "./todayUtc";

describe("todayUtc", () => {
  it("formats the UTC calendar date as YYYY-MM-DD", () => {
    expect(todayUtc(new Date("2026-10-03T23:59:59Z"))).toBe("2026-10-03");
  });

  it("uses the UTC date, not the local one, near midnight", () => {
    expect(todayUtc(new Date("2026-10-04T00:00:01Z"))).toBe("2026-10-04");
  });
});
