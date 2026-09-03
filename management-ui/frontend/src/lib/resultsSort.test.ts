import { describe, expect, it } from "vitest";
import { nextSortState } from "./resultsSort";

describe("nextSortState", () => {
  it("sorts a fresh column ascending when nothing is active yet", () => {
    expect(nextSortState(null, "icao_hex")).toEqual({ column: "icao_hex", dir: "asc" });
  });

  it("sorts a newly-clicked different column ascending, replacing the active one", () => {
    const current = { column: "icao_hex" as const, dir: "desc" as const };
    expect(nextSortState(current, "registration")).toEqual({ column: "registration", dir: "asc" });
  });

  it("toggles direction when the same column is clicked again", () => {
    const current = { column: "icao_hex" as const, dir: "asc" as const };
    expect(nextSortState(current, "icao_hex")).toEqual({ column: "icao_hex", dir: "desc" });
  });

  it("toggles back to ascending on a third click of the same column", () => {
    const current = { column: "icao_hex" as const, dir: "desc" as const };
    expect(nextSortState(current, "icao_hex")).toEqual({ column: "icao_hex", dir: "asc" });
  });
});
