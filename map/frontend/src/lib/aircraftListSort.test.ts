import { describe, expect, it } from "vitest";
import { nextAircraftListSortState, sortAircraftListRows, type AircraftListSortState } from "./aircraftListSort";

describe("nextAircraftListSortState", () => {
  it("clicking a different column switches to it, ascending", () => {
    const current: AircraftListSortState = { columnKey: "distance", dir: "asc" };
    expect(nextAircraftListSortState(current, "ident")).toEqual({ columnKey: "ident", dir: "asc" });
  });

  it("clicking the same column reverses direction", () => {
    const current: AircraftListSortState = { columnKey: "distance", dir: "asc" };
    expect(nextAircraftListSortState(current, "distance")).toEqual({ columnKey: "distance", dir: "desc" });
  });

  it("clicking the same column a second time flips back to ascending", () => {
    const current: AircraftListSortState = { columnKey: "distance", dir: "desc" };
    expect(nextAircraftListSortState(current, "distance")).toEqual({ columnKey: "distance", dir: "asc" });
  });
});

interface Row {
  id: string;
  value: string | number | null;
}

function row(id: string, value: string | number | null): Row {
  return { id, value };
}

describe("sortAircraftListRows -- numeric columns (e.g. Altitude, Distance)", () => {
  it("sorts ascending", () => {
    const rows = [row("a", 300), row("b", 100), row("c", 200)];
    const sorted = sortAircraftListRows(rows, (r) => r.value, "asc");
    expect(sorted.map((r) => r.id)).toEqual(["b", "c", "a"]);
  });

  it("sorts descending", () => {
    const rows = [row("a", 300), row("b", 100), row("c", 200)];
    const sorted = sortAircraftListRows(rows, (r) => r.value, "desc");
    expect(sorted.map((r) => r.id)).toEqual(["a", "c", "b"]);
  });

  it("does not mutate the input array", () => {
    const rows = [row("a", 300), row("b", 100)];
    const original = [...rows];
    sortAircraftListRows(rows, (r) => r.value, "asc");
    expect(rows).toEqual(original);
  });
});

describe("sortAircraftListRows -- string columns (e.g. Ident, Registration)", () => {
  it("sorts ascending, case-insensitively via localeCompare", () => {
    const rows = [row("a", "United"), row("b", "American"), row("c", "delta")];
    const sorted = sortAircraftListRows(rows, (r) => r.value, "asc");
    expect(sorted.map((r) => r.id)).toEqual(["b", "c", "a"]);
  });

  it("sorts descending", () => {
    const rows = [row("a", "United"), row("b", "American"), row("c", "delta")];
    const sorted = sortAircraftListRows(rows, (r) => r.value, "desc");
    expect(sorted.map((r) => r.id)).toEqual(["a", "c", "b"]);
  });
});

describe("sortAircraftListRows -- blank values always sort last", () => {
  it("puts null last on an ascending sort", () => {
    const rows = [row("a", null), row("b", 50), row("c", 10)];
    const sorted = sortAircraftListRows(rows, (r) => r.value, "asc");
    expect(sorted.map((r) => r.id)).toEqual(["c", "b", "a"]);
  });

  it("still puts null last on a descending sort -- never flips to the front", () => {
    const rows = [row("a", null), row("b", 50), row("c", 10)];
    const sorted = sortAircraftListRows(rows, (r) => r.value, "desc");
    expect(sorted.map((r) => r.id)).toEqual(["b", "c", "a"]);
  });

  it("keeps every-value-blank rows in their relative order", () => {
    const rows = [row("a", null), row("b", null)];
    const sorted = sortAircraftListRows(rows, (r) => r.value, "asc");
    expect(sorted.map((r) => r.id)).toEqual(["a", "b"]);
  });
});
