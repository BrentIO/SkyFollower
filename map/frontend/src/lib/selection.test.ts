import { describe, expect, it } from "vitest";
import { nextSelection } from "./selection";

describe("nextSelection", () => {
  it("selects an aircraft clicked from an empty selection", () => {
    const result = nextSelection(new Set(), "A1");
    expect(result).toEqual(new Set(["A1"]));
  });

  it("replaces the current selection when a different aircraft is clicked", () => {
    const result = nextSelection(new Set(["A"]), "B");
    expect(result).toEqual(new Set(["B"]));
    expect(result.has("A")).toBe(false);
  });

  it("deselects when the already-selected aircraft is clicked again", () => {
    const result = nextSelection(new Set(["A"]), "A");
    expect(result).toEqual(new Set());
  });

  it("never grows past one entry regardless of prior selection size", () => {
    const result = nextSelection(new Set(["A", "B"]), "C");
    expect(result).toEqual(new Set(["C"]));
  });
});
