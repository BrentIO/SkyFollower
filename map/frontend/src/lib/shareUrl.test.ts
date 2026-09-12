import { describe, expect, it } from "vitest";
import { readSelectionFromSearch, searchWithSelection } from "./shareUrl";

describe("readSelectionFromSearch", () => {
  it("reads the icao_hex out of an ?aircraft= param", () => {
    expect(readSelectionFromSearch("?aircraft=A1B2C3")).toBe("A1B2C3");
  });

  it("returns null when the param is absent", () => {
    expect(readSelectionFromSearch("")).toBeNull();
    expect(readSelectionFromSearch("?other=1")).toBeNull();
  });

  it("reads the param out among other unrelated params", () => {
    expect(readSelectionFromSearch("?foo=bar&aircraft=DEADBE&baz=1")).toBe("DEADBE");
  });
});

describe("searchWithSelection", () => {
  it("sets the aircraft param on a bare URL when selecting", () => {
    expect(searchWithSelection("", "A1B2C3")).toBe("?aircraft=A1B2C3");
  });

  it("clears the aircraft param back to a bare search when deselecting", () => {
    expect(searchWithSelection("?aircraft=A1B2C3", null)).toBe("");
  });

  it("replaces the aircraft param when switching selection to a different aircraft", () => {
    expect(searchWithSelection("?aircraft=A1B2C3", "DEADBE")).toBe("?aircraft=DEADBE");
  });

  it("is a no-op re-set when the same aircraft is already selected", () => {
    expect(searchWithSelection("?aircraft=A1B2C3", "A1B2C3")).toBe("?aircraft=A1B2C3");
  });

  it("preserves unrelated params when selecting", () => {
    expect(searchWithSelection("?foo=bar", "A1B2C3")).toBe("?foo=bar&aircraft=A1B2C3");
  });

  it("preserves unrelated params when deselecting", () => {
    expect(searchWithSelection("?foo=bar&aircraft=A1B2C3", null)).toBe("?foo=bar");
  });

  it("round-trips: selecting then reading back reproduces the same icao_hex", () => {
    const search = searchWithSelection("", "A1B2C3");
    expect(readSelectionFromSearch(search)).toBe("A1B2C3");
  });

  it("round-trips a URL copied mid-session: constructing the same URL/param reproduces the same selection", () => {
    // Simulates "select on one tab, copy the address bar, open it
    // elsewhere" -- the copied URL's search string, read fresh, must name
    // the same aircraft the first tab had selected.
    const copiedSearch = searchWithSelection("", "DEADBE");
    const reopened = readSelectionFromSearch(copiedSearch);
    expect(reopened).toBe("DEADBE");
  });
});
