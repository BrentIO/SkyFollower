import { describe, expect, it } from "vitest";
import { applySnapshot, applyWsEvent } from "./aircraftState";
import { followTargetPosition, isFollowLost, shouldCancelFollowOnDrag } from "./followTarget";

describe("followTargetPosition", () => {
  it("returns null when nothing is being followed", () => {
    const aircraft = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }]);
    expect(followTargetPosition(aircraft, null)).toBeNull();
  });

  it("returns the followed aircraft's current position", () => {
    const aircraft = applySnapshot([{ icao_hex: "A1B2C3", lat: 33.9, lon: -118.4 }]);
    expect(followTargetPosition(aircraft, "A1B2C3")).toEqual({ lat: 33.9, lon: -118.4 });
  });

  it("returns null when the followed aircraft has no known position", () => {
    const aircraft = applySnapshot([{ icao_hex: "A1B2C3" }]);
    expect(followTargetPosition(aircraft, "A1B2C3")).toBeNull();
  });

  it("returns null when the followed aircraft isn't tracked at all", () => {
    const aircraft = applySnapshot([]);
    expect(followTargetPosition(aircraft, "UNKNOWN")).toBeNull();
  });

  it("keeps returning the last known position after the aircraft goes stale/hidden", () => {
    let aircraft = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }]);
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });
    expect(followTargetPosition(aircraft, "A1B2C3")).toEqual({ lat: 1, lon: 2 });
  });

  it("keeps returning the last known position after a deferred (pendingRemoval) eviction", () => {
    let aircraft = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }]);
    aircraft = applyWsEvent(aircraft, { type: "remove", icao_hex: "A1B2C3" }, { protectedIcaoHex: "A1B2C3" });
    expect(followTargetPosition(aircraft, "A1B2C3")).toEqual({ lat: 1, lon: 2 });
  });

  it("updates as the followed aircraft moves", () => {
    let aircraft = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }]);
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.5, lon: 2.5 });
    expect(followTargetPosition(aircraft, "A1B2C3")).toEqual({ lat: 1.5, lon: 2.5 });
  });
});

describe("isFollowLost", () => {
  const followed = { icao_hex: "A1B2C3", hidden: false, pendingRemoval: false };

  it("is false when nothing is being followed", () => {
    expect(isFollowLost(followed, null)).toBe(false);
  });

  it("is false for an aircraft other than the followed one, even if lost", () => {
    expect(isFollowLost({ icao_hex: "OTHER", hidden: true, pendingRemoval: false }, "A1B2C3")).toBe(false);
  });

  it("is false for the followed aircraft while it's still live", () => {
    expect(isFollowLost(followed, "A1B2C3")).toBe(false);
  });

  it("is true for the followed aircraft once hidden", () => {
    expect(isFollowLost({ ...followed, hidden: true }, "A1B2C3")).toBe(true);
  });

  it("is true for the followed aircraft once its eviction is deferred (pendingRemoval)", () => {
    expect(isFollowLost({ ...followed, pendingRemoval: true }, "A1B2C3")).toBe(true);
  });

  it("is false for a merely-selected (protectedId) aircraft while it's still live", () => {
    expect(isFollowLost(followed, null, "A1B2C3")).toBe(false);
  });

  it("is true for a merely-selected (protectedId, not followed) aircraft once hidden", () => {
    expect(isFollowLost({ ...followed, hidden: true }, null, "A1B2C3")).toBe(true);
  });

  it("is true for a merely-selected (protectedId) aircraft once its eviction is deferred", () => {
    expect(isFollowLost({ ...followed, pendingRemoval: true }, null, "A1B2C3")).toBe(true);
  });

  it("is false for an aircraft other than either the followed or protected one, even if lost", () => {
    expect(isFollowLost({ icao_hex: "OTHER", hidden: true, pendingRemoval: false }, "A1B2C3", "D4E5F6")).toBe(false);
  });

  it("is true when the aircraft is both the followed and the protected (selected) one", () => {
    expect(isFollowLost({ ...followed, hidden: true }, "A1B2C3", "A1B2C3")).toBe(true);
  });
});

describe("shouldCancelFollowOnDrag", () => {
  it("is true for a genuine user-driven drag while Follow is active", () => {
    expect(shouldCancelFollowOnDrag({ originalEvent: {} }, "A1B2C3")).toBe(true);
  });

  it("is false for a programmatic camera move (no originalEvent) -- Follow's own easeTo, Zoom To, and the recenter button", () => {
    expect(shouldCancelFollowOnDrag({ originalEvent: undefined }, "A1B2C3")).toBe(false);
  });

  it("is false when nothing is being followed, even for a genuine drag", () => {
    expect(shouldCancelFollowOnDrag({ originalEvent: {} }, null)).toBe(false);
  });
});
