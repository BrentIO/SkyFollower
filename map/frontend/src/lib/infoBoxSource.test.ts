import { describe, expect, it } from "vitest";
import { applySnapshot, applyWsEvent, type AircraftMap } from "./aircraftState";
import {
  buildInfoBoxLabelSourceDiff,
  infoBoxLabelFeature,
  infoBoxLabelFeatureCollection,
  type LabelFilter,
} from "./infoBoxSource";
import { altitudeZIndex } from "./labelStackOrder";

function withOnePositionedAircraft(icaoHex = "A1B2C3", extra: Record<string, unknown> = {}): AircraftMap {
  return applySnapshot([{ icao_hex: icaoHex, lat: 1, lon: 2, alt: 1000, ...extra }]);
}

function noFilter(overrides: Partial<LabelFilter> = {}): LabelFilter {
  return { selected: new Set(), showAll: false, hoveredId: null, ...overrides };
}

describe("infoBoxLabelFeature -- inclusion (selected/hovered/showAll, #1808)", () => {
  it("is excluded when not selected, not hovered, and showAll is off (same default as the removed DOM InfoBoxLayer)", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", { ident: "UAL123" });
    expect(infoBoxLabelFeature(aircraft.A1B2C3, noFilter())).toBeNull();
  });

  it("is included when showAll (Labels: All) is on, regardless of selection/hover", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", { ident: "UAL123" });
    expect(infoBoxLabelFeature(aircraft.A1B2C3, noFilter({ showAll: true }))).not.toBeNull();
  });

  it("is included when selected, even with showAll off", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", { ident: "UAL123" });
    expect(infoBoxLabelFeature(aircraft.A1B2C3, noFilter({ selected: new Set(["A1B2C3"]) }))).not.toBeNull();
  });

  it("is included when hovered, even with showAll off and not selected", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", { ident: "UAL123" });
    expect(infoBoxLabelFeature(aircraft.A1B2C3, noFilter({ hoveredId: "A1B2C3" }))).not.toBeNull();
  });

  it("hovering a different aircraft doesn't include this one", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", { ident: "UAL123" });
    expect(infoBoxLabelFeature(aircraft.A1B2C3, noFilter({ hoveredId: "OTHER" }))).toBeNull();
  });

  it("excludes an aircraft with no known position regardless of the label filter", () => {
    const aircraft = applySnapshot([{ icao_hex: "A1B2C3", ident: "UAL123" }]);
    expect(infoBoxLabelFeature(aircraft.A1B2C3, noFilter({ showAll: true }))).toBeNull();
  });

  it("excludes a hidden aircraft with no Follow/protected exception, same as its icon (isAircraftVisible)", () => {
    const base = withOnePositionedAircraft("A1B2C3", { ident: "UAL123" });
    const hidden = applyWsEvent(base, { type: "hide", icao_hex: "A1B2C3" });
    expect(infoBoxLabelFeature(hidden.A1B2C3, noFilter({ showAll: true }))).toBeNull();
  });

  it("keeps a hidden Followed aircraft's label, same exception its icon gets", () => {
    const base = withOnePositionedAircraft("A1B2C3", { ident: "UAL123" });
    const hidden = applyWsEvent(base, { type: "hide", icao_hex: "A1B2C3" });
    expect(infoBoxLabelFeature(hidden.A1B2C3, noFilter({ showAll: true }), { followId: "A1B2C3" })).not.toBeNull();
  });

  it("Isolate hides every other aircraft's label, same hard filter as its icon", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", { ident: "UAL123" });
    expect(
      infoBoxLabelFeature(aircraft.A1B2C3, noFilter({ showAll: true }), { isolateId: "OTHER" }),
    ).toBeNull();
  });

  it("is excluded when every line would be empty (no ident/altitude/speed/registration/type resolved yet)", () => {
    // withOnePositionedAircraft's own default carries alt: 1000 -- override
    // it here to get a genuinely empty-content aircraft (freshly tracked,
    // nothing but a raw position resolved yet).
    const aircraft = withOnePositionedAircraft("A1B2C3", { alt: null });
    expect(infoBoxLabelFeature(aircraft.A1B2C3, noFilter({ showAll: true }))).toBeNull();
  });
});

describe("infoBoxLabelFeature -- content (identLine/detailLines/hasBothLines, #1808)", () => {
  it("stamps a stable id (icao_hex) and Point geometry at the aircraft's position, required for updateData()", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", { ident: "UAL123" });
    const feature = infoBoxLabelFeature(aircraft.A1B2C3, noFilter({ showAll: true }));
    expect(feature?.id).toBe("A1B2C3");
    expect(feature?.geometry).toEqual({ type: "Point", coordinates: [2, 1] });
    expect(feature?.properties?.icao_hex).toBe("A1B2C3");
  });

  it("ident only: identLine set, detailLines empty, hasBothLines false", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", { ident: "UAL123", alt: null });
    const feature = infoBoxLabelFeature(aircraft.A1B2C3, noFilter({ showAll: true }));
    expect(feature?.properties?.identLine).toBe("UAL123");
    expect(feature?.properties?.detailLines).toBe("");
    expect(feature?.properties?.hasBothLines).toBe(false);
  });

  it("altitude only (no ident): identLine empty, detailLines set, hasBothLines false -- no stray leading blank line", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", { alt: 5000 });
    const feature = infoBoxLabelFeature(aircraft.A1B2C3, noFilter({ showAll: true }));
    expect(feature?.properties?.identLine).toBe("");
    expect(feature?.properties?.detailLines).toBe("5000");
    expect(feature?.properties?.hasBothLines).toBe(false);
  });

  it("ident + altitude: hasBothLines true, so the layer's format expression inserts the separating newline", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", { ident: "UAL123", alt: 5000 });
    const feature = infoBoxLabelFeature(aircraft.A1B2C3, noFilter({ showAll: true }));
    expect(feature?.properties?.identLine).toBe("UAL123");
    expect(feature?.properties?.detailLines).toBe("5000");
    expect(feature?.properties?.hasBothLines).toBe(true);
  });

  it("altitude/speed and registration/type both known: detailLines joins them with a newline, same order buildInfoBoxLines specifies", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", {
      ident: "UAL123",
      alt: 5000,
      velocity: 250,
      aircraft: { icao_hex: "A1B2C3", registration: "N988DL", type_designator: "B752" },
    });
    const feature = infoBoxLabelFeature(aircraft.A1B2C3, noFilter({ showAll: true }));
    expect(feature?.properties?.detailLines).toBe("5000 250kt\nN988DL B752");
  });

  it("only registration/type known (no ident, no altitude/speed): detailLines is just that line, hasBothLines false", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", {
      alt: null,
      aircraft: { icao_hex: "A1B2C3", registration: "N988DL", type_designator: "B752" },
    });
    const feature = infoBoxLabelFeature(aircraft.A1B2C3, noFilter({ showAll: true }));
    expect(feature?.properties?.identLine).toBe("");
    expect(feature?.properties?.detailLines).toBe("N988DL B752");
    expect(feature?.properties?.hasBothLines).toBe(false);
  });

  it("sortKey is altitudeZIndex(alt) -- higher altitude sorts higher, so it draws on top when boxes overlap (lib/labelStackOrder.ts)", () => {
    const low = withOnePositionedAircraft("A1B2C3", { ident: "LOW1", alt: 2000 });
    const high = withOnePositionedAircraft("D4E5F6", { ident: "HIGH1", alt: 40000 });
    const lowFeature = infoBoxLabelFeature(low.A1B2C3, noFilter({ showAll: true }));
    const highFeature = infoBoxLabelFeature(high.D4E5F6, noFilter({ showAll: true }));
    expect(lowFeature?.properties?.sortKey).toBe(altitudeZIndex(2000));
    expect(highFeature?.properties?.sortKey).toBe(altitudeZIndex(40000));
    expect(highFeature!.properties!.sortKey as number).toBeGreaterThan(lowFeature!.properties!.sortKey as number);
  });

  it("an unknown altitude gets the floor sortKey (UNKNOWN_ALTITUDE_Z_INDEX via altitudeZIndex(null))", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", { ident: "UAL123", alt: null });
    const feature = infoBoxLabelFeature(aircraft.A1B2C3, noFilter({ showAll: true }));
    expect(feature?.properties?.sortKey).toBe(altitudeZIndex(null));
  });
});

describe("infoBoxLabelFeatureCollection", () => {
  it("includes only the labeled subset of the fleet, not every tracked aircraft (#1808's core CPU fix)", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3", { ident: "SELECTED" });
    aircraft = { ...aircraft, ...withOnePositionedAircraft("D4E5F6", { ident: "NOT_LABELED" }) };
    const fc = infoBoxLabelFeatureCollection(aircraft, noFilter({ selected: new Set(["A1B2C3"]) }));
    expect(fc.features.map((f) => f.properties?.icao_hex)).toEqual(["A1B2C3"]);
  });

  it("with showAll on, includes every visible, non-empty-content aircraft", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3", { ident: "ONE" });
    aircraft = { ...aircraft, ...withOnePositionedAircraft("D4E5F6", { ident: "TWO" }) };
    const fc = infoBoxLabelFeatureCollection(aircraft, noFilter({ showAll: true }));
    expect(fc.features.map((f) => f.properties?.icao_hex).sort()).toEqual(["A1B2C3", "D4E5F6"]);
  });
});

describe("buildInfoBoxLabelSourceDiff (#1808, presentIds guard per #1838)", () => {
  it("adds a changed hex that now qualifies for a label", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", { ident: "UAL123" });
    const diff = buildInfoBoxLabelSourceDiff(["A1B2C3"], aircraft, noFilter({ showAll: true }), new Set());
    expect(diff.add).toHaveLength(1);
    expect(diff.add?.[0].id).toBe("A1B2C3");
    expect(diff.remove).toHaveLength(0);
  });

  it("removes a changed hex no longer present in the aircraft map at all, if it was actually in the source", () => {
    const diff = buildInfoBoxLabelSourceDiff(["GONE123"], {}, noFilter({ showAll: true }), new Set(["GONE123"]));
    expect(diff.add).toHaveLength(0);
    expect(diff.remove).toEqual(["GONE123"]);
  });

  it("removes a changed hex that's present but no longer qualifies (e.g. its content emptied out, or it's not in the label filter)", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", { ident: "UAL123" });
    const diff = buildInfoBoxLabelSourceDiff(["A1B2C3"], aircraft, noFilter(), new Set(["A1B2C3"]));
    expect(diff.add).toHaveLength(0);
    expect(diff.remove).toEqual(["A1B2C3"]);
  });

  it("#1838: does not remove a changed hex that never qualified and was never in the source (labels off, no-op tick)", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3", { ident: "UAL123" });
    const diff = buildInfoBoxLabelSourceDiff(["A1B2C3"], aircraft, noFilter(), new Set());
    expect(diff.add).toHaveLength(0);
    expect(diff.remove).toHaveLength(0);
  });

  it("only touches the hexes named in changedIcaoHexes, not the whole fleet", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3", { ident: "ONE" });
    aircraft = { ...aircraft, ...withOnePositionedAircraft("D4E5F6", { ident: "TWO" }) };
    const diff = buildInfoBoxLabelSourceDiff(["A1B2C3"], aircraft, noFilter({ showAll: true }), new Set());
    expect(diff.add).toHaveLength(1);
    expect(diff.add?.[0].id).toBe("A1B2C3");
  });
});
