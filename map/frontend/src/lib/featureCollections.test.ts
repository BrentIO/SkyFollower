import { describe, expect, it } from "vitest";
import { applySnapshot, applyTrailSeed, applyWsEvent, MAX_TRAIL_POINTS, type AircraftMap, type TrailPoint } from "./aircraftState";
import {
  aircraftFeature,
  aircraftFeatureCollection,
  buildAircraftSourceDiff,
  buildTrailSourceDiff,
  isAircraftVisible,
  isEmptySourceDiff,
  TRAIL_BLOCK_SIZE,
  trailFeatureCollection,
  trailSegmentFeatures,
} from "./featureCollections";

// Test-only helper for exercising trail-block behavior directly against
// TrailPoint[] without going through a full WS event per point (the
// acceptance criteria's 64-point-boundary and cap-truncation cases need
// dozens of points, which would be unreadable as individual applyWsEvent
// calls). Mirrors pushTrailPoint's shape (latitude/longitude/altitude),
// just without its same-as-last dedupe/cap -- callers here fully control
// point count.
function makeTrail(count: number, startLat = 1): TrailPoint[] {
  return Array.from({ length: count }, (_, i) => ({
    latitude: startLat + i * 0.001,
    longitude: 2 + i * 0.001,
    altitude: 1000,
  }));
}

function withTrail(icaoHex: string, trail: TrailPoint[]): AircraftMap {
  const aircraft = applySnapshot([{ icao_hex: icaoHex, lat: trail[0].latitude, lon: trail[0].longitude, alt: 1000 }]);
  return { ...aircraft, [icaoHex]: { ...aircraft[icaoHex], trail } };
}

function withOnePositionedAircraft(icaoHex = "A1B2C3"): AircraftMap {
  return applySnapshot([{ icao_hex: icaoHex, lat: 1, lon: 2, alt: 1000 }]);
}

describe("aircraftFeatureCollection", () => {
  it("includes a visible, positioned aircraft", () => {
    const aircraft = withOnePositionedAircraft();
    const fc = aircraftFeatureCollection(aircraft, new Set());
    expect(fc.features).toHaveLength(1);
    expect(fc.features[0].properties?.icao_hex).toBe("A1B2C3");
  });

  it("excludes a hidden aircraft even though it still has a position", () => {
    const base = withOnePositionedAircraft();
    const hidden = applyWsEvent(base, { type: "hide", icao_hex: "A1B2C3" });
    const fc = aircraftFeatureCollection(hidden, new Set());
    expect(fc.features).toHaveLength(0);
  });

  it("excludes an aircraft with no known position regardless of hidden state", () => {
    const aircraft = applySnapshot([{ icao_hex: "A1B2C3" }]);
    const fc = aircraftFeatureCollection(aircraft, new Set());
    expect(fc.features).toHaveLength(0);
  });
});

describe("trailFeatureCollection", () => {
  it("includes trail segments for a visible aircraft in the visible-ids set", () => {
    let aircraft = withOnePositionedAircraft();
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    const fc = trailFeatureCollection(aircraft, new Set(["A1B2C3"]));
    expect(fc.features.length).toBeGreaterThan(0);
  });

  it("excludes a hidden aircraft's trail even though its trail data is retained", () => {
    let aircraft = withOnePositionedAircraft();
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });

    // The trail data itself must still be there client-side...
    expect(aircraft.A1B2C3.trail.length).toBeGreaterThan(0);
    // ...but it must not be drawn while hidden.
    const fc = trailFeatureCollection(aircraft, new Set(["A1B2C3"]));
    expect(fc.features).toHaveLength(0);
  });

  it("excludes an aircraft not in the visible-ids set regardless of hidden state", () => {
    const aircraft = withOnePositionedAircraft();
    const fc = trailFeatureCollection(aircraft, new Set());
    expect(fc.features).toHaveLength(0);
  });

  it("re-includes the bridged trail once the aircraft un-hides via a new position event", () => {
    let aircraft = withOnePositionedAircraft();
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 5, lon: 6 });

    const fc = trailFeatureCollection(aircraft, new Set(["A1B2C3"]));
    // One segment bridging the pre-gap point to the post-gap point.
    expect(fc.features).toHaveLength(1);
  });
});

describe("aircraftFeatureCollection -- Isolate (isolateId)", () => {
  it("hides every aircraft except the isolated one", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = { ...aircraft, ...withOnePositionedAircraft("D4E5F6") };
    const fc = aircraftFeatureCollection(aircraft, new Set(), { isolateId: "A1B2C3" });
    expect(fc.features.map((f) => f.properties?.icao_hex)).toEqual(["A1B2C3"]);
  });

  it("shows everyone when isolateId is null/undefined", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = { ...aircraft, ...withOnePositionedAircraft("D4E5F6") };
    const fc = aircraftFeatureCollection(aircraft, new Set(), { isolateId: null });
    expect(fc.features).toHaveLength(2);
  });

  it("hides a newly-appearing aircraft too, same as any other aircraft while isolated", () => {
    const base = withOnePositionedAircraft("A1B2C3");
    const withNewArrival = applyWsEvent(base, { type: "position", icao_hex: "NEW123", lat: 9, lon: 9 });
    const fc = aircraftFeatureCollection(withNewArrival, new Set(), { isolateId: "A1B2C3" });
    expect(fc.features.map((f) => f.properties?.icao_hex)).toEqual(["A1B2C3"]);
  });
});

describe("aircraftFeatureCollection -- Follow-lost dimming", () => {
  it("keeps a hidden followed aircraft visible, marked stale (dimmed)", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });
    const fc = aircraftFeatureCollection(aircraft, new Set(), { followId: "A1B2C3" });
    expect(fc.features).toHaveLength(1);
    expect(fc.features[0].properties?.stale).toBe(true);
  });

  it("does not affect a hidden aircraft that isn't the followed one", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });
    const fc = aircraftFeatureCollection(aircraft, new Set(), { followId: "OTHER" });
    expect(fc.features).toHaveLength(0);
  });

  it("leaves an already-stale (not hidden) followed aircraft's stale flag as-is", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "stale", icao_hex: "A1B2C3" });
    const fc = aircraftFeatureCollection(aircraft, new Set(), { followId: "A1B2C3" });
    expect(fc.features[0].properties?.stale).toBe(true);
  });
});

describe("aircraftFeatureCollection -- selected (protectedId, not Followed) lost dimming", () => {
  it("keeps a hidden selected-but-not-followed aircraft visible, marked stale (dimmed)", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });
    const fc = aircraftFeatureCollection(aircraft, new Set(), { protectedId: "A1B2C3" });
    expect(fc.features).toHaveLength(1);
    expect(fc.features[0].properties?.stale).toBe(true);
  });

  it("does not affect a hidden aircraft that isn't selected", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });
    const fc = aircraftFeatureCollection(aircraft, new Set(), { protectedId: "OTHER" });
    expect(fc.features).toHaveLength(0);
  });

  it("toggling Follow on an already-lost selected aircraft causes no visibility change", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });
    const beforeFollow = aircraftFeatureCollection(aircraft, new Set(), { protectedId: "A1B2C3" });
    const afterFollow = aircraftFeatureCollection(aircraft, new Set(), { protectedId: "A1B2C3", followId: "A1B2C3" });
    expect(beforeFollow.features).toHaveLength(1);
    expect(afterFollow.features).toHaveLength(1);
    expect(beforeFollow.features[0].properties?.stale).toBe(afterFollow.features[0].properties?.stale);
  });
});

describe("trailFeatureCollection -- Isolate (isolateId)", () => {
  it("hides every other aircraft's trail even if it's in the visible-ids set", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    aircraft = { ...aircraft, ...withOnePositionedAircraft("D4E5F6") };
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "D4E5F6", lat: 1.1, lon: 2.1 });

    const fc = trailFeatureCollection(aircraft, new Set(["A1B2C3", "D4E5F6"]), { isolateId: "A1B2C3" });
    expect(fc.features.every((f) => f.properties?.icao_hex === "A1B2C3")).toBe(true);
    expect(fc.features.length).toBeGreaterThan(0);
  });
});

describe("trailFeatureCollection -- Follow-lost dimming", () => {
  it("keeps a hidden followed aircraft's trail visible and flags it dimmed", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });

    const fc = trailFeatureCollection(aircraft, new Set(["A1B2C3"]), { followId: "A1B2C3" });
    expect(fc.features.length).toBeGreaterThan(0);
    expect(fc.features.every((f) => f.properties?.dimmed === true)).toBe(true);
  });

  it("a normal (not hidden/followed) trail is not marked dimmed", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    const fc = trailFeatureCollection(aircraft, new Set(["A1B2C3"]));
    expect(fc.features.every((f) => f.properties?.dimmed === false)).toBe(true);
  });
});

describe("aircraftFeature -- single-feature builder (#1775)", () => {
  it("stamps a stable id (icao_hex) on every feature, required for updateData()", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3");
    const feature = aircraftFeature(aircraft.A1B2C3, new Set());
    expect(feature?.id).toBe("A1B2C3");
  });

  it("returns null (not a feature) for an aircraft that shouldn't be drawn", () => {
    const base = withOnePositionedAircraft("A1B2C3");
    const hidden = applyWsEvent(base, { type: "hide", icao_hex: "A1B2C3" });
    expect(aircraftFeature(hidden.A1B2C3, new Set())).toBeNull();
  });

  it("aircraftFeatureCollection's features are exactly what aircraftFeature would build per aircraft", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = { ...aircraft, ...withOnePositionedAircraft("D4E5F6") };
    const fc = aircraftFeatureCollection(aircraft, new Set(["A1B2C3"]), { followId: "D4E5F6" });
    for (const f of fc.features) {
      const hex = f.properties?.icao_hex as string;
      expect(f).toEqual(aircraftFeature(aircraft[hex], new Set(["A1B2C3"]), { followId: "D4E5F6" }));
    }
  });
});

describe("aircraftFeature -- lighter-than-air heading override (#1788)", () => {
  it("forces heading to 0 for a BALL-shaped (balloon/airship) aircraft regardless of reported hdg", () => {
    const aircraft = applySnapshot([
      {
        icao_hex: "A1B2C3",
        lat: 1,
        lon: 2,
        alt: 1000,
        hdg: 270,
        aircraft: { icao_hex: "A1B2C3", emitter_category: "B2" },
      },
    ]);
    expect(aircraft.A1B2C3.shape).toBe("BALL");
    const feature = aircraftFeature(aircraft.A1B2C3, new Set());
    expect(feature?.properties?.heading).toBe(0);
  });

  it("leaves a non-BALL-shaped aircraft's reported hdg untouched", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft.A1B2C3.hdg = 270;
    const feature = aircraftFeature(aircraft.A1B2C3, new Set());
    expect(feature?.properties?.heading).toBe(270);
  });
});

describe("trailSegmentFeatures -- per-run builder with stable ids (#1775, run-grouping per #1820, blocked per #1838)", () => {
  it("stamps a stable `${icao_hex}:${block}:${index}` id on every run", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    // Distinct altitudes so each point-to-point step gets its own color and
    // this genuinely produces multiple runs -- see the dedicated #1820 test
    // below for the same-altitude (single-run) case.
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1, alt: 5000 });
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.2, lon: 2.2, alt: 35000 });
    const runs = trailSegmentFeatures(aircraft.A1B2C3, false);
    expect(runs.length).toBeGreaterThanOrEqual(2);
    // A 3-point trail fits entirely in block 0.
    runs.forEach((run, index) => {
      expect(run.id).toBe(`A1B2C3:0:${index}`);
    });
  });

  it("#1820: a steady-altitude trail collapses into a single run, not one feature per point", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3"); // alt: 1000, at lat 1 / lon 2
    for (let i = 1; i <= 10; i++) {
      aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1 + i * 0.01, lon: 2 + i * 0.01 });
    }
    // No `alt` on any position event -- merges forward the existing 1000
    // value every time (aircraftState.ts's field-merge semantics), so
    // every point shares the same altitudeColor() and this 11-point trail
    // must produce exactly one run, not 10 individual segment features.
    const runs = trailSegmentFeatures(aircraft.A1B2C3, false);
    expect(runs).toHaveLength(1);
    expect((runs[0].geometry as { coordinates: unknown[] }).coordinates).toHaveLength(11);
  });

  it("ids are stable across repeated calls over the same trail (unchanged geometry)", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    const first = trailSegmentFeatures(aircraft.A1B2C3, false);
    const second = trailSegmentFeatures(aircraft.A1B2C3, false);
    expect(first.map((f) => f.id)).toEqual(second.map((f) => f.id));
  });

  it("carries the dimmed flag passed in, independent of hidden/follow state", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    const runs = trailSegmentFeatures(aircraft.A1B2C3, true);
    expect(runs.every((f) => f.properties?.dimmed === true)).toBe(true);
  });
});

describe("trailFeatureCollection -- selected (protectedId, not Followed) lost dimming", () => {
  it("keeps a hidden selected-but-not-followed aircraft's trail visible and flags it dimmed", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });

    const fc = trailFeatureCollection(aircraft, new Set(["A1B2C3"]), { protectedId: "A1B2C3" });
    expect(fc.features.length).toBeGreaterThan(0);
    expect(fc.features.every((f) => f.properties?.dimmed === true)).toBe(true);
  });

  it("does not affect a hidden aircraft's trail that isn't selected", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });

    const fc = trailFeatureCollection(aircraft, new Set(["A1B2C3"]), { protectedId: "OTHER" });
    expect(fc.features).toHaveLength(0);
  });
});

// #1808: extracted out of aircraftFeature so the isolate/hidden visibility
// rule it encodes can be applied consistently anywhere else that needs to
// answer "is this aircraft currently drawn on the map" (e.g.
// components/MapView.tsx's own info-box item filtering), rather than
// risking a label ever drifting out of sync with its own aircraft's icon.
describe("isAircraftVisible -- shared aircraft/label visibility rule (#1808)", () => {
  it("a normal (not hidden, not isolated-out) aircraft is visible", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3");
    expect(isAircraftVisible(aircraft.A1B2C3)).toBe(true);
  });

  it("a hidden aircraft with no Follow/protectedId exception is not visible", () => {
    const base = withOnePositionedAircraft("A1B2C3");
    const hidden = applyWsEvent(base, { type: "hide", icao_hex: "A1B2C3" });
    expect(isAircraftVisible(hidden.A1B2C3)).toBe(false);
  });

  it("a hidden aircraft that's the followId is visible (Follow-lost exception)", () => {
    const base = withOnePositionedAircraft("A1B2C3");
    const hidden = applyWsEvent(base, { type: "hide", icao_hex: "A1B2C3" });
    expect(isAircraftVisible(hidden.A1B2C3, { followId: "A1B2C3" })).toBe(true);
  });

  it("a hidden aircraft that's the protectedId is visible (selected-lost exception)", () => {
    const base = withOnePositionedAircraft("A1B2C3");
    const hidden = applyWsEvent(base, { type: "hide", icao_hex: "A1B2C3" });
    expect(isAircraftVisible(hidden.A1B2C3, { protectedId: "A1B2C3" })).toBe(true);
  });

  it("isolate is a hard filter, overriding any other aircraft's visibility regardless of hidden state", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3");
    expect(isAircraftVisible(aircraft.A1B2C3, { isolateId: "OTHER" })).toBe(false);
    expect(isAircraftVisible(aircraft.A1B2C3, { isolateId: "A1B2C3" })).toBe(true);
  });

  it("deliberately doesn't check hasPosition -- aircraftFeature/infoBoxLabelFeature must check that themselves first", () => {
    // A position-less aircraft is otherwise a normal, non-hidden aircraft --
    // isAircraftVisible alone says "visible" here; only the caller's own
    // hasPosition() check actually excludes it.
    const noPosition = applySnapshot([{ icao_hex: "A1B2C3" }]);
    expect(isAircraftVisible(noPosition.A1B2C3)).toBe(true);
  });
});

describe("buildAircraftSourceDiff (#1775)", () => {
  it("adds a changed hex that still resolves to a visible feature", () => {
    const aircraft = withOnePositionedAircraft("A1B2C3");
    const diff = buildAircraftSourceDiff(["A1B2C3"], aircraft, new Set());
    expect(diff.add).toHaveLength(1);
    expect(diff.add?.[0].id).toBe("A1B2C3");
    expect(diff.remove).toHaveLength(0);
  });

  it("removes a changed hex no longer present in the aircraft map at all", () => {
    const diff = buildAircraftSourceDiff(["GONE123"], {}, new Set());
    expect(diff.add).toHaveLength(0);
    expect(diff.remove).toEqual(["GONE123"]);
  });

  it("removes a changed hex that's present but no longer visible (hidden, no exception)", () => {
    const base = withOnePositionedAircraft("A1B2C3");
    const hidden = applyWsEvent(base, { type: "hide", icao_hex: "A1B2C3" });
    const diff = buildAircraftSourceDiff(["A1B2C3"], hidden, new Set());
    expect(diff.add).toHaveLength(0);
    expect(diff.remove).toEqual(["A1B2C3"]);
  });

  it("only touches the hexes named in changedIcaoHexes, not the whole fleet", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = { ...aircraft, ...withOnePositionedAircraft("D4E5F6") };
    const diff = buildAircraftSourceDiff(["A1B2C3"], aircraft, new Set());
    expect(diff.add).toHaveLength(1);
    expect(diff.add?.[0].id).toBe("A1B2C3");
  });
});

describe("buildTrailSourceDiff (#1775, blocked per #1838)", () => {
  it("adds every current block for a newly-touched, visible hex with no prior sync record", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    const { diff, syncState } = buildTrailSourceDiff(["A1B2C3"], aircraft, new Set(["A1B2C3"]), new Map());
    expect(diff.add?.length).toBeGreaterThan(0);
    expect(diff.remove).toHaveLength(0);
    expect(syncState.get("A1B2C3")?.ids).toEqual(diff.add?.map((f) => f.id));
    expect(syncState.get("A1B2C3")?.base).toBe(0);
  });

  it("a pure same-color append re-upserts only the last block's current run in place (#1820: run count does not grow) and removes nothing stale", () => {
    // Deliberately not a suffix-only optimization within a block (see
    // buildTrailSourceDiff's own doc comment) -- re-adding an unchanged
    // run is a harmless upsert, and recomputing fresh every touched-hex
    // tick is what keeps a reseed (tested separately below) correct
    // without needing extra state to distinguish "appended" from
    // "replaced". No `alt` on either position event -- both merge
    // forward the same 1000 value, so this stays one run the whole way
    // through, not two.
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    const first = buildTrailSourceDiff(["A1B2C3"], aircraft, new Set(["A1B2C3"]), new Map());
    expect(first.syncState.get("A1B2C3")?.ids).toEqual(["A1B2C3:0:0"]);

    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.2, lon: 2.2 });
    const second = buildTrailSourceDiff(["A1B2C3"], aircraft, new Set(["A1B2C3"]), first.syncState);

    expect(second.diff.remove).toHaveLength(0);
    expect(second.diff.add).toHaveLength(1); // same one run, re-sent with its new (longer) geometry
    expect(second.diff.add?.[0].id).toBe("A1B2C3:0:0");
    expect((second.diff.add?.[0].geometry as { coordinates: unknown[] }).coordinates).toHaveLength(3);
    expect(second.syncState.get("A1B2C3")?.ids).toEqual(["A1B2C3:0:0"]);
  });

  it("an append that changes color starts a new run alongside the still-closed prior one, in the same block", () => {
    // A segment is colored by its *earlier* point (see trailSegments.ts),
    // so a color transition only shows up on the segment leading *out of*
    // the point whose altitude changed -- i.e. one point later than where
    // the new altitude was received. p0=1000, p1=35000 sets up that
    // transition (segment0, p0->p1, colored 1000); appending p2 is what
    // actually materializes segment1 (p1->p2, colored 35000) as a new run.
    let aircraft = withOnePositionedAircraft("A1B2C3"); // alt: 1000
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1, alt: 35000 });
    const first = buildTrailSourceDiff(["A1B2C3"], aircraft, new Set(["A1B2C3"]), new Map());
    expect(first.syncState.get("A1B2C3")?.ids).toEqual(["A1B2C3:0:0"]);

    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.2, lon: 2.2 });
    const second = buildTrailSourceDiff(["A1B2C3"], aircraft, new Set(["A1B2C3"]), first.syncState);

    expect(second.diff.remove).toHaveLength(0);
    expect(second.syncState.get("A1B2C3")?.ids).toEqual(["A1B2C3:0:0", "A1B2C3:0:1"]);
  });

  it("a 64-point boundary crossing only re-sends the new block, leaving the earlier (unchanged) block untouched", () => {
    const hex = "A1B2C3";
    // TRAIL_BLOCK_SIZE+1 points (65) is exactly block 0's full [0,64]
    // range -- one point short of spilling into block 1.
    let aircraft = withTrail(hex, makeTrail(TRAIL_BLOCK_SIZE + 1));
    const first = buildTrailSourceDiff([hex], aircraft, new Set([hex]), new Map());
    const firstIds = first.syncState.get(hex)!.ids;
    expect(firstIds).toEqual([`${hex}:0:0`]);

    // One more point crosses the boundary into block 1.
    const grown = [...aircraft[hex].trail, { latitude: 9, longitude: 9, altitude: 1000 }];
    aircraft = { ...aircraft, [hex]: { ...aircraft[hex], trail: grown } };
    const second = buildTrailSourceDiff([hex], aircraft, new Set([hex]), first.syncState);

    expect(second.diff.remove).toHaveLength(0);
    expect(second.diff.add?.map((f) => f.id)).toEqual([`${hex}:1:0`]);
    expect(second.syncState.get(hex)?.ids.sort()).toEqual([`${hex}:0:0`, `${hex}:1:0`]);
  });

  it("front truncation at the cap: drops the fully-emptied leading block(s), rebuilds only the trimmed-front block and the appended tail, not every block in between", () => {
    const hex = "A1B2C3";
    const full = makeTrail(MAX_TRAIL_POINTS); // already at the cap
    let aircraft = withTrail(hex, full);
    const first = buildTrailSourceDiff([hex], aircraft, new Set([hex]), new Map());
    const firstState = first.syncState.get(hex)!;
    const totalBlocks = firstState.ids.length; // one run per block here (constant altitude)
    expect(totalBlocks).toBeGreaterThan(300);

    // Simulate a batch of pushTrailPoint calls that each appended one
    // point and trimmed one off the front (aircraftState.ts's
    // MAX_TRAIL_POINTS cap) landing in a single throttled MapView tick --
    // drop 70 points off the front (more than one block's worth), append
    // 70 new ones, net length unchanged.
    const dropped = 70;
    const trimmed = [...full.slice(dropped), ...makeTrail(dropped, 50)];
    aircraft = { ...aircraft, [hex]: { ...aircraft[hex], trail: trimmed } };

    const second = buildTrailSourceDiff([hex], aircraft, new Set([hex]), first.syncState);
    const secondState = second.syncState.get(hex)!;
    expect(secondState.base).toBe(dropped);

    const removedBlocks = (second.diff.remove ?? []).map((id) => Number(String(id).split(":")[1]));
    const addedBlocks = new Set(second.diff.add?.map((f) => Number((f.id as string).split(":")[1])) ?? []);

    // At least one whole leading block (block 0) was dropped outright...
    expect(removedBlocks).toContain(0);
    // ...only a small number of blocks were touched at all (the trimmed
    // front block plus the appended tail's block(s)), nowhere near the
    // ~391 blocks a full re-send would touch.
    expect(addedBlocks.size).toBeGreaterThan(0);
    expect(addedBlocks.size).toBeLessThan(10);
    expect(addedBlocks.size + removedBlocks.length).toBeLessThan(totalBlocks / 2);
  });

  it("a dimmed change forces a full re-send even though the trail itself didn't change", () => {
    // isFollowLost (the dimmed rule) requires both a Follow/protectedId
    // match *and* the aircraft actually being hidden -- see
    // followTarget.ts -- so the realistic transition is a *followed*
    // aircraft going hidden (dimmed false -> true) while staying included
    // throughout via the Follow-lost exception (featureCollections.ts's
    // trailIncluded).
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    const first = buildTrailSourceDiff(["A1B2C3"], aircraft, new Set(["A1B2C3"]), new Map(), { followId: "A1B2C3" });
    const previousIds = first.syncState.get("A1B2C3")!.ids;
    expect(first.syncState.get("A1B2C3")?.dimmed).toBe(false);

    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });
    const second = buildTrailSourceDiff(["A1B2C3"], aircraft, new Set(["A1B2C3"]), first.syncState, {
      followId: "A1B2C3",
    });
    expect(second.diff.remove).toEqual(previousIds);
    expect(second.diff.add?.length).toBeGreaterThan(0);
    expect(second.diff.add?.every((f) => f.properties?.dimmed === true)).toBe(true);
    expect(second.syncState.get("A1B2C3")?.dimmed).toBe(true);
  });

  it("removes every previously-synced block id for a hex that's no longer visible (losing trail visibility)", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    const first = buildTrailSourceDiff(["A1B2C3"], aircraft, new Set(["A1B2C3"]), new Map());
    const previousIds = first.syncState.get("A1B2C3")!.ids;
    expect(previousIds.length).toBeGreaterThan(0);

    // No longer in the visible-ids set (e.g. deselected while historyAll is off).
    const second = buildTrailSourceDiff(["A1B2C3"], aircraft, new Set(), first.syncState);
    expect(second.diff.remove).toEqual(previousIds);
    expect(second.diff.add).toHaveLength(0);
    expect(second.syncState.has("A1B2C3")).toBe(false);
  });

  it("a trail reseed (wholesale replace) correctly removes stale ids and adds the new set, not just a suffix", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    const first = buildTrailSourceDiff(["A1B2C3"], aircraft, new Set(["A1B2C3"]), new Map());

    // Reseed with an entirely different (here, longer) server-fetched trail --
    // same mechanism as selecting an aircraft (lib/aircraftState.ts's
    // applyTrailSeed), which replaces trail wholesale rather than appending.
    // All-null altitude -> every point shares the same (black) color, so
    // this is one run of 4 coordinates, not one run per point-pair.
    aircraft = applyTrailSeed(aircraft, "A1B2C3", [
      { lat: 9, lon: 9, alt: null },
      { lat: 9.1, lon: 9.1, alt: null },
      { lat: 9.2, lon: 9.2, alt: null },
      { lat: 9.3, lon: 9.3, alt: null },
    ]);
    const second = buildTrailSourceDiff(["A1B2C3"], aircraft, new Set(["A1B2C3"]), first.syncState);

    // The reseeded trail's run must be (re-)added with its new geometry --
    // not silently skipped because the id count happened to shrink, which
    // would leave stale coordinates on screen.
    expect(second.diff.add?.length).toBe(1);
    const [feature] = second.diff.add ?? [];
    const coords = (feature.geometry as { coordinates: number[][] }).coordinates;
    expect(coords).toHaveLength(4);
    for (const coord of coords) {
      expect(coord[0]).toBeGreaterThan(8);
    }
    expect(second.syncState.get("A1B2C3")?.base).toBe(0);
  });

  it("only touches the hexes named in changedIcaoHexes", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    aircraft = { ...aircraft, ...withOnePositionedAircraft("D4E5F6") };
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "D4E5F6", lat: 5.1, lon: 6.1 });

    const { diff } = buildTrailSourceDiff(["A1B2C3"], aircraft, new Set(["A1B2C3", "D4E5F6"]), new Map());
    expect(diff.add?.every((f) => f.properties?.icao_hex === "A1B2C3")).toBe(true);
  });

  // #1838 acceptance criteria: the setData (full-rebuild) path and the
  // updateData (incremental diff) path must produce identical feature
  // sets for the same state -- otherwise the two could silently drift
  // apart on the actual block-splitting/run-grouping rules.
  it("the setData full-rebuild path and the incremental diff path produce identical feature sets for the same state", () => {
    const hex = "A1B2C3";
    const trail = makeTrail(200); // spans several blocks
    const aircraft = withTrail(hex, trail);

    const viaDiff = buildTrailSourceDiff([hex], aircraft, new Set([hex]), new Map());
    const viaFullRebuild = trailFeatureCollection(aircraft, new Set([hex]));

    const sortById = (fs: readonly { id?: string | number | null }[] | undefined) =>
      [...(fs ?? [])].sort((a, b) => String(a.id).localeCompare(String(b.id)));

    expect(sortById(viaDiff.diff.add)).toEqual(sortById(viaFullRebuild.features));
  });
});

describe("isEmptySourceDiff", () => {
  it("is true only when there is nothing to add, remove, update, or clear", () => {
    expect(isEmptySourceDiff({})).toBe(true);
    expect(isEmptySourceDiff({ add: [], remove: [] })).toBe(true);
    expect(isEmptySourceDiff({ remove: ["A1B2C3"] })).toBe(false);
    expect(isEmptySourceDiff({ removeAll: true })).toBe(false);
    expect(isEmptySourceDiff({ add: [{ type: "Feature", id: "x", geometry: { type: "Point", coordinates: [0, 0] }, properties: {} }] })).toBe(false);
  });

  it("is what an untouched-trail tick produces with no trails visible", () => {
    let aircraft = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2, alt: 1000 }]);
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    const { diff } = buildTrailSourceDiff(["A1B2C3"], aircraft, new Set(), new Map());
    expect(isEmptySourceDiff(diff)).toBe(true);
  });
});
