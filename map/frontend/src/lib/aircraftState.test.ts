import { describe, expect, it } from "vitest";
import type { MapFlight, MapWsEvent } from "../api/types";
import {
  applySnapshot,
  applyTrailSeed,
  applyWsEvent,
  applyWsEvents,
  MAX_TRAIL_POINTS,
  releasePendingRemoval,
} from "./aircraftState";

describe("applySnapshot", () => {
  it("seeds a trail point from each aircraft's current position, when known", () => {
    const snapshot: MapFlight[] = [{ icao_hex: "A1B2C3", lat: 33.94, lon: -118.4, alt: 1000 }];
    const state = applySnapshot(snapshot);
    expect(state.A1B2C3.trail).toEqual([{ latitude: 33.94, longitude: -118.4, altitude: 1000 }]);
    expect(state.A1B2C3.stale).toBe(false);
  });

  it("leaves the trail empty when position is unknown", () => {
    const snapshot: MapFlight[] = [{ icao_hex: "A1B2C3", ident: "DAL659" }];
    const state = applySnapshot(snapshot);
    expect(state.A1B2C3.trail).toEqual([]);
  });
});

describe("applyWsEvent -- position/metadata merge", () => {
  it("merges a position event onto an existing aircraft without clobbering unrelated fields", () => {
    const base = applySnapshot([{ icao_hex: "A1B2C3", ident: "DAL659", alt: 1000 }]);
    const event: MapWsEvent = { type: "position", icao_hex: "A1B2C3", hdg: 270 };
    const next = applyWsEvent(base, event);
    expect(next.A1B2C3.ident).toBe("DAL659"); // untouched
    expect(next.A1B2C3.alt).toBe(1000); // untouched
    expect(next.A1B2C3.hdg).toBe(270); // newly applied
  });

  it("creates a brand-new aircraft record from a position event alone", () => {
    const next = applyWsEvent({}, { type: "position", icao_hex: "A1B2C3", lat: 1, lon: 2 });
    expect(next.A1B2C3).toBeDefined();
    expect(next.A1B2C3.lat).toBe(1);
    expect(next.A1B2C3.stale).toBe(false);
  });

  it("appends a trail point on a position event carrying a new lat/lon", () => {
    const base = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2, alt: 1000 }]);
    const next = applyWsEvent(base, {
      type: "position",
      icao_hex: "A1B2C3",
      lat: 1.1,
      lon: 2.1,
      alt: 1500,
    });
    expect(next.A1B2C3.trail).toHaveLength(2);
    expect(next.A1B2C3.trail[1]).toEqual({ latitude: 1.1, longitude: 2.1, altitude: 1500 });
  });

  it("does not duplicate a trail point when the position hasn't actually changed", () => {
    const base = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }]);
    const next = applyWsEvent(base, { type: "position", icao_hex: "A1B2C3", lat: 1, lon: 2, hdg: 90 });
    expect(next.A1B2C3.trail).toHaveLength(1);
  });

  it("does not append a trail point for a metadata event (client trail is position-driven only)", () => {
    const base = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }]);
    const next = applyWsEvent(base, {
      type: "metadata",
      icao_hex: "A1B2C3",
      lat: 5,
      lon: 6,
      ident: "DAL659",
    });
    expect(next.A1B2C3.trail).toHaveLength(1); // unchanged from the snapshot seed
    expect(next.A1B2C3.ident).toBe("DAL659");
    expect(next.A1B2C3.lat).toBe(5); // metadata's own position fields still applied to current-state
  });

  it("un-fades (clears stale) on any live position/metadata update", () => {
    const base = applySnapshot([{ icao_hex: "A1B2C3" }]);
    const staled = applyWsEvent(base, { type: "stale", icao_hex: "A1B2C3" });
    expect(staled.A1B2C3.stale).toBe(true);
    const revived = applyWsEvent(staled, { type: "position", icao_hex: "A1B2C3", hdg: 10 });
    expect(revived.A1B2C3.stale).toBe(false);
  });

  it("un-hides (clears hidden) on any live position/metadata update", () => {
    const base = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }]);
    const hidden = applyWsEvent(base, { type: "hide", icao_hex: "A1B2C3" });
    expect(hidden.A1B2C3.hidden).toBe(true);
    const revived = applyWsEvent(hidden, { type: "position", icao_hex: "A1B2C3", lat: 1.5, lon: 2.5 });
    expect(revived.A1B2C3.hidden).toBe(false);
    // The pre-gap trail point survives the hide/reveal round trip.
    expect(revived.A1B2C3.trail[0]).toEqual({ latitude: 1, longitude: 2, altitude: null });
    expect(revived.A1B2C3.trail).toHaveLength(2);
  });
});

describe("applyWsEvent -- hide", () => {
  it("marks an existing aircraft hidden without deleting it or its trail", () => {
    const base = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }]);
    const next = applyWsEvent(base, { type: "hide", icao_hex: "A1B2C3" });
    expect(next.A1B2C3).toBeDefined();
    expect(next.A1B2C3.hidden).toBe(true);
    expect(next.A1B2C3.trail).toEqual(base.A1B2C3.trail);
  });

  it("is a no-op (same reference) for a hide event on an untracked aircraft", () => {
    const base = applySnapshot([]);
    const next = applyWsEvent(base, { type: "hide", icao_hex: "UNKNOWN" });
    expect(next).toBe(base);
  });

  it("is a no-op (same reference) when the aircraft is already hidden", () => {
    const base = applyWsEvent(applySnapshot([{ icao_hex: "A1B2C3" }]), { type: "hide", icao_hex: "A1B2C3" });
    const next = applyWsEvent(base, { type: "hide", icao_hex: "A1B2C3" });
    expect(next).toBe(base);
  });
});

describe("applyWsEvent -- stale/remove", () => {
  it("marks an existing aircraft stale", () => {
    const base = applySnapshot([{ icao_hex: "A1B2C3" }]);
    const next = applyWsEvent(base, { type: "stale", icao_hex: "A1B2C3" });
    expect(next.A1B2C3.stale).toBe(true);
  });

  it("is a no-op (same reference) for a stale event on an untracked aircraft", () => {
    const base = applySnapshot([]);
    const next = applyWsEvent(base, { type: "stale", icao_hex: "UNKNOWN" });
    expect(next).toBe(base);
  });

  it("removes an aircraft entirely on a remove event", () => {
    const base = applySnapshot([{ icao_hex: "A1B2C3" }]);
    const next = applyWsEvent(base, { type: "remove", icao_hex: "A1B2C3" });
    expect(next.A1B2C3).toBeUndefined();
    expect("A1B2C3" in next).toBe(false);
  });

  it("is a no-op (same reference) for a remove event on an untracked aircraft", () => {
    const base = applySnapshot([]);
    const next = applyWsEvent(base, { type: "remove", icao_hex: "UNKNOWN" });
    expect(next).toBe(base);
  });
});

describe("trail cap", () => {
  it("drops the oldest points once the cap is exceeded, keeping the newest", () => {
    let state = applySnapshot([{ icao_hex: "A1B2C3", lat: 0, lon: 0, alt: 0 }]);
    const overflow = 10;
    for (let i = 1; i <= MAX_TRAIL_POINTS + overflow; i++) {
      state = applyWsEvent(state, { type: "position", icao_hex: "A1B2C3", lat: i, lon: i, alt: i });
    }
    const trail = state.A1B2C3.trail;
    expect(trail).toHaveLength(MAX_TRAIL_POINTS);
    // The newest point (the last one pushed) survives...
    expect(trail[trail.length - 1].latitude).toBe(MAX_TRAIL_POINTS + overflow);
    // ...and the oldest `overflow` points (including the seed point at 0) were dropped.
    expect(trail[0].latitude).toBe(overflow + 1);
  });

  it("never exceeds the cap even across many more pushes than the cap", () => {
    let state = applySnapshot([{ icao_hex: "A1B2C3", lat: 0, lon: 0 }]);
    for (let i = 1; i <= MAX_TRAIL_POINTS * 3; i++) {
      state = applyWsEvent(state, { type: "position", icao_hex: "A1B2C3", lat: i, lon: i });
    }
    expect(state.A1B2C3.trail.length).toBe(MAX_TRAIL_POINTS);
  });
});

describe("applyTrailSeed", () => {
  it("replaces the client trail with the converted server trail", () => {
    let state = applySnapshot([{ icao_hex: "A1B2C3", lat: 9, lon: 9, alt: 500 }]);
    state = applyWsEvent(state, { type: "position", icao_hex: "A1B2C3", lat: 9.1, lon: 9.1 });
    expect(state.A1B2C3.trail.length).toBe(2);

    const next = applyTrailSeed(state, "A1B2C3", [
      { lat: 1, lon: 1, alt: 100 },
      { lat: 2, lon: 2, alt: null },
      { lat: 3, lon: 3, alt: 300 },
    ]);

    expect(next.A1B2C3.trail).toEqual([
      { latitude: 1, longitude: 1, altitude: 100 },
      { latitude: 2, longitude: 2, altitude: null },
      { latitude: 3, longitude: 3, altitude: 300 },
    ]);
  });

  it("caps the seeded trail to MAX_TRAIL_POINTS, keeping the newest", () => {
    const state = applySnapshot([{ icao_hex: "A1B2C3", lat: 0, lon: 0 }]);
    const wireTrail = Array.from({ length: MAX_TRAIL_POINTS + 25 }, (_, i) => ({
      lat: i,
      lon: i,
      alt: null,
    }));

    const next = applyTrailSeed(state, "A1B2C3", wireTrail);

    expect(next.A1B2C3.trail).toHaveLength(MAX_TRAIL_POINTS);
    expect(next.A1B2C3.trail[next.A1B2C3.trail.length - 1].latitude).toBe(MAX_TRAIL_POINTS + 24);
  });

  it("is a no-op for an aircraft not currently in state", () => {
    const state = applySnapshot([{ icao_hex: "A1B2C3" }]);
    const next = applyTrailSeed(state, "UNKNOWN", [{ lat: 1, lon: 1, alt: null }]);
    expect(next).toBe(state);
  });

  it("is a no-op for an empty server trail", () => {
    const state = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }]);
    const next = applyTrailSeed(state, "A1B2C3", []);
    expect(next).toBe(state);
  });

  it("leaves other fields on the record untouched", () => {
    const state = applySnapshot([{ icao_hex: "A1B2C3", lat: 5, lon: 5, ident: "DAL1" }]);
    const next = applyTrailSeed(state, "A1B2C3", [{ lat: 1, lon: 1, alt: null }]);
    expect(next.A1B2C3.ident).toBe("DAL1");
    expect(next.A1B2C3.lat).toBe(5);
  });
});

describe("applyWsEvents", () => {
  it("applies a batch of events in order", () => {
    const base = applySnapshot([]);
    const events: MapWsEvent[] = [
      { type: "position", icao_hex: "A1B2C3", lat: 1, lon: 2 },
      { type: "metadata", icao_hex: "A1B2C3", ident: "DAL659" },
      { type: "stale", icao_hex: "A1B2C3" },
    ];
    const next = applyWsEvents(base, events);
    expect(next.A1B2C3.ident).toBe("DAL659");
    expect(next.A1B2C3.lat).toBe(1);
    expect(next.A1B2C3.stale).toBe(true);
  });

  it("a remove later in the same batch wins over an earlier position in that batch", () => {
    const base = applySnapshot([]);
    const events: MapWsEvent[] = [
      { type: "position", icao_hex: "A1B2C3", lat: 1, lon: 2 },
      { type: "remove", icao_hex: "A1B2C3" },
    ];
    const next = applyWsEvents(base, events);
    expect("A1B2C3" in next).toBe(false);
  });
});

describe("icon shape resolution", () => {
  it("resolves the shape from the snapshot's aircraft enrichment", () => {
    const state = applySnapshot([
      { icao_hex: "A1B2C3", lat: 1, lon: 2, aircraft: { icao_hex: "A1B2C3", type_designator: "B738" } },
    ]);
    expect(state.A1B2C3.shape).toBe("B738");
    expect(state.A1B2C3.iconScale).toBeGreaterThan(0);
  });

  it("defaults to the fallback shape before any enrichment arrives", () => {
    const state = applyWsEvent({}, { type: "position", icao_hex: "A1B2C3", lat: 1, lon: 2 });
    expect(state.A1B2C3.shape).toBe("UNIDENTIFIED");
  });

  it("re-resolves on a metadata event and then persists through position events", () => {
    let state = applyWsEvent({}, { type: "position", icao_hex: "A1B2C3", lat: 1, lon: 2 });
    expect(state.A1B2C3.shape).toBe("UNIDENTIFIED");

    state = applyWsEvent(state, {
      type: "metadata",
      icao_hex: "A1B2C3",
      aircraft: { icao_hex: "A1B2C3", type_designator: "A320" },
    });
    expect(state.A1B2C3.shape).toBe("A320");
    const scaleAfterMetadata = state.A1B2C3.iconScale;

    // A later position event carries no `aircraft` -- shape must not change.
    state = applyWsEvent(state, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    expect(state.A1B2C3.shape).toBe("A320");
    expect(state.A1B2C3.iconScale).toBe(scaleAfterMetadata);
  });

  it("resolves the shape from the emitter category alone when there is no enrichment", () => {
    const state = applyWsEvent({}, {
      type: "metadata",
      icao_hex: "A1B2C3",
      aircraft: { icao_hex: "A1B2C3", emitter_category: "A7" },
    });
    expect(state.A1B2C3.shape).toBe("H60");
  });
});

describe("eviction deferral -- ApplyWsEventsOptions.protectedIcaoHex / releasePendingRemoval", () => {
  it("removes an aircraft normally when it isn't the protected one", () => {
    const base = applySnapshot([{ icao_hex: "A1B2C3" }]);
    const next = applyWsEvent(base, { type: "remove", icao_hex: "A1B2C3" }, { protectedIcaoHex: "OTHER" });
    expect("A1B2C3" in next).toBe(false);
  });

  it("defers (does not delete) a remove for the protected aircraft, flagging pendingRemoval instead", () => {
    const base = applySnapshot([{ icao_hex: "A1B2C3" }]);
    const next = applyWsEvent(base, { type: "remove", icao_hex: "A1B2C3" }, { protectedIcaoHex: "A1B2C3" });
    expect(next.A1B2C3).toBeDefined();
    expect(next.A1B2C3.pendingRemoval).toBe(true);
  });

  it("is idempotent -- a second deferred remove for the same aircraft is a no-op (same reference)", () => {
    const base = applyWsEvent(applySnapshot([{ icao_hex: "A1B2C3" }]), { type: "remove", icao_hex: "A1B2C3" }, {
      protectedIcaoHex: "A1B2C3",
    });
    const next = applyWsEvent(base, { type: "remove", icao_hex: "A1B2C3" }, { protectedIcaoHex: "A1B2C3" });
    expect(next).toBe(base);
  });

  it("a later live update cancels a deferred removal (contact resumed before the panel closed)", () => {
    let state = applySnapshot([{ icao_hex: "A1B2C3" }]);
    state = applyWsEvent(state, { type: "remove", icao_hex: "A1B2C3" }, { protectedIcaoHex: "A1B2C3" });
    expect(state.A1B2C3.pendingRemoval).toBe(true);
    state = applyWsEvent(state, { type: "position", icao_hex: "A1B2C3", lat: 1, lon: 2 }, { protectedIcaoHex: "A1B2C3" });
    expect(state.A1B2C3.pendingRemoval).toBe(false);
  });

  it("applyWsEvents threads protectedIcaoHex through every event in the batch", () => {
    const base = applySnapshot([{ icao_hex: "A1B2C3" }]);
    const events: MapWsEvent[] = [{ type: "remove", icao_hex: "A1B2C3" }];
    const next = applyWsEvents(base, events, { protectedIcaoHex: "A1B2C3" });
    expect(next.A1B2C3.pendingRemoval).toBe(true);
  });

  it("releasePendingRemoval applies a deferred removal", () => {
    const base = applyWsEvent(applySnapshot([{ icao_hex: "A1B2C3" }]), { type: "remove", icao_hex: "A1B2C3" }, {
      protectedIcaoHex: "A1B2C3",
    });
    const next = releasePendingRemoval(base, "A1B2C3");
    expect("A1B2C3" in next).toBe(false);
  });

  it("releasePendingRemoval is a no-op (same reference) when nothing was deferred", () => {
    const base = applySnapshot([{ icao_hex: "A1B2C3" }]);
    const next = releasePendingRemoval(base, "A1B2C3");
    expect(next).toBe(base);
  });

  it("releasePendingRemoval is a no-op (same reference) for an untracked aircraft", () => {
    const base = applySnapshot([]);
    const next = releasePendingRemoval(base, "UNKNOWN");
    expect(next).toBe(base);
  });
});

describe("Trace Points sample accumulation (AircraftRecord.tracePoints)", () => {
  const now = 1785499200000; // 2026-07-31T12:00:00Z

  it("seeds a trace point from the snapshot's current position/velocity, when known", () => {
    const state = applySnapshot([{ icao_hex: "A1B2C3", lat: 33.94, lon: -118.4, alt: 1000.4, velocity: 415.6 }], now);
    expect(state.A1B2C3.tracePoints).toEqual([
      { latitude: 33.94, longitude: -118.4, altitude: 1000, velocity: 416, epochSeconds: now / 1000 },
    ]);
  });

  it("leaves tracePoints empty when position is unknown", () => {
    const state = applySnapshot([{ icao_hex: "A1B2C3", ident: "DAL659" }], now);
    expect(state.A1B2C3.tracePoints).toEqual([]);
  });

  it("appends a trace point on a position event, not on a metadata event", () => {
    let state = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }], now);
    state = applyWsEvent(state, { type: "metadata", icao_hex: "A1B2C3", lat: 5, lon: 6, ident: "DAL1" }, { now });
    expect(state.A1B2C3.tracePoints).toHaveLength(1); // unchanged

    state = applyWsEvent(state, { type: "position", icao_hex: "A1B2C3", lat: 5.1, lon: 6.1, velocity: 300 }, { now });
    expect(state.A1B2C3.tracePoints).toHaveLength(2);
    expect(state.A1B2C3.tracePoints[1]).toEqual({
      latitude: 5.1,
      longitude: 6.1,
      altitude: null,
      velocity: 300,
      epochSeconds: now / 1000,
    });
  });

  it("does not duplicate a trace point when the position hasn't actually changed", () => {
    let state = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }], now);
    state = applyWsEvent(state, { type: "position", icao_hex: "A1B2C3", lat: 1, lon: 2, velocity: 300 }, { now });
    expect(state.A1B2C3.tracePoints).toHaveLength(1);
  });

  it("caps accumulated trace points at MAX_TRAIL_POINTS, keeping the newest", () => {
    let state = applySnapshot([{ icao_hex: "A1B2C3", lat: 0, lon: 0 }], now);
    for (let i = 1; i <= MAX_TRAIL_POINTS + 10; i++) {
      state = applyWsEvent(state, { type: "position", icao_hex: "A1B2C3", lat: i, lon: i }, { now });
    }
    expect(state.A1B2C3.tracePoints).toHaveLength(MAX_TRAIL_POINTS);
    expect(state.A1B2C3.tracePoints[state.A1B2C3.tracePoints.length - 1].latitude).toBe(MAX_TRAIL_POINTS + 10);
  });
});
