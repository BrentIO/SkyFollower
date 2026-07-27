import * as maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import { TerraDraw, TerraDrawPolygonMode, TerraDrawSelectMode } from "terra-draw";
import { TerraDrawMapLibreGLAdapter } from "terra-draw-maplibre-gl-adapter";
import { useEffect, useRef, useState } from "react";
import { AreaNameModal } from "../components/AreaNameModal";
import { ConfirmModal } from "../components/ConfirmModal";
import { createArea, deleteArea, listAreas, updateArea, type Area } from "../api/areas";
import { ApiError } from "../api/client";
import { useToast } from "../hooks/useToast";

// maplibre-gl ships its worker as a separate file (maplibre-gl-worker.mjs)
// with a hardcoded relative import of a second file
// (maplibre-gl-shared.mjs) -- neither is something Vite/Rollup can
// discover and bundle on its own (the worker is only ever loaded at
// runtime via a URL, and its own internal import is resolved by the
// browser, not by our build). vite.config.ts's maplibreWorkerAssets
// plugin copies both files, verbatim and under these exact names, to
// /assets/ in both dev and build, which is what this path points at.
// Without this, the map silently never fires its `load` event and
// everything gated on that (area list, name labels) hangs forever.
maplibregl.setWorkerUrl("/assets/maplibre-gl-worker.mjs");

// "positron" (CARTO's well-known light/grayscale basemap design, served
// here by the same OpenFreeMap provider as the rest of this file -- no
// new third-party domain) instead of "liberty"'s full-color style, per
// request. Verify this URL is still live if the map ever shows a
// blank/broken basemap.
const MAP_STYLE = "https://tiles.openfreemap.org/styles/positron";

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value));
}

// Terra Draw's addFeatures() silently drops (doesn't throw for) any
// feature that fails its mode's validation -- e.g. excessive coordinate
// precision, self-intersection -- so a "temporary" feature id we just
// added isn't guaranteed to actually be in the store. removeFeatures()
// throws for an unknown id, so every cleanup call needs this check first
// rather than assuming the add succeeded.
function removeFeatureIfPresent(draw: TerraDraw, id: string): void {
  if (draw.getSnapshotFeature(id)) draw.removeFeatures([id]);
}

function computeBounds(areas: Area[]): maplibregl.LngLatBoundsLike | null {
  let minLng = Infinity;
  let minLat = Infinity;
  let maxLng = -Infinity;
  let maxLat = -Infinity;
  let found = false;
  for (const area of areas) {
    for (const ring of area.geometry.coordinates) {
      for (const [lng, lat] of ring) {
        found = true;
        if (lng < minLng) minLng = lng;
        if (lng > maxLng) maxLng = lng;
        if (lat < minLat) minLat = lat;
        if (lat > maxLat) maxLat = lat;
      }
    }
  }
  return found ? [[minLng, minLat], [maxLng, maxLat]] : null;
}

// Plain average of the outer ring's points -- good enough for label
// placement, not a claim of geometric precision (no turf dependency just
// for this).
function computeCentroid(geometry: Area["geometry"]): [number, number] {
  const ring = geometry.coordinates[0] ?? [];
  if (ring.length === 0) return [0, 0];
  let sumLng = 0;
  let sumLat = 0;
  for (const [lng, lat] of ring) {
    sumLng += lng;
    sumLat += lat;
  }
  return [sumLng / ring.length, sumLat / ring.length];
}

// Floor offset for degenerate (near-zero-extent) shapes, in degrees --
// keeps a duplicate visibly distinct even for a tiny polygon, without
// this dominating the offset of a large one.
const MIN_OFFSET_DEGREES = 0.0008;

// Terra Draw's default coordinatePrecision is 9 decimal places; it
// silently rejects (not throws -- see offsetGeometry below) any feature
// with a coordinate needing more than that many digits to round-trip
// exactly. Round well under that ceiling so this never collides with it.
const OFFSET_COORDINATE_PRECISION = 7;

function roundCoordinate(value: number): number {
  const factor = 10 ** OFFSET_COORDINATE_PRECISION;
  return Math.round(value * factor) / factor;
}

// Offsets every coordinate by a fixed fraction of the shape's own
// bounding-box size (floored so it's still visible for a small polygon),
// so a duplicate lands overlapping-but-distinguishable from its source
// and is immediately grabbable rather than sitting exactly on top of it.
//
// Coordinates are rounded after offsetting -- floating-point addition
// routinely produces a result needing 14+ decimal digits (e.g.
// -81.28992976386266 + 0.003 === -81.28692976386266, a 14-digit tail)
// even though both inputs individually satisfy Terra Draw's precision
// limit. Terra Draw's addFeatures() validates each feature against its
// mode's rules (including this precision check) and, on failure, simply
// drops it from the store without throwing -- so an un-rounded offset
// here doesn't error, it just silently produces a duplicate that was
// never actually added, which then crashes the *next* step (removing
// the "temporary" feature that was never really there).
function offsetGeometry(geometry: Area["geometry"]): Area["geometry"] {
  let minLng = Infinity;
  let minLat = Infinity;
  let maxLng = -Infinity;
  let maxLat = -Infinity;
  for (const ring of geometry.coordinates) {
    for (const [lng, lat] of ring) {
      if (lng < minLng) minLng = lng;
      if (lng > maxLng) maxLng = lng;
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
    }
  }
  const dLng = Math.max((maxLng - minLng) * 0.15, MIN_OFFSET_DEGREES);
  const dLat = Math.max((maxLat - minLat) * 0.15, MIN_OFFSET_DEGREES);
  return {
    ...geometry,
    coordinates: geometry.coordinates.map((ring) =>
      ring.map(([lng, lat]) => [roundCoordinate(lng + dLng), roundCoordinate(lat + dLat)]),
    ),
  };
}

function labelsFeatureCollection(areas: Area[]): GeoJSON.FeatureCollection {
  return {
    type: "FeatureCollection",
    features: areas.map((area) => ({
      type: "Feature",
      properties: { name: area.name || area.identifier },
      geometry: { type: "Point", coordinates: computeCentroid(area.geometry) },
    })),
  };
}

function DeleteAreaMessage({ area }: { area: Area }) {
  const idCode = (
    <code className="rounded bg-slate-100 px-1 py-0.5 font-mono text-[0.85em] dark:bg-slate-800">
      {area.identifier}
    </code>
  );
  return area.name ? (
    <>
      This will permanently delete '{area.name}' ({idCode}).
    </>
  ) : (
    <>This will permanently delete {idCode}.</>
  );
}

export function AreasView() {
  const { showToast } = useToast();
  const mapContainerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const drawRef = useRef<TerraDraw | null>(null);

  const [areas, setAreas] = useState<Area[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [mapReady, setMapReady] = useState(false);

  const [original, setOriginal] = useState<Area | null>(null);
  const [draft, setDraft] = useState<Area | null>(null);

  const [pendingSwitch, setPendingSwitch] = useState<(() => void) | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<Area | null>(null);
  const [pendingDrawFeatureId, setPendingDrawFeatureId] = useState<string | null>(null);
  // Only set by duplicateArea(), to suggest "<name> copy" in the naming
  // modal -- a freshly drawn shape leaves this null and the modal starts
  // blank as before.
  const [pendingNameSuggestion, setPendingNameSuggestion] = useState<string | null>(null);

  const dirty = draft !== null && original !== null && JSON.stringify(draft) !== JSON.stringify(original);

  function requestSwitch(action: () => void) {
    if (dirty) {
      setPendingSwitch(() => action);
    } else {
      action();
    }
  }

  // Terra Draw's event listeners are registered exactly once, when the
  // map's 'load' event fires (see the mount effect below), so they'd
  // otherwise close over that first render's state forever. Each handler
  // here is redefined every render and stashed in a ref; the one-time
  // listener always calls through `<name>Ref.current(...)`, so it always
  // sees the current render's state without needing to be re-registered.
  const handleDrawFinishRef = useRef((featureId: string) => {
    setPendingNameSuggestion(null);
    setPendingDrawFeatureId(featureId);
  });
  handleDrawFinishRef.current = (featureId: string) => {
    setPendingNameSuggestion(null);
    setPendingDrawFeatureId(featureId);
  };

  const handleDrawChangeRef = useRef((ids: string[]) => {
    if (!draft || !ids.includes(draft.identifier)) return;
    const feature = drawRef.current?.getSnapshotFeature(draft.identifier);
    if (!feature || feature.geometry.type !== "Polygon") return;
    setDraft({ ...draft, geometry: feature.geometry as Area["geometry"] });
  });
  handleDrawChangeRef.current = (ids: string[]) => {
    if (!draft || !ids.includes(draft.identifier)) return;
    const feature = drawRef.current?.getSnapshotFeature(draft.identifier);
    if (!feature || feature.geometry.type !== "Polygon") return;
    setDraft({ ...draft, geometry: feature.geometry as Area["geometry"] });
  };

  const handleDrawSelectRef = useRef((featureId: string) => {
    if (featureId === pendingDrawFeatureId) return;
    if (draft?.identifier === featureId) return;
    const match = areas.find((a) => a.identifier === featureId);
    if (!match) return;
    requestSwitch(() => {
      setDraft(clone(match));
      setOriginal(clone(match));
    });
  });
  handleDrawSelectRef.current = (featureId: string) => {
    if (featureId === pendingDrawFeatureId) return;
    if (draft?.identifier === featureId) return;
    const match = areas.find((a) => a.identifier === featureId);
    if (!match) return;
    requestSwitch(() => {
      setDraft(clone(match));
      setOriginal(clone(match));
    });
  };

  useEffect(() => {
    if (!mapContainerRef.current || mapRef.current) return;

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: MAP_STYLE,
      center: [0, 0],
      zoom: 1,
      // Locked to a flat, north-up 2D view -- areas are drawn/edited as
      // plain lat/lng polygons, so neither a 3D tilt (pitch) nor a
      // rotated (non-north-up) compass bearing makes drawing easier, only
      // more disorienting. maxPitch: 0 is the hard guarantee against
      // pitch (no gesture path can exceed it); the rest, here and via the
      // two disableRotation() calls below, block every gesture path that
      // could change pitch or bearing (mouse drag, touch, keyboard) at
      // the source, rather than just the primary (mouse drag) one.
      maxPitch: 0,
      pitchWithRotate: false,
      dragRotate: false,
      touchPitch: false,
    });
    mapRef.current = map;
    map.touchZoomRotate.disableRotation(); // keep pinch-zoom, drop two-finger twist-to-rotate
    map.keyboard.disableRotation(); // keep pan/zoom shortcuts, drop Shift+Left/Right rotate
    // showCompass: false -- rotation is locked (see the constructor
    // options above and the two disableRotation() calls), so a
    // reset-bearing compass button has nothing to do.
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");

    map.on("load", () => {
      map.addSource("area-labels", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      map.addLayer({
        id: "area-labels",
        type: "symbol",
        source: "area-labels",
        layout: {
          "text-field": ["get", "name"],
          // Without an explicit text-font, MapLibre falls back to the
          // style-spec default ("Open Sans Regular, Arial Unicode MS
          // Regular"), which OpenFreeMap's glyph server doesn't serve for
          // this style (only the Noto Sans family) -- causing a 404 per
          // glyph range for every rendered character.
          "text-font": ["Noto Sans Regular"],
          "text-size": 12,
          "text-anchor": "center",
        },
        paint: { "text-color": "#0f172a", "text-halo-color": "#ffffff", "text-halo-width": 1.5 },
      });

      // Terra Draw's MapLibre adapter must be created after the map's
      // style has loaded (per its own adapter guide), so the whole
      // instance is built here rather than immediately after the Map
      // itself.
      const draw = new TerraDraw({
        adapter: new TerraDrawMapLibreGLAdapter({ map }),
        // Terra Draw's default id strategy only accepts 36-character
        // (UUID-shaped) ids, which area identifiers like "LI" aren't --
        // this lets an area's identifier double as its Terra Draw
        // feature id directly, the same 1:1 mapping used before
        // switching drawing libraries, rather than maintaining a
        // separate id-translation table.
        idStrategy: {
          isValidId: (id): id is string => typeof id === "string" && id.length > 0,
          getId: () => crypto.randomUUID(),
        },
        modes: [
          new TerraDrawSelectMode({
            flags: {
              polygon: {
                feature: {
                  draggable: true,
                  coordinates: { midpoints: true, draggable: true, deletable: true },
                },
              },
            },
          }),
          new TerraDrawPolygonMode(),
        ],
      });
      drawRef.current = draw;
      draw.start();
      draw.setMode("select");

      draw.on("finish", (id, context) => {
        // "finish" also fires for completed drags in select mode
        // (dragFeature/dragCoordinate/dragCoordinateResize) -- only a
        // brand new polygon (action "draw") should prompt for a name.
        if (context.action === "draw") handleDrawFinishRef.current(String(id));
      });
      draw.on("change", (ids, type) => {
        if (type === "update") handleDrawChangeRef.current(ids.map(String));
      });
      draw.on("select", (id) => handleDrawSelectRef.current(String(id)));

      setMapReady(true);
    });

    return () => {
      drawRef.current?.stop();
      map.remove();
      mapRef.current = null;
      drawRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!mapReady) return;
    let cancelled = false;
    async function load() {
      try {
        const loaded = await listAreas();
        if (cancelled) return;
        setAreas(loaded);
        const draw = drawRef.current;
        if (draw && loaded.length > 0) {
          draw.addFeatures(
            loaded.map((area) => ({
              id: area.identifier,
              type: "Feature" as const,
              properties: { mode: "polygon", name: area.name },
              geometry: area.geometry,
            })),
          );
        }
        const bounds = computeBounds(loaded);
        if (bounds) mapRef.current?.fitBounds(bounds, { padding: 40, animate: false });
      } catch (err) {
        if (!cancelled) showToast("error", err instanceof Error ? err.message : "Failed to load areas");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, [mapReady, showToast]);

  // Keeps the label source (committed areas only, not in-progress edits) in
  // sync whenever the saved area list changes.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapReady) return;
    const source = map.getSource("area-labels") as maplibregl.GeoJSONSource | undefined;
    source?.setData(labelsFeatureCollection(areas));
  }, [areas, mapReady]);

  function selectArea(area: Area) {
    requestSwitch(() => {
      setDraft(clone(area));
      setOriginal(clone(area));
      drawRef.current?.selectFeature(area.identifier);
    });
  }

  function startDrawing() {
    requestSwitch(() => {
      setDraft(null);
      setOriginal(null);
      drawRef.current?.setMode("polygon");
    });
  }

  // Duplicates the currently selected (and possibly in-progress-edited)
  // shape: clones its on-map geometry with a visible offset, then reuses
  // the exact same naming-modal -> create-area flow as a freshly drawn
  // polygon (handleNameConfirm doesn't care how the pending feature got
  // onto the map).
  function duplicateArea() {
    if (!draft) return;
    const sourceGeometry = draft.geometry;
    const sourceName = draft.name;
    requestSwitch(() => {
      const draw = drawRef.current;
      if (!draw) return;
      const tempId = crypto.randomUUID();
      draw.addFeatures([
        {
          id: tempId,
          type: "Feature",
          properties: { mode: "polygon", name: sourceName },
          geometry: offsetGeometry(sourceGeometry),
        },
      ]);
      setDraft(null);
      setOriginal(null);
      setPendingNameSuggestion(sourceName ? `${sourceName} copy` : "");
      setPendingDrawFeatureId(tempId);
    });
  }

  async function handleNameConfirm(identifier: string, name: string) {
    const draw = drawRef.current;
    const tempId = pendingDrawFeatureId;
    setPendingDrawFeatureId(null);
    setPendingNameSuggestion(null);
    if (!draw || !tempId) return;

    const feature = draw.getSnapshotFeature(tempId);
    if (!feature) {
      // Never actually made it into the store -- addFeatures() rejected
      // it during validation (see offsetGeometry/removeFeatureIfPresent).
      // Nothing to clean up, and nothing to save.
      showToast("error", "That shape could not be created -- its geometry was rejected.");
      return;
    }
    if (feature.geometry.type !== "Polygon") {
      removeFeatureIfPresent(draw, tempId);
      return;
    }

    setSaving(true);
    try {
      const saved = await createArea({ identifier, name, geometry: feature.geometry as Area["geometry"] });
      removeFeatureIfPresent(draw, tempId);
      draw.addFeatures([
        {
          id: saved.identifier,
          type: "Feature",
          properties: { mode: "polygon", name: saved.name },
          geometry: saved.geometry,
        },
      ]);
      draw.setMode("select");
      setAreas((current) => [...current, saved]);
      setDraft(clone(saved));
      setOriginal(clone(saved));
      showToast("success", `Area '${saved.identifier}' created.`);
    } catch (err) {
      removeFeatureIfPresent(draw, tempId);
      showToast("error", err instanceof ApiError ? err.message : "Failed to create area.");
    } finally {
      setSaving(false);
    }
  }

  function handleNameCancel() {
    const draw = drawRef.current;
    if (pendingDrawFeatureId && draw) {
      removeFeatureIfPresent(draw, pendingDrawFeatureId);
    }
    setPendingDrawFeatureId(null);
    setPendingNameSuggestion(null);
    draw?.setMode("select");
  }

  async function handleSave() {
    if (!draft) return;
    setSaving(true);
    try {
      const saved = await updateArea(draft.identifier, draft);
      setAreas((current) => current.map((a) => (a.identifier === saved.identifier ? saved : a)));
      setDraft(clone(saved));
      setOriginal(clone(saved));
      showToast("success", `Area '${saved.identifier}' saved.`);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to save area.");
    } finally {
      setSaving(false);
    }
  }

  function handleDiscard() {
    if (!original) return;
    setDraft(clone(original));
    drawRef.current?.updateFeatureGeometry(original.identifier, original.geometry);
  }

  async function handleDeleteConfirmed() {
    if (!deleteTarget) return;
    try {
      await deleteArea(deleteTarget.identifier);
      drawRef.current?.removeFeatures([deleteTarget.identifier]);
      setAreas((current) => current.filter((a) => a.identifier !== deleteTarget.identifier));
      if (draft?.identifier === deleteTarget.identifier) {
        setDraft(null);
        setOriginal(null);
      }
      showToast("success", `Area '${deleteTarget.identifier}' deleted.`);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to delete area.");
    } finally {
      setDeleteTarget(null);
    }
  }

  return (
    <div className="flex flex-col gap-4 md:h-full md:flex-row md:gap-6">
      <div className="flex flex-col gap-2 md:w-72 md:shrink-0">
        <button
          type="button"
          onClick={startDrawing}
          className="rounded-md border border-sky-600 px-3 py-2 text-sm font-medium text-sky-600 hover:bg-sky-50 dark:border-sky-400 dark:text-sky-400 dark:hover:bg-sky-950"
        >
          Draw New Area
        </button>

        {loading ? (
          <p className="text-slate-400">Loading areas...</p>
        ) : (
          <ul className="flex max-h-64 flex-col gap-1 overflow-y-auto md:max-h-none">
            {areas.map((area) => {
              const isSelected = draft?.identifier === area.identifier;
              return (
                <li
                  key={area.identifier}
                  className={`rounded-r-md border-l-4 ${
                    isSelected
                      ? "border-sky-600 bg-slate-100 dark:border-sky-400 dark:bg-slate-800"
                      : "border-transparent hover:bg-slate-100 dark:hover:bg-slate-800"
                  }`}
                >
                  {isSelected && draft ? (
                    <div className="flex flex-col gap-2 px-3 py-2">
                      <input
                        type="text"
                        value={draft.name}
                        onChange={(e) => setDraft({ ...draft, name: e.target.value })}
                        placeholder="Display name"
                        className="rounded-md border border-slate-300 px-2 py-1 text-sm dark:border-slate-600 dark:bg-slate-900"
                      />
                      <div className="flex flex-wrap gap-2">
                        <button
                          type="button"
                          onClick={handleDiscard}
                          disabled={!dirty}
                          className="rounded-md border border-slate-300 px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
                        >
                          Discard
                        </button>
                        <button
                          type="button"
                          onClick={handleSave}
                          disabled={!dirty || saving}
                          className="rounded-md bg-sky-600 px-2 py-1 text-xs font-medium text-white hover:bg-sky-700 disabled:opacity-40"
                        >
                          Save
                        </button>
                        <button
                          type="button"
                          onClick={duplicateArea}
                          className="rounded-md border border-slate-300 px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
                        >
                          Duplicate
                        </button>
                        <button
                          type="button"
                          onClick={() => setDeleteTarget(area)}
                          className="ml-auto rounded-md border border-red-300 px-3 py-1.5 text-sm font-medium text-red-600 hover:bg-red-50 dark:border-red-800 dark:hover:bg-red-950"
                        >
                          Delete
                        </button>
                      </div>
                    </div>
                  ) : (
                    <div className="flex items-center">
                      <button
                        type="button"
                        onClick={() => selectArea(area)}
                        title={area.identifier}
                        className="flex-1 truncate px-3 py-2 text-left text-sm text-slate-700 dark:text-slate-200"
                      >
                        {area.name || area.identifier}
                      </button>
                    </div>
                  )}
                </li>
              );
            })}
            {areas.length === 0 && (
              <li className="px-3 py-2 text-sm text-slate-400">No areas yet. Draw one on the map.</li>
            )}
          </ul>
        )}
      </div>

      <div className="relative h-[400px] overflow-hidden rounded-md border border-slate-200 md:h-auto md:flex-1 dark:border-slate-700">
        {/*
          h-full/w-full, not absolute + inset-0: MapLibre attaches its own
          `maplibregl-map` class directly to this div (it's the `container`
          passed to `new maplibregl.Map()`), and that class sets
          `position: relative`. Since maplibregl-map's rule happens to land
          later in the built CSS than Tailwind's `.absolute`, it wins the
          cascade (equal specificity, later source order) and silently
          overrides `position: absolute` -- without which `inset-0` no
          longer stretches this div to fill its parent, so it collapses to
          near-zero height instead. height/width: 100% has no such
          conflict with maplibregl-map's own position: relative.
        */}
        <div ref={mapContainerRef} className="h-full w-full" />
      </div>

      <ConfirmModal
        open={pendingSwitch !== null}
        title="Discard unsaved changes?"
        message="You have unsaved changes to this area. Switching now will discard them."
        confirmLabel="Discard"
        onConfirm={() => {
          pendingSwitch?.();
          setPendingSwitch(null);
        }}
        onCancel={() => setPendingSwitch(null)}
      />

      <ConfirmModal
        open={deleteTarget !== null}
        title="Delete area?"
        message={deleteTarget ? <DeleteAreaMessage area={deleteTarget} /> : ""}
        confirmLabel="Delete"
        onConfirm={handleDeleteConfirmed}
        onCancel={() => setDeleteTarget(null)}
      />

      <AreaNameModal
        open={pendingDrawFeatureId !== null}
        existingIdentifiers={areas.map((a) => a.identifier)}
        initialName={pendingNameSuggestion ?? undefined}
        onConfirm={handleNameConfirm}
        onCancel={handleNameCancel}
      />
    </div>
  );
}
