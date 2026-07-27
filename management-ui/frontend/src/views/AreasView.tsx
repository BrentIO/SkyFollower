import * as maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";
import MapboxDraw from "@mapbox/mapbox-gl-draw";
import "@mapbox/mapbox-gl-draw/dist/mapbox-gl-draw.css";
import { Trash2 } from "lucide-react";
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

// Free, no-API-key MapLibre style -- see AreasEditor spec's "Map
// Configuration" note. Verify this URL is still live if the map ever shows
// a blank/broken basemap.
// "positron" (CARTO's well-known light/grayscale basemap design, served
// here by the same OpenFreeMap provider as the rest of this file -- no
// new third-party domain) instead of "liberty"'s full-color style, per
// request. Verify this URL is still live if the map ever shows a
// blank/broken basemap.
const MAP_STYLE = "https://tiles.openfreemap.org/styles/positron";

// mapbox-gl-draw's own event payloads aren't part of maplibregl's typed
// event map (it fires custom event names on the map's event bus), so `.on`
// calls below go through this narrow, honest escape hatch rather than
// pretending the shape is fully known.
type DrawEventMap = {
  on(type: string, listener: (e: { features: GeoJSON.Feature[] }) => void): void;
};

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value));
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
  const drawRef = useRef<MapboxDraw | null>(null);

  const [areas, setAreas] = useState<Area[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [mapReady, setMapReady] = useState(false);

  const [original, setOriginal] = useState<Area | null>(null);
  const [draft, setDraft] = useState<Area | null>(null);

  const [pendingSwitch, setPendingSwitch] = useState<(() => void) | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<Area | null>(null);
  const [pendingDrawFeatureId, setPendingDrawFeatureId] = useState<string | null>(null);

  const dirty = draft !== null && original !== null && JSON.stringify(draft) !== JSON.stringify(original);

  function requestSwitch(action: () => void) {
    if (dirty) {
      setPendingSwitch(() => action);
    } else {
      action();
    }
  }

  // Draw's map event listeners are registered exactly once, on mount (see
  // the map-init effect below), so they'd otherwise close over that first
  // render's state forever. Each handler here is redefined every render
  // and stashed in a ref; the one-time listener always calls through
  // `<name>Ref.current(...)`, so it always sees the current render's state
  // without needing to be re-registered.
  const handleDrawCreateRef = useRef((featureId: string) => {
    setPendingDrawFeatureId(featureId);
  });
  handleDrawCreateRef.current = (featureId: string) => {
    setPendingDrawFeatureId(featureId);
  };

  const handleDrawUpdateRef = useRef((featureId: string, geometry: Area["geometry"]) => {
    if (!draft || draft.identifier !== featureId) return;
    setDraft({ ...draft, geometry });
  });
  handleDrawUpdateRef.current = (featureId: string, geometry: Area["geometry"]) => {
    if (!draft || draft.identifier !== featureId) return;
    setDraft({ ...draft, geometry });
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
    map.addControl(new maplibregl.NavigationControl(), "top-right");

    const draw = new MapboxDraw({ displayControlsDefault: false });
    drawRef.current = draw;
    // MapboxDraw's published types target mapboxgl.Map; maplibregl.Map
    // implements a compatible-enough IControl contract for how Draw
    // actually uses it at runtime (onAdd/onRemove + the map's event bus).
    map.addControl(draw as unknown as maplibregl.IControl);

    const drawEvents = map as unknown as DrawEventMap;
    drawEvents.on("draw.create", (e) => {
      const feature = e.features[0];
      if (feature) handleDrawCreateRef.current(String(feature.id));
    });
    drawEvents.on("draw.update", (e) => {
      const feature = e.features[0];
      if (feature && feature.geometry.type === "Polygon") {
        handleDrawUpdateRef.current(String(feature.id), feature.geometry as Area["geometry"]);
      }
    });
    drawEvents.on("draw.selectionchange", (e) => {
      const feature = e.features[0];
      if (feature) handleDrawSelectRef.current(String(feature.id));
    });

    map.on("load", () => {
      map.addSource("area-labels", {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
      });
      map.addLayer({
        id: "area-labels",
        type: "symbol",
        source: "area-labels",
        layout: { "text-field": ["get", "name"], "text-size": 12, "text-anchor": "center" },
        paint: { "text-color": "#0f172a", "text-halo-color": "#ffffff", "text-halo-width": 1.5 },
      });
      setMapReady(true);
    });

    return () => {
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
        if (draw) {
          for (const area of loaded) {
            draw.add({
              type: "Feature",
              id: area.identifier,
              properties: { name: area.name },
              geometry: area.geometry,
            });
          }
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
      drawRef.current?.changeMode("direct_select", { featureId: area.identifier });
    });
  }

  function startDrawing() {
    requestSwitch(() => {
      setDraft(null);
      setOriginal(null);
      drawRef.current?.changeMode("draw_polygon");
    });
  }

  async function handleNameConfirm(identifier: string, name: string) {
    const draw = drawRef.current;
    const tempId = pendingDrawFeatureId;
    setPendingDrawFeatureId(null);
    if (!draw || !tempId) return;

    const feature = draw.getAll().features.find((f) => String(f.id) === tempId);
    if (!feature || feature.geometry.type !== "Polygon") {
      draw.delete(tempId);
      return;
    }

    setSaving(true);
    try {
      const saved = await createArea({ identifier, name, geometry: feature.geometry as Area["geometry"] });
      draw.delete(tempId);
      draw.add({
        type: "Feature",
        id: saved.identifier,
        properties: { name: saved.name },
        geometry: saved.geometry,
      });
      setAreas((current) => [...current, saved]);
      setDraft(clone(saved));
      setOriginal(clone(saved));
      showToast("success", `Area '${saved.identifier}' created.`);
    } catch (err) {
      draw.delete(tempId);
      showToast("error", err instanceof ApiError ? err.message : "Failed to create area.");
    } finally {
      setSaving(false);
    }
  }

  function handleNameCancel() {
    if (pendingDrawFeatureId) {
      drawRef.current?.delete(pendingDrawFeatureId);
    }
    setPendingDrawFeatureId(null);
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
    const draw = drawRef.current;
    if (draw) {
      draw.delete(original.identifier);
      draw.add({
        type: "Feature",
        id: original.identifier,
        properties: { name: original.name },
        geometry: original.geometry,
      });
    }
  }

  async function handleDeleteConfirmed() {
    if (!deleteTarget) return;
    try {
      await deleteArea(deleteTarget.identifier);
      drawRef.current?.delete(deleteTarget.identifier);
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
                          onClick={() => setDeleteTarget(area)}
                          className="ml-auto rounded-md px-2 py-1 text-xs font-medium text-red-600 hover:bg-red-50 dark:text-red-400 dark:hover:bg-red-950"
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
                      <button
                        type="button"
                        onClick={() => setDeleteTarget(area)}
                        aria-label={`Delete ${area.name || area.identifier}`}
                        className="mr-2 shrink-0 text-slate-400 hover:text-red-600 dark:hover:text-red-400"
                      >
                        <Trash2 size={14} />
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

      <div className="relative min-h-[400px] flex-1 overflow-hidden rounded-md border border-slate-200 dark:border-slate-700">
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
        onConfirm={handleNameConfirm}
        onCancel={handleNameCancel}
      />
    </div>
  );
}
