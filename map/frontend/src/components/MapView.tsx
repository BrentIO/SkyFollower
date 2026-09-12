import * as maplibregl from "maplibre-gl";
import { useEffect, useRef, useState } from "react";
import { MAP_STYLE } from "../lib/maplibreSetup";
import { buildShapeIconImageData, shapeIconId } from "../lib/aircraftIcon";
import { AIRCRAFT_SHAPES } from "../lib/aircraftShapes.generated";
import { FALLBACK_SHAPE } from "../lib/aircraftIconResolver";
import { MUTED_GRAY } from "../lib/crosshairIcon";
import { loadConfig, type AppConfig } from "../lib/config";
import { useMapFlights } from "../hooks/useMapFlights";
import { useProcessorRoster } from "../hooks/useProcessorRoster";
import {
  aircraftFeatureCollection,
  EMPTY_FEATURE_COLLECTION,
  hasPosition,
  trailFeatureCollection,
} from "../lib/featureCollections";
import {
  AIRCRAFT_LAYER_ID,
  AIRCRAFT_SOURCE_ID,
  RANGE_RING_LABEL_LAYER_ID,
  RANGE_RING_LABEL_SOURCE_ID,
  RANGE_RING_LAYER_ID,
  RANGE_RING_SOURCE_ID,
  SELECTABLE_LAYER_IDS,
  TRAIL_HIT_AREA_LAYER_ID,
  TRAIL_LAYER_ID,
  TRAIL_SOURCE_ID,
} from "../lib/mapLayerIds";
import { rangeRingLabelsFeatureCollection, rangeRingsFeatureCollection } from "../lib/rangeRings";
import { nextSelection } from "../lib/selection";
import { aircraftNeedingHistorySeed } from "../lib/trailSeeding";
import { AircraftDetailPanel } from "./AircraftDetailPanel";
import { ControlsPanel } from "./ControlsPanel";
import { InfoBoxLayer, type InfoBoxLayerItem } from "./InfoBoxLayer";

// Builds and registers one silhouette's SDF image with MapLibre, once.
// `shapeKey` is an AIRCRAFT_SHAPES key; an unknown key (a shape the
// resolver picked but the generated set somehow lacks) falls back to the
// FALLBACK_SHAPE art. A canvas failure is swallowed -- the layer's
// `icon-image` then just resolves to nothing for that aircraft rather than
// crashing the map.
function registerShapeImage(map: maplibregl.Map, shapeKey: string): void {
  const id = shapeIconId(shapeKey);
  if (map.hasImage(id)) return;
  const shape = AIRCRAFT_SHAPES[shapeKey] ?? AIRCRAFT_SHAPES[FALLBACK_SHAPE];
  if (!shape) return;
  try {
    map.addImage(id, buildShapeIconImageData(shape), { sdf: true });
  } catch (err) {
    console.warn(`Could not build aircraft icon for shape ${shapeKey}:`, err);
  }
}

// Top-level export: fetches runtime config (GET /api/config -- see
// map/lib/config.ts's loadConfig) once before the actual map ever mounts,
// since the map's initial center/zoom and home marker depend on it. A
// brief loading state is expected and fine here; the fetch is one small
// same-origin round-trip made once per page load.
export function MapView() {
  const [config, setConfig] = useState<AppConfig | null>(null);

  useEffect(() => {
    let cancelled = false;
    loadConfig().then((cfg) => {
      if (!cancelled) setConfig(cfg);
    });
    return () => {
      cancelled = true;
    };
  }, []);

  if (!config) {
    return (
      <div className="flex h-full w-full items-center justify-center text-sm text-gray-400">
        Loading map…
      </div>
    );
  }

  return <MapViewInner config={config} />;
}

// Full-viewport MapLibre live map: aircraft icon layer (heading rotation,
// altitude-colored fill), live trails, floating info boxes shown only for
// selected/hovered aircraft (or every aircraft via "Labels: All"),
// floating top-right controls, and a "home" reference-point marker/recenter.
// See the issue this implements for the full design spec. Only ever mounted
// once `config` has resolved (see MapView above), so every `config.home`
// read below is a plain, already-loaded value -- no further async handling
// needed in here.
function MapViewInner({ config }: { config: AppConfig }) {
  const { aircraft, connected, seedTrailFor } = useMapFlights(config.wsUrl, config.restFlightsUrl);
  const roster = useProcessorRoster(config.restProcessorsUrl);

  const mapContainerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [mapLoaded, setMapLoaded] = useState(false);

  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [historyAll, setHistoryAll] = useState(false);
  const [labelsAll, setLabelsAll] = useState(false);
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const [screenPositions, setScreenPositions] = useState<Record<string, { x: number; y: number }>>({});

  // Kept in a ref so the map's 'move' listener (attached once, on mount)
  // always reads current aircraft positions rather than closing over a
  // stale snapshot from whenever that listener was attached.
  const aircraftRef = useRef(aircraft);
  aircraftRef.current = aircraft;

  // When an aircraft is newly selected, pull the server's accumulated trail
  // for it (GET /api/flights/{icao_hex}) so the drawn trail covers the whole
  // flight -- not just what this browser has seen since it connected, and
  // regardless of a page reload. Fires only on the unselected -> selected
  // edge; deselecting and reselecting re-fetches (cheap, gets fresher data).
  const prevSelectedRef = useRef<Set<string>>(new Set());
  useEffect(() => {
    for (const icaoHex of selected) {
      if (!prevSelectedRef.current.has(icaoHex)) seedTrailFor(icaoHex);
    }
    prevSelectedRef.current = selected;
  }, [selected, seedTrailFor]);

  // "History: All" should show every tracked aircraft's full server trail,
  // not just whatever it's accumulated client-side since page load, and not
  // just the aircraft that happen to be individually selected. Seeds every
  // aircraft not yet seeded whenever the toggle is on -- covering both the
  // initial flip and any aircraft that newly appears while it's already on.
  // `historySeededRef` only grows, so toggling off/on or a later WS tick for
  // an already-seeded aircraft never re-fetches it.
  const historySeededRef = useRef<Set<string>>(new Set());
  useEffect(() => {
    const needed = aircraftNeedingHistorySeed(historyAll, Object.keys(aircraft), historySeededRef.current);
    for (const icaoHex of needed) {
      historySeededRef.current.add(icaoHex);
      seedTrailFor(icaoHex);
    }
  }, [historyAll, aircraft, seedTrailFor]);

  // --- Map construction (once) ---------------------------------------
  useEffect(() => {
    if (!mapContainerRef.current) return;

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: MAP_STYLE,
      center: config.home ? [config.home.longitude, config.home.latitude] : [0, 0],
      zoom: config.home ? 9 : 1,
      // Locked north-up -- the aircraft icon rotates via icon-rotate,
      // never the map itself. Same lock pattern as every existing
      // MapLibre view in management-ui/frontend.
      pitchWithRotate: false,
      dragRotate: false,
      // The basemap style carries its own OSM/CARTO/OpenFreeMap attribution;
      // this adds the aircraft-silhouette credit (GPL-3.0 -- see the repo's
      // THIRD-PARTY-NOTICES.md).
      attributionControl: {
        customAttribution:
          'Aircraft shapes © <a href="https://github.com/RexKramer1/AircraftShapesSVG" target="_blank" rel="noreferrer">RexKramer1</a> (GPL-3.0)',
      },
    });
    mapRef.current = map;
    map.touchZoomRotate.disableRotation();
    map.keyboard.disableRotation();
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-left");

    function syncScreenPositions() {
      const current = aircraftRef.current;
      const positions: Record<string, { x: number; y: number }> = {};
      for (const a of Object.values(current)) {
        if (!hasPosition(a)) continue;
        const p = map.project([a.lon, a.lat]);
        positions[a.icao_hex] = { x: p.x, y: p.y };
      }
      setScreenPositions(positions);
    }

    map.on("load", () => {
      // The fallback silhouette, so `icon-image` always resolves to a
      // registered image; every other shape is registered lazily the first
      // time an aircraft needs it (see ensureShapeImages in the sync effect
      // below).
      registerShapeImage(map, FALLBACK_SHAPE);

      // Static "home" range rings (100/150/200nmi) -- computed once from
      // config.home, which never changes after this component mounts (see
      // MapView above). Added before the trail/aircraft layers so they
      // render beneath live traffic. Not part of SELECTABLE_LAYER_IDS, so
      // clicking a ring or its label never triggers aircraft selection.
      map.addSource(RANGE_RING_SOURCE_ID, {
        type: "geojson",
        data: rangeRingsFeatureCollection(config.home),
      });
      map.addLayer({
        id: RANGE_RING_LAYER_ID,
        type: "line",
        source: RANGE_RING_SOURCE_ID,
        paint: { "line-color": "#000000", "line-width": 1 },
      });

      map.addSource(RANGE_RING_LABEL_SOURCE_ID, {
        type: "geojson",
        data: rangeRingLabelsFeatureCollection(config.home),
      });
      map.addLayer({
        id: RANGE_RING_LABEL_LAYER_ID,
        type: "symbol",
        source: RANGE_RING_LABEL_SOURCE_ID,
        layout: {
          "text-field": ["get", "label"],
          "text-size": 11,
          "text-anchor": "top",
          "text-offset": [0, 0.3],
          "text-allow-overlap": true,
        },
        paint: {
          "text-color": "#000000",
          "text-halo-color": "#ffffff",
          "text-halo-width": 1.5,
        },
      });

      map.addSource(TRAIL_SOURCE_ID, { type: "geojson", data: EMPTY_FEATURE_COLLECTION });
      map.addLayer({
        id: TRAIL_LAYER_ID,
        type: "line",
        source: TRAIL_SOURCE_ID,
        layout: { "line-cap": "round", "line-join": "round" },
        paint: { "line-color": ["get", "color"], "line-width": 2.5, "line-opacity": 0.85 },
      });
      // Invisible, much wider line over the same geometry -- this is the
      // layer in SELECTABLE_LAYER_IDS, so click/hover get a generous target
      // while the rendered trail above stays exactly as thin as it looks.
      map.addLayer({
        id: TRAIL_HIT_AREA_LAYER_ID,
        type: "line",
        source: TRAIL_SOURCE_ID,
        layout: { "line-cap": "round", "line-join": "round" },
        paint: { "line-color": "#000000", "line-width": 14, "line-opacity": 0 },
      });

      map.addSource(AIRCRAFT_SOURCE_ID, { type: "geojson", data: EMPTY_FEATURE_COLLECTION });
      map.addLayer({
        id: AIRCRAFT_LAYER_ID,
        type: "symbol",
        source: AIRCRAFT_SOURCE_ID,
        layout: {
          // Per-aircraft silhouette -- feature property `shape` is the
          // AIRCRAFT_SHAPES key (aircraftIconResolver.ts); its SDF image is
          // registered under shapeIconId() lazily. `icon_scale` applies the
          // shape's real relative size on top of the base size.
          "icon-image": ["concat", "sf-ac-", ["get", "shape"]],
          "icon-rotate": ["get", "heading"],
          "icon-rotation-alignment": "map",
          "icon-allow-overlap": true,
          "icon-ignore-placement": true,
          "icon-size": ["*", 0.55, ["coalesce", ["get", "icon_scale"], 1]],
        },
        paint: {
          // Icon fill is altitude-based; it never changes on selection --
          // selection is shown only via the halo below.
          "icon-color": ["get", "color"],
          "icon-halo-color": ["case", ["boolean", ["get", "selected"], false], "#ffffff", "#000000"],
          "icon-halo-width": ["case", ["boolean", ["get", "selected"], false], 3, 1],
          "icon-halo-blur": ["case", ["boolean", ["get", "selected"], false], 0.5, 0],
          "icon-opacity": ["case", ["boolean", ["get", "stale"], false], 0.4, 1],
        },
      });

      // Only SELECTABLE_LAYER_IDS ever gets click/hover handlers -- range
      // rings and their labels are intentionally not in that list, so they
      // can never be selected or hovered (see #1587).
      for (const layerId of SELECTABLE_LAYER_IDS) {
        map.on("click", layerId, (e) => {
          const icaoHex = e.features?.[0]?.properties?.icao_hex as string | undefined;
          if (!icaoHex) return;
          setSelected((prev) => nextSelection(prev, icaoHex));
        });
        map.on("mouseenter", layerId, () => {
          map.getCanvas().style.cursor = "pointer";
        });
        map.on("mouseleave", layerId, () => {
          map.getCanvas().style.cursor = "";
          setHoveredId(null);
        });
        // Transient label-on-hover -- tracked separately from click-select
        // so a hovered box disappears again on mouseleave rather than
        // sticking around like a selection does.
        map.on("mousemove", layerId, (e) => {
          const icaoHex = e.features?.[0]?.properties?.icao_hex as string | undefined;
          setHoveredId(icaoHex ?? null);
        });
      }

      // Home / centered reference point -- a fixed marker from config,
      // never derived from received data.
      if (config.home) {
        const el = document.createElement("div");
        el.style.display = "flex";
        el.style.flexDirection = "column";
        el.style.alignItems = "center";
        el.style.gap = "2px";
        el.style.pointerEvents = "none";
        el.innerHTML =
          `<div style="width:12px;height:12px;border-radius:50%;background:#000000;"></div>` +
          `<span style="font-size:9px;font-weight:600;letter-spacing:0.05em;color:${MUTED_GRAY};text-shadow:0 1px 2px rgba(255,255,255,0.8);">HOME</span>`;
        new maplibregl.Marker({ element: el, anchor: "center" })
          .setLngLat([config.home.longitude, config.home.latitude])
          .addTo(map);
      }

      map.on("move", syncScreenPositions);
      syncScreenPositions();
      setMapLoaded(true);
    });

    return () => {
      map.off("move", syncScreenPositions);
      map.remove();
      mapRef.current = null;
    };
    // Intentionally created once -- `config` (this component's own prop)
    // is only ever set once per page load by MapView above; it can change
    // between page loads (it's now a runtime GET /api/config fetch, not a
    // build-time constant), but never while this component is mounted.
  }, []);

  // --- Keep the aircraft/trail sources and screen positions in sync ---
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded) return;

    const visibleTrailIds = historyAll ? new Set(Object.keys(aircraft)) : selected;

    const fc = aircraftFeatureCollection(aircraft, selected);
    // Register the SDF image for every silhouette in the current set that
    // isn't registered yet, *before* the source data references it -- a
    // typical session touches a few dozen of the ~180 shapes.
    for (const f of fc.features) {
      const shape = f.properties?.shape;
      if (typeof shape === "string") registerShapeImage(map, shape);
    }
    (map.getSource(AIRCRAFT_SOURCE_ID) as maplibregl.GeoJSONSource | undefined)?.setData(fc);
    (map.getSource(TRAIL_SOURCE_ID) as maplibregl.GeoJSONSource | undefined)?.setData(
      trailFeatureCollection(aircraft, visibleTrailIds),
    );

    const positions: Record<string, { x: number; y: number }> = {};
    for (const a of Object.values(aircraft)) {
      if (!hasPosition(a)) continue;
      const p = map.project([a.lon, a.lat]);
      positions[a.icao_hex] = { x: p.x, y: p.y };
    }
    setScreenPositions(positions);
  }, [aircraft, selected, historyAll, mapLoaded]);

  function handleRecenter() {
    const map = mapRef.current;
    if (!map || !config.home) return;
    // Preserves whatever zoom level the user is already at -- only the
    // center changes.
    map.easeTo({ center: [config.home.longitude, config.home.latitude] });
  }

  // `selected` is always max-one-element (see lib/selection.ts's
  // nextSelection), so this is simply "the selected aircraft, if any and
  // if still tracked" -- no further reduction needed.
  const selectedIcaoHex = selected.values().next().value;
  const selectedAircraft = selectedIcaoHex ? aircraft[selectedIcaoHex] : undefined;

  const infoBoxItems: InfoBoxLayerItem[] = Object.values(aircraft)
    .filter(hasPosition)
    .filter((a) => !a.hidden)
    .filter((a) => screenPositions[a.icao_hex] !== undefined)
    .map((a) => ({
      id: a.icao_hex,
      x: screenPositions[a.icao_hex].x,
      y: screenPositions[a.icao_hex].y,
      aircraft: a,
    }));

  return (
    <div className="relative h-full w-full">
      <div ref={mapContainerRef} className="h-full w-full" />
      {mapLoaded && (
        <InfoBoxLayer items={infoBoxItems} selected={selected} showAll={labelsAll} hoveredId={hoveredId} />
      )}
      {selectedAircraft && (
        <AircraftDetailPanel
          aircraft={selectedAircraft}
          home={config.home}
          onClose={() => setSelected(new Set())}
        />
      )}
      <ControlsPanel
        wsConnected={connected}
        roster={roster}
        aircraftCount={Object.keys(aircraft).length}
        historyAll={historyAll}
        onToggleHistoryAll={() => setHistoryAll((prev) => !prev)}
        labelsAll={labelsAll}
        onToggleLabelsAll={() => setLabelsAll((prev) => !prev)}
        onRecenter={handleRecenter}
        recenterDisabled={!config.home}
      />
    </div>
  );
}
