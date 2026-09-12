import * as maplibregl from "maplibre-gl";
import { useEffect, useRef, useState } from "react";
import { MAP_STYLE } from "../lib/maplibreSetup";
import { buildShapeIconImageData, shapeIconId } from "../lib/aircraftIcon";
import { AIRCRAFT_SHAPES } from "../lib/aircraftShapes.generated";
import { basemapLabelLayerIds } from "../lib/basemapLabels";
import { FALLBACK_SHAPE } from "../lib/aircraftIconResolver";
import { MUTED_GRAY } from "../lib/crosshairIcon";
import { loadConfig, type AppConfig } from "../lib/config";
import { useMapFlights } from "../hooks/useMapFlights";
import { useProcessorRoster } from "../hooks/useProcessorRoster";
import { useRangeOutline } from "../hooks/useRangeOutline";
import {
  aircraftFeatureCollection,
  EMPTY_FEATURE_COLLECTION,
  hasPosition,
  trailFeatureCollection,
} from "../lib/featureCollections";
import {
  AIRCRAFT_LAYER_ID,
  AIRCRAFT_SOURCE_ID,
  RANGE_OUTLINE_LAYER_ID,
  RANGE_OUTLINE_SOURCE_ID,
  RANGE_RING_LABEL_LAYER_ID,
  RANGE_RING_LABEL_SOURCE_ID,
  RANGE_RING_LAYER_ID,
  RANGE_RING_SOURCE_ID,
  SELECTABLE_LAYER_IDS,
  TRACE_POINTS_CIRCLE_LAYER_ID,
  TRACE_POINTS_LABEL_LAYER_ID,
  TRACE_POINTS_SOURCE_ID,
  TRAIL_HIT_AREA_LAYER_ID,
  TRAIL_LAYER_ID,
  TRAIL_SOURCE_ID,
} from "../lib/mapLayerIds";
import { deepLinkAircraftAvailable, deepLinkReadyToZoom } from "../lib/deepLink";
import { followTargetPosition } from "../lib/followTarget";
import { rangeRingLabelsFeatureCollection, rangeRingsFeatureCollection } from "../lib/rangeRings";
import { infoBoxOffsetForZoom } from "../lib/infoBoxOffset";
import { nextSelection } from "../lib/selection";
import { readSelectionFromSearch, searchWithSelection } from "../lib/shareUrl";
import { tracePointsFeatureCollection } from "../lib/tracePoints";
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
  const [selected, setSelected] = useState<Set<string>>(new Set());
  // `selected` is always max-one-element (see lib/selection.ts's
  // nextSelection), so this is simply "the selected icao_hex, if any" --
  // no further reduction needed. Computed up front (rather than only near
  // the render return) since useMapFlights below needs it for eviction
  // deferral.
  const selectedIcaoHex = selected.values().next().value ?? null;

  const { aircraft, connected, seedTrailFor, releaseHold } = useMapFlights(
    config.wsUrl,
    config.restFlightsUrl,
    selectedIcaoHex,
  );
  const roster = useProcessorRoster(config.restProcessorsUrl);

  const mapContainerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [mapLoaded, setMapLoaded] = useState(false);

  const [historyAll, setHistoryAll] = useState(false);
  const [labelsAll, setLabelsAll] = useState(false);
  // Daily reception range outline overlay -- defaults off, matching
  // historyAll/labelsAll's convention. Polling (see useRangeOutline below)
  // only happens while this is true, so leaving it off costs nothing.
  const [rangeOutlineVisible, setRangeOutlineVisible] = useState(false);
  const rangeOutline = useRangeOutline(config.apiBaseUrl, rangeOutlineVisible && !!config.home);
  // Basemap's own text labels (place names, road names/shields, water
  // names, airport labels) -- defaults on so the basemap is unchanged out
  // of the box; turning it off is what hides the basemap's text. Distinct
  // from labelsAll above, which is about aircraft info boxes.
  const [mapLabelsOn, setMapLabelsOn] = useState(false);
  // The basemap's own text-bearing layer ids, computed once on "load" (see
  // basemapLabelLayerIds) -- the basemap style doesn't gain/lose layers at
  // runtime, so there's no need to recompute this on every toggle.
  const basemapLabelLayerIdsRef = useRef<string[]>([]);
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  const [screenPositions, setScreenPositions] = useState<
    Record<string, { x: number; y: number; offset: number }>
  >({});

  // Action-row state (see components/AircraftDetailPanel.tsx). isolateId is
  // *derived* from isolateEnabled + selectedIcaoHex rather than captured
  // separately -- that's what makes Isolate automatically re-target to a
  // newly-selected aircraft instead of clearing (the issue's explicit
  // Isolate behavior). followId/tracePointsEnabled are plain state because
  // they deliberately do *not* carry over to a different selection -- see
  // the reset effect below.
  const [isolateEnabled, setIsolateEnabled] = useState(false);
  const isolateId = isolateEnabled ? selectedIcaoHex : null;
  const [followId, setFollowId] = useState<string | null>(null);
  const [tracePointsEnabled, setTracePointsEnabled] = useState(false);

  // Kept in a ref so the map's 'move' listener (attached once, on mount)
  // always reads current aircraft positions rather than closing over a
  // stale snapshot from whenever that listener was attached.
  const aircraftRef = useRef(aircraft);
  aircraftRef.current = aircraft;

  // Fires whenever the selection moves away from an aircraft -- either to
  // a different one (reselect) or to none (the panel's close button /
  // deselect). This is where the close-button contract's non-Isolate
  // parts live: Follow and Trace Points don't carry over to a different
  // aircraft (unlike Isolate, which is deliberately derived above so it
  // *does* re-target), and any eviction this frontend deferred while the
  // panel was open for the previous aircraft (see aircraftState.ts's
  // pendingRemoval) is applied now. Isolate itself is only fully reset on
  // an actual close (selectedIcaoHex becoming null) -- a mere reselect
  // deliberately leaves it on, re-targeted to the new pick.
  const prevSelectedIcaoHexRef = useRef<string | null>(null);
  useEffect(() => {
    const prevIcaoHex = prevSelectedIcaoHexRef.current;
    if (prevIcaoHex !== null && prevIcaoHex !== selectedIcaoHex) {
      setFollowId(null);
      setTracePointsEnabled(false);
      if (selectedIcaoHex === null) setIsolateEnabled(false);
      releaseHold(prevIcaoHex);
    }
    prevSelectedIcaoHexRef.current = selectedIcaoHex;
  }, [selectedIcaoHex, releaseHold]);

  // Shareable URL (see lib/shareUrl.ts/lib/deepLink.ts): the icao_hex
  // named in the URL's ?aircraft= param at mount, if any -- read once via
  // a lazy initializer (never state, since it must not change once the
  // page has loaded) and held here until it's been selected + Zoomed To,
  // or abandoned, by the two effects below.
  const pendingDeepLinkIcaoHexRef = useRef<string | null>(readSelectionFromSearch(window.location.search));

  // Deep-link-on-load, part 1: once the aircraft named in the URL shows up
  // in tracked state -- immediately, if it was already in the initial
  // GET /api/flights snapshot, or once a WS event adds it soon after --
  // select it. Abandoned (ref cleared, never retried) if the operator
  // makes their own selection first: a manual click should never be
  // stomped on by a deep link resolving late. If the aircraft never
  // appears at all, this simply never fires again -- the "fail silently"
  // behavior the issue calls for.
  useEffect(() => {
    const pending = pendingDeepLinkIcaoHexRef.current;
    if (!pending) return;
    if (selectedIcaoHex !== null) {
      pendingDeepLinkIcaoHexRef.current = null;
      return;
    }
    if (deepLinkAircraftAvailable(aircraft, pending)) {
      setSelected(new Set([pending]));
    }
  }, [aircraft, selectedIcaoHex]);

  // Deep-link-on-load, part 2: once the deep-linked aircraft is selected
  // and has a known position, Zoom To it -- reusing the exact same
  // recenter-preserving-zoom mechanism as the panel's own Zoom To button
  // (handleZoomTo below), against whatever the map's initial/default zoom
  // is, per the issue. One-shot: the ref is cleared right after so a
  // later manual deselect/reselect of the same aircraft never re-triggers
  // it.
  useEffect(() => {
    const pending = pendingDeepLinkIcaoHexRef.current;
    if (!pending || selectedIcaoHex !== pending) return;
    if (!deepLinkReadyToZoom(aircraft, pending, mapLoaded)) return;
    handleZoomTo();
    pendingDeepLinkIcaoHexRef.current = null;
  }, [aircraft, selectedIcaoHex, mapLoaded]);

  // Keeps the address bar in sync with the current selection, continuously
  // -- not just on initial load -- so a link copied at any point in a
  // session reproduces that exact selection: selecting sets
  // ?aircraft=<icao_hex>, deselecting clears it back to the bare map URL.
  // Driven off `selectedIcaoHex` (rather than duplicated at each place
  // `selected` changes) so every current and future call site -- the
  // click handler, the panel's close button, Isolate re-targeting the
  // same selection -- is covered by this one effect. Skips the very first
  // run (mount): the URL already reflects the right thing at that point
  // (bare, or a deep link's own param that the two effects above are
  // still trying to resolve), so writing here on mount could clobber an
  // unresolved deep link. Uses history.replaceState rather than
  // pushState -- see this change's PR description for why.
  const isFirstSelectionSyncRef = useRef(true);
  useEffect(() => {
    if (isFirstSelectionSyncRef.current) {
      isFirstSelectionSyncRef.current = false;
      return;
    }
    const nextSearch = searchWithSelection(window.location.search, selectedIcaoHex);
    window.history.replaceState(null, "", `${window.location.pathname}${nextSearch}${window.location.hash}`);
  }, [selectedIcaoHex]);

  // Follow: recenters on every position update for the followed aircraft,
  // preserving whatever zoom is already active (no `zoom` key passed to
  // easeTo) -- same rule as Zoom To's one-shot handler below. Keeps
  // returning the aircraft's last known position even after it goes
  // stale/hidden or its eviction is deferred (see followTargetPosition's
  // own docstring), so the map simply stops receiving new recenters and
  // stays parked at the last place the aircraft was actually seen -- the
  // "held in view" behavior Follow specifies on loss.
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const target = followTargetPosition(aircraft, followId);
    if (!target) return;
    map.easeTo({ center: [target.lon, target.lat] });
  }, [aircraft, followId]);

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
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "bottom-left");

    function syncScreenPositions() {
      const current = aircraftRef.current;
      const offset = infoBoxOffsetForZoom(map.getZoom());
      const positions: Record<string, { x: number; y: number; offset: number }> = {};
      for (const a of Object.values(current)) {
        if (!hasPosition(a)) continue;
        const p = map.project([a.lon, a.lat]);
        positions[a.icao_hex] = { x: p.x, y: p.y, offset };
      }
      setScreenPositions(positions);
    }

    map.on("load", () => {
      // Discover the basemap's own text-bearing layers once, before any of
      // SkyFollower's own overlay layers are added below -- so this list is
      // purely the remote style's own place-name/road/water/POI text
      // layers, whatever it happens to call them (see basemapLabelLayerIds'
      // own docstring for why this isn't a hardcoded ID list).
      basemapLabelLayerIdsRef.current = basemapLabelLayerIds(map.getStyle().layers);

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

      // Daily reception range outline (toggle-able, see the effect below) --
      // empty until the toggle is on and the first poll resolves. Added
      // alongside the static range-ring layers above so it renders beneath
      // live traffic; not part of SELECTABLE_LAYER_IDS, so it's never a
      // click/hover target.
      map.addSource(RANGE_OUTLINE_SOURCE_ID, { type: "geojson", data: EMPTY_FEATURE_COLLECTION });
      map.addLayer({
        id: RANGE_OUTLINE_LAYER_ID,
        type: "line",
        source: RANGE_OUTLINE_SOURCE_ID,
        // The API's polygon/line vertices are [lon, lat, alt_ft]; MapLibre's
        // 2-D `line` layer only ever consumes the first two components, so
        // altitude is silently ignored here -- expected for this flat v1
        // envelope line, not a bug.
        paint: { "line-color": "#196363", "line-width": 2 },
      });

      map.addSource(TRAIL_SOURCE_ID, { type: "geojson", data: EMPTY_FEATURE_COLLECTION });
      map.addLayer({
        id: TRAIL_LAYER_ID,
        type: "line",
        source: TRAIL_SOURCE_ID,
        layout: { "line-cap": "round", "line-join": "round" },
        paint: {
          "line-color": ["get", "color"],
          "line-width": 2.5,
          // Dims a Follow-lost aircraft's trail the same way its icon is
          // dimmed (see AIRCRAFT_LAYER_ID's icon-opacity below) instead of
          // letting it disappear -- see featureCollections.ts's
          // trailFeatureCollection, which sets this property.
          "line-opacity": ["case", ["boolean", ["get", "dimmed"], false], 0.35, 0.85],
        },
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

      // Aircraft detail panel's Trace Points action (lib/tracePoints.ts) --
      // always present, like the sources above; driven to an empty
      // FeatureCollection when off rather than layout-visibility-toggled
      // (see the sync effect below), matching this component's existing
      // convention for the aircraft/trail sources. Not in
      // SELECTABLE_LAYER_IDS -- a trace point dot/label is display-only,
      // never a click target of its own.
      map.addSource(TRACE_POINTS_SOURCE_ID, { type: "geojson", data: EMPTY_FEATURE_COLLECTION });
      map.addLayer({
        id: TRACE_POINTS_CIRCLE_LAYER_ID,
        type: "circle",
        source: TRACE_POINTS_SOURCE_ID,
        paint: {
          "circle-color": ["get", "color"],
          "circle-radius": 4,
          "circle-stroke-width": 1,
          "circle-stroke-color": ["get", "strokeColor"],
        },
      });
      map.addLayer({
        id: TRACE_POINTS_LABEL_LAYER_ID,
        type: "symbol",
        source: TRACE_POINTS_SOURCE_ID,
        layout: {
          "text-field": ["get", "label"],
          "text-size": 11,
          "text-anchor": "bottom-left",
          "text-offset": [0.6, -0.6],
          "text-justify": "left",
          // false is MapLibre's own default -- explicit here since this
          // collision behavior *is* the decluttering mechanism (see
          // symbol-sort-key below, same as management-ui's TracePointsControl).
          "text-allow-overlap": false,
          "text-ignore-placement": false,
          "symbol-sort-key": ["get", "sortKey"],
        },
        paint: {
          "text-color": "#0f172a",
          "text-halo-color": "#ffffff",
          "text-halo-width": 1.5,
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

  // "Map Labels" toggle -- shows/hides the basemap's own text layers
  // (computed once on load, see basemapLabelLayerIdsRef above). Runs
  // whenever the toggle flips; the layer-id list itself never changes
  // after load, so there's no need to recompute it here.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded) return;
    const visibility = mapLabelsOn ? "visible" : "none";
    for (const layerId of basemapLabelLayerIdsRef.current) {
      map.setLayoutProperty(layerId, "visibility", visibility);
    }
  }, [mapLabelsOn, mapLoaded]);

  // "Range Outline" toggle -- pushes the polled envelope (useRangeOutline
  // above, which itself stops polling and reports an empty
  // FeatureCollection whenever this is off/no home configured) into the
  // source. Keyed on `rangeOutline` so a poll result while it's on updates
  // the map on arrival, and on the toggle so switching off clears the
  // overlay immediately rather than waiting for the hook's own reset to
  // flow through.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded) return;
    (map.getSource(RANGE_OUTLINE_SOURCE_ID) as maplibregl.GeoJSONSource | undefined)?.setData(
      rangeOutlineVisible ? rangeOutline : EMPTY_FEATURE_COLLECTION,
    );
  }, [rangeOutline, rangeOutlineVisible, mapLoaded]);

  // --- Keep the aircraft/trail sources and screen positions in sync ---
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded) return;

    const visibleTrailIds = historyAll ? new Set(Object.keys(aircraft)) : selected;
    const visibility = { isolateId, followId };

    const fc = aircraftFeatureCollection(aircraft, selected, visibility);
    // Register the SDF image for every silhouette in the current set that
    // isn't registered yet, *before* the source data references it -- a
    // typical session touches a few dozen of the ~180 shapes.
    for (const f of fc.features) {
      const shape = f.properties?.shape;
      if (typeof shape === "string") registerShapeImage(map, shape);
    }
    (map.getSource(AIRCRAFT_SOURCE_ID) as maplibregl.GeoJSONSource | undefined)?.setData(fc);
    (map.getSource(TRAIL_SOURCE_ID) as maplibregl.GeoJSONSource | undefined)?.setData(
      trailFeatureCollection(aircraft, visibleTrailIds, visibility),
    );

    // Trace Points -- empty data when off or nothing selected, same
    // always-present-source convention as the layers above.
    const tracePoints = tracePointsEnabled && selectedIcaoHex ? (aircraft[selectedIcaoHex]?.tracePoints ?? []) : [];
    (map.getSource(TRACE_POINTS_SOURCE_ID) as maplibregl.GeoJSONSource | undefined)?.setData(
      tracePointsFeatureCollection(tracePoints),
    );

    const offset = infoBoxOffsetForZoom(map.getZoom());
    const positions: Record<string, { x: number; y: number; offset: number }> = {};
    for (const a of Object.values(aircraft)) {
      if (!hasPosition(a)) continue;
      const p = map.project([a.lon, a.lat]);
      positions[a.icao_hex] = { x: p.x, y: p.y, offset };
    }
    setScreenPositions(positions);
  }, [aircraft, selected, historyAll, mapLoaded, isolateId, followId, tracePointsEnabled, selectedIcaoHex]);

  // `selectedIcaoHex` is always still tracked when non-null, *except*
  // during the render right after a `remove` deletes an unprotected
  // aircraft out from under a stale selection -- practically unreachable
  // since selectedIcaoHex is exactly what protects it, but the lookup
  // stays defensive either way.
  const selectedAircraft = selectedIcaoHex ? aircraft[selectedIcaoHex] : undefined;

  function handleRecenter() {
    const map = mapRef.current;
    if (!map || !config.home) return;
    // Preserves whatever zoom level the user is already at -- only the
    // center changes.
    map.easeTo({ center: [config.home.longitude, config.home.latitude] });
  }

  // Zoom To: one-shot recenter on the selected aircraft's current
  // position, preserving whatever zoom is already active (no `zoom` key)
  // -- explicitly not a snap to a fixed close-up zoom, per the issue.
  function handleZoomTo() {
    const map = mapRef.current;
    if (!map || !selectedAircraft || selectedAircraft.lat == null || selectedAircraft.lon == null) return;
    map.easeTo({ center: [selectedAircraft.lon, selectedAircraft.lat] });
  }

  function handleToggleFollow() {
    if (!selectedIcaoHex) return;
    const turningOn = followId !== selectedIcaoHex;
    setFollowId(turningOn ? selectedIcaoHex : null);
    if (turningOn) handleZoomTo(); // Zoom To once immediately, then the recenter effect above takes over.
  }

  const infoBoxItems: InfoBoxLayerItem[] = Object.values(aircraft)
    .filter(hasPosition)
    .filter((a) => !a.hidden)
    .filter((a) => !isolateId || a.icao_hex === isolateId)
    .filter((a) => screenPositions[a.icao_hex] !== undefined)
    .map((a) => ({
      id: a.icao_hex,
      x: screenPositions[a.icao_hex].x,
      y: screenPositions[a.icao_hex].y,
      offset: screenPositions[a.icao_hex].offset,
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
          isolateActive={isolateEnabled}
          onToggleIsolate={() => setIsolateEnabled((prev) => !prev)}
          onZoomTo={handleZoomTo}
          followActive={followId === selectedIcaoHex}
          onToggleFollow={handleToggleFollow}
          tracePointsActive={tracePointsEnabled}
          onToggleTracePoints={() => setTracePointsEnabled((prev) => !prev)}
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
        mapLabelsOn={mapLabelsOn}
        onToggleMapLabels={() => setMapLabelsOn((prev) => !prev)}
        rangeOutlineVisible={rangeOutlineVisible}
        onToggleRangeOutline={() => setRangeOutlineVisible((prev) => !prev)}
        rangeOutlineDisabled={!config.home}
        onRecenter={handleRecenter}
        recenterDisabled={!config.home}
      />
    </div>
  );
}
