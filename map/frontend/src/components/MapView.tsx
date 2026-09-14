import * as maplibregl from "maplibre-gl";
import { useEffect, useRef, useState } from "react";
import { MAP_STYLE } from "../lib/maplibreSetup";
import { buildShapeIconImageData, shapeIconId } from "../lib/aircraftIcon";
import { AIRCRAFT_SHAPES } from "../lib/aircraftShapes.generated";
import { basemapLabelLayerIds } from "../lib/basemapLabels";
import { FALLBACK_SHAPE } from "../lib/aircraftIconResolver";
import { loadConfig, type AppConfig } from "../lib/config";
import { loadPersistedControls, savePersistedControls } from "../lib/controlsPersistence";
import { useMapFlights } from "../hooks/useMapFlights";
import { useProcessorRoster } from "../hooks/useProcessorRoster";
import { useRangeOutline } from "../hooks/useRangeOutline";
import {
  aircraftFeatureCollection,
  buildAircraftSourceDiff,
  buildTrailSourceDiff,
  EMPTY_FEATURE_COLLECTION,
  trailFeatureCollection,
} from "../lib/featureCollections";
import { diffAircraftMaps } from "../lib/aircraftMapDiff";
import type { AircraftMap } from "../lib/aircraftState";
import {
  AIRCRAFT_LAYER_ID,
  AIRCRAFT_SELECTION_RING_LAYER_ID,
  AIRCRAFT_SOURCE_ID,
  CENTER_POINT_CIRCLE_LAYER_ID,
  CENTER_POINT_SOURCE_ID,
  INFO_BOX_LAYER_ID,
  INFO_BOX_SOURCE_ID,
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
import { followTargetPosition, shouldCancelFollowOnDrag } from "../lib/followTarget";
import {
  centerPointFeatureCollection,
  rangeRingLabelsFeatureCollection,
  rangeRingsFeatureCollection,
} from "../lib/rangeRings";
import { INFO_BOX_TEXT_OFFSET_REFERENCE_PX, infoBoxTextOffsetZoomExpression } from "../lib/infoBoxOffset";
import { INFO_BOX_ICON_ID, registerInfoBoxIcon } from "../lib/infoBoxIcon";
import { buildInfoBoxLabelSourceDiff, infoBoxLabelFeatureCollection, type LabelFilter } from "../lib/infoBoxSource";
import { topIcaoHex } from "../lib/mapHitTest";
import { nextSelection } from "../lib/selection";
import { readSelectionFromSearch, searchWithSelection } from "../lib/shareUrl";
import { createTrailingThrottle, MAP_SYNC_THROTTLE_MS } from "../lib/syncThrottle";
import { tracePointsFeatureCollection } from "../lib/tracePoints";
import { aircraftNeedingHistorySeed } from "../lib/trailSeeding";
import { AircraftDetailPanel } from "./AircraftDetailPanel";
import { AircraftListPanel } from "./AircraftListPanel";
import { ControlsPanel } from "./ControlsPanel";
import type { AddLayerObject } from "maplibre-gl";

// maplibre-gl's public d.ts doesn't itself export a named
// "SymbolLayerSpecification"/"ExpressionSpecification" type (only the
// broader `AddLayerObject` union `map.addLayer` accepts) -- this narrows
// that union down to the symbol-layer's own `layout` shape, just so
// INFO_BOX_LAYER_ID's dynamically-built `text-field`/`text-offset`
// expressions below (built by a function call, so TS can't infer their
// literal array type the way it can for a plain inline expression like
// `["get", "heading"]`) can be cast against the real expected type instead
// of `any`.
type SymbolLayout = NonNullable<Extract<AddLayerObject, { type: "symbol" }>["layout"]>;

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
// since the map's initial center/zoom and center marker depend on it. A
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
// floating top-right controls, and a "center" reference-point marker/recenter.
// See the issue this implements for the full design spec. Only ever mounted
// once `config` has resolved (see MapView above), so every `config.center`
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

  // Initial values come from this browser's localStorage (per-browser only
  // -- never synced across devices/accounts), falling back to today's
  // hardcoded defaults for a fresh browser or blocked storage -- see
  // lib/controlsPersistence.ts. The save effect below is what keeps
  // storage in sync with these as the operator toggles them.
  const [historyAll, setHistoryAll] = useState(() => loadPersistedControls().historyAll);
  const [labelsAll, setLabelsAll] = useState(() => loadPersistedControls().labelsAll);
  // Daily reception range outline overlay -- defaults off, matching
  // historyAll/labelsAll's convention. Polling (see useRangeOutline below)
  // only happens while this is true, so leaving it off costs nothing.
  const [rangeOutlineVisible, setRangeOutlineVisible] = useState(
    () => loadPersistedControls().rangeOutlineVisible,
  );
  const rangeOutline = useRangeOutline(config.apiBaseUrl, rangeOutlineVisible && !!config.center);
  // Basemap's own text labels (place names, road names/shields, water
  // names, airport labels) -- defaults on so the basemap is unchanged out
  // of the box; turning it off is what hides the basemap's text. Distinct
  // from labelsAll above, which is about aircraft info boxes.
  const [mapLabelsOn, setMapLabelsOn] = useState(() => loadPersistedControls().mapLabelsOn);

  // Persists the four control toggles above to localStorage on every
  // change, so a reload restores them via the lazy initializers above
  // instead of resetting to today's hardcoded defaults.
  useEffect(() => {
    savePersistedControls({ historyAll, labelsAll, mapLabelsOn, rangeOutlineVisible });
  }, [historyAll, labelsAll, mapLabelsOn, rangeOutlineVisible]);

  // Whole-page Fullscreen API toggle -- not persisted like the four above,
  // since it's a transient browser-chrome state rather than an operator
  // preference (reloading always starts out of fullscreen). Checked once
  // rather than tracked live: `fullscreenEnabled` reflects permission/
  // support, which doesn't change over a page's lifetime the way
  // `fullscreenElement` does.
  const [fullscreenSupported] = useState(() => document.fullscreenEnabled);
  const [fullscreen, setFullscreen] = useState(() => document.fullscreenElement != null);

  // Esc, F11, and OS-level gestures all exit fullscreen without going
  // through handleToggleFullscreen below, so the button's state can't rely
  // on optimistic state set only inside that click handler -- same class
  // of "something outside our own handler changed the state" problem the
  // map's own dragstart listener handles for Follow further down.
  useEffect(() => {
    const handleFullscreenChange = () => setFullscreen(document.fullscreenElement != null);
    document.addEventListener("fullscreenchange", handleFullscreenChange);
    return () => document.removeEventListener("fullscreenchange", handleFullscreenChange);
  }, []);

  // Fullscreens the whole page (document.documentElement), not
  // mapContainerRef.current -- the map container is a sibling of
  // ControlsPanel/AircraftDetailPanel, not their parent, so fullscreening
  // it alone would drop those DOM overlays from the fullscreen view (the
  // info-box labels are unaffected either way -- they're a MapLibre layer
  // rendered inside mapContainerRef's own canvas, see INFO_BOX_LAYER_ID).
  function handleToggleFullscreen() {
    if (document.fullscreenElement != null) {
      void document.exitFullscreen();
    } else {
      void document.documentElement.requestFullscreen();
    }
  }

  // The basemap's own text-bearing layer ids, computed once on "load" (see
  // basemapLabelLayerIds) -- the basemap style doesn't gain/lose layers at
  // runtime, so there's no need to recompute this on every toggle.
  const basemapLabelLayerIdsRef = useRef<string[]>([]);
  const [hoveredId, setHoveredId] = useState<string | null>(null);

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

  // Kept in a ref so the map's 'dragstart' listener (attached once, on
  // mount) always reads the *current* Follow target, not whatever it was
  // when the listener was attached.
  const followIdRef = useRef(followId);
  followIdRef.current = followId;

  // Shared by every place a user's manual navigation should cancel Follow
  // -- the mount effect's dragstart handler below, and the Center/recenter
  // button (handleRecenter below), rather than repeating `setFollowId(null)`
  // inline at each call site.
  function cancelFollow() {
    setFollowId(null);
  }

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
      center: config.center ? [config.center.longitude, config.center.latitude] : [0, 0],
      zoom: config.center ? 9 : 1,
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

    // A genuine pointer/touch-driven drag should cancel Follow -- fighting
    // the operator's own input is exactly the bug this fixes. Follow's own
    // recenter, Zoom To, and the recenter button all move the camera via
    // `easeTo`, which never carries `originalEvent`, so those never reach
    // this as a cancel (see shouldCancelFollowOnDrag).
    map.on("dragstart", (e) => {
      if (shouldCancelFollowOnDrag(e, followIdRef.current)) cancelFollow();
    });

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

      // Static "center" range rings (100/150/200nmi) -- computed once from
      // config.center, which never changes after this component mounts (see
      // MapView above). Added before the trail/aircraft layers so they
      // render beneath live traffic. Not part of SELECTABLE_LAYER_IDS, so
      // clicking a ring or its label never triggers aircraft selection.
      map.addSource(RANGE_RING_SOURCE_ID, {
        type: "geojson",
        data: rangeRingsFeatureCollection(config.center),
      });
      map.addLayer({
        id: RANGE_RING_LAYER_ID,
        type: "line",
        source: RANGE_RING_SOURCE_ID,
        paint: { "line-color": "#000000", "line-width": 1 },
      });

      map.addSource(RANGE_RING_LABEL_SOURCE_ID, {
        type: "geojson",
        data: rangeRingLabelsFeatureCollection(config.center),
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
          // selection is shown via the halo below (icon_scale >= 1) or the
          // separate AIRCRAFT_SELECTION_RING_LAYER_ID circle (icon_scale <
          // 1, added right after this layer below).
          "icon-color": ["get", "color"],
          "icon-halo-color": ["case", ["boolean", ["get", "selected"], false], "#ffffff", "#000000"],
          // #1806: three rounds of scaling this halo down for icon_scale < 1
          // (#1705, #1742/#1758, #1763/#1767) each reduced but never
          // eliminated a residual wash/box for small icon_scale, because the
          // wash's actual source can't be fixed by retuning icon-halo-width/
          // -blur at all -- see the derivation below. So icon_scale < 1 no
          // longer uses this halo; it's given a fixed-size, non-SDF
          // selection ring instead (AIRCRAFT_SELECTION_RING_LAYER_ID). This
          // halo now only ever applies -- at its original fixed values,
          // exactly as MapLibre already renders it correctly -- when
          // icon_scale >= 1 (e.g. B77L at 1.435, confirmed clean in #1806).
          //
          // The math (symbol_sdf.fragment.glsl, verified against the real
          // vendored shader, not just the style-spec docs):
          //   halo_edge  = (6 - halo_width/fontScale) / SDF_PX
          //   gamma_halo = (halo_blur * 1.19/SDF_PX + EDGE_GAMMA) / fontScale
          // (u_gamma_scale ~= 1; fontScale is this layer's icon-size, i.e.
          // 0.55 * icon_scale). A background texel (SDF value 0, i.e.
          // anywhere outside SDF_RADIUS_PX of the silhouette) gets *some*
          // nonzero halo alpha whenever `halo_edge - gamma_halo < 0` -- and
          // gets *fully opaque* halo colour once the texel's own SDF value
          // exceeds `halo_edge + gamma_halo`. That's the box: once the lower
          // bound goes negative, there's no distance value low enough to stay
          // fully transparent, so the "ring" smears across the entire
          // bounding square instead of sitting only at the silhouette's edge.
          //
          // Both halo_width's and halo_blur's own contributions to that
          // bound are already scale-invariant for icon_scale < 1 (each
          // carries the same icon_scale factor as fontScale, so it cancels
          // -- see the #1763 test below). But EDGE_GAMMA is a shader-side
          // constant MapLibre adds unconditionally, with no matching
          // icon_scale factor to cancel against -- so EDGE_GAMMA/fontScale
          // grows without bound as icon_scale shrinks, and neither
          // icon-halo-width nor icon-halo-blur can subtract it back out:
          // both only ever *add* to gamma_halo (regardless of sign of the
          // value fed in -- MapLibre's halo-blur is not meant to go
          // negative), so no combination of them can shrink an already-too-
          // large gamma_halo. Plugging in this layer's actual numbers shows
          // the lower bound is negative for *every* icon_scale < 1 (it's
          // strictly increasing in icon_scale, and already slightly negative
          // at the icon_scale = 1 reference point everything else here is
          // pinned to) -- i.e. this is a real mathematical floor of the SDF
          // halo technique as used here, not merely under-tuned constants.
          // See MapView.test.ts for the worked numbers, including the
          // issue's own icon_scale = 0.722 (E55P/C25B) and 0.989 (GALX/
          // GLF6) cases.
          "icon-halo-width": [
            "case",
            [
              "all",
              ["boolean", ["get", "selected"], false],
              [">=", ["coalesce", ["get", "icon_scale"], 1], 1],
            ],
            3,
            0,
          ],
          "icon-halo-blur": [
            "case",
            [
              "all",
              ["boolean", ["get", "selected"], false],
              [">=", ["coalesce", ["get", "icon_scale"], 1], 1],
            ],
            0.08,
            0,
          ],
          "icon-opacity": ["case", ["boolean", ["get", "stale"], false], 0.4, 1],
        },
      });

      // #1806: fixed-size (non-scaled) selection ring for icon_scale < 1,
      // replacing the icon's own icon-halo-* for that range -- see the long
      // comment on AIRCRAFT_LAYER_ID's icon-halo-width above for why the SDF
      // halo technique can't produce a clean fitted ring there at all, at
      // any icon-halo-width/-blur value. A plain circle layer has no SDF
      // texture-space math to overflow -- circle-radius/-stroke-width are
      // screen pixels the whole way through, so this renders identically
      // regardless of the selected aircraft's icon_scale. Shares
      // AIRCRAFT_SOURCE_ID (already carries `selected`/`icon_scale`/`stale`
      // per feature), so it needs no separate data-sync wiring. The filter
      // does the icon_scale >= 1 split instead of paint, since every paint
      // value here is otherwise unconditional. Not in SELECTABLE_LAYER_IDS
      // -- display-only, like TRACE_POINTS_CIRCLE_LAYER_ID.
      map.addLayer({
        id: AIRCRAFT_SELECTION_RING_LAYER_ID,
        type: "circle",
        source: AIRCRAFT_SOURCE_ID,
        filter: [
          "all",
          ["boolean", ["get", "selected"], false],
          ["<", ["coalesce", ["get", "icon_scale"], 1], 1],
        ],
        paint: {
          // Transparent fill -- only the stroke is drawn, so this never
          // occludes the aircraft icon it surrounds regardless of layer
          // order. Radius/stroke-width are a deliberately simple fixed
          // size (not derived from icon_scale -- that's the whole point),
          // chosen to roughly match the reference icon_scale = 1 icon's
          // on-screen footprint; needs visual confirmation against real
          // small-icon_scale aircraft (see #1806).
          "circle-radius": 10,
          "circle-color": "rgba(0, 0, 0, 0)",
          "circle-stroke-color": "#ffffff",
          "circle-stroke-width": 2,
          "circle-opacity": ["case", ["boolean", ["get", "stale"], false], 0.4, 1],
          "circle-stroke-opacity": ["case", ["boolean", ["get", "stale"], false], 0.4, 1],
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
      map.addLayer(
        {
          id: TRACE_POINTS_CIRCLE_LAYER_ID,
          type: "circle",
          source: TRACE_POINTS_SOURCE_ID,
          paint: {
            "circle-color": ["get", "color"],
            "circle-radius": 4,
            "circle-stroke-width": 1,
            "circle-stroke-color": ["get", "strokeColor"],
          },
        },
        // Inserted below TRAIL_LAYER_ID (#1794) -- otherwise these 8px dots
        // paint over the 2.5px trail line, and since both share the same
        // altitude color, tightly-spaced points (a loitering/holding-pattern
        // aircraft) merge into a solid blob that reads as "no line" at all.
        // The trail line now always paints on top, same as a route line over
        // waypoint markers in any other mapping app.
        TRAIL_LAYER_ID,
      );
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

      // Only SELECTABLE_LAYER_IDS ever drives selection/hover -- range
      // rings and their labels are intentionally not in that list, so they
      // can never be selected or hovered (see #1587).
      //
      // Both handlers query all of SELECTABLE_LAYER_IDS at once (via
      // queryRenderedFeatures) rather than registering one delegated
      // `map.on(event, layerId, ...)` per layer. AIRCRAFT_LAYER_ID and
      // TRAIL_HIT_AREA_LAYER_ID overlap by design (a trail's most recent
      // segment terminates exactly at the aircraft's own icon), and
      // MapLibre's delegated per-layer registration does its own
      // independent hit test per layer -- so the previous per-layer loop
      // invoked the click handler twice for a single physical click on an
      // icon, toggling selection on and immediately back off (see
      // mapHitTest.ts's topIcaoHex for the full writeup). Querying once
      // and taking the first hit guarantees exactly one decision per
      // physical event, regardless of how many layers the point
      // intersects.
      map.on("click", (e) => {
        const features = map.queryRenderedFeatures(e.point, { layers: SELECTABLE_LAYER_IDS as string[] });
        const icaoHex = topIcaoHex(features);
        if (!icaoHex) {
          // A click that misses every selectable feature is a background
          // click -- closes the detail panel and deselects (#1792), same
          // as the panel's own close button.
          setSelected(new Set());
          return;
        }
        setSelected((prev) => nextSelection(prev, icaoHex));
      });
      // Hover cursor + transient label-on-hover, consolidated the same
      // way for consistency (double-firing here was harmless -- both
      // per-layer registrations always agreed on the same icao_hex/cursor
      // -- but this avoids the same bug class if hover ever becomes
      // state-toggling). A single `mousemove` over the whole map, rather
      // than delegated `mouseenter`/`mouseleave`/`mousemove` per layer,
      // covers all three: cursor and hoveredId both reset to their "not
      // hovering" value the moment the query stops matching, which is
      // what mouseleave did explicitly before.
      map.on("mousemove", (e) => {
        const features = map.queryRenderedFeatures(e.point, { layers: SELECTABLE_LAYER_IDS as string[] });
        const icaoHex = topIcaoHex(features);
        map.getCanvas().style.cursor = icaoHex ? "pointer" : "";
        setHoveredId(icaoHex ?? null);
      });

      // Centered reference point -- a fixed marker from config, never
      // derived from received data. Rendered as a map layer (a GeoJSON
      // source feature, same as the range rings above), not a DOM
      // `Marker`: a DOM marker is appended into MapLibre's own canvas
      // container as a sibling of its WebGL canvas, which paints as one
      // opaque surface, so a DOM element is either entirely in front of
      // it (visible, but always on top of every aircraft icon -- the
      // original bug) or entirely behind it (behind *all* of the canvas's
      // paint, not just the icons on it -- invisible outright, this
      // regression). There is no z-index value that puts a sibling DOM
      // element behind only some of what a single canvas draws. Putting
      // the marker in the same paint pipeline as the aircraft icons
      // (`beforeId: AIRCRAFT_LAYER_ID`) makes "behind aircraft icons" an
      // ordinary layer-order concern instead, the same way the range
      // rings above are guaranteed to render beneath live traffic.
      map.addSource(CENTER_POINT_SOURCE_ID, {
        type: "geojson",
        data: centerPointFeatureCollection(config.center),
      });
      map.addLayer(
        {
          id: CENTER_POINT_CIRCLE_LAYER_ID,
          type: "circle",
          source: CENTER_POINT_SOURCE_ID,
          paint: {
            "circle-color": "#000000",
            "circle-radius": 6,
          },
        },
        AIRCRAFT_LAYER_ID,
      );

      // Info-box labels (issue #1808) -- a GPU-rendered symbol layer
      // replacing the removed DOM-based InfoBoxLayer.tsx component (one
      // absolutely-positioned <div> per labeled aircraft, restyled up to
      // 20Hz during pan/zoom/Follow -- the dominant cost behind sustained
      // >100% CPU with "Labels: All" on). Added last, with no `beforeId`,
      // so it stacks above every other layer added above (including
      // AIRCRAFT_LAYER_ID and the trace-point dots/labels) -- matching the
      // DOM version, which as a sibling overlay always painted above the
      // whole map canvas regardless of what was drawn on it.
      registerInfoBoxIcon(map);
      map.addSource(INFO_BOX_SOURCE_ID, { type: "geojson", data: EMPTY_FEATURE_COLLECTION });
      map.addLayer({
        id: INFO_BOX_LAYER_ID,
        type: "symbol",
        source: INFO_BOX_SOURCE_ID,
        layout: {
          // The stretchable rounded-rect background (lib/infoBoxIcon.ts),
          // resized per feature to fit its own rendered text via
          // icon-text-fit -- MapLibre's purpose-built "chat bubble behind
          // text" mechanism (9-slice image + content/stretchX/stretchY
          // metadata registered on INFO_BOX_ICON_ID). icon-text-fit-padding
          // approximates the removed DOM box's `px-1.5 py-1` Tailwind
          // padding (6px horizontal / 4px vertical) as [top, right,
          // bottom, left].
          "icon-image": INFO_BOX_ICON_ID,
          "icon-text-fit": "both",
          "icon-text-fit-padding": [4, 6, 4, 6],
          "icon-allow-overlap": true,
          "icon-ignore-placement": true,
          // Ident line first, larger (font-scale 1.15, roughly matching the
          // DOM box's 12px-vs-10.5px ident/detail size ratio), then the
          // altitude/speed and registration/type lines -- joined with "\n"
          // only when both halves are actually present (`hasBothLines`),
          // so an aircraft missing its ident (or missing every detail
          // line) never renders a stray blank line. See
          // lib/infoBoxSource.ts's infoBoxLabelFeature for how
          // identLine/detailLines/hasBothLines are derived from
          // lib/infoBox.ts's buildInfoBoxLines().
          "text-field": [
            "format",
            ["get", "identLine"],
            { "font-scale": 1.15 },
            ["case", ["get", "hasBothLines"], "\n", ""],
            {},
            ["get", "detailLines"],
            {},
          ] as SymbolLayout["text-field"],
          "text-size": INFO_BOX_TEXT_OFFSET_REFERENCE_PX,
          // Anchored at the aircraft's own point, offset diagonally
          // down-right by a zoom-scaled gap (lib/infoBoxOffset.ts) --
          // same anchor/offset relationship InfoBoxLayer.tsx's removed
          // `left: x + offset; top: y + offset` had, now expressed as a
          // MapLibre zoom expression (evaluated GPU/style-engine-side, no
          // per-frame JS cost) instead of a per-tick JS computation.
          "text-anchor": "top-left",
          "text-justify": "left",
          "text-offset": infoBoxTextOffsetZoomExpression() as SymbolLayout["text-offset"],
          "text-allow-overlap": true,
          "text-ignore-placement": true,
          // Boxes are free to overlap (no collision-avoidance nudging,
          // same as the removed DOM version) -- where they do, the
          // higher-altitude aircraft's box should draw on top. MapLibre's
          // per-feature paint-order control within one layer,
          // `symbol-sort-key`, replaces the DOM version's inline CSS
          // z-index (lib/labelStackOrder.ts's altitudeZIndex(), which the
          // source's `sortKey` property is set from -- see
          // lib/infoBoxSource.ts). Needs live visual confirmation (see
          // this PR's description) that ascending sort-key order actually
          // paints last/on-top for this MapLibre version, rather than
          // first/underneath.
          "symbol-sort-key": ["get", "sortKey"],
        },
        paint: {
          "text-color": "#ffffff",
        },
      });

      setMapLoaded(true);
    });

    return () => {
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
  // FeatureCollection whenever this is off/no center configured) into the
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

  // Coalesces this component's lifetime worth of sync-effect runs (below)
  // into at most one source update per MAP_SYNC_THROTTLE_MS -- see
  // syncThrottle.ts's module docstring for why (a WebSocket batch arrives
  // on a new `aircraft` object reference regardless of how many aircraft
  // it actually touched, and every dependency here forces the same
  // check). One throttle instance for this component's whole lifetime,
  // not per-render -- a fresh instance on every render would reset
  // `lastRunAt` each time and never actually coalesce anything.
  const syncThrottleRef = useRef(createTrailingThrottle(MAP_SYNC_THROTTLE_MS));

  // Drops any trailing-edge rebuild still pending when this component
  // unmounts, so it never fires against a map that mount effect's own
  // cleanup has already torn down (map.remove()).
  useEffect(() => {
    return () => syncThrottleRef.current.cancel();
  }, []);

  // Bookkeeping for the sync effect's incremental-diff path (#1775) --
  // read/written only inside that effect's throttled callback, never
  // rendered from, so plain refs rather than state. `prevAircraftRef`
  // is what diffAircraftMaps() compares each tick's `aircraft` against;
  // `syncedTrailSegmentIdsRef` is this MapView instance's own record of
  // which trail-segment feature ids it last pushed per icao_hex, needed
  // because TRAIL_SOURCE_ID holds a variable number of features per
  // aircraft (one per trail segment) rather than the aircraft source's
  // clean one-feature-per-hex mapping -- see featureCollections.ts's
  // buildTrailSourceDiff.
  const prevAircraftRef = useRef<AircraftMap>({});
  const syncedTrailSegmentIdsRef = useRef<Map<string, string[]>>(new Map());

  // The sync effect's own record of the *visibility-affecting* inputs
  // (everything the full feature-collection builders take besides
  // `aircraft` itself) as of its last run, so that run can tell "only
  // aircraft data changed this tick" (-> cheap incremental diff) apart
  // from "a toggle/selection changed too" (-> full rebuild). A toggle
  // like historyAll can flip visibility for an arbitrary, not-cheaply-
  // diffable subset of the whole fleet at once (e.g. "Trails: All" going
  // on reveals every tracked aircraft's trail simultaneously) -- rare and
  // user-driven, not the sustained-WS-traffic cost this issue targets, so
  // a full rebuild on *those* ticks is the right, proportionate trade-off
  // rather than trying to diff that case cheaply too.
  const prevVisibilityInputsRef = useRef<{
    historyAll: boolean;
    selected: Set<string>;
    isolateId: string | null;
    followId: string | null;
    protectedId: string | null;
    tracePointsEnabled: boolean;
  } | null>(null);

  // Same idea as prevVisibilityInputsRef, but for INFO_BOX_SOURCE_ID's own
  // (different) set of visibility-affecting inputs -- tracked separately
  // so a "Labels: All"/hover-only change doesn't force a full rebuild of
  // the aircraft/trail sources too (and vice versa: historyAll/
  // tracePointsEnabled don't affect which aircraft are labeled, so they're
  // deliberately absent here). isolateId/followId/protectedId are shared
  // with the aircraft/trail check above -- a label should never outlive,
  // or lag behind, its own icon's visibility (see featureCollections.ts's
  // isAircraftVisible, which both the aircraft and label feature builders
  // call).
  const prevLabelInputsRef = useRef<{
    selected: Set<string>;
    isolateId: string | null;
    followId: string | null;
    protectedId: string | null;
    labelsAll: boolean;
    hoveredId: string | null;
  } | null>(null);

  // --- Keep the aircraft/trail/info-box sources in sync ---------------
  //
  // Two paths per source, chosen fresh on every throttled run (#1775),
  // decided *independently* for the aircraft/trail sources vs.
  // INFO_BOX_SOURCE_ID (issue #1808) -- each has its own visibility-input
  // comparison (prevVisibilityInputsRef / prevLabelInputsRef) so e.g. a
  // hover-only change (label-relevant, not aircraft/trail-relevant) never
  // forces a full aircraft/trail rebuild, and a historyAll toggle
  // (aircraft/trail-relevant, not label-relevant) never forces a full
  // label rebuild:
  //
  // - Full rebuild (setData()): the first run ever, or any run where that
  //   source's own visibility-affecting inputs changed since the last run
  //   -- see prevVisibilityInputsRef/prevLabelInputsRef's own comments for
  //   why that case stays a full rebuild rather than trying to diff it too.
  // - Incremental diff (updateData()): every other run -- only `aircraft`
  //   itself changed, from live WS traffic. diffAircraftMaps() finds
  //   exactly which icao_hexes actually changed (by object reference, see
  //   aircraftMapDiff.ts) against the previous run's snapshot, once, up
  //   front -- shared by every source's diff path below, since it's the
  //   same underlying question ("which aircraft records actually changed
  //   this tick") for all three. featureCollections.ts's
  //   buildAircraftSourceDiff/buildTrailSourceDiff and lib/infoBoxSource.ts's
  //   buildInfoBoxLabelSourceDiff turn just those into a GeoJSONSourceDiff
  //   each -- so cost scales with how much of the fleet moved, not how
  //   large the fleet is. This is the fix for the profiled root cause:
  //   setData() reprocessing every feature from scratch on every tick
  //   regardless of how many aircraft actually changed (see the issue this
  //   implements), plus (#1808) doing that whole-fleet work even for the
  //   labeled-only subset InfoBoxLayer.tsx actually rendered.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded) return;

    syncThrottleRef.current.request(() => {
      const visibility = { isolateId, followId, protectedId: selectedIcaoHex };
      const visibilityInputs = {
        historyAll,
        selected,
        isolateId,
        followId,
        protectedId: selectedIcaoHex,
        tracePointsEnabled,
      };
      const prevInputs = prevVisibilityInputsRef.current;
      const visibilityChanged =
        !prevInputs ||
        prevInputs.historyAll !== historyAll ||
        prevInputs.selected !== selected ||
        prevInputs.isolateId !== isolateId ||
        prevInputs.followId !== followId ||
        prevInputs.protectedId !== selectedIcaoHex ||
        prevInputs.tracePointsEnabled !== tracePointsEnabled;
      prevVisibilityInputsRef.current = visibilityInputs;

      const labelFilter: LabelFilter = { selected, showAll: labelsAll, hoveredId };
      const labelInputs = { selected, isolateId, followId, protectedId: selectedIcaoHex, labelsAll, hoveredId };
      const prevLabelInputs = prevLabelInputsRef.current;
      const labelVisibilityChanged =
        !prevLabelInputs ||
        prevLabelInputs.selected !== selected ||
        prevLabelInputs.isolateId !== isolateId ||
        prevLabelInputs.followId !== followId ||
        prevLabelInputs.protectedId !== selectedIcaoHex ||
        prevLabelInputs.labelsAll !== labelsAll ||
        prevLabelInputs.hoveredId !== hoveredId;
      prevLabelInputsRef.current = labelInputs;

      // Computed once, up front, regardless of which branch(es) below need
      // it -- cheap (reference comparisons only, see aircraftMapDiff.ts),
      // and both the aircraft/trail diff path and the label diff path key
      // off the same changed-hex set.
      const changed = diffAircraftMaps(prevAircraftRef.current, aircraft);

      const aircraftSource = map.getSource(AIRCRAFT_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
      const trailSource = map.getSource(TRAIL_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
      const labelSource = map.getSource(INFO_BOX_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;

      if (visibilityChanged) {
        const fc = aircraftFeatureCollection(aircraft, selected, visibility);
        // Register the SDF image for every silhouette in the current set
        // that isn't registered yet, *before* the source data references
        // it -- a typical session touches a few dozen of the ~180 shapes.
        for (const f of fc.features) {
          const shape = f.properties?.shape;
          if (typeof shape === "string") registerShapeImage(map, shape);
        }
        aircraftSource?.setData(fc);

        const visibleTrailIds = historyAll ? new Set(Object.keys(aircraft)) : selected;
        const trailFc = trailFeatureCollection(aircraft, visibleTrailIds, visibility);
        trailSource?.setData(trailFc);
        // Re-sync this MapView instance's own bookkeeping of what's
        // actually in the source now, so the next data-only tick's
        // incremental diff starts from the right baseline.
        const bySegmentHex = new Map<string, string[]>();
        for (const f of trailFc.features) {
          const hex = f.properties?.icao_hex;
          if (typeof hex !== "string" || f.id == null) continue;
          const ids = bySegmentHex.get(hex) ?? [];
          ids.push(String(f.id));
          bySegmentHex.set(hex, ids);
        }
        syncedTrailSegmentIdsRef.current = bySegmentHex;
      } else if (changed.size > 0) {
        const aircraftDiff = buildAircraftSourceDiff(changed, aircraft, selected, visibility);
        for (const f of aircraftDiff.add ?? []) {
          const shape = f.properties?.shape;
          if (typeof shape === "string") registerShapeImage(map, shape);
        }
        aircraftSource?.updateData(aircraftDiff);

        const visibleTrailIds = historyAll ? new Set(Object.keys(aircraft)) : selected;
        const trailResult = buildTrailSourceDiff(
          changed,
          aircraft,
          visibleTrailIds,
          syncedTrailSegmentIdsRef.current,
          visibility,
        );
        trailSource?.updateData(trailResult.diff);
        syncedTrailSegmentIdsRef.current = trailResult.syncedSegmentIds;
      }

      // INFO_BOX_SOURCE_ID: same full-rebuild-vs-diff split as the
      // aircraft/trail sources above, but gated on labelVisibilityChanged
      // instead -- see prevLabelInputsRef's own comment for why this is a
      // separate flag. No per-feature shape image registration needed
      // (unlike the aircraft source): every label feature shares the one
      // fixed INFO_BOX_ICON_ID, registered once at map load.
      if (labelVisibilityChanged) {
        labelSource?.setData(infoBoxLabelFeatureCollection(aircraft, labelFilter, visibility));
      } else if (changed.size > 0) {
        labelSource?.updateData(buildInfoBoxLabelSourceDiff(changed, aircraft, labelFilter, visibility));
      }

      prevAircraftRef.current = aircraft;

      // Trace Points -- empty data when off or nothing selected, same
      // always-present-source convention as the layers above. Small (one
      // aircraft's own sample buffer, capped at MAX_TRACE_POINTS) and only
      // relevant while a detail panel is open, so this stays a plain
      // setData() on every run rather than joining the diff paths above.
      const tracePoints = tracePointsEnabled && selectedIcaoHex ? (aircraft[selectedIcaoHex]?.tracePoints ?? []) : [];
      (map.getSource(TRACE_POINTS_SOURCE_ID) as maplibregl.GeoJSONSource | undefined)?.setData(
        tracePointsFeatureCollection(tracePoints),
      );
    });
  }, [
    aircraft,
    selected,
    historyAll,
    mapLoaded,
    isolateId,
    followId,
    tracePointsEnabled,
    selectedIcaoHex,
    labelsAll,
    hoveredId,
  ]);

  // `selectedIcaoHex` is always still tracked when non-null, *except*
  // during the render right after a `remove` deletes an unprotected
  // aircraft out from under a stale selection -- practically unreachable
  // since selectedIcaoHex is exactly what protects it, but the lookup
  // stays defensive either way.
  const selectedAircraft = selectedIcaoHex ? aircraft[selectedIcaoHex] : undefined;

  function handleRecenter() {
    const map = mapRef.current;
    if (!map || !config.center) return;
    // Recentering is an explicit navigation action, same as a manual drag --
    // it should win outright rather than race Follow's own recenter effect,
    // which would otherwise re-fire on the very next `aircraft` update and
    // snap the view right back toward the followed aircraft.
    cancelFollow();
    // Preserves whatever zoom level the user is already at -- only the
    // center changes.
    map.easeTo({ center: [config.center.longitude, config.center.latitude] });
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

  // AircraftListPanel row click -- same single-select mechanism as clicking
  // an aircraft's icon on the map (see the "click" handler in the mount
  // effect above), reused rather than a second selection code path. Also
  // enables Isolate (#1791), matching the side panel's own Isolate button
  // for a freshly-selected aircraft -- picking an aircraft from the list is
  // exactly the "focus on just this one" gesture Isolate exists for.
  function handleSelectFromList(icaoHex: string) {
    setSelected((prev) => nextSelection(prev, icaoHex));
    setIsolateEnabled(true);
  }

  return (
    // Flex row: the map area (below) and AircraftListPanel are real
    // siblings, not an overlay on top of an unchanged-width map -- so the
    // drawer's own box width actually pushes the map narrower while open
    // (see the issue this implements). `min-w-0` overrides a flex item's
    // default `min-width: auto`, which would otherwise refuse to let the
    // map area shrink below its content's natural size.
    <div className="flex h-full w-full">
      <div className="relative min-w-0 flex-1">
        <div ref={mapContainerRef} className="h-full w-full" />
        {selectedAircraft && (
          <AircraftDetailPanel
            aircraft={selectedAircraft}
            center={config.center}
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
          historyAll={historyAll}
          onToggleHistoryAll={() => setHistoryAll((prev) => !prev)}
          labelsAll={labelsAll}
          onToggleLabelsAll={() => setLabelsAll((prev) => !prev)}
          mapLabelsOn={mapLabelsOn}
          onToggleMapLabels={() => setMapLabelsOn((prev) => !prev)}
          rangeOutlineVisible={rangeOutlineVisible}
          onToggleRangeOutline={() => setRangeOutlineVisible((prev) => !prev)}
          rangeOutlineDisabled={!config.center}
          onRecenter={handleRecenter}
          recenterDisabled={!config.center}
          fullscreen={fullscreen}
          onToggleFullscreen={handleToggleFullscreen}
          fullscreenDisabled={!fullscreenSupported}
        />
      </div>
      <AircraftListPanel
        aircraft={aircraft}
        aircraftCount={Object.keys(aircraft).length}
        wsConnected={connected}
        roster={roster}
        center={config.center}
        selected={selected}
        onSelect={handleSelectFromList}
      />
    </div>
  );
}
