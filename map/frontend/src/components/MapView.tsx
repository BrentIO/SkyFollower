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
  hasPosition,
  isEmptySourceDiff,
  trailFeatureCollection,
  type TrailSyncState,
} from "../lib/featureCollections";
import { diffAircraftMaps } from "../lib/aircraftMapDiff";
import type { AircraftMap, TracePoint } from "../lib/aircraftState";
import {
  AIRCRAFT_LAYER_ID,
  AIRCRAFT_OUTLINE_LAYER_ID,
  AIRCRAFT_SOURCE_ID,
  CENTER_POINT_CIRCLE_LAYER_ID,
  CENTER_POINT_SOURCE_ID,
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
import { followTargetPosition, isFollowLost, shouldCancelFollowOnDrag } from "../lib/followTarget";
import {
  centerPointFeatureCollection,
  rangeRingLabelsFeatureCollection,
  rangeRingsFeatureCollection,
} from "../lib/rangeRings";
import { infoBoxOffsetForZoom } from "../lib/infoBoxOffset";
import { isWithinCenterTolerance } from "../lib/mapCentered";
import {
  nextRadarState,
  planRadarPlaybackFrames,
  RADAR_AMBIENT_CACHE_CAPACITY,
  RADAR_FRAME_INTERVAL_MS,
  RADAR_FRAME_LOAD_TIMEOUT_MS,
  RADAR_MAX_ZOOM,
  RADAR_MIN_ZOOM,
  RADAR_REFRESH_INTERVAL_MS,
  RADAR_TILE_SIZE,
  radarAmbientFrameId,
  radarFetchAction,
  radarFrameTileUrl,
  radarPlaybackFrameId,
  type RadarAmbientCacheEntry,
  type RadarFetchCacheEntry,
  type RadarState,
} from "../lib/radar";
import { topIcaoHex } from "../lib/mapHitTest";
import { nextSelection } from "../lib/selection";
import { readSelectionFromSearch, searchWithSelection } from "../lib/shareUrl";
import { createTrailingThrottle, MAP_SYNC_THROTTLE_MS, SCREEN_POSITION_THROTTLE_MS } from "../lib/syncThrottle";
import { tracePointsFeatureCollection } from "../lib/tracePoints";
import { aircraftNeedingHistorySeed } from "../lib/trailSeeding";
import { AircraftDetailPanel } from "./AircraftDetailPanel";
import { AircraftListPanel } from "./AircraftListPanel";
import { ControlsPanel } from "./ControlsPanel";
import { InfoBoxLayer, type InfoBoxLayerItem } from "./InfoBoxLayer";

// Empty singleton so an unchanged "off" state compares equal by reference
// tick to tick (see the sync effect).
const NO_TRACE_POINTS: readonly TracePoint[] = [];

// The basemap style (maplibreSetup.ts's MAP_STYLE) doesn't serve MapLibre's
// default font stack at its `glyphs` URL, so those layers would fall back to
// slow per-codepoint local rendering. "Noto Sans Regular" is what the style's
// own layers use and its glyphs endpoint actually has.
const BASEMAP_TEXT_FONT = ["Noto Sans Regular"];

// Aircraft-silhouette credit (GPL-3.0, see THIRD-PARTY-NOTICES.md); the
// basemap style carries its own attribution separately.
const BASE_CUSTOM_ATTRIBUTION =
  'Aircraft shapes © <a href="https://github.com/RexKramer1/AircraftShapesSVG" target="_blank" rel="noreferrer">RexKramer1</a> (GPL-3.0)';
// Appended alongside the above only while the radar layer is on.
const RADAR_CUSTOM_ATTRIBUTION =
  'Radar © <a href="https://mesonet.agron.iastate.edu/" target="_blank" rel="noreferrer">Iowa Environmental Mesonet</a>';

// Registers one silhouette's SDF image with MapLibre if not already present.
// Falls back to FALLBACK_SHAPE art for an unknown shape key; swallows a
// canvas failure so `icon-image` just resolves to nothing rather than
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

// Fetches runtime config once before the map mounts, since initial
// center/zoom and the center marker depend on it.
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

// Full-viewport MapLibre live map: aircraft icons, live trails, floating
// info boxes, top-right controls, and a "center" reference-point marker.
// Only mounted once `config` has resolved (see MapView above), so every
// `config.center` read below is a plain, already-loaded value.
function MapViewInner({ config }: { config: AppConfig }) {
  const [selected, setSelected] = useState<Set<string>>(new Set());
  // `selected` is always max-one-element (see lib/selection.ts's
  // nextSelection). Computed up front since useMapFlights below needs it
  // for eviction deferral.
  const selectedIcaoHex = selected.values().next().value ?? null;

  const { aircraft, connected, seedTrailFor, seedTrailForMany, releaseHold } = useMapFlights(
    config.wsUrl,
    config.restFlightsUrl,
    selectedIcaoHex,
  );
  const roster = useProcessorRoster(config.restProcessorsUrl);

  const mapContainerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [mapLoaded, setMapLoaded] = useState(false);

  // Persisted per-browser (lib/controlsPersistence.ts), falling back to
  // hardcoded defaults for a fresh browser or blocked storage.
  const [historyAll, setHistoryAll] = useState(() => loadPersistedControls().historyAll);
  const [labelsAll, setLabelsAll] = useState(() => loadPersistedControls().labelsAll);
  // Polling (useRangeOutline below) only happens while this is true.
  const [rangeOutlineVisible, setRangeOutlineVisible] = useState(
    () => loadPersistedControls().rangeOutlineVisible,
  );
  const rangeOutline = useRangeOutline(config.apiBaseUrl, rangeOutlineVisible && !!config.center);
  const [rangeRingsVisible, setRangeRingsVisible] = useState(
    () => loadPersistedControls().rangeRingsVisible,
  );
  // Basemap's own text labels, distinct from labelsAll (aircraft info boxes).
  const [mapLabelsOn, setMapLabelsOn] = useState(() => loadPersistedControls().mapLabelsOn);
  // Live weather radar overlay: a tri-state value driving a single
  // icon-column button (off -> on -> animate -> off on each click, see
  // ControlsPanel's Radar button and handleCycleRadar below). Only
  // "on"-ness persists across a reload -- "animate" never does (a reload
  // always starts paused on the current snapshot, if radar was left on).
  const [radarState, setRadarState] = useState<RadarState>(() =>
    loadPersistedControls().radarOn ? "on" : "off",
  );
  // Derived, not independent state, so every effect below can keep
  // reading these two plain booleans.
  const radarOn = radarState !== "off";
  const radarPlaying = radarState === "animate";
  const [radarOpacity, setRadarOpacity] = useState(() => loadPersistedControls().radarOpacity);
  // Multiplies both the aircraft icon-size expression (below) and
  // InfoBoxLayer's rendered size; 1 is the no-op default.
  const [displayScale, setDisplayScale] = useState(() => loadPersistedControls().displayScale);

  // Keeps storage in sync as the operator toggles these, so a reload
  // restores them via the lazy initializers above.
  useEffect(() => {
    savePersistedControls({
      historyAll,
      labelsAll,
      mapLabelsOn,
      rangeOutlineVisible,
      rangeRingsVisible,
      radarOn,
      radarOpacity,
      displayScale,
    });
  }, [
    historyAll,
    labelsAll,
    mapLabelsOn,
    rangeOutlineVisible,
    rangeRingsVisible,
    radarOn,
    radarOpacity,
    displayScale,
  ]);

  // True only during the playback effect's frame-prefetch phase, surfaced
  // as a loading spinner on the Radar button.
  const [radarPlaybackLoading, setRadarPlaybackLoading] = useState(false);
  // Which per-frame playback layer is currently shown (raster-opacity > 0),
  // so the opacity-sync effect below pushes slider updates to only that one
  // layer instead of all 7 at once. A ref: updated by the playback interval,
  // read only by effects, never rendered.
  const radarActiveFrameIdRef = useRef<string | null>(null);
  // Ambient rolling cache, keyed by ring-buffer slot, value is the capture
  // timestamp for that slot's source. Refs, not state: written by an
  // interval, read only by other effects.
  const radarAmbientCacheRef = useRef<Map<number, number>>(new Map());
  // Next ring-buffer slot to (re)capture into; wraps at
  // RADAR_AMBIENT_CACHE_CAPACITY so the oldest entry is naturally overwritten
  // first.
  const radarNextAmbientSlotRef = useRef(0);
  // Which slot is newest, i.e. shown as "current" whenever playback isn't
  // running. Null only before the first ambient capture completes.
  const radarNewestAmbientSlotRef = useRef<number | null>(null);
  // Fetched playback frames (the plan's "fetch" gaps, as opposed to ones
  // reused from the ambient cache above) persist across animate sessions
  // instead of being torn down the instant the operator cancels out of
  // animate -- a fetch that was mid-flight keeps loading in the background,
  // and a quick re-entry into animate can reuse it. Keyed by offsetMinutes;
  // torn down entirely only when radarOn itself goes false (see the
  // ambient-capture effect's cleanup below). See lib/radar.ts's
  // radarFetchAction for the pure decision of what to do with a cache entry.
  const radarPlaybackFetchCacheRef = useRef<Map<number, RadarFetchCacheEntry>>(new Map());
  // Mirrors of state the ambient-capture interval reads at fire time
  // without restarting on every change -- restarting on radarPlaying would
  // stop capture from running silently through Play/Pause.
  const radarOpacityRef = useRef(radarOpacity);
  useEffect(() => {
    radarOpacityRef.current = radarOpacity;
  }, [radarOpacity]);
  const radarPlayingRef = useRef(radarPlaying);
  useEffect(() => {
    radarPlayingRef.current = radarPlaying;
  }, [radarPlaying]);
  // Cycles the tri-state value forward (off -> on -> animate -> off).
  // Immediate -- no gating on radarPlaybackLoading -- so clicking during
  // animate's prefetch spinner instantly steps state rather than waiting
  // it out; the playback effect's own cleanup reacts to radarPlaying
  // flipping false, exactly as it always has.
  function handleCycleRadar() {
    setRadarState((prev) => nextRadarState(prev));
  }

  // Not persisted: transient browser-chrome state, not an operator
  // preference. `fullscreenEnabled` reflects permission/support and is
  // checked once rather than tracked live.
  const [fullscreenSupported] = useState(() => document.fullscreenEnabled);
  const [fullscreen, setFullscreen] = useState(() => document.fullscreenElement != null);

  // Esc, F11, and OS-level gestures exit fullscreen without going through
  // handleToggleFullscreen, so the button's state can't rely on optimistic
  // state set only inside that click handler.
  useEffect(() => {
    const handleFullscreenChange = () => setFullscreen(document.fullscreenElement != null);
    document.addEventListener("fullscreenchange", handleFullscreenChange);
    return () => document.removeEventListener("fullscreenchange", handleFullscreenChange);
  }, []);

  // Fullscreens the whole page, not mapContainerRef.current -- the map
  // container is a sibling of ControlsPanel/AircraftDetailPanel/InfoBoxLayer,
  // not their parent, so fullscreening it alone would drop those overlays.
  function handleToggleFullscreen() {
    if (document.fullscreenElement != null) {
      void document.exitFullscreen();
    } else {
      void document.documentElement.requestFullscreen();
    }
  }

  // Computed once on "load" -- the basemap style doesn't gain/lose layers
  // at runtime.
  const basemapLabelLayerIdsRef = useRef<string[]>([]);
  // Kept so the radar-attribution effect can remove/replace it, the only
  // supported way to change `customAttribution` after construction.
  const attributionControlRef = useRef<maplibregl.AttributionControl | null>(null);
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  // InfoBoxLayer.tsx's per-aircraft screen position, kept in sync by the
  // map's "move" listener and the data-sync effect below.
  const [screenPositions, setScreenPositions] = useState<
    Record<string, { x: number; y: number; offset: number }>
  >({});

  // Action-row state (see components/AircraftDetailPanel.tsx). isolateId is
  // *derived* so Isolate automatically re-targets to a newly-selected
  // aircraft instead of clearing. followId/tracePointsEnabled are plain
  // state since they deliberately do *not* carry over to a different
  // selection (see the reset effect below).
  const [isolateEnabled, setIsolateEnabled] = useState(false);
  const isolateId = isolateEnabled ? selectedIcaoHex : null;
  const [followId, setFollowId] = useState<string | null>(null);
  const [tracePointsEnabled, setTracePointsEnabled] = useState(false);

  // Drives the Center button's active/inactive styling. Recomputed on
  // `load` and on `moveend`, deliberately not on `move` (fires continuously
  // during pan/zoom/animation) -- see this project's per-frame map-event
  // perf history.
  const [isCentered, setIsCentered] = useState(false);

  // Kept in a ref so the map's 'dragstart' listener (attached once, on
  // mount) always reads the *current* Follow target.
  const followIdRef = useRef(followId);
  followIdRef.current = followId;

  // Read by the map's "move" listener, attached once, so it sees *current*
  // aircraft positions rather than a stale closure.
  const aircraftRef = useRef(aircraft);
  aircraftRef.current = aircraft;

  function cancelFollow() {
    setFollowId(null);
  }

  // Fires whenever the selection moves away from an aircraft. Follow and
  // Trace Points don't carry over to a different aircraft (unlike Isolate,
  // which is derived above so it does re-target), and any eviction deferred
  // while the panel was open (aircraftState.ts's pendingRemoval) is applied
  // now. Isolate is only reset on an actual close, not a mere reselect.
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

  // The icao_hex named in the URL's ?aircraft= param at mount, if any --
  // read once via a lazy initializer, held until selected + Zoomed To (or
  // abandoned) by the two effects below.
  const pendingDeepLinkIcaoHexRef = useRef<string | null>(readSelectionFromSearch(window.location.search));

  // Deep-link-on-load, part 1: once the aircraft named in the URL shows up
  // in tracked state, select it. Abandoned if the operator makes their own
  // selection first -- a manual click must never be stomped on by a deep
  // link resolving late.
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
  // and has a known position, Zoom To it (same mechanism as the panel's
  // own Zoom To button). One-shot: the ref is cleared right after so a
  // later manual deselect/reselect never re-triggers it.
  useEffect(() => {
    const pending = pendingDeepLinkIcaoHexRef.current;
    if (!pending || selectedIcaoHex !== pending) return;
    if (!deepLinkReadyToZoom(aircraft, pending, mapLoaded)) return;
    handleZoomTo();
    pendingDeepLinkIcaoHexRef.current = null;
  }, [aircraft, selectedIcaoHex, mapLoaded]);

  // A fresh selection (map click or Aircraft List row click) gets the same
  // one-shot recenter as Zoom To, once its position is known. Deliberately
  // not Follow: fires once per new selectedIcaoHex, never again as
  // `aircraft` keeps updating. centeredForIcaoHexRef tracks which selection
  // has already been centered for, so re-clicking the same aircraft is a
  // no-op while Isolate re-targeting a different one centers again.
  const centeredForIcaoHexRef = useRef<string | null>(null);
  useEffect(() => {
    if (selectedIcaoHex === null) {
      centeredForIcaoHexRef.current = null;
      return;
    }
    if (centeredForIcaoHexRef.current === selectedIcaoHex) return;
    if (!deepLinkReadyToZoom(aircraft, selectedIcaoHex, mapLoaded)) return;
    handleZoomTo();
    centeredForIcaoHexRef.current = selectedIcaoHex;
  }, [selectedIcaoHex, aircraft, mapLoaded]);

  // Keeps the address bar in sync with the current selection so a copied
  // link reproduces it. Skips the very first run (mount) so writing here
  // doesn't clobber an unresolved deep link still being resolved by the two
  // effects above. Uses history.replaceState, not pushState, so selection
  // changes don't pollute browser back-history.
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
  // preserving the active zoom. followTargetPosition keeps returning the
  // last known position even after it goes stale/hidden, so the map stays
  // parked there rather than jumping -- the "held in view" behavior on loss.
  useEffect(() => {
    const map = mapRef.current;
    if (!map) return;
    const target = followTargetPosition(aircraft, followId);
    if (!target) return;
    map.easeTo({ center: [target.lon, target.lat] });
  }, [aircraft, followId]);

  // When an aircraft is newly selected, pull its server-accumulated trail so
  // the drawn trail covers the whole flight, not just what this browser has
  // seen since connecting. Fires only on the unselected -> selected edge.
  const prevSelectedRef = useRef<Set<string>>(new Set());
  useEffect(() => {
    for (const icaoHex of selected) {
      if (!prevSelectedRef.current.has(icaoHex)) seedTrailFor(icaoHex);
    }
    prevSelectedRef.current = selected;
  }, [selected, seedTrailFor]);

  // "History: All" seeds every tracked aircraft not yet seeded, covering
  // both the initial flip and any aircraft that newly appears while it's on.
  // `historySeededRef` only grows, so it never re-fetches an aircraft.
  const historySeededRef = useRef<Set<string>>(new Set());
  useEffect(() => {
    const needed = aircraftNeedingHistorySeed(historyAll, Object.keys(aircraft), historySeededRef.current);
    if (needed.length === 0) return;
    for (const icaoHex of needed) {
      historySeededRef.current.add(icaoHex);
    }
    // Batched into one request instead of one fetchFlightHistory per
    // aircraft -- with 100+ tracked aircraft, N individual requests
    // dominated page-load latency (HTTP/1.1's ~6-in-flight cap queues the
    // rest). See issue #2052.
    seedTrailForMany(needed);
  }, [historyAll, aircraft, seedTrailForMany]);

  // One throttle instance for this component's whole lifetime so "move"
  // (fires every camera-transform frame during pan/pinch/easeTo) can't
  // trigger an unthrottled full-fleet `project()` pass per frame.
  const screenPositionThrottleRef = useRef(createTrailingThrottle(SCREEN_POSITION_THROTTLE_MS));
  useEffect(() => {
    return () => screenPositionThrottleRef.current.cancel();
  }, []);

  // --- Map construction (once) ---------------------------------------
  useEffect(() => {
    if (!mapContainerRef.current) return;

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: MAP_STYLE,
      center: config.center ? [config.center.longitude, config.center.latitude] : [0, 0],
      zoom: config.center ? 9 : 1,
      // Locked north-up -- the aircraft icon rotates via icon-rotate,
      // never the map itself.
      pitchWithRotate: false,
      dragRotate: false,
      // No symbol fade-in/out: with the default fade, every live data
      // update starts a new symbol placement whose fade is still running
      // when the next update lands, so MapLibre's render loop never goes
      // idle. Symbols simply appear/disappear instead.
      fadeDuration: 0,
      // Added manually below (attributionControlRef) instead of via this
      // option, since its credit list needs to grow/shrink later (radar
      // credit) and MapLibre has no public method to change
      // `customAttribution` after construction other than replacing the
      // control.
      attributionControl: false,
    });
    mapRef.current = map;
    attributionControlRef.current = new maplibregl.AttributionControl({
      customAttribution: BASE_CUSTOM_ATTRIBUTION,
    });
    map.addControl(attributionControlRef.current);
    map.touchZoomRotate.disableRotation();
    map.keyboard.disableRotation();

    // A genuine pointer/touch-driven drag cancels Follow. Programmatic
    // moves (Follow's own recenter, Zoom To, the recenter button) use
    // `easeTo`, which never carries `originalEvent`, so those never reach
    // this as a cancel (see shouldCancelFollowOnDrag).
    map.on("dragstart", (e) => {
      if (shouldCancelFollowOnDrag(e, followIdRef.current)) cancelFollow();
    });

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

    // Called directly, unthrottled, once right after registration below so
    // the initial paint isn't delayed by the throttle window.
    function throttledSyncScreenPositions() {
      screenPositionThrottleRef.current.request(syncScreenPositions);
    }

    map.on("move", throttledSyncScreenPositions);
    syncScreenPositions();

    // Projects both points through the map's current transform (see
    // lib/mapCentered.ts's isWithinCenterTolerance for why pixel distance
    // rather than a lat/lon epsilon). Only attached when a center is
    // actually configured; otherwise the Center button stays disabled.
    function updateIsCentered() {
      if (!config.center) return;
      const current = map.project(map.getCenter());
      const target = map.project([config.center.longitude, config.center.latitude]);
      setIsCentered(isWithinCenterTolerance(current, target));
    }
    if (config.center) {
      map.on("moveend", updateIsCentered);
    }


    map.on("load", () => {
      // Discover the basemap's own text-bearing layers before any overlay
      // layers are added below, so this list is purely the remote style's
      // own text layers (see basemapLabelLayerIds' own docstring).
      basemapLabelLayerIdsRef.current = basemapLabelLayerIds(map.getStyle().layers);

      // Every other shape is registered lazily the first time an aircraft
      // needs it (see registerShapeImage calls in the sync effect below).
      registerShapeImage(map, FALLBACK_SHAPE);

      // Static "center" range rings, computed once from config.center.
      // Added before the trail/aircraft layers so they render beneath live
      // traffic; not in SELECTABLE_LAYER_IDS, so clicking one never
      // triggers aircraft selection.
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
          "text-font": BASEMAP_TEXT_FONT,
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

      // Empty until the toggle is on and the first poll resolves (see the
      // effect below); not in SELECTABLE_LAYER_IDS, so never a click/hover
      // target.
      map.addSource(RANGE_OUTLINE_SOURCE_ID, { type: "geojson", data: EMPTY_FEATURE_COLLECTION });
      map.addLayer({
        id: RANGE_OUTLINE_LAYER_ID,
        type: "line",
        source: RANGE_OUTLINE_SOURCE_ID,
        // The API's vertices are [lon, lat, alt_ft]; MapLibre's 2-D `line`
        // layer only consumes the first two, so altitude is silently
        // ignored here -- expected, not a bug.
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
          // dimmed, instead of letting it disappear (see
          // featureCollections.ts's trailFeatureCollection).
          "line-opacity": ["case", ["boolean", ["get", "dimmed"], false], 0.35, 0.85],
        },
      });
      // Invisible, much wider line over the same geometry -- this is the
      // layer in SELECTABLE_LAYER_IDS, so click/hover get a generous target
      // while the rendered trail above stays as thin as it looks.
      map.addLayer({
        id: TRAIL_HIT_AREA_LAYER_ID,
        type: "line",
        source: TRAIL_SOURCE_ID,
        layout: { "line-cap": "round", "line-join": "round" },
        paint: { "line-color": "#000000", "line-width": 14, "line-opacity": 0 },
      });

      // maxzoom is capped below the map's typical display zoom: MapLibre's
      // GeoJSON worker source rebuilds and re-uploads a tile's entire bucket
      // whenever any feature inside it changes, so every sync tick was
      // invalidating most visible tiles and firing a render for each.
      // Capping maxzoom makes MapLibre over-zoom a single cached low-zoom
      // tile instead. Not applied to TRAIL_SOURCE_ID, whose LineStrings
      // would visibly simplify at a low maxzoom; pure Point geometry (this
      // source) only gets a small, fixed position-quantization error.
      map.addSource(AIRCRAFT_SOURCE_ID, { type: "geojson", data: EMPTY_FEATURE_COLLECTION, maxzoom: 8 });

      // Fitted silhouette outline for icon_scale < 1 -- a fixed-radius
      // circle doesn't fit an elongated airframe. Draws the same per-shape
      // SDF icon as AIRCRAFT_LAYER_ID at a larger icon-size, added *before*
      // it so the real icon paints over it and only the enlarged edge shows
      // through. Plain icon-size scaling rather than icon-halo-width/-blur,
      // since that halo technique fails below icon_scale 1 (see
      // AIRCRAFT_LAYER_ID's icon-halo-width comment). icon-size/icon-color
      // are data-driven on `selected`: a barely-visible 1.075x outline
      // unselected, a bolder 1.15x ring selected -- matches every
      // icon_scale < 1 aircraft on screen now, not just a selected one.
      map.addLayer({
        id: AIRCRAFT_OUTLINE_LAYER_ID,
        type: "symbol",
        source: AIRCRAFT_SOURCE_ID,
        filter: ["<", ["coalesce", ["get", "icon_scale"], 1], 1],
        layout: {
          "icon-image": ["concat", "sf-ac-", ["get", "shape"]],
          "icon-rotate": ["get", "heading"],
          "icon-rotation-alignment": "map",
          "icon-allow-overlap": true,
          "icon-ignore-placement": true,
          // `displayScale` is baked in at load time, not reactive; the
          // effect below pushes later changes via setLayoutProperty, since
          // MapLibre can't make a layout property track outside state.
          "icon-size": [
            "*",
            0.55,
            displayScale,
            ["coalesce", ["get", "icon_scale"], 1],
            ["case", ["boolean", ["get", "selected"], false], 1.15, 1.075],
          ],
        },
        paint: {
          "icon-color": ["case", ["boolean", ["get", "selected"], false], "#ffffff", "#000000"],
          "icon-opacity": ["case", ["boolean", ["get", "stale"], false], 0.4, 1],
        },
      });

      map.addLayer({
        id: AIRCRAFT_LAYER_ID,
        type: "symbol",
        source: AIRCRAFT_SOURCE_ID,
        layout: {
          // `shape` is the AIRCRAFT_SHAPES key (aircraftIconResolver.ts);
          // its SDF image is registered under shapeIconId() lazily.
          "icon-image": ["concat", "sf-ac-", ["get", "shape"]],
          "icon-rotate": ["get", "heading"],
          "icon-rotation-alignment": "map",
          "icon-allow-overlap": true,
          "icon-ignore-placement": true,
          // Same displayScale mechanism as AIRCRAFT_OUTLINE_LAYER_ID above,
          // so the outline and the real icon stay in lockstep.
          "icon-size": ["*", 0.55, displayScale, ["coalesce", ["get", "icon_scale"], 1]],
        },
        paint: {
          // Icon fill is altitude-based and never changes on selection;
          // selection is shown via the halo below (icon_scale >= 1) or
          // AIRCRAFT_OUTLINE_LAYER_ID (icon_scale < 1).
          "icon-color": ["get", "color"],
          "icon-halo-color": ["case", ["boolean", ["get", "selected"], false], "#ffffff", "#000000"],
          // Halo is disabled below icon_scale 1: MapLibre's SDF halo shader
          // adds a fixed EDGE_GAMMA term that doesn't scale down with icon
          // size, so past a threshold the halo alpha never reaches zero and
          // smears into a solid box instead of a ring -- not fixable by
          // retuning icon-halo-width/-blur (see MapView.test.ts for the
          // worked math). AIRCRAFT_OUTLINE_LAYER_ID's outline covers that
          // range instead. At/above icon_scale 1, a small width/blur adds a
          // permanent thin outline for unselected icons; selected icons keep
          // the original bold ring.
          "icon-halo-width": [
            "case",
            [
              "all",
              ["boolean", ["get", "selected"], false],
              [">=", ["coalesce", ["get", "icon_scale"], 1], 1],
            ],
            3,
            ["case", [">=", ["coalesce", ["get", "icon_scale"], 1], 1], 1, 0],
          ],
          "icon-halo-blur": [
            "case",
            [
              "all",
              ["boolean", ["get", "selected"], false],
              [">=", ["coalesce", ["get", "icon_scale"], 1], 1],
            ],
            0.08,
            ["case", [">=", ["coalesce", ["get", "icon_scale"], 1], 1], 0.02, 0],
          ],
          "icon-opacity": ["case", ["boolean", ["get", "stale"], false], 0.4, 1],
        },
      });

      // Always present, driven to an empty FeatureCollection when off (see
      // the sync effect below). Not in SELECTABLE_LAYER_IDS -- display-only.
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
        // Inserted below TRAIL_LAYER_ID -- otherwise these dots paint over
        // the thinner trail line, and tightly-spaced points (a
        // loitering/holding-pattern aircraft) merge into a solid blob.
        TRAIL_LAYER_ID,
      );
      map.addLayer({
        id: TRACE_POINTS_LABEL_LAYER_ID,
        type: "symbol",
        source: TRACE_POINTS_SOURCE_ID,
        layout: {
          "text-field": ["get", "label"],
          "text-font": BASEMAP_TEXT_FONT,
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

      // Range rings and their labels are deliberately not in
      // SELECTABLE_LAYER_IDS, so they can never be selected or hovered.
      //
      // Both handlers query all of SELECTABLE_LAYER_IDS at once rather than
      // registering a delegated handler per layer: AIRCRAFT_LAYER_ID and
      // TRAIL_HIT_AREA_LAYER_ID overlap by design (a trail's last segment
      // terminates at the aircraft's own icon), and per-layer registration
      // would hit-test each layer independently, invoking the click handler
      // twice for one physical click (see mapHitTest.ts's topIcaoHex).
      // Querying once and taking the first hit guarantees exactly one
      // decision per event.
      map.on("click", (e) => {
        const features = map.queryRenderedFeatures(e.point, { layers: SELECTABLE_LAYER_IDS as string[] });
        const icaoHex = topIcaoHex(features);
        if (!icaoHex) {
          // A click that misses every selectable feature is a background
          // click -- closes the detail panel and deselects.
          setSelected(new Set());
          return;
        }
        setSelected((prev) => nextSelection(prev, icaoHex));
      });
      map.on("mousemove", (e) => {
        const features = map.queryRenderedFeatures(e.point, { layers: SELECTABLE_LAYER_IDS as string[] });
        const icaoHex = topIcaoHex(features);
        map.getCanvas().style.cursor = icaoHex ? "pointer" : "";
        setHoveredId(icaoHex ?? null);
      });

      // Rendered as a map layer, not a DOM `Marker`: a DOM marker is a
      // sibling of MapLibre's WebGL canvas, which paints as one opaque
      // surface, so it can only be entirely in front of or behind all
      // aircraft icons -- there's no z-index that puts it behind only some
      // of a canvas's paint. `beforeId: AIRCRAFT_LAYER_ID` makes "behind
      // aircraft icons" an ordinary layer-order concern instead.
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

      // The Center button must read active from first render, not only
      // after an explicit click or the first subsequent `moveend`.
      updateIsCentered();

      setMapLoaded(true);
    });

    return () => {
      map.off("move", throttledSyncScreenPositions);
      map.remove();
      mapRef.current = null;
    };
    // Intentionally created once -- `config` (this component's own prop)
    // is only ever set once per page load by MapView above; it can change
    // between page loads (it's now a runtime GET /api/config fetch, not a
    // build-time constant), but never while this component is mounted.
  }, []);

  // Shows/hides the basemap's own text layers (computed once on load, see
  // basemapLabelLayerIdsRef above).
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded) return;
    const visibility = mapLabelsOn ? "visible" : "none";
    for (const layerId of basemapLabelLayerIdsRef.current) {
      map.setLayoutProperty(layerId, "visibility", visibility);
    }
  }, [mapLabelsOn, mapLoaded]);

  // A live `icon-size` layout-property update only, never a layer rebuild
  // -- makes a later slider change take effect immediately, since the two
  // layers' addLayer calls above only bake in displayScale at mount time.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded) return;
    map.setLayoutProperty(AIRCRAFT_LAYER_ID, "icon-size", [
      "*",
      0.55,
      displayScale,
      ["coalesce", ["get", "icon_scale"], 1],
    ]);
    map.setLayoutProperty(AIRCRAFT_OUTLINE_LAYER_ID, "icon-size", [
      "*",
      0.55,
      displayScale,
      ["coalesce", ["get", "icon_scale"], 1],
      ["case", ["boolean", ["get", "selected"], false], 1.15, 1.075],
    ]);
  }, [displayScale, mapLoaded]);

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

  // Unlike Range Outline above, this geometry is static (computed once from
  // config.center on load), so toggling `visibility` is enough.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded) return;
    const visibility = rangeRingsVisible ? "visible" : "none";
    map.setLayoutProperty(RANGE_RING_LAYER_ID, "visibility", visibility);
    map.setLayoutProperty(RANGE_RING_LABEL_LAYER_ID, "visibility", visibility);
  }, [rangeRingsVisible, mapLoaded]);

  // Swaps in a fresh AttributionControl with the radar credit appended
  // while on, and back to the base credit once off (see
  // attributionControlRef's own comment for why a fresh instance).
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded || !attributionControlRef.current) return;
    map.removeControl(attributionControlRef.current);
    attributionControlRef.current = new maplibregl.AttributionControl({
      customAttribution: radarOn ? [BASE_CUSTOM_ATTRIBUTION, RADAR_CUSTOM_ATTRIBUTION] : BASE_CUSTOM_ATTRIBUTION,
    });
    map.addControl(attributionControlRef.current);
  }, [radarOn, mapLoaded]);

  // A ring buffer of RADAR_AMBIENT_CACHE_CAPACITY per-slot sources+layers,
  // added/removed *whole* on radarOn so nothing fetches a tile while off.
  // Captures on toggle-on and every RADAR_REFRESH_INTERVAL_MS after,
  // deliberately regardless of radarPlaying (read from a ref, not the
  // dependency array) -- the cache must keep filling *through* playback,
  // not just before it.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded || !radarOn) return;
    const radarMap = map;

    function captureAmbientFrame() {
      const slot = radarNextAmbientSlotRef.current;
      radarNextAmbientSlotRef.current = (slot + 1) % RADAR_AMBIENT_CACHE_CAPACITY;
      const id = radarAmbientFrameId(slot);
      const url = radarFrameTileUrl(0);
      const existingSource = radarMap.getSource(id) as maplibregl.RasterTileSource | undefined;
      if (existingSource) {
        // Ring buffer wrapped onto a previously-used slot -- retarget rather
        // than removing/re-adding, so only this slot's tiles are re-fetched.
        existingSource.setTiles([url]);
      } else {
        radarMap.addSource(id, {
          type: "raster",
          tiles: [url],
          tileSize: RADAR_TILE_SIZE,
          minzoom: RADAR_MIN_ZOOM,
          maxzoom: RADAR_MAX_ZOOM,
        });
      }
      if (!radarMap.getLayer(id)) {
        radarMap.addLayer(
          { id, type: "raster", source: id, paint: { "raster-opacity": 0 } },
          RANGE_RING_LAYER_ID,
        );
      }
      radarAmbientCacheRef.current.set(slot, Date.now());

      const previousNewest = radarNewestAmbientSlotRef.current;
      radarNewestAmbientSlotRef.current = slot;
      // Playback owns the visible radar layer for its own duration; this
      // capture still lands in the cache but doesn't touch the display
      // until playback's own cleanup restores it.
      if (radarPlayingRef.current) return;
      radarMap.setPaintProperty(id, "raster-opacity", radarOpacityRef.current);
      if (previousNewest !== null && previousNewest !== slot) {
        const previousId = radarAmbientFrameId(previousNewest);
        if (radarMap.getLayer(previousId)) radarMap.setPaintProperty(previousId, "raster-opacity", 0);
      }
    }

    captureAmbientFrame();
    const interval = setInterval(captureAmbientFrame, RADAR_REFRESH_INTERVAL_MS);

    return () => {
      clearInterval(interval);
      for (let slot = 0; slot < RADAR_AMBIENT_CACHE_CAPACITY; slot++) {
        const id = radarAmbientFrameId(slot);
        if (radarMap.getLayer(id)) radarMap.removeLayer(id);
        if (radarMap.getSource(id)) radarMap.removeSource(id);
      }
      radarAmbientCacheRef.current.clear();
      radarNextAmbientSlotRef.current = 0;
      radarNewestAmbientSlotRef.current = null;
      // radarOn going false is the one hard "no radar network activity at
      // all" boundary -- any playback-fetch frame left over from a
      // cancelled-but-still-loading animate session (the playback effect
      // below otherwise leaves these alone on cancel) is torn down here.
      for (const offsetMinutes of radarPlaybackFetchCacheRef.current.keys()) {
        const id = radarPlaybackFrameId(offsetMinutes);
        if (radarMap.getLayer(id)) radarMap.removeLayer(id);
        if (radarMap.getSource(id)) radarMap.removeSource(id);
      }
      radarPlaybackFetchCacheRef.current.clear();
    };
  }, [radarOn, mapLoaded]);

  // Radar opacity -- a live `raster-opacity` paint-property update only,
  // never a tile re-fetch. Re-runs whenever the layer(s) that should carry
  // it might have just changed (radarOn/radarPlaying/mapLoaded), so a
  // freshly-shown layer immediately picks up the current slider value
  // rather than whatever stale default its own addLayer call used.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded) return;
    if (!radarPlaying) {
      // Whichever ambient slot is newest is on screen as "current".
      const newest = radarNewestAmbientSlotRef.current;
      if (newest !== null) {
        const id = radarAmbientFrameId(newest);
        if (map.getLayer(id)) map.setPaintProperty(id, "raster-opacity", radarOpacity);
      }
    }
    // Only the one playback frame currently shown -- every other frame
    // layer stays at raster-opacity 0 (still loaded, just invisible; see
    // the playback effect below for why visibility:none isn't used
    // instead). May be a reused ambient-cache layer id.
    const activeId = radarActiveFrameIdRef.current;
    if (activeId && map.getLayer(activeId)) {
      map.setPaintProperty(activeId, "raster-opacity", radarOpacity);
    }
  }, [radarOpacity, radarOn, radarPlaying, mapLoaded]);

  // Steps through RADAR_PLAYBACK_OFFSETS_MINUTES (last 30 minutes, oldest
  // to newest) on a fixed interval, looping while `radarPlaying` is true.
  // Each frame gets its own source+layer, loaded in parallel and animated
  // by toggling opacity, rather than retargeting a single source (which
  // forces MapLibre to re-request the entire zoom pyramid on every
  // retarget). planRadarPlaybackFrames first reuses whichever ambient-cache
  // frames (see the effect above) are close enough to a target slot, so
  // only actual gaps need a fresh fetch.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded || !radarOn || !radarPlaying) return;
    // TS doesn't carry the null-narrowing above into the nested `function`
    // declarations below -- this re-binding gives them a non-nullable ref.
    const radarMap = map;

    let cancelled = false;
    let interval: ReturnType<typeof setInterval> | undefined;
    let rafHandle: number | undefined;

    // Snapshot the cache once, at the moment Play is pressed -- re-planning
    // mid-loop would change an already-animating frame's identity.
    const cacheEntries: RadarAmbientCacheEntry[] = Array.from(radarAmbientCacheRef.current.entries()).map(
      ([slot, timestampMs]) => ({ slot, timestampMs }),
    );
    const plan = planRadarPlaybackFrames(cacheEntries, Date.now());
    const frameIds = plan.map((step) =>
      step.source.kind === "ambient" ? radarAmbientFrameId(step.source.slot) : radarPlaybackFrameId(step.offsetMinutes),
    );

    // Hide every ambient slot's own "current" display for the loop's
    // duration; the loop's own opacity-toggling below turns the right one
    // back on at the right moment.
    cacheEntries.forEach(({ slot }) => {
      const id = radarAmbientFrameId(slot);
      if (radarMap.getLayer(id)) radarMap.setPaintProperty(id, "raster-opacity", 0);
    });

    // For each gap the plan above found, consult radarPlaybackFetchCacheRef
    // (persists across animate sessions -- see its own declaration above)
    // rather than unconditionally fetching fresh every time animate starts:
    // "reuse" an already-loaded prior attempt outright, "wait" on one
    // that's unresolved but still within its retry cooldown, or "issue" a
    // fetch (fresh, or retargeting a stalled existing source past cooldown).
    const now = Date.now();
    plan.forEach((step) => {
      if (step.source.kind !== "fetch") return;
      const { offsetMinutes } = step;
      const action = radarFetchAction(radarPlaybackFetchCacheRef.current.get(offsetMinutes), now);
      if (action !== "issue") return;
      radarPlaybackFetchCacheRef.current.set(offsetMinutes, { attemptedAtMs: now, loaded: false });
      const id = radarPlaybackFrameId(offsetMinutes);
      const url = radarFrameTileUrl(offsetMinutes);
      const existingSource = map.getSource(id) as maplibregl.RasterTileSource | undefined;
      if (existingSource) {
        existingSource.setTiles([url]);
      } else {
        map.addSource(id, {
          type: "raster",
          tiles: [url],
          tileSize: RADAR_TILE_SIZE,
          minzoom: RADAR_MIN_ZOOM,
          maxzoom: RADAR_MAX_ZOOM,
        });
      }
      // Every layer stays layout-visible: a raster layer's tiles are only
      // requested for a source whose layer is actually visible, so
      // visibility:"none" would prevent loading entirely. raster-opacity 0
      // is the paint-only equivalent that doesn't gate loading.
      if (!map.getLayer(id)) {
        map.addLayer(
          { id, type: "raster", source: id, paint: { "raster-opacity": 0 } },
          RANGE_RING_LAYER_ID,
        );
      }
    });

    // Resolves once every frame reports loaded on 3 consecutive animation
    // frames, or after RADAR_FRAME_LOAD_TIMEOUT_MS -- whichever comes
    // first. Requiring 3 straight `true` reads guards against
    // isSourceLoaded() false-positiving on the very first tick, before
    // MapLibre has dispatched any tile request for a source just added. A
    // frame reused from the ambient cache or radarPlaybackFetchCacheRef's
    // own "reuse" case was already loaded before this effect ran, so it
    // reads "loaded" on the very first check.
    function waitForAllFramesToLoad(): Promise<void> {
      return new Promise((resolve) => {
        const deadline = Date.now() + RADAR_FRAME_LOAD_TIMEOUT_MS;
        let consecutiveLoadedReads = 0;
        function check() {
          if (cancelled) {
            resolve();
            return;
          }
          // Opportunistically mark any fetch-cache entry loaded the moment
          // its own source resolves, independent of whether every frame in
          // this run's plan has -- so a later animate session (even one
          // whose own wait loop never started because the operator
          // cancelled first) still finds this one reusable.
          for (const [offsetMinutes, entry] of radarPlaybackFetchCacheRef.current) {
            if (!entry.loaded && radarMap.isSourceLoaded(radarPlaybackFrameId(offsetMinutes))) {
              entry.loaded = true;
            }
          }
          const allLoaded = frameIds.every((id) => radarMap.isSourceLoaded(id));
          consecutiveLoadedReads = allLoaded ? consecutiveLoadedReads + 1 : 0;
          if (consecutiveLoadedReads >= 3 || Date.now() > deadline) {
            resolve();
            return;
          }
          rafHandle = requestAnimationFrame(check);
        }
        check();
      });
    }

    async function loadThenPlay() {
      setRadarPlaybackLoading(true);
      await waitForAllFramesToLoad();
      if (cancelled) return;
      setRadarPlaybackLoading(false);

      let frameIndex = 0;
      radarActiveFrameIdRef.current = frameIds[frameIndex];
      radarMap.setPaintProperty(frameIds[frameIndex], "raster-opacity", radarOpacity);
      interval = setInterval(() => {
        const previousId = frameIds[frameIndex];
        frameIndex = (frameIndex + 1) % frameIds.length;
        radarActiveFrameIdRef.current = frameIds[frameIndex];
        radarMap.setPaintProperty(frameIds[frameIndex], "raster-opacity", radarOpacity);
        radarMap.setPaintProperty(previousId, "raster-opacity", 0);
      }, RADAR_FRAME_INTERVAL_MS);
    }

    loadThenPlay();

    return () => {
      cancelled = true;
      setRadarPlaybackLoading(false);
      radarActiveFrameIdRef.current = null;
      if (rafHandle !== undefined) cancelAnimationFrame(rafHandle);
      if (interval) clearInterval(interval);
      // Fetched frames are not torn down here -- an in-flight fetch keeps
      // loading in the background instead of being discarded the instant
      // the operator cancels out of animate, and radarPlaybackFetchCacheRef
      // lets a subsequent animate session reuse whatever finished. Just
      // hide them; actual teardown happens only when radarOn itself goes
      // false (the ambient-capture effect's own cleanup).
      plan.forEach((step) => {
        if (step.source.kind !== "fetch") return;
        const id = radarPlaybackFrameId(step.offsetMinutes);
        if (map.getLayer(id)) map.setPaintProperty(id, "raster-opacity", 0);
      });
      // Restore the ambient cache's own "current" display: hide every
      // populated slot, then show only the newest (which may have advanced
      // if a capture landed mid-playback).
      const cache = radarAmbientCacheRef.current;
      cache.forEach((_timestampMs, slot) => {
        const id = radarAmbientFrameId(slot);
        if (map.getLayer(id)) map.setPaintProperty(id, "raster-opacity", 0);
      });
      const newest = radarNewestAmbientSlotRef.current;
      if (newest !== null) {
        const id = radarAmbientFrameId(newest);
        if (map.getLayer(id)) map.setPaintProperty(id, "raster-opacity", radarOpacityRef.current);
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- radarOpacity
    // intentionally excluded: the dedicated opacity effect above keeps the
    // active layer's paint property current without rebuilding per drag.
  }, [radarOn, radarPlaying, mapLoaded]);

  // Coalesces sync-effect runs (below) into at most one source update per
  // MAP_SYNC_THROTTLE_MS (see syncThrottle.ts). One throttle instance for
  // this component's whole lifetime, not per-render -- a fresh instance
  // per render would never actually coalesce anything.
  const syncThrottleRef = useRef(createTrailingThrottle(MAP_SYNC_THROTTLE_MS));

  // Drops any trailing-edge rebuild still pending on unmount, so it never
  // fires against a map the mount effect's cleanup already tore down.
  useEffect(() => {
    return () => syncThrottleRef.current.cancel();
  }, []);

  // Bookkeeping for the sync effect's incremental-diff path, read/written
  // only inside that effect's throttled callback, so plain refs rather
  // than state. `prevAircraftRef` is what diffAircraftMaps() compares each
  // tick's `aircraft` against; `trailSyncRef` records which trail-block
  // feature ids were last pushed per icao_hex, needed because
  // TRAIL_SOURCE_ID holds a variable number of features per aircraft (one
  // per trail-block color run), unlike the aircraft source's
  // one-feature-per-hex mapping (see featureCollections.ts's
  // buildTrailSourceDiff).
  const prevAircraftRef = useRef<AircraftMap>({});
  const trailSyncRef = useRef<Map<string, TrailSyncState>>(new Map());
  // null until the first sync, so that first run always writes the source.
  const syncedTracePointsRef = useRef<readonly TracePoint[] | null>(null);

  // Records the *visibility-affecting* inputs as of the sync effect's last
  // run, so it can tell "only aircraft data changed" (cheap incremental
  // diff) apart from "a toggle/selection changed too" (full rebuild). A
  // toggle like historyAll can flip visibility for an arbitrary subset of
  // the fleet at once, so those ticks get a full rebuild rather than a diff.
  const prevVisibilityInputsRef = useRef<{
    historyAll: boolean;
    selected: Set<string>;
    isolateId: string | null;
    followId: string | null;
    protectedId: string | null;
    tracePointsEnabled: boolean;
  } | null>(null);

  // Keeps the aircraft/trail sources and InfoBoxLayer's screen positions in
  // sync. Two paths for the aircraft/trail sources, chosen fresh each run:
  // full rebuild (setData()) on the first run or when visibility-affecting
  // inputs changed; incremental diff (updateData()) otherwise, via
  // diffAircraftMaps()/buildAircraftSourceDiff/buildTrailSourceDiff, so cost
  // scales with how much of the fleet moved rather than fleet size.
  // Screen positions are recomputed unconditionally every tick since
  // they're cheap per-aircraft `map.project()` output.
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

      // Cheap (reference comparisons only); both diff paths below key off
      // the same changed-hex set.
      const changed = diffAircraftMaps(prevAircraftRef.current, aircraft);

      const aircraftSource = map.getSource(AIRCRAFT_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
      const trailSource = map.getSource(TRAIL_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;

      if (visibilityChanged) {
        const fc = aircraftFeatureCollection(aircraft, selected, visibility);
        // Register each silhouette's SDF image before the source data
        // references it.
        for (const f of fc.features) {
          const shape = f.properties?.shape;
          if (typeof shape === "string") registerShapeImage(map, shape);
        }
        aircraftSource?.setData(fc);

        const visibleTrailIds = historyAll ? new Set(Object.keys(aircraft)) : selected;
        const trailFc = trailFeatureCollection(aircraft, visibleTrailIds, visibility);
        trailSource?.setData(trailFc);
        // Re-sync bookkeeping of what's actually in the source now, so the
        // next data-only tick's incremental diff starts from the right
        // baseline.
        const nextTrailSync = new Map<string, TrailSyncState>();
        const byHex = new Map<string, { ids: string[]; dimmed: boolean }>();
        for (const f of trailFc.features) {
          const hex = f.properties?.icao_hex;
          if (typeof hex !== "string" || f.id == null) continue;
          const entry = byHex.get(hex) ?? { ids: [], dimmed: Boolean(f.properties?.dimmed) };
          entry.ids.push(String(f.id));
          byHex.set(hex, entry);
        }
        for (const [hex, entry] of byHex) {
          const record = aircraft[hex];
          if (!record) continue;
          nextTrailSync.set(hex, { trailRef: record.trail, base: 0, dimmed: entry.dimmed, ids: entry.ids });
        }
        trailSyncRef.current = nextTrailSync;
      } else if (changed.size > 0) {
        const aircraftDiff = buildAircraftSourceDiff(changed, aircraft, selected, visibility);
        for (const f of aircraftDiff.add ?? []) {
          const shape = f.properties?.shape;
          if (typeof shape === "string") registerShapeImage(map, shape);
        }
        if (!isEmptySourceDiff(aircraftDiff)) aircraftSource?.updateData(aircraftDiff);

        const visibleTrailIds = historyAll ? new Set(Object.keys(aircraft)) : selected;
        const trailResult = buildTrailSourceDiff(changed, aircraft, visibleTrailIds, trailSyncRef.current, visibility);
        if (!isEmptySourceDiff(trailResult.diff)) trailSource?.updateData(trailResult.diff);
        trailSyncRef.current = trailResult.syncState;
      }

      {
        const offset = infoBoxOffsetForZoom(map.getZoom());
        const positions: Record<string, { x: number; y: number; offset: number }> = {};
        for (const a of Object.values(aircraft)) {
          if (!hasPosition(a)) continue;
          const p = map.project([a.lon, a.lat]);
          positions[a.icao_hex] = { x: p.x, y: p.y, offset };
        }
        setScreenPositions(positions);
      }

      prevAircraftRef.current = aircraft;

      // Only pushed when the buffer actually changed by reference --
      // re-sending an identical/empty collection every tick still costs
      // MapLibre a worker round-trip and tile reload.
      const tracePoints =
        tracePointsEnabled && selectedIcaoHex ? (aircraft[selectedIcaoHex]?.tracePoints ?? NO_TRACE_POINTS) : NO_TRACE_POINTS;
      if (tracePoints !== syncedTracePointsRef.current) {
        (map.getSource(TRACE_POINTS_SOURCE_ID) as maplibregl.GeoJSONSource | undefined)?.setData(
          tracePointsFeatureCollection(tracePoints),
        );
        syncedTracePointsRef.current = tracePoints;
      }
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

  // Defensive lookup: selectedIcaoHex is what protects an aircraft from
  // eviction, but the render right after a `remove` could briefly race it.
  const selectedAircraft = selectedIcaoHex ? aircraft[selectedIcaoHex] : undefined;

  function handleRecenter() {
    const map = mapRef.current;
    if (!map || !config.center) return;
    // Explicit navigation should win outright rather than race Follow's own
    // recenter effect, which would otherwise re-fire on the next `aircraft`
    // update and snap back toward the followed aircraft.
    cancelFollow();
    map.easeTo({ center: [config.center.longitude, config.center.latitude] });
  }

  // One-shot recenter on the selected aircraft, preserving the active zoom
  // (no `zoom` key) rather than snapping to a fixed close-up.
  function handleZoomTo() {
    const map = mapRef.current;
    if (!map || !selectedAircraft || selectedAircraft.lat == null || selectedAircraft.lon == null) return;
    map.easeTo({ center: [selectedAircraft.lon, selectedAircraft.lat] });
  }

  function handleToggleFollow() {
    if (!selectedIcaoHex) return;
    const turningOn = followId !== selectedIcaoHex;
    setFollowId(turningOn ? selectedIcaoHex : null);
    if (turningOn) handleZoomTo(); // Zoom To once, then the recenter effect above takes over.
  }

  // Same single-select mechanism as clicking an aircraft's icon on the map.
  // Also enables Isolate, matching the side panel's own Isolate button for
  // a freshly-selected aircraft.
  function handleSelectFromList(icaoHex: string) {
    setSelected((prev) => nextSelection(prev, icaoHex));
    setIsolateEnabled(true);
  }

  // InfoBoxLayer.tsx's props -- every tracked, positioned aircraft that
  // should show a label; InfoBoxLayer itself decides which actually render
  // a box (selected/showAll/hoveredId).
  const infoBoxItems: InfoBoxLayerItem[] = Object.values(aircraft)
    .filter(hasPosition)
    // A Followed or currently-selected (panel-open) aircraft stays in the
    // info-box set even once hidden, instead of vanishing mid-panel.
    .filter((a) => !a.hidden || isFollowLost(a, followId, selectedIcaoHex))
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
    // Flex row: the map area and AircraftListPanel are real siblings, so
    // the drawer's box width pushes the map narrower while open. `min-w-0`
    // overrides a flex item's default `min-width: auto`.
    <div className="flex h-full w-full">
      <div className="relative min-w-0 flex-1">
        <div ref={mapContainerRef} className="h-full w-full" />
        {mapLoaded && (
          <InfoBoxLayer
            items={infoBoxItems}
            selected={selected}
            showAll={labelsAll}
            hoveredId={hoveredId}
            displayScale={displayScale}
          />
        )}
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
          rangeRingsVisible={rangeRingsVisible}
          onToggleRangeRings={() => setRangeRingsVisible((prev) => !prev)}
          rangeRingsDisabled={!config.center}
          onRecenter={handleRecenter}
          recenterDisabled={!config.center}
          recenterActive={isCentered}
          fullscreen={fullscreen}
          onToggleFullscreen={handleToggleFullscreen}
          fullscreenDisabled={!fullscreenSupported}
          radarState={radarState}
          onCycleRadar={handleCycleRadar}
          radarOpacity={radarOpacity}
          onRadarOpacityChange={setRadarOpacity}
          radarPlaybackLoading={radarPlaybackLoading}
          displayScale={displayScale}
          onDisplayScaleChange={setDisplayScale}
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
