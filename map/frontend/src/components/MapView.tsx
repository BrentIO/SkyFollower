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
  planRadarPlaybackFrames,
  RADAR_AMBIENT_CACHE_CAPACITY,
  RADAR_FRAME_INTERVAL_MS,
  RADAR_FRAME_LOAD_TIMEOUT_MS,
  RADAR_MAX_ZOOM,
  RADAR_MIN_ZOOM,
  RADAR_REFRESH_INTERVAL_MS,
  RADAR_TILE_SIZE,
  radarAmbientFrameId,
  radarFrameTileUrl,
  radarPlaybackFrameId,
  type RadarAmbientCacheEntry,
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

// Shared "nothing to draw" Trace Points buffer, so an unchanged-while-off
// state compares equal by reference tick to tick (see the sync effect).
const NO_TRACE_POINTS: readonly TracePoint[] = [];

// #1815: RANGE_RING_LABEL_LAYER_ID and TRACE_POINTS_LABEL_LAYER_ID below
// omit `text-font`, which defaults to MapLibre's own built-in stack,
// `["Open Sans Regular", "Arial Unicode MS Regular"]` -- a font this app's
// basemap style (maplibreSetup.ts's MAP_STYLE, openfreemap.org's
// "positron") doesn't actually serve at its `glyphs` URL. Every glyph
// range request for that stack 404s, and MapLibre falls back to rendering
// each codepoint locally rather than from the CDN's font atlas -- a real
// per-unique-codepoint cost (and a permanent stream of failed network
// requests) that's easy to mistake for "just how expensive text
// rendering is" rather than a fixable misconfiguration. The positron
// style's own layers all use "Noto Sans Regular"/"Bold"/"Italic" (the
// only stack its glyphs endpoint actually has); using the same one here
// avoids the 404/local-fallback path entirely.
const BASEMAP_TEXT_FONT = ["Noto Sans Regular"];

// Always-shown attribution -- the aircraft-silhouette credit (GPL-3.0, see
// the repo's THIRD-PARTY-NOTICES.md). The basemap style carries its own
// OSM/CARTO/OpenFreeMap attribution separately.
const BASE_CUSTOM_ATTRIBUTION =
  'Aircraft shapes © <a href="https://github.com/RexKramer1/AircraftShapesSVG" target="_blank" rel="noreferrer">RexKramer1</a> (GPL-3.0)';
// Appended alongside the above only while the radar layer is on (#1896) --
// public-domain NOAA data via Iowa Environmental Mesonet, see lib/radar.ts.
const RADAR_CUSTOM_ATTRIBUTION =
  'Radar © <a href="https://mesonet.agron.iastate.edu/" target="_blank" rel="noreferrer">Iowa Environmental Mesonet</a>';

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
  // Live weather radar overlay (#1896) -- on/off and opacity persist the
  // same way as the four toggles above; radarPlaying does not (see its
  // own useState below).
  const [radarOn, setRadarOn] = useState(() => loadPersistedControls().radarOn);
  const [radarOpacity, setRadarOpacity] = useState(() => loadPersistedControls().radarOpacity);
  // #2000: multiplies both the aircraft icon-size expression (below) and
  // InfoBoxLayer's rendered size -- see ControlsPanel.tsx's displayScale
  // prop doc for why this is a manual, persisted control rather than an
  // automatic devicePixelRatio-derived one. 1 is the no-op default, so an
  // operator who never touches the new control sees no change at all.
  const [displayScale, setDisplayScale] = useState(() => loadPersistedControls().displayScale);

  // Persists the control toggles above to localStorage on every change, so
  // a reload restores them via the lazy initializers above instead of
  // resetting to today's hardcoded defaults.
  useEffect(() => {
    savePersistedControls({
      historyAll,
      labelsAll,
      mapLabelsOn,
      rangeOutlineVisible,
      radarOn,
      radarOpacity,
      displayScale,
    });
  }, [historyAll, labelsAll, mapLabelsOn, rangeOutlineVisible, radarOn, radarOpacity, displayScale]);

  // Whether the last-30-minutes playback loop is animating -- transient UI
  // state, not persisted (a reload always starts paused on the current
  // snapshot, same as every other momentary action in this component,
  // e.g. isolateEnabled/tracePointsEnabled below). Turning radar off while
  // playing also stops playback, via handleToggleRadar below -- otherwise
  // turning it back on later would silently resume animating.
  const [radarPlaying, setRadarPlaying] = useState(false);
  // #1910: true only during the playback effect's frame-prefetch phase
  // (below) -- surfaced on the Play button as a loading spinner so the
  // pause before the loop visibly starts reads as "loading," not a
  // stalled click. Transient, like radarPlaying itself.
  const [radarPlaybackLoading, setRadarPlaybackLoading] = useState(false);
  // Which of the 7 per-frame playback layers (radarPlaybackFrameId) is
  // currently the one actually shown (raster-opacity > 0), so the
  // dedicated opacity-sync effect below knows which single layer to push
  // a live slider value to instead of blanket-applying it to all 7 (which
  // would make every frame visible simultaneously, defeating playback
  // entirely). Null whenever playback isn't running. A ref, not state --
  // updated every 500ms by the playback interval and only ever read by
  // an effect, never rendered.
  const radarActiveFrameIdRef = useRef<string | null>(null);
  // #1965: the ambient rolling cache -- keyed by ring-buffer slot (0..
  // RADAR_AMBIENT_CACHE_CAPACITY-1), value is the capture timestamp of
  // whatever's currently loaded into that slot's source. Refs, not state:
  // written every RADAR_REFRESH_INTERVAL_MS by an effect and read only by
  // other effects (the opacity sync and the playback planner), never
  // rendered directly.
  const radarAmbientCacheRef = useRef<Map<number, number>>(new Map());
  // Next ring-buffer slot to (re)capture into -- wraps at
  // RADAR_AMBIENT_CACHE_CAPACITY so the oldest entry is naturally what gets
  // overwritten first, exactly the "oldest evicted" behavior #1965 asks
  // for, without a separate eviction step.
  const radarNextAmbientSlotRef = useRef(0);
  // Which slot is the newest -- i.e. which one should be shown as "current"
  // whenever playback isn't running. Null only before the first ambient
  // capture completes.
  const radarNewestAmbientSlotRef = useRef<number | null>(null);
  // Mirrors of state that the ambient-capture interval (below) needs to
  // read at fire time without restarting the interval on every change --
  // restarting on radarOpacity would be harmless but wasteful, and
  // restarting on radarPlaying would defeat #1965's entire point (capture
  // is supposed to keep running, silently, through Play/Pause).
  const radarOpacityRef = useRef(radarOpacity);
  useEffect(() => {
    radarOpacityRef.current = radarOpacity;
  }, [radarOpacity]);
  const radarPlayingRef = useRef(radarPlaying);
  useEffect(() => {
    radarPlayingRef.current = radarPlaying;
  }, [radarPlaying]);
  function handleToggleRadar() {
    setRadarOn((prev) => {
      const next = !prev;
      if (!next) setRadarPlaying(false);
      return next;
    });
  }

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
  // ControlsPanel/AircraftDetailPanel/InfoBoxLayer, not their parent, so
  // fullscreening it alone would drop those DOM overlays from the
  // fullscreen view.
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
  // The manually-added AttributionControl instance (see the mount effect
  // below) -- kept so the radar-attribution effect can remove/replace it,
  // the only supported way to change `customAttribution` after
  // construction (#1896).
  const attributionControlRef = useRef<maplibregl.AttributionControl | null>(null);
  const [hoveredId, setHoveredId] = useState<string | null>(null);
  // InfoBoxLayer.tsx's per-aircraft screen position, kept in sync by the
  // map's "move" listener (mount effect below) and the data-sync effect
  // further down.
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

  // Whether the camera is currently centered on `config.center` -- drives
  // the Center button's active/inactive styling (#1847, ControlsPanel's
  // `recenterActive` prop). Recomputed on the map's `load` event (so the
  // button reads active immediately on first render, matching the initial
  // camera position set from `config.center` below) and on every
  // subsequent `moveend` (see the mount effect's `updateIsCentered`) --
  // deliberately *not* on `move`, which fires continuously (up to the
  // frame rate) during every pan/zoom/animation. This project has real,
  // repeated perf history around per-frame map-event costs (#1830/#1831's
  // idle-redraw-loop fix, #1838's trail-diffing rework); `moveend` fires
  // once when the camera actually settles, which is all this needs.
  const [isCentered, setIsCentered] = useState(false);

  // Kept in a ref so the map's 'dragstart' listener (attached once, on
  // mount) always reads the *current* Follow target, not whatever it was
  // when the listener was attached.
  const followIdRef = useRef(followId);
  followIdRef.current = followId;

  // Read by the map's "move" listener (mount effect below), which is
  // attached once and must read *current* aircraft positions rather than
  // closing over a stale snapshot from whenever it was attached. Same
  // reason as followIdRef above.
  const aircraftRef = useRef(aircraft);
  aircraftRef.current = aircraft;

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

  // One throttle instance for this component's whole lifetime (not
  // per-effect-run) so `"move"` -- which fires on every camera-transform
  // frame, continuously for the duration of any pan/pinch/easeTo -- can't
  // trigger an unthrottled full-fleet `project()` pass per frame. See
  // syncThrottle.ts's SCREEN_POSITION_THROTTLE_MS docstring for why this is
  // a separate, tighter window than the data-source sync's own throttle
  // below rather than reusing it.
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
      // never the map itself. Same lock pattern as every existing
      // MapLibre view in management-ui/frontend.
      pitchWithRotate: false,
      dragRotate: false,
      // No symbol fade-in/out. With the default 300ms fade, every live
      // data update (every MAP_SYNC_THROTTLE_MS) starts a new symbol
      // placement whose fade is still running when the next update
      // lands, so MapLibre's render loop never goes idle: an instrumented
      // repro (135 aircraft, trails/labels off, camera still) measured
      // ~57 full-map redraws/sec with `_placementDirty` re-arming the
      // loop after nearly every frame, matching a production DevTools
      // trace (~51 frames/sec, GPU process ~83% busy). With fade off:
      // ~9 redraws/sec and roughly a third of the Chrome process-tree CPU.
      // Symbols simply appear/disappear instead of fading.
      fadeDuration: 0,
      // The basemap style carries its own OSM/CARTO/OpenFreeMap attribution.
      // No `customAttribution` here -- the control itself is added manually
      // just below (attributionControlRef), not via this constructor
      // option, because its credit list needs to grow/shrink later (the
      // radar credit, #1896) and MapLibre's AttributionControl has no
      // public method to change `customAttribution` after construction;
      // removeControl()/addControl() with a fresh instance is the only
      // supported way to do that.
      attributionControl: false,
    });
    mapRef.current = map;
    attributionControlRef.current = new maplibregl.AttributionControl({
      customAttribution: BASE_CUSTOM_ATTRIBUTION,
    });
    map.addControl(attributionControlRef.current);
    map.touchZoomRotate.disableRotation();
    map.keyboard.disableRotation();

    // A genuine pointer/touch-driven drag should cancel Follow -- fighting
    // the operator's own input is exactly the bug this fixes. Follow's own
    // recenter, Zoom To, and the recenter button all move the camera via
    // `easeTo`, which never carries `originalEvent`, so those never reach
    // this as a cancel (see shouldCancelFollowOnDrag).
    map.on("dragstart", (e) => {
      if (shouldCancelFollowOnDrag(e, followIdRef.current)) cancelFollow();
    });

    // InfoBoxLayer.tsx's per-aircraft screen-position sync.
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

    // The throttled wrapper is what's actually registered on "move" below
    // -- syncScreenPositions() is still called directly, unthrottled, once
    // right after registration, so the initial paint isn't delayed by the
    // throttle's own window.
    function throttledSyncScreenPositions() {
      screenPositionThrottleRef.current.request(syncScreenPositions);
    }

    map.on("move", throttledSyncScreenPositions);
    syncScreenPositions();

    // #1847: recompute whether the camera is currently centered on
    // `config.center`, projecting both points through the map's current
    // transform (see lib/mapCentered.ts's isWithinCenterTolerance for why
    // pixel distance rather than a lat/lon epsilon). `config.center` is
    // captured directly from this effect's own closure -- it never changes
    // while this component is mounted (see this effect's closing comment).
    // Only attached/evaluated when a center is actually configured; when
    // it isn't, the Center button stays disabled and there's nothing to be
    // "centered on" (isCentered stays false, its initial value, and is
    // simply never recomputed).
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

      // #1844: capped well below the map's typical display zoom (initial
      // zoom is 9 above; infoBoxOffset.ts's MAX_OFFSET_ZOOM/MIN_OFFSET_ZOOM
      // (4-10) is this app's documented "normal" operating range). MapLibre's
      // GeoJSON worker source rebuilds + re-uploads a tile's ENTIRE bucket
      // whenever any feature inside it changes (geojson_source.ts's
      // shouldReloadTile checks tile bounds against the diff's affected
      // bounds, not per-feature) -- every ~500ms sync tick, so with History:
      // All spreading aircraft across most visible tiles at the default
      // zoom, most/all of them were getting invalidated and re-uploaded
      // every tick, each firing its own render (see #1844's traced
      // burst-then-idle FireAnimationFrame pattern). Capping maxzoom makes
      // MapLibre "over-zoom" a single cached low-zoom tile instead --
      // confirmed against the actual vendored `@maplibre/geojson-vt` +
      // `covering_tiles.ts` behavior in #1844's PR description, not just
      // reasoned about: at zoom 9 with maxzoom 8, a synthetic 150-aircraft
      // scatter across a 200nmi scope collapsed from 106 invalidatable
      // tiles to 36. This is *not* applied to TRAIL_SOURCE_ID, whose
      // LineStrings would visibly simplify at a low maxzoom -- that source
      // has its own tile-invalidation fix from #1840. Pure Point geometry
      // (this source) has no simplification downside, only a small, fixed
      // position-quantization error (measured ~10.8m worst-case at maxzoom
      // 8 -- sub-pixel through zoom ~14, a few px only at extreme zoom-in).
      map.addSource(AIRCRAFT_SOURCE_ID, { type: "geojson", data: EMPTY_FEATURE_COLLECTION, maxzoom: 8 });

      // #1816 (follow-up to #1806/#1813): dilated-silhouette outline for
      // icon_scale < 1, replacing #1813's fixed circle-radius ring -- that
      // circle didn't fit an elongated, non-circular airframe (too small
      // around a near-1 icon_scale shape's wingspan, floating with dead
      // space around a 0.6-floor shape's fuselage). This layer instead
      // draws the *same* per-shape SDF icon as AIRCRAFT_LAYER_ID below, at
      // a slightly larger icon-size -- the same "duplicate, enlarged copy
      // underneath" trick a CSS text-stroke uses. Added *before*
      // AIRCRAFT_LAYER_ID (this whole map is layer-order-is-paint-order,
      // like every other pair here) so the real icon paints over it and
      // only the enlarged silhouette's edge shows through, as a fitted
      // outline rather than a bounding circle.
      //
      // Deliberately plain icon-size scaling, not icon-halo-width/-blur --
      // sidesteps #1806's actual root cause entirely (AIRCRAFT_LAYER_ID's
      // icon-halo-width comment below has the full derivation): that bug
      // was specific to the SDF halo shader's EDGE_GAMMA term not
      // canceling against a shrinking fontScale, and has no equivalent
      // here since this is just two copies of the same well-formed SDF
      // image at two sizes, not a halo threshold in texture space.
      //
      // #1912: this layer used to be selected-only (a selection ring).
      // Reconsidering #1857 in light of the radar overlay's green-on-green
      // icon collision (and more generally, tar1090-style contrast against
      // any busy background) settled on a permanent thin black outline for
      // every aircraft, not just a selection indicator -- so both
      // icon-size and icon-color below are now data-driven on `selected`
      // instead of the layer's filter requiring it: unselected gets a
      // thin black outline (1.075x, an outline meant to be barely
      // noticed), selected keeps the original larger white ring (1.15x,
      // meant to stand out) unchanged. The two are visually distinct at a
      // glance and the selected case's on-screen gap sizing/rationale
      // below is unchanged from #1816.
      //
      // The 1.15 enlargement factor is chosen so the *absolute* on-screen
      // gap it produces at icon_scale's upper end (just under 1, e.g.
      // GLF6's 0.989) lands close to the 3px halo width AIRCRAFT_LAYER_ID's
      // real SDF halo already uses at icon_scale >= 1 -- continuity across
      // that boundary, not an arbitrary constant. The unselected 1.075
      // factor is half that excess (half the enlargement, roughly half the
      // visible gap) -- a deliberately subtler outline than the selection
      // ring, matching the thin-outline candidate picked from #1912's
      // rendered comparison against a live radar tile. Being a uniform
      // scale rather than a true constant-width morphological dilation,
      // the enlarged copy grows proportionally with the shape rather than
      // by a fixed pixel margin -- a wingtip or nose far from the icon's
      // own center picks up more visible gap than the fuselage sides do,
      // and the gap itself shrinks in absolute px as icon_scale drops
      // toward the 0.6 floor. Accepted trade-off per #1816: still reads as
      // an outline fitted to the actual airframe, which a circle never
      // did, at the cost of not being a perfectly even-width ring. A
      // future pre-baked, per-shape dilated SDF variant (also discussed in
      // #1816) could give a true constant-width outline if this proves not
      // enough, at the cost of the dilation amount needing to vary with
      // icon_scale to stay constant on screen -- meaningfully more
      // generation-time work, not attempted here.
      //
      // Cost note (#1912): this layer previously matched at most one
      // aircraft (the selected one, if any, and only if its icon_scale <
      // 1). It now matches every icon_scale < 1 aircraft on screen -- a
      // materially larger render cost than before, measured live rather
      // than assumed (see map/README.md's performance notes for the
      // reading this shipped with).
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
          // #2000: `displayScale` (a plain number, read from closure at
          // layer-creation time) is the operator-facing multiplier on top
          // of the base 0.55/icon_scale sizing -- 1 by default, a no-op.
          // This is a fixed value baked in at `map.on("load")` time, not
          // reactive on its own; the effect below pushes any later change
          // via setLayoutProperty, since MapLibre has no way to make a
          // layout property track outside state directly.
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
          // Per-aircraft silhouette -- feature property `shape` is the
          // AIRCRAFT_SHAPES key (aircraftIconResolver.ts); its SDF image is
          // registered under shapeIconId() lazily. `icon_scale` applies the
          // shape's real relative size on top of the base size.
          "icon-image": ["concat", "sf-ac-", ["get", "shape"]],
          "icon-rotate": ["get", "heading"],
          "icon-rotation-alignment": "map",
          "icon-allow-overlap": true,
          "icon-ignore-placement": true,
          // #2000: see AIRCRAFT_OUTLINE_LAYER_ID's icon-size comment above --
          // same displayScale multiplier, same "baked in at mount, kept
          // live by the effect below" mechanism, applied here too so the
          // outline and the real icon always stay in lockstep.
          "icon-size": ["*", 0.55, displayScale, ["coalesce", ["get", "icon_scale"], 1]],
        },
        paint: {
          // Icon fill is altitude-based; it never changes on selection --
          // selection is shown via the halo below (icon_scale >= 1) or the
          // dilated-silhouette AIRCRAFT_OUTLINE_LAYER_ID added just above
          // this layer (icon_scale < 1, #1816).
          "icon-color": ["get", "color"],
          "icon-halo-color": ["case", ["boolean", ["get", "selected"], false], "#ffffff", "#000000"],
          // #1806: three rounds of scaling this halo down for icon_scale < 1
          // (#1705, #1742/#1758, #1763/#1767) each reduced but never
          // eliminated a residual wash/box for small icon_scale, because the
          // wash's actual source can't be fixed by retuning icon-halo-width/
          // -blur at all -- see the derivation below. So icon_scale < 1 no
          // longer uses this halo; it's given a dilated-silhouette outline
          // instead (AIRCRAFT_OUTLINE_LAYER_ID, #1816). This halo only ever
          // applies -- at its original fixed values, exactly as MapLibre
          // already renders it correctly -- when icon_scale >= 1 (e.g. B77L
          // at 1.435, confirmed clean in #1806).
          //
          // #1912: below icon_scale = 1, unselected aircraft used to get
          // width/blur 0 here (no halo at all -- nothing stood in for a
          // permanent outline at that scale until AIRCRAFT_OUTLINE_LAYER_ID
          // was broadened to cover it). At/above icon_scale = 1, unselected
          // aircraft now get a small nonzero width/blur instead of 0 -- a
          // permanent thin black outline, the same tar1090-style contrast
          // fix, using the icon's own halo since it's already proven safe
          // in this range (no wash bug above icon_scale 1 -- see the
          // derivation below, which is entirely about icon_scale < 1).
          // Selected aircraft keep the original bold white ring unchanged.
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

      // The map's initial camera position is already `config.center` (see
      // this effect's `center`/`zoom` above) -- the Center button must
      // read active from first render, not only after an explicit click or
      // the first subsequent `moveend` (#1847).
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

  // #2000: display-scale multiplier -- a live `icon-size` layout-property
  // update only, never a layer rebuild. The two layers' addLayer calls
  // above already bake in whatever displayScale was at mount time (correct
  // for first paint); this effect is what makes a later slider change take
  // effect immediately, same "push a live property update on state change"
  // shape as the radar-opacity effect above uses for raster-opacity.
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

  // Radar attribution (#1896) -- swaps in a fresh AttributionControl with
  // the IEM/NOAA credit appended while radar is on, and back to the base
  // credit alone once it's off. removeControl()/addControl() with a new
  // instance is the only supported way to change `customAttribution` after
  // construction (see the mount effect's own comment on attributionControlRef).
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded || !attributionControlRef.current) return;
    map.removeControl(attributionControlRef.current);
    attributionControlRef.current = new maplibregl.AttributionControl({
      customAttribution: radarOn ? [BASE_CUSTOM_ATTRIBUTION, RADAR_CUSTOM_ATTRIBUTION] : BASE_CUSTOM_ATTRIBUTION,
    });
    map.addControl(attributionControlRef.current);
  }, [radarOn, mapLoaded]);

  // Ambient rolling cache (#1965, replacing #1896's single reused
  // current-snapshot source/layer) -- a small ring buffer of
  // RADAR_AMBIENT_CACHE_CAPACITY per-slot sources+layers (radarAmbientFrameId),
  // added/removed *whole* on radarOn, so there's still a hard guarantee
  // nothing fetches a tile while off. One capture fires immediately on
  // toggle-on, then again every RADAR_REFRESH_INTERVAL_MS -- deliberately
  // regardless of radarPlaying (read from a ref, not the dependency array),
  // because #1965's entire point is that this keeps filling the cache
  // *through* playback, not just before it. `beforeId: RANGE_RING_LAYER_ID`
  // matches #1896's original "above base map, below everything this app
  // draws" stacking.
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
        // Ring buffer wrapped back onto a previously-used slot -- retarget
        // (forces a reload, same as the old single-source refresh) rather
        // than removing/re-adding, so only this one slot's tiles are
        // re-fetched, not the whole cache.
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
      // Playback owns the visible radar layer for its own duration (see
      // the playback effect below) -- this capture still lands in the
      // cache either way, it just doesn't touch what's on screen, or which
      // slot is "newest," until playback's own cleanup restores the
      // current display.
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
      // Whichever ambient slot is newest is what's on screen as "current"
      // (see the ambient-capture effect above).
      const newest = radarNewestAmbientSlotRef.current;
      if (newest !== null) {
        const id = radarAmbientFrameId(newest);
        if (map.getLayer(id)) map.setPaintProperty(id, "raster-opacity", radarOpacity);
      }
    }
    // Only the one playback frame currently being shown (see
    // radarActiveFrameIdRef) -- every other frame layer stays at
    // raster-opacity 0 (still fully loaded, just invisible; see the
    // playback effect below for why visibility:none isn't used instead).
    // May itself be a reused ambient-cache layer id (#1965) -- setPaintProperty
    // doesn't care which effect originally added a given layer id.
    const activeId = radarActiveFrameIdRef.current;
    if (activeId && map.getLayer(activeId)) {
      map.setPaintProperty(activeId, "raster-opacity", radarOpacity);
    }
  }, [radarOpacity, radarOn, radarPlaying, mapLoaded]);

  // Playback (#1896; rebuilt in #1910's 2nd attempt; reworked again in
  // #1965) -- steps through RADAR_PLAYBACK_OFFSETS_MINUTES (last 30
  // minutes, oldest to newest, ending on the current frame) on a fixed
  // interval, looping continuously while `radarPlaying` is true.
  //
  // #1910's first attempt (a single reused source, retargeted via
  // setTiles()) was verified live to never actually become fast: MapLibre
  // requests a raster source's *entire* zoom pyramid on every retarget
  // (z0-z8, ~20 tile requests per frame), serialized across 7 frames, 20-30+
  // real seconds before playback started. #1910's 2nd attempt (one
  // source+layer per frame, all loaded in parallel, animated by toggling
  // raster-opacity) fixed *how* loading is detected and animated, but not
  // *how much* has to be fetched: every Play press still fetched all 7
  // frames from scratch. #1965 closes that gap by planning against the
  // ambient rolling cache (see the effect above) first -- planRadarPlaybackFrames
  // reuses whichever already-loaded ambient frames are close enough to a
  // target slot, and only the actual gaps get a fresh
  // source+layer+fetch, same flip-book mechanism #1910 already built.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded || !radarOn || !radarPlaying) return;
    // TS doesn't carry the null-narrowing above into the nested `function`
    // declarations below (they're not treated as immediately-invoked) --
    // this re-binding gives them a non-nullable reference to close over.
    const radarMap = map;

    let cancelled = false;
    let interval: ReturnType<typeof setInterval> | undefined;
    let rafHandle: number | undefined;

    // Snapshot the cache once, at the moment Play is pressed -- re-planning
    // mid-loop would mean an already-animating frame's identity changing
    // underneath it.
    const cacheEntries: RadarAmbientCacheEntry[] = Array.from(radarAmbientCacheRef.current.entries()).map(
      ([slot, timestampMs]) => ({ slot, timestampMs }),
    );
    const plan = planRadarPlaybackFrames(cacheEntries, Date.now());
    const frameIds = plan.map((step) =>
      step.source.kind === "ambient" ? radarAmbientFrameId(step.source.slot) : radarPlaybackFrameId(step.offsetMinutes),
    );
    const fetchedFrameIds = plan
      .filter((step) => step.source.kind === "fetch")
      .map((step) => radarPlaybackFrameId(step.offsetMinutes));

    // Hide every ambient slot's own "current" display for the loop's
    // duration -- including whichever one(s) this plan reuses as a
    // playback frame; the loop's own opacity-toggling below is what turns
    // the right one back on at the right moment.
    cacheEntries.forEach(({ slot }) => {
      const id = radarAmbientFrameId(slot);
      if (radarMap.getLayer(id)) radarMap.setPaintProperty(id, "raster-opacity", 0);
    });

    plan.forEach((step) => {
      if (step.source.kind !== "fetch") return;
      const id = radarPlaybackFrameId(step.offsetMinutes);
      map.addSource(id, {
        type: "raster",
        tiles: [radarFrameTileUrl(step.offsetMinutes)],
        tileSize: RADAR_TILE_SIZE,
        minzoom: RADAR_MIN_ZOOM,
        maxzoom: RADAR_MAX_ZOOM,
      });
      // Every layer stays layout-visible the whole time -- a raster
      // layer's tiles are only ever requested for a source whose layer is
      // actually visible (verified live: visibility:"none" here meant
      // the tiles never loaded at all, opacity or not, even well past
      // RADAR_FRAME_LOAD_TIMEOUT_MS). raster-opacity 0 is the paint-only
      // equivalent of "hidden" that doesn't gate loading -- every frame
      // genuinely loads in parallel, and "which one is shown" is purely
      // which one currently has a nonzero opacity.
      map.addLayer(
        {
          id,
          type: "raster",
          source: id,
          paint: { "raster-opacity": 0 },
        },
        RANGE_RING_LAYER_ID,
      );
    });

    // Resolves once every one of the 7 frames reports loaded on 3
    // consecutive animation frames, or after RADAR_FRAME_LOAD_TIMEOUT_MS
    // total -- whichever comes first. Requiring 3 straight `true` reads
    // (not just one) guards against isSourceLoaded() reporting a false
    // positive on the very first tick, before MapLibre has dispatched any
    // tile request yet for a source that was only just added -- verified
    // live this false positive is real (an addSource+addLayer pair
    // doesn't synchronously start loading; checking again immediately
    // found every source trivially "loaded" with zero tiles actually
    // fetched). A frame reused from the ambient cache (#1965) was already
    // loaded before this effect ran at all, so it reads "loaded" on the
    // very first check -- the fewer gaps the plan above found, the sooner
    // this resolves. Polling via requestAnimationFrame rather than an
    // event/timeout pair per frame also sidesteps the separate fragility
    // #1910's first attempt hit: no dependency on which specific MapLibre
    // event fires when for a specific source.
    function waitForAllFramesToLoad(): Promise<void> {
      return new Promise((resolve) => {
        const deadline = Date.now() + RADAR_FRAME_LOAD_TIMEOUT_MS;
        let consecutiveLoadedReads = 0;
        function check() {
          if (cancelled) {
            resolve();
            return;
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
      // Only the frames this run actually fetched fresh -- reused
      // ambient-cache frames are owned by the ambient-capture effect above
      // and outlive this loop.
      fetchedFrameIds.forEach((id) => {
        if (map.getLayer(id)) map.removeLayer(id);
        if (map.getSource(id)) map.removeSource(id);
      });
      // Restore the ambient cache's own "current" display. Hide every
      // populated slot first -- whichever frame this loop happened to be
      // showing when it stopped may itself be an ambient slot -- then show
      // only the newest (which may have advanced past what this loop
      // started with, if a capture landed mid-playback).
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
    // intentionally excluded here too, same reasoning as the ambient-capture
    // effect above (the dedicated opacity effect keeps the active layer's
    // paint property current without needing to rebuild anything per drag).
  }, [radarOn, radarPlaying, mapLoaded]);

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
  // `trailSyncRef` is this MapView instance's own record (TrailSyncState,
  // #1838) of which trail-block feature ids it last pushed per icao_hex,
  // needed because TRAIL_SOURCE_ID holds a variable number of features per
  // aircraft (one per trail-block color run) rather than the aircraft
  // source's clean one-feature-per-hex mapping -- see featureCollections.ts's
  // buildTrailSourceDiff.
  const prevAircraftRef = useRef<AircraftMap>({});
  const trailSyncRef = useRef<Map<string, TrailSyncState>>(new Map());
  // null until the first sync, so that first run always writes the source.
  const syncedTracePointsRef = useRef<readonly TracePoint[] | null>(null);

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

  // --- Keep the aircraft/trail sources and InfoBoxLayer's screen
  // positions in sync -----------------------------------------------------
  //
  // Two paths for the aircraft/trail sources, chosen fresh on every
  // throttled run (#1775):
  //
  // - Full rebuild (setData()): the first run ever, or any run where the
  //   visibility-affecting inputs changed since the last run -- see
  //   prevVisibilityInputsRef's own comment for why that case stays a full
  //   rebuild rather than trying to diff it too.
  // - Incremental diff (updateData()): every other run -- only `aircraft`
  //   itself changed, from live WS traffic. diffAircraftMaps() finds
  //   exactly which icao_hexes actually changed (by object reference, see
  //   aircraftMapDiff.ts) against the previous run's snapshot, once, up
  //   front -- shared by both sources' diff paths below, since it's the
  //   same underlying question ("which aircraft records actually changed
  //   this tick") for both. featureCollections.ts's
  //   buildAircraftSourceDiff/buildTrailSourceDiff turn just those into a
  //   GeoJSONSourceDiff each -- so cost scales with how much of the fleet
  //   moved, not how large the fleet is. This is the fix for the profiled
  //   root cause: setData() reprocessing every feature from scratch on
  //   every tick regardless of how many aircraft actually changed (see the
  //   issue this implements).
  //
  // InfoBoxLayer.tsx's screen positions (recomputed unconditionally below,
  // every throttled tick) aren't part of that diff/rebuild split -- they're
  // plain per-aircraft `map.project()` output, cheap enough that there's no
  // separate "did visibility change" gate the way the MapLibre sources
  // need one. This covers position/metadata-only WS updates that the
  // map's own "move" listener (mount effect above) wouldn't catch on its
  // own, since that one only fires on camera movement.
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

      // Computed once, up front -- cheap (reference comparisons only, see
      // aircraftMapDiff.ts), and both the aircraft and trail diff paths
      // below key off the same changed-hex set.
      const changed = diffAircraftMaps(prevAircraftRef.current, aircraft);

      const aircraftSource = map.getSource(AIRCRAFT_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;
      const trailSource = map.getSource(TRAIL_SOURCE_ID) as maplibregl.GeoJSONSource | undefined;

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
        // incremental diff starts from the right baseline. Every block
        // feature just built for a hex shares that hex's `dimmed` value
        // (trailFeatureCollection computes it once per aircraft), so it's
        // read off the first feature seen rather than recomputed here.
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

      // InfoBoxLayer.tsx screen positions: recomputed every throttled tick
      // (see this effect's own module comment above for why this doesn't
      // join the diff/rebuild split above).
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

      // Trace Points -- empty data when off or nothing selected, same
      // always-present-source convention as the layers above. Small (one
      // aircraft's own sample buffer, capped at MAX_TRACE_POINTS) and only
      // relevant while a detail panel is open, so this stays a plain
      // setData() rather than joining the diff paths above -- but only when
      // the buffer actually changed (by reference, same contract as
      // aircraftMapDiff.ts): re-sending an identical/empty collection on
      // every tick still costs MapLibre a worker round-trip and tile reload.
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

  // InfoBoxLayer.tsx's props -- every tracked, positioned aircraft that
  // should show a label; InfoBoxLayer itself decides which of these
  // actually render a box (selected/showAll/hoveredId, passed through
  // below), same contract as before #1808's GPU-layer detour.
  const infoBoxItems: InfoBoxLayerItem[] = Object.values(aircraft)
    .filter(hasPosition)
    // Same bypass as aircraftFeatureCollection/trailFeatureCollection --
    // a Followed or currently-selected (panel-open) aircraft stays in
    // the info-box set even once hidden, instead of vanishing while its
    // panel is still open.
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
    // Flex row: the map area (below) and AircraftListPanel are real
    // siblings, not an overlay on top of an unchanged-width map -- so the
    // drawer's own box width actually pushes the map narrower while open
    // (see the issue this implements). `min-w-0` overrides a flex item's
    // default `min-width: auto`, which would otherwise refuse to let the
    // map area shrink below its content's natural size.
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
          onRecenter={handleRecenter}
          recenterDisabled={!config.center}
          recenterActive={isCentered}
          fullscreen={fullscreen}
          onToggleFullscreen={handleToggleFullscreen}
          fullscreenDisabled={!fullscreenSupported}
          radarOn={radarOn}
          onToggleRadar={handleToggleRadar}
          radarOpacity={radarOpacity}
          onRadarOpacityChange={setRadarOpacity}
          radarPlaying={radarPlaying}
          onToggleRadarPlaying={() => setRadarPlaying((prev) => !prev)}
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
