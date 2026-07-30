import * as maplibregl from "maplibre-gl";
import {
  TerraDraw,
  TerraDrawLineStringMode,
  TerraDrawPointMode,
  TerraDrawPolygonMode,
  TerraDrawSelectMode,
} from "terra-draw";
import { TerraDrawMapLibreGLAdapter } from "terra-draw-maplibre-gl-adapter";
import { ChevronDown, ChevronUp, Lock, MapPinPlusInside, Unlock } from "lucide-react";
import { mdiExportVariant, mdiFileImportOutline, mdiShapePolygonPlus, mdiVectorPolylinePlus } from "@mdi/js";
import { useEffect, useRef, useState } from "react";
import { AreaNameModal, IDENTIFIER_PATTERN } from "../components/AreaNameModal";
import { ConfirmModal } from "../components/ConfirmModal";
import { ImportAreaModal, type ImportedFeature } from "../components/ImportAreaModal";
import { MdiIcon } from "../components/MdiIcon";
import { createArea, deleteArea, geometryDisplayNoun, listAreas, updateArea, type Area } from "../api/areas";
import { ApiError } from "../api/client";
import { useToast } from "../hooks/useToast";
import { MAP_STYLE } from "../lib/maplibreSetup";

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value));
}

// GeoJSON Feature shape shared by the all-areas and single-area exports --
// geometry copied as-is, properties carrying the full Area shape minus
// geometry itself.
function areaToFeature(area: Area) {
  return {
    type: "Feature" as const,
    geometry: area.geometry,
    properties: {
      identifier: area.identifier,
      name: area.name,
      locked: area.locked,
    },
  };
}

function downloadGeoJson(featureCollection: unknown, filename: string): void {
  const blob = new Blob([JSON.stringify(featureCollection, null, 2)], {
    type: "application/geo+json",
  });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
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

// Visits every [lng, lat] coordinate pair in a geometry, regardless of
// type -- the one thing computeBounds/offsetGeometry actually need, so
// neither has to duplicate a type switch of its own.
function forEachCoordinate(geometry: Area["geometry"], fn: (coord: [number, number]) => void): void {
  switch (geometry.type) {
    case "Polygon":
      for (const ring of geometry.coordinates) for (const c of ring) fn(c as [number, number]);
      break;
    case "LineString":
      for (const c of geometry.coordinates) fn(c as [number, number]);
      break;
    case "Point":
      fn(geometry.coordinates as [number, number]);
      break;
  }
}

// Same shape as forEachCoordinate, but transforms instead of just visiting
// -- offsetGeometry's per-type mapping.
function mapCoordinates(
  geometry: Area["geometry"],
  fn: (coord: [number, number]) => [number, number],
): Area["geometry"] {
  switch (geometry.type) {
    case "Polygon":
      return {
        ...geometry,
        coordinates: geometry.coordinates.map((ring) => ring.map((c) => fn(c as [number, number]))),
      };
    case "LineString":
      return { ...geometry, coordinates: geometry.coordinates.map((c) => fn(c as [number, number])) };
    case "Point":
      return { ...geometry, coordinates: fn(geometry.coordinates as [number, number]) };
  }
}

function computeBounds(areas: Area[]): maplibregl.LngLatBoundsLike | null {
  let minLng = Infinity;
  let minLat = Infinity;
  let maxLng = -Infinity;
  let maxLat = -Infinity;
  let found = false;
  for (const area of areas) {
    forEachCoordinate(area.geometry, ([lng, lat]) => {
      found = true;
      if (lng < minLng) minLng = lng;
      if (lng > maxLng) maxLng = lng;
      if (lat < minLat) minLat = lat;
      if (lat > maxLat) maxLat = lat;
    });
  }
  return found ? [[minLng, minLat], [maxLng, maxLat]] : null;
}

// Rough average glyph advance width for a bold sans-serif, as a fraction of
// font size -- avoids depending on canvas measureText with the actual
// "Noto Sans Bold" (which isn't necessarily loaded as a usable browser
// font just because MapLibre's glyph server serves it -- see #620). Biased
// slightly wide on purpose, so a shape wraps a little early rather than
// text creeping past its edge.
const LABEL_FONT_SIZE_PX = 14;
const AVG_GLYPH_WIDTH_RATIO = 0.62;
// Shapes smaller than this on screen aren't worth wrapping into -- keeps
// the existing fixed-size/no-wrap behavior for anything genuinely tiny,
// matching the "still requires zooming in for tiny shapes" decision.
const MIN_FIT_WIDTH_PX = 40;

function estimateTextWidthPx(text: string): number {
  return text.length * LABEL_FONT_SIZE_PX * AVG_GLYPH_WIDTH_RATIO;
}

// Screen-space width (#590) -- Polygon/LineString only (a Point has no
// width to fit). Projects the shape's geographic bounding box through the
// live map (so it reflects the current zoom, not a fixed geographic size)
// to get an actual on-screen pixel width, then converts to the em-based
// unit MapLibre's text-max-width layout property expects. Returns
// undefined (falls back to MapLibre's own default wrap width, 10ems) when
// the shape is too small to bother fitting, or the name already fits at
// the default width without needing to wrap tighter.
function computeMaxWidthEms(map: maplibregl.Map, area: Area, name: string): number | undefined {
  if (area.geometry.type === "Point") return undefined;
  const bounds = computeBounds([area]);
  if (!bounds) return undefined;
  const [[minLng, minLat], [maxLng, maxLat]] = bounds as [[number, number], [number, number]];
  const centerLat = (minLat + maxLat) / 2;
  const left = map.project([minLng, centerLat]);
  const right = map.project([maxLng, centerLat]);
  const widthPx = Math.abs(right.x - left.x);
  if (widthPx < MIN_FIT_WIDTH_PX) return undefined;

  const availablePx = widthPx * 0.9; // small margin so text doesn't touch the shape's own edge
  if (estimateTextWidthPx(name) <= availablePx) return undefined; // already fits, no need to wrap tighter than default

  const maxWidthEms = availablePx / LABEL_FONT_SIZE_PX;
  return Math.max(maxWidthEms, 2); // a floor so a very narrow shape doesn't wrap to one character per line
}

function segmentLength(a: number[], b: number[]): number {
  const dx = b[0] - a[0];
  const dy = b[1] - a[1];
  return Math.sqrt(dx * dx + dy * dy);
}

// Midpoint by cumulative length along the line, not just the middle
// coordinate index -- a line with an uneven vertex spacing (e.g. one long
// leg and several short ones near an airport) would otherwise place the
// label well off-center visually.
function lineStringMidpoint(coordinates: number[][]): [number, number] {
  if (coordinates.length === 1) return [coordinates[0][0], coordinates[0][1]];
  const lengths: number[] = [];
  let total = 0;
  for (let i = 0; i < coordinates.length - 1; i++) {
    const len = segmentLength(coordinates[i], coordinates[i + 1]);
    lengths.push(len);
    total += len;
  }
  const half = total / 2;
  let accumulated = 0;
  for (let i = 0; i < lengths.length; i++) {
    const next = accumulated + lengths[i];
    if (next >= half || i === lengths.length - 1) {
      const t = lengths[i] > 0 ? (half - accumulated) / lengths[i] : 0;
      const [x1, y1] = coordinates[i];
      const [x2, y2] = coordinates[i + 1];
      return [x1 + (x2 - x1) * t, y1 + (y2 - y1) * t];
    }
    accumulated = next;
  }
  return [coordinates[0][0], coordinates[0][1]];
}

// Label anchor point per geometry type: Polygon keeps the plain outer-ring
// average (good enough for placement, not a claim of geometric precision --
// no turf dependency just for this); LineString uses the by-length
// midpoint above; Point is trivially itself.
function labelPosition(geometry: Area["geometry"]): [number, number] {
  switch (geometry.type) {
    case "Polygon": {
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
    case "LineString":
      return lineStringMidpoint(geometry.coordinates);
    case "Point":
      return [geometry.coordinates[0], geometry.coordinates[1]];
  }
}

// Terra Draw feature properties.mode must equal the owning mode's own
// name ("polygon"/"linestring"/"point") -- addFeatures() validates against
// it (see offsetGeometry's own comment on validation) -- so every place a
// feature is added to the map needs the mode name matching its actual
// geometry type, not a hardcoded "polygon".
function geometryToModeName(type: Area["geometry"]["type"]): "polygon" | "linestring" | "point" {
  switch (type) {
    case "Polygon":
      return "polygon";
    case "LineString":
      return "linestring";
    case "Point":
      return "point";
  }
}

// Narrows an arbitrary GeoJSON geometry (as returned by Terra Draw's own
// getSnapshotFeature -- typed loosely since it also handles modes/
// geometries this app never uses) down to the three types Area actually
// supports.
function isAreaGeometryType(type: string): type is Area["geometry"]["type"] {
  return type === "Polygon" || type === "LineString" || type === "Point";
}

// Reads a just-finished draw's actual geometry type off Terra Draw's own
// snapshot -- falls back to "Polygon" if the feature vanished (rejected by
// mode validation) or reports an unsupported type; the naming modal never
// opens for those cases anyway (see handleNameConfirm's own check), so this
// is only ever seen transiently before pendingDrawFeatureId is cleared.
function snapshotGeometryType(draw: TerraDraw | null, featureId: string): Area["geometry"]["type"] {
  const type = draw?.getSnapshotFeature(featureId)?.geometry.type;
  return type && isAreaGeometryType(type) ? type : "Polygon";
}

// Terra Draw's select-mode drag/vertex-edit flags per drawing-mode name --
// shared by the initial TerraDrawSelectMode construction and every later
// setSelectDraggable() call so the two can never drift apart. A Point
// feature has no midpoints/vertices distinct from the feature itself, so
// it only needs the feature-level draggable flag.
function selectModeFlags(locked: boolean) {
  const coordinates = { midpoints: !locked, draggable: !locked, deletable: !locked };
  return {
    polygon: { feature: { draggable: !locked, coordinates } },
    linestring: { feature: { draggable: !locked, coordinates } },
    point: { feature: { draggable: !locked } },
  };
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
  forEachCoordinate(geometry, ([lng, lat]) => {
    if (lng < minLng) minLng = lng;
    if (lng > maxLng) maxLng = lng;
    if (lat < minLat) minLat = lat;
    if (lat > maxLat) maxLat = lat;
  });
  // A Point's bbox is zero-sized (min === max on both axes), so the
  // fraction term is always 0 and only the MIN_OFFSET_DEGREES floor
  // applies -- still a visible, deliberate offset, not a no-op.
  const dLng = Math.max((maxLng - minLng) * 0.15, MIN_OFFSET_DEGREES);
  const dLat = Math.max((maxLat - minLat) * 0.15, MIN_OFFSET_DEGREES);
  return mapCoordinates(geometry, ([lng, lat]) => [
    roundCoordinate(lng + dLng),
    roundCoordinate(lat + dLat),
  ]);
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

  // Mobile-only accordion state for the area list -- ignored at the md+
  // breakpoint, where the list is always visible regardless (see the
  // className on the <ul> below).
  const [mobileListOpen, setMobileListOpen] = useState(false);

  const [pendingSwitch, setPendingSwitch] = useState<(() => void) | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<Area | null>(null);
  const [pendingDrawFeatureId, setPendingDrawFeatureId] = useState<string | null>(null);
  // Only set by duplicateArea(), to suggest "<name> copy" in the naming
  // modal -- a freshly drawn shape leaves this null and the modal starts
  // blank as before.
  const [pendingNameSuggestion, setPendingNameSuggestion] = useState<string | null>(null);
  // Drives the naming modal's "Name this area/line/point" title -- set
  // alongside pendingDrawFeatureId at both call sites that open it.
  const [pendingGeometryType, setPendingGeometryType] = useState<Area["geometry"]["type"]>("Polygon");
  // locked value to apply once the pending feature is actually created --
  // always false for a fresh draw/duplicate (existing behavior), but an
  // imported feature's properties.locked, when present, must survive
  // through to createArea() even if the identifier/name still need the
  // AreaNameModal detour.
  const [pendingLocked, setPendingLocked] = useState(false);
  const [importModalOpen, setImportModalOpen] = useState(false);

  const dirty = draft !== null && original !== null && JSON.stringify(draft) !== JSON.stringify(original);

  function requestSwitch(action: () => void) {
    if (dirty) {
      setPendingSwitch(() => action);
    } else {
      action();
    }
  }

  // The area-labels source's single source of truth (#590) -- called
  // whenever anything that affects a label's position or fit changes: the
  // saved area list, an in-progress drag/vertex edit (via
  // handleDrawChangeRef below, passing the in-progress geometry in place
  // of the saved one), and zoom (screen-space width changes even though
  // the shape's geographic size doesn't).
  function refreshLabelSource(areasForLabels: Area[]) {
    const map = mapRef.current;
    if (!map) return;
    const source = map.getSource("area-labels") as maplibregl.GeoJSONSource | undefined;
    if (!source) return;
    source.setData({
      type: "FeatureCollection",
      features: areasForLabels.map((area) => {
        const name = area.name || area.identifier;
        const maxWidthEms = computeMaxWidthEms(map, area, name);
        return {
          type: "Feature",
          properties: { name, ...(maxWidthEms !== undefined ? { maxWidthEms } : {}) },
          geometry: { type: "Point", coordinates: labelPosition(area.geometry) },
        };
      }),
    });
  }

  // Substitutes the in-progress draft's geometry for its saved counterpart
  // in the areas list -- shared by handleDrawChangeRef's live-drag refresh
  // and the zoom handler, both of which need "what's on screen right now"
  // rather than "what's actually saved" while an edit is in progress.
  function areasWithDraftGeometry(): Area[] {
    if (!draft) return areas;
    return areas.map((a) => (a.identifier === draft.identifier ? { ...a, geometry: draft.geometry } : a));
  }

  // Terra Draw's event listeners are registered exactly once, when the
  // map's 'load' event fires (see the mount effect below), so they'd
  // otherwise close over that first render's state forever. Each handler
  // here is redefined every render and stashed in a ref; the one-time
  // listener always calls through `<name>Ref.current(...)`, so it always
  // sees the current render's state without needing to be re-registered.
  const handleDrawFinishRef = useRef((featureId: string) => {
    setPendingNameSuggestion(null);
    setPendingGeometryType(snapshotGeometryType(drawRef.current, featureId));
    setPendingLocked(false);
    setPendingDrawFeatureId(featureId);
  });
  handleDrawFinishRef.current = (featureId: string) => {
    setPendingNameSuggestion(null);
    setPendingGeometryType(snapshotGeometryType(drawRef.current, featureId));
    setPendingLocked(false);
    setPendingDrawFeatureId(featureId);
  };

  const handleDrawChangeRef = useRef((ids: string[]) => {
    if (!draft || !ids.includes(draft.identifier)) return;
    const feature = drawRef.current?.getSnapshotFeature(draft.identifier);
    if (!feature || !isAreaGeometryType(feature.geometry.type)) return;
    const geometry = feature.geometry as Area["geometry"];
    setDraft({ ...draft, geometry });
    // Live-track the label position/fit while dragging or reshaping (#590)
    // -- not just after Save, which is all the areas-list effect below
    // would otherwise cover.
    refreshLabelSource(areas.map((a) => (a.identifier === draft.identifier ? { ...a, geometry } : a)));
  });
  handleDrawChangeRef.current = (ids: string[]) => {
    if (!draft || !ids.includes(draft.identifier)) return;
    const feature = drawRef.current?.getSnapshotFeature(draft.identifier);
    if (!feature || !isAreaGeometryType(feature.geometry.type)) return;
    const geometry = feature.geometry as Area["geometry"];
    setDraft({ ...draft, geometry });
    refreshLabelSource(areas.map((a) => (a.identifier === draft.identifier ? { ...a, geometry } : a)));
  };

  const handleDrawSelectRef = useRef((featureId: string) => {
    if (featureId === pendingDrawFeatureId) return;
    if (draft?.identifier === featureId) return;
    const match = areas.find((a) => a.identifier === featureId);
    if (!match) return;
    requestSwitch(() => {
      setDraft(clone(match));
      setOriginal(clone(match));
      setSelectDraggable(match.locked);
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
      setSelectDraggable(match.locked);
    });
  };

  // computeMaxWidthEms's fit is screen-space, not geographic (#590) -- a
  // shape's on-screen width changes with zoom even though its real size
  // doesn't, so labels need re-fitting on zoom too, not just when a
  // shape's geometry or the saved area list changes. Rotation/pitch are
  // both locked (see the map constructor/disableRotation calls below), so
  // zoom is the only view change that affects projected width.
  const handleZoomRef = useRef(() => {
    refreshLabelSource(areasWithDraftGeometry());
  });
  handleZoomRef.current = () => {
    refreshLabelSource(areasWithDraftGeometry());
  };

  // Terra Draw's select-mode drag/vertex-edit flags are configured per
  // drawing-mode name ("polygon"), not per feature -- there's no built-in
  // way to lock one shape while leaving others draggable. Since only one
  // area is ever selected/editable at a time in this UI, updateModeOptions()
  // is called every time selection changes (see call sites below) to
  // dynamically re-target those global flags at whichever area just became
  // selected, which has the same effect as a true per-feature lock.
  function setSelectDraggable(locked: boolean) {
    drawRef.current?.updateModeOptions("select", { flags: selectModeFlags(locked) });
  }

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
          // glyph range for every rendered character. Bold confirmed
          // servable (200, real glyph data) before switching from Regular,
          // to match LookupView.tsx's route-stop label weight/size.
          "text-font": ["Noto Sans Bold"],
          "text-size": 14,
          "text-anchor": "center",
          // Wraps to computeMaxWidthEms's per-area value (#590) when the
          // shape is wide enough to be worth fitting into; MapLibre's own
          // default (10ems) otherwise -- effectively the "current
          // fixed-size behavior" fallback for a too-small shape or a Point.
          "text-max-width": ["coalesce", ["get", "maxWidthEms"], 10],
        },
        // #3f97e0 matches Terra Draw's own default stroke/fill color
        // (polygonOutlineColor/lineStringColor/pointColor) -- AreasView.tsx
        // never overrides Terra Draw's style defaults, so this is the
        // actual color every shape renders in.
        paint: { "text-color": "#3f97e0", "text-halo-color": "#ffffff", "text-halo-width": 1.5 },
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
            flags: selectModeFlags(false),
            // Terra Draw's own Delete/Escape key handling bypasses this
            // app's state entirely -- Delete would remove the feature from
            // the map without calling the delete API or going through the
            // confirm modal (leaving the map out of sync with the saved
            // area list), and it doesn't consult the draggable/deletable
            // flags used below to enforce locking. Disabled so every
            // deletion goes through handleDeleteConfirmed. rotate/scale are
            // no-ops already (rotateable/scaleable are never set below) but
            // disabled too for clarity.
            keyEvents: { deselect: null, delete: null, rotate: null, scale: null },
          }),
          new TerraDrawPolygonMode(),
          new TerraDrawLineStringMode(),
          new TerraDrawPointMode(),
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
      map.on("zoom", () => handleZoomRef.current());

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
              properties: { mode: geometryToModeName(area.geometry.type), name: area.name },
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

  // Keeps the label source (committed areas -- handleDrawChangeRef covers
  // in-progress edits separately, see #590) in sync whenever the saved
  // area list changes.
  useEffect(() => {
    if (!mapReady) return;
    refreshLabelSource(areas);
  }, [areas, mapReady]);

  function selectArea(area: Area) {
    requestSwitch(() => {
      setDraft(clone(area));
      setOriginal(clone(area));
      setSelectDraggable(area.locked);
      drawRef.current?.selectFeature(area.identifier);
      setMobileListOpen(false);
    });
  }

  function startDrawing(type: "polygon" | "linestring" | "point") {
    requestSwitch(() => {
      setDraft(null);
      setOriginal(null);
      drawRef.current?.setMode(type);
      setMobileListOpen(false);
    });
  }

  // Duplicates the currently selected (and possibly in-progress-edited)
  // shape: clones its on-map geometry with a visible offset, then reuses
  // the exact same naming-modal -> create-area flow as a freshly drawn
  // shape (handleNameConfirm doesn't care how the pending feature got onto
  // the map). Preserves the source's geometry type (a duplicated
  // LineString stays a LineString, etc.).
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
          properties: { mode: geometryToModeName(sourceGeometry.type), name: sourceName },
          geometry: offsetGeometry(sourceGeometry),
        },
      ]);
      setDraft(null);
      setOriginal(null);
      setPendingNameSuggestion(sourceName ? `${sourceName} copy` : "");
      setPendingGeometryType(sourceGeometry.type);
      setPendingLocked(false);
      setPendingDrawFeatureId(tempId);
    });
  }

  // Client-side only -- areas is already the full in-memory area list
  // (populated by listAreas() on load, kept in sync on create/update/delete),
  // so there's no server round trip needed to build the export file.
  function exportAllAreas() {
    downloadGeoJson(
      { type: "FeatureCollection", features: areas.map(areaToFeature) },
      "areas.geojson",
    );
  }

  // Exports just the selected area, from draft rather than the saved areas
  // list -- same source Duplicate already reads from, so an in-progress,
  // unsaved edit is reflected rather than stale server data.
  function exportSelectedArea() {
    if (!draft) return;
    downloadGeoJson(
      { type: "FeatureCollection", features: [areaToFeature(draft)] },
      `${draft.identifier}.geojson`,
    );
  }

  // Shared tail end of "a pending draw-map feature becomes a real, saved
  // Area" -- used both by handleNameConfirm (identifier/name came from the
  // AreaNameModal) and handleImportFeature's direct-create path (identifier/
  // name already resolved from the imported feature's own properties, no
  // modal needed). `locked` is a parameter rather than always `false`
  // because imports must be able to preserve properties.locked -- every
  // other caller (draw/duplicate) still just passes `false`.
  async function createAreaFromPendingFeature(tempId: string, identifier: string, name: string, locked: boolean) {
    const draw = drawRef.current;
    if (!draw) return;

    const feature = draw.getSnapshotFeature(tempId);
    if (!feature) {
      // Never actually made it into the store -- addFeatures() rejected
      // it during validation (see offsetGeometry/removeFeatureIfPresent).
      // Nothing to clean up, and nothing to save.
      showToast("error", "That shape could not be created -- its geometry was rejected.");
      return;
    }
    if (!isAreaGeometryType(feature.geometry.type)) {
      removeFeatureIfPresent(draw, tempId);
      return;
    }

    setSaving(true);
    try {
      const saved = await createArea({
        identifier,
        name,
        geometry: feature.geometry as Area["geometry"],
        locked,
      });
      removeFeatureIfPresent(draw, tempId);
      draw.addFeatures([
        {
          id: saved.identifier,
          type: "Feature",
          properties: { mode: geometryToModeName(saved.geometry.type), name: saved.name },
          geometry: saved.geometry,
        },
      ]);
      draw.setMode("select");
      setAreas((current) => [...current, saved]);
      setDraft(clone(saved));
      setOriginal(clone(saved));
      setSelectDraggable(saved.locked);
      showToast("success", `${geometryDisplayNoun(saved.geometry.type)} '${saved.identifier}' created.`);
    } catch (err) {
      removeFeatureIfPresent(draw, tempId);
      showToast("error", err instanceof ApiError ? err.message : "Failed to create area.");
    } finally {
      setSaving(false);
    }
  }

  async function handleNameConfirm(identifier: string, name: string) {
    const tempId = pendingDrawFeatureId;
    const locked = pendingLocked;
    setPendingDrawFeatureId(null);
    setPendingNameSuggestion(null);
    setPendingLocked(false);
    if (!tempId) return;
    await createAreaFromPendingFeature(tempId, identifier, name, locked);
  }

  // Places an imported feature onto the draw map exactly like a fresh draw
  // or a duplicate, then either creates it immediately (name + a usable,
  // non-duplicate identifier both present in the feature's properties) or
  // falls through to the existing AreaNameModal -- reusing its identifier
  // validation/duplicate rejection rather than reimplementing it here.
  function handleImportFeature(feature: ImportedFeature) {
    requestSwitch(() => {
      const draw = drawRef.current;
      if (!draw) return;

      const props = feature.properties ?? {};
      const rawName = typeof props.name === "string" ? props.name : "";
      const rawIdentifier = typeof props.identifier === "string" ? props.identifier : "";
      const rawLocked = typeof props.locked === "boolean" ? props.locked : false;
      const identifierUsable =
        rawIdentifier.trim() !== "" &&
        IDENTIFIER_PATTERN.test(rawIdentifier) &&
        !areas.some((a) => a.identifier === rawIdentifier);

      const tempId = crypto.randomUUID();
      draw.addFeatures([
        {
          id: tempId,
          type: "Feature",
          properties: { mode: geometryToModeName(feature.geometry.type), name: rawName },
          geometry: feature.geometry,
        },
      ]);
      setDraft(null);
      setOriginal(null);

      if (rawName.trim() && identifierUsable) {
        void createAreaFromPendingFeature(tempId, rawIdentifier, rawName.trim(), rawLocked);
      } else {
        setPendingNameSuggestion(rawName || null);
        setPendingGeometryType(feature.geometry.type);
        setPendingLocked(rawLocked);
        setPendingDrawFeatureId(tempId);
      }
    });
  }

  function handleNameCancel() {
    const draw = drawRef.current;
    if (pendingDrawFeatureId && draw) {
      removeFeatureIfPresent(draw, pendingDrawFeatureId);
    }
    setPendingDrawFeatureId(null);
    setPendingNameSuggestion(null);
    setPendingLocked(false);
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
      showToast("success", `${geometryDisplayNoun(saved.geometry.type)} '${saved.identifier}' saved.`);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to save area.");
    } finally {
      setSaving(false);
    }
  }

  // Saves immediately rather than going through the dirty/Save flow -- a
  // direct state flip like delete, not an in-progress geometry/name edit
  // the user might want to discard.
  async function toggleLock() {
    if (!draft) return;
    setSaving(true);
    try {
      const saved = await updateArea(draft.identifier, { ...draft, locked: !draft.locked });
      setAreas((current) => current.map((a) => (a.identifier === saved.identifier ? saved : a)));
      setDraft(clone(saved));
      setOriginal(clone(saved));
      setSelectDraggable(saved.locked);
      const noun = geometryDisplayNoun(saved.geometry.type);
      showToast("success", saved.locked ? `${noun} '${saved.identifier}' locked.` : `${noun} '${saved.identifier}' unlocked.`);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to update area.");
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
      showToast("success", `${geometryDisplayNoun(deleteTarget.geometry.type)} '${deleteTarget.identifier}' deleted.`);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to delete area.");
    } finally {
      setDeleteTarget(null);
    }
  }

  return (
    <div className="flex flex-col gap-4 md:h-full md:flex-row md:gap-6">
      <div className="flex flex-col gap-2 md:w-72 md:shrink-0">
        <div className="flex gap-2">
          <button
            type="button"
            onClick={() => startDrawing("polygon")}
            aria-label="Draw polygon"
            title="Draw polygon"
            className="flex flex-1 items-center justify-center rounded-md border border-sky-600 px-2 py-2 text-sky-600 hover:bg-sky-50 dark:border-sky-400 dark:text-sky-400 dark:hover:bg-sky-950"
          >
            <MdiIcon path={mdiShapePolygonPlus} size={18} />
          </button>
          <button
            type="button"
            onClick={() => startDrawing("linestring")}
            aria-label="Draw line"
            title="Draw line"
            className="flex flex-1 items-center justify-center rounded-md border border-sky-600 px-2 py-2 text-sky-600 hover:bg-sky-50 dark:border-sky-400 dark:text-sky-400 dark:hover:bg-sky-950"
          >
            <MdiIcon path={mdiVectorPolylinePlus} size={18} />
          </button>
          <button
            type="button"
            onClick={() => startDrawing("point")}
            aria-label="Draw point"
            title="Draw point"
            className="flex flex-1 items-center justify-center rounded-md border border-sky-600 px-2 py-2 text-sky-600 hover:bg-sky-50 dark:border-sky-400 dark:text-sky-400 dark:hover:bg-sky-950"
          >
            <MapPinPlusInside size={18} />
          </button>
          <button
            type="button"
            onClick={() => setImportModalOpen(true)}
            aria-label="Import"
            title="Import"
            className="flex flex-1 items-center justify-center rounded-md border border-slate-300 px-2 py-2 text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
          >
            <MdiIcon path={mdiFileImportOutline} size={18} />
          </button>
          <button
            type="button"
            onClick={exportAllAreas}
            disabled={areas.length === 0}
            aria-label="Export all"
            title="Export all"
            className="flex flex-1 items-center justify-center rounded-md border border-slate-300 px-2 py-2 text-slate-700 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
          >
            <MdiIcon path={mdiExportVariant} size={18} />
          </button>
        </div>

        <button
          type="button"
          onClick={() => setMobileListOpen((open) => !open)}
          aria-expanded={mobileListOpen}
          className="flex items-center justify-center gap-2 rounded-md bg-slate-100 px-3 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-200 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-700 md:hidden"
        >
          {mobileListOpen ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
          <span>Areas</span>
          {mobileListOpen ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
        </button>

        {loading ? (
          <p className="text-slate-400">Loading areas...</p>
        ) : (
          <ul
            className={`${mobileListOpen ? "flex" : "hidden"} max-h-64 flex-col gap-1 overflow-y-auto md:flex md:max-h-none`}
          >
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
                          className="ml-auto rounded-md border border-red-300 px-2 py-1 text-xs font-medium text-red-600 hover:bg-red-50 dark:border-red-800 dark:hover:bg-red-950"
                        >
                          Delete
                        </button>
                      </div>
                      {/* Separate row -- more shape-level toggles (beyond
                          lock) will land here alongside it. */}
                      <div className="flex flex-wrap gap-2">
                        <button
                          type="button"
                          onClick={toggleLock}
                          disabled={saving}
                          className="flex items-center gap-1 rounded-md border border-slate-300 px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
                        >
                          {draft.locked ? <Unlock size={12} /> : <Lock size={12} />}
                          {draft.locked ? "Unlock" : "Lock"}
                        </button>
                        <button
                          type="button"
                          onClick={exportSelectedArea}
                          className="flex items-center gap-1 rounded-md border border-slate-300 px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
                        >
                          <MdiIcon path={mdiExportVariant} size={12} />
                          Export
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
        title={deleteTarget ? `Delete ${geometryDisplayNoun(deleteTarget.geometry.type).toLowerCase()}?` : "Delete area?"}
        message={deleteTarget ? <DeleteAreaMessage area={deleteTarget} /> : ""}
        confirmLabel="Delete"
        onConfirm={handleDeleteConfirmed}
        onCancel={() => setDeleteTarget(null)}
      />

      <AreaNameModal
        open={pendingDrawFeatureId !== null}
        existingIdentifiers={areas.map((a) => a.identifier)}
        initialName={pendingNameSuggestion ?? undefined}
        geometryType={pendingGeometryType}
        onConfirm={handleNameConfirm}
        onCancel={handleNameCancel}
      />
      <ImportAreaModal
        open={importModalOpen}
        onImport={(feature) => {
          setImportModalOpen(false);
          handleImportFeature(feature);
        }}
        onCancel={() => setImportModalOpen(false)}
      />
    </div>
  );
}
