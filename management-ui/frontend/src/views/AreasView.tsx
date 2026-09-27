import * as maplibregl from "maplibre-gl";
import {
  TerraDraw,
  TerraDrawLineStringMode,
  TerraDrawPointMode,
  TerraDrawPolygonMode,
  TerraDrawSelectMode,
  type GeoJSONStoreFeatures,
  type HexColor,
} from "terra-draw";
import { TerraDrawMapLibreGLAdapter } from "terra-draw-maplibre-gl-adapter";
import { Lock, MapPinPlusInside, Unlock } from "lucide-react";
import { mdiExportVariant, mdiFileImportOutline, mdiShapePolygonPlus, mdiVectorPolylinePlus } from "@mdi/js";
import { useEffect, useRef, useState } from "react";
import { AreaNameModal, IDENTIFIER_PATTERN } from "../components/AreaNameModal";
import { ConfirmModal } from "../components/ConfirmModal";
import { ImportAreaModal } from "../components/ImportAreaModal";
import { ImportConflictModal, type ConflictChoice } from "../components/ImportConflictModal";
import {
  collidingIdentifiers,
  importAreasBatch,
  resolveImportIdentities,
  roundGeometryPrecision,
  type ImportedFeature,
} from "../lib/areaImport";
import { MdiIcon } from "../components/MdiIcon";
import { createArea, deleteArea, geometryDisplayNoun, listAreas, updateArea, type Area } from "../api/areas";
import { ApiError } from "../api/client";
import { useToast } from "../hooks/useToast";
import { MAP_STYLE } from "../lib/maplibreSetup";
import { uuidv4 } from "../lib/uuid";

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value));
}

// Terra Draw's own default stroke/fill/marker color; the fallback for areas with no color of their own.
const DEFAULT_SHAPE_COLOR: HexColor = "#3f97e0";

// Area.fill/stroke/marker-color aren't format-validated on the backend; narrows to HexColor, undefined for non-hex values.
function asHexColor(value: unknown): HexColor | undefined {
  return typeof value === "string" && value.startsWith("#") ? (value as HexColor) : undefined;
}

const STYLE_KEYS = [
  "fill",
  "fill-opacity",
  "stroke",
  "stroke-width",
  "stroke-opacity",
  "marker-color",
  "marker-size",
  "marker-symbol",
] as const;
type StyleFields = Partial<Pick<Area, (typeof STYLE_KEYS)[number]>>;

// Picks only the style keys actually set, to carry a shape's color into a duplicate or into Terra Draw feature properties.
function pickStyleFields(source: StyleFields): StyleFields {
  const style: StyleFields = {};
  for (const key of STYLE_KEYS) {
    if (source[key] !== undefined) (style as Record<string, unknown>)[key] = source[key];
  }
  return style;
}

// Same as pickStyleFields, but from an imported feature's untyped properties -- validates each value's type first.
function extractStyleFields(props: Record<string, unknown>): StyleFields {
  const style: StyleFields = {};
  if (typeof props.fill === "string") style.fill = props.fill;
  if (typeof props["fill-opacity"] === "number") style["fill-opacity"] = props["fill-opacity"];
  if (typeof props.stroke === "string") style.stroke = props.stroke;
  if (typeof props["stroke-width"] === "number") style["stroke-width"] = props["stroke-width"];
  if (typeof props["stroke-opacity"] === "number") style["stroke-opacity"] = props["stroke-opacity"];
  if (typeof props["marker-color"] === "string") style["marker-color"] = props["marker-color"];
  const markerSize = props["marker-size"];
  if (markerSize === "small" || markerSize === "medium" || markerSize === "large") {
    style["marker-size"] = markerSize;
  }
  if (typeof props["marker-symbol"] === "string") style["marker-symbol"] = props["marker-symbol"];
  return style;
}

// Per-feature Terra Draw styling callbacks -- read the feature's own style property, falling back to the default color when unset.
function featureFillColor(feature: GeoJSONStoreFeatures): HexColor {
  return asHexColor(feature.properties?.fill) ?? DEFAULT_SHAPE_COLOR;
}
function featureStrokeColor(feature: GeoJSONStoreFeatures): HexColor {
  return asHexColor(feature.properties?.stroke) ?? DEFAULT_SHAPE_COLOR;
}
function featureMarkerColor(feature: GeoJSONStoreFeatures): HexColor {
  return asHexColor(feature.properties?.["marker-color"]) ?? DEFAULT_SHAPE_COLOR;
}

// Label color follows the shape's own color: stroke for Polygon/LineString, marker color for Point.
function areaLabelColor(area: Area): string | undefined {
  return area.geometry.type === "Point" ? area["marker-color"] : area.stroke;
}

// Shared GeoJSON Feature shape for both the all-areas and single-area exports.
function areaToFeature(area: Area) {
  return {
    type: "Feature" as const,
    geometry: area.geometry,
    properties: {
      identifier: area.identifier,
      name: area.name,
      locked: area.locked,
      ...pickStyleFields(area),
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

// addFeatures() silently drops features that fail mode validation; removeFeatures() throws for an unknown id, so check presence first.
function removeFeatureIfPresent(draw: TerraDraw, id: string): void {
  if (draw.getSnapshotFeature(id)) draw.removeFeatures([id]);
}

// Shared coordinate-visiting switch so callers (computeBounds, offsetGeometry) don't each duplicate a type switch.
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

// Same as forEachCoordinate, but transforms coordinates instead of just visiting them.
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

// Estimated glyph width for "Noto Sans Bold", since it may not be loaded as a usable browser font for canvas measureText.
// Biased slightly wide so text wraps early rather than overflowing.
const LABEL_FONT_SIZE_PX = 14;
const AVG_GLYPH_WIDTH_RATIO = 0.62;
// Shapes smaller than this on screen keep the fixed-size/no-wrap fallback.
const MIN_FIT_WIDTH_PX = 40;

function estimateTextWidthPx(text: string): number {
  return text.length * LABEL_FONT_SIZE_PX * AVG_GLYPH_WIDTH_RATIO;
}

// Projects the shape's bounding box through the live map to get its on-screen pixel width, then converts to ems for text-max-width.
// Returns undefined (MapLibre's 10em default) when too small to fit, or the name already fits without wrapping.
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

// Midpoint by cumulative length, not the middle coordinate index -- avoids an off-center label when vertex spacing is uneven.
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

// Area-weighted (shoelace) centroid -- unaffected by uneven vertex density, unlike a plain vertex average.
// Falls back to the vertex average for a degenerate (zero-area) ring, where the formula would divide by zero.
function polygonCentroid(ring: number[][]): [number, number] {
  let area = 0;
  let cx = 0;
  let cy = 0;
  for (let i = 0; i < ring.length - 1; i++) {
    const [x0, y0] = ring[i];
    const [x1, y1] = ring[i + 1];
    const cross = x0 * y1 - x1 * y0;
    area += cross;
    cx += (x0 + x1) * cross;
    cy += (y0 + y1) * cross;
  }
  area /= 2;
  if (area === 0) {
    let sumLng = 0;
    let sumLat = 0;
    for (const [lng, lat] of ring) {
      sumLng += lng;
      sumLat += lat;
    }
    return [sumLng / ring.length, sumLat / ring.length];
  }
  return [cx / (6 * area), cy / (6 * area)];
}

function labelPosition(geometry: Area["geometry"]): [number, number] {
  switch (geometry.type) {
    case "Polygon": {
      const ring = geometry.coordinates[0] ?? [];
      if (ring.length === 0) return [0, 0];
      return polygonCentroid(ring);
    }
    case "LineString":
      return lineStringMidpoint(geometry.coordinates);
    case "Point":
      return [geometry.coordinates[0], geometry.coordinates[1]];
  }
}

// Screen-space alignment guides shown while dragging (snap lines; no built-in Terra Draw feature for this).
// axis "x" = vertical guide at constant screen X; "y" = horizontal at constant screen Y. from/to are the perpendicular span drawn.
interface AlignmentGuide {
  axis: "x" | "y";
  pos: number;
  from: number;
  to: number;
}

const GUIDE_TOLERANCE_PX = 7;

// Dragged shape contributes every vertex plus its centroid; other areas only contribute bounding-box edges and centroid
// (comparing against every other shape's individual vertices would be noisier without being more useful).
function computeAlignmentGuides(map: maplibregl.Map, draggedArea: Area, otherAreas: Area[]): AlignmentGuide[] {
  const draggedPoints: maplibregl.Point[] = [];
  forEachCoordinate(draggedArea.geometry, (coord) => draggedPoints.push(map.project(coord)));
  draggedPoints.push(map.project(labelPosition(draggedArea.geometry)));

  const guides: AlignmentGuide[] = [];

  for (const other of otherAreas) {
    if (other.identifier === draggedArea.identifier) continue;
    const bounds = computeBounds([other]);
    if (!bounds) continue;
    const [[minLng, minLat], [maxLng, maxLat]] = bounds as [[number, number], [number, number]];
    const corner1 = map.project([minLng, maxLat]);
    const corner2 = map.project([maxLng, minLat]);
    const left = Math.min(corner1.x, corner2.x);
    const right = Math.max(corner1.x, corner2.x);
    const top = Math.min(corner1.y, corner2.y);
    const bottom = Math.max(corner1.y, corner2.y);
    const centroid = map.project(labelPosition(other.geometry));

    for (const p of draggedPoints) {
      for (const x of [left, right, centroid.x]) {
        if (Math.abs(p.x - x) <= GUIDE_TOLERANCE_PX) {
          guides.push({ axis: "x", pos: x, from: Math.min(p.y, top), to: Math.max(p.y, bottom) });
        }
      }
      for (const y of [top, bottom, centroid.y]) {
        if (Math.abs(p.y - y) <= GUIDE_TOLERANCE_PX) {
          guides.push({ axis: "y", pos: y, from: Math.min(p.x, left), to: Math.max(p.x, right) });
        }
      }
    }
  }
  return guides;
}

// Merges guides landing on (near enough) the same line, so one alignment isn't drawn many times over.
function dedupeAlignmentGuides(guides: AlignmentGuide[]): AlignmentGuide[] {
  const merged = new Map<string, AlignmentGuide>();
  for (const g of guides) {
    const key = `${g.axis}:${Math.round(g.pos)}`;
    const existing = merged.get(key);
    if (existing) {
      existing.from = Math.min(existing.from, g.from);
      existing.to = Math.max(existing.to, g.to);
    } else {
      merged.set(key, { ...g });
    }
  }
  return Array.from(merged.values());
}

// Terra Draw requires properties.mode to match the owning mode's name; addFeatures() validates against it.
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

// Narrows Terra Draw's loosely-typed geometry (it supports modes/geometries this app never uses) to the three types Area supports.
function isAreaGeometryType(type: string): type is Area["geometry"]["type"] {
  return type === "Polygon" || type === "LineString" || type === "Point";
}

// Falls back to "Polygon" if the feature vanished (rejected by mode validation) or reports an unsupported type;
// transient only, before pendingDrawFeatureId is cleared.
function snapshotGeometryType(draw: TerraDraw | null, featureId: string): Area["geometry"]["type"] {
  const type = draw?.getSnapshotFeature(featureId)?.geometry.type;
  return type && isAreaGeometryType(type) ? type : "Polygon";
}

// Shared by the initial TerraDrawSelectMode construction and every setSelectDraggable() call so the two can't drift apart.
// Point has no midpoints/vertices distinct from the feature itself, so it only needs the feature-level draggable flag.
function selectModeFlags(locked: boolean) {
  const coordinates = { midpoints: !locked, draggable: !locked, deletable: !locked };
  return {
    polygon: { feature: { draggable: !locked, coordinates } },
    linestring: { feature: { draggable: !locked, coordinates } },
    point: { feature: { draggable: !locked } },
  };
}

// Floor offset (degrees) so a duplicate of a tiny/degenerate shape is still visibly distinct.
const MIN_OFFSET_DEGREES = 0.0008;

// Terra Draw's default coordinatePrecision is 9 decimal places (silently rejects features exceeding it); stay well under that ceiling.
const OFFSET_COORDINATE_PRECISION = 7;

function roundCoordinate(value: number, precision: number): number {
  const factor = 10 ** precision;
  return Math.round(value * factor) / factor;
}

// Offsets coordinates by a fraction of the shape's bounding-box size (floored for small shapes) so a duplicate is distinguishable, not stacked on its source.
// Rounds after offsetting: floating-point addition can produce 14+ decimal digits even from precision-safe inputs, and Terra Draw silently
// drops (rather than errors on) any feature exceeding its coordinate precision limit, which would otherwise crash the next cleanup step.
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
  // A Point's bbox is zero-sized, so only the MIN_OFFSET_DEGREES floor applies -- still a deliberate offset, not a no-op.
  const dLng = Math.max((maxLng - minLng) * 0.15, MIN_OFFSET_DEGREES);
  const dLat = Math.max((maxLat - minLat) * 0.15, MIN_OFFSET_DEGREES);
  return mapCoordinates(geometry, ([lng, lat]) => [
    roundCoordinate(lng + dLng, OFFSET_COORDINATE_PRECISION),
    roundCoordinate(lat + dLat, OFFSET_COORDINATE_PRECISION),
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
  // Mirrors `areas` for read access inside async flows (e.g. the import batch below), where the closed-over `areas`
  // value would otherwise be stale by the time the awaits resolve.
  const areasRef = useRef<Area[]>(areas);
  useEffect(() => {
    areasRef.current = areas;
  }, [areas]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [mapReady, setMapReady] = useState(false);

  const [original, setOriginal] = useState<Area | null>(null);
  const [draft, setDraft] = useState<Area | null>(null);

  const [pendingSwitch, setPendingSwitch] = useState<(() => void) | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<Area | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [pendingDrawFeatureId, setPendingDrawFeatureId] = useState<string | null>(null);
  // Set only by duplicateArea(), to suggest "<name> copy" in the naming modal; a fresh draw leaves this null.
  const [pendingNameSuggestion, setPendingNameSuggestion] = useState<string | null>(null);
  // Drives the naming modal's "Name this area/line/point" title.
  const [pendingGeometryType, setPendingGeometryType] = useState<Area["geometry"]["type"]>("Polygon");
  // Locked value applied once the pending feature is created; false for a fresh draw/duplicate, but must carry
  // an imported feature's properties.locked through even via the AreaNameModal detour.
  const [pendingLocked, setPendingLocked] = useState(false);
  // Style fields applied once the pending feature is created: empty for a fresh draw, copied for a duplicate,
  // extracted from properties for an import.
  const [pendingStyle, setPendingStyle] = useState<StyleFields>({});
  const [importModalOpen, setImportModalOpen] = useState(false);
  // Set together right before ImportConflictModal opens: the raw batch plus its colliding identifiers. Both null/empty = modal closed.
  // Only populated by the multi-feature batch path; a single-feature import still falls through to AreaNameModal.
  const [pendingImportFeatures, setPendingImportFeatures] = useState<ImportedFeature[] | null>(null);
  const [conflictIdentifiers, setConflictIdentifiers] = useState<string[]>([]);
  // Non-empty only while a shape is actively dragged/reshaped; cleared when the drag ends or selection changes.
  const [guideLines, setGuideLines] = useState<AlignmentGuide[]>([]);

  const dirty = draft !== null && original !== null && JSON.stringify(draft) !== JSON.stringify(original);

  function requestSwitch(action: () => void) {
    if (dirty) {
      setPendingSwitch(() => action);
    } else {
      action();
    }
  }

  // Called whenever anything affecting label position/fit changes: the saved area list, an in-progress drag/vertex edit, or zoom.
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
        const color = areaLabelColor(area);
        return {
          type: "Feature",
          properties: {
            name,
            geometryType: area.geometry.type,
            ...(maxWidthEms !== undefined ? { maxWidthEms } : {}),
            ...(color ? { color } : {}),
          },
          geometry: { type: "Point", coordinates: labelPosition(area.geometry) },
        };
      }),
    });
  }

  // Substitutes the in-progress draft's geometry into the areas list -- used where "what's on screen now" is needed over "what's saved".
  function areasWithDraftGeometry(): Area[] {
    if (!draft) return areas;
    return areas.map((a) => (a.identifier === draft.identifier ? { ...a, geometry: draft.geometry } : a));
  }

  // Terra Draw's listeners are registered once (map's 'load' event), so they'd otherwise close over that render's state forever.
  // Each handler here is redefined every render and stashed in a ref; the listener calls `<name>Ref.current(...)` to see current state.
  const handleDrawFinishRef = useRef((featureId: string) => {
    setPendingNameSuggestion(null);
    setPendingGeometryType(snapshotGeometryType(drawRef.current, featureId));
    setPendingLocked(false);
    setPendingStyle({});
    setPendingDrawFeatureId(featureId);
  });
  handleDrawFinishRef.current = (featureId: string) => {
    setPendingNameSuggestion(null);
    setPendingGeometryType(snapshotGeometryType(drawRef.current, featureId));
    setPendingLocked(false);
    setPendingStyle({});
    setPendingDrawFeatureId(featureId);
  };

  // Uses setDraft's functional-updater form, not a spread of the closed-over `draft`: a geometry-change event can land in the same
  // tick as a properties-target update (e.g. a color picker), and spreading the stale closure would silently discard that change.
  const handleDrawChangeRef = useRef((ids: string[]) => {
    if (!draft || !ids.includes(draft.identifier)) return;
    const feature = drawRef.current?.getSnapshotFeature(draft.identifier);
    if (!feature || !isAreaGeometryType(feature.geometry.type)) return;
    const geometry = feature.geometry as Area["geometry"];
    setDraft((prev) => (prev ? { ...prev, geometry } : prev));
    // Live-tracks label position/fit while dragging, not just after Save.
    refreshLabelSource(areas.map((a) => (a.identifier === draft.identifier ? { ...a, geometry } : a)));
    const map = mapRef.current;
    if (map) {
      setGuideLines(dedupeAlignmentGuides(computeAlignmentGuides(map, { ...draft, geometry }, areas)));
    }
  });
  handleDrawChangeRef.current = (ids: string[]) => {
    if (!draft || !ids.includes(draft.identifier)) return;
    const feature = drawRef.current?.getSnapshotFeature(draft.identifier);
    if (!feature || !isAreaGeometryType(feature.geometry.type)) return;
    const geometry = feature.geometry as Area["geometry"];
    setDraft((prev) => (prev ? { ...prev, geometry } : prev));
    refreshLabelSource(areas.map((a) => (a.identifier === draft.identifier ? { ...a, geometry } : a)));
    const map = mapRef.current;
    if (map) {
      setGuideLines(dedupeAlignmentGuides(computeAlignmentGuides(map, { ...draft, geometry }, areas)));
    }
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

  // Label fit is screen-space; a shape's projected width changes with zoom even though its geometry doesn't.
  // Rotation/pitch are locked, so zoom is the only view change that affects it.
  const handleZoomRef = useRef(() => {
    refreshLabelSource(areasWithDraftGeometry());
  });
  handleZoomRef.current = () => {
    refreshLabelSource(areasWithDraftGeometry());
  };

  // Terra Draw's drag/vertex-edit flags are per-mode, not per-feature. Since only one area is ever selected at a time,
  // re-targeting these global flags on every selection change (call sites below) has the same effect as a true per-feature lock.
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
      // Locked to a flat, north-up 2D view -- pitch/rotation only disorient area drawing/editing, never help it.
      // maxPitch: 0 blocks pitch outright; these plus the disableRotation() calls below block every gesture path that could change bearing.
      maxPitch: 0,
      pitchWithRotate: false,
      dragRotate: false,
      touchPitch: false,
    });
    mapRef.current = map;
    map.touchZoomRotate.disableRotation(); // keep pinch-zoom, drop two-finger twist-to-rotate
    map.keyboard.disableRotation(); // keep pan/zoom shortcuts, drop Shift+Left/Right rotate
    // Rotation is locked, so a reset-bearing compass control has nothing to do.
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
          // MapLibre's style-spec default font isn't served by OpenFreeMap's glyph server for this style (only Noto Sans),
          // causing 404s per glyph. Matches LookupView.tsx's label weight/size.
          "text-font": ["Noto Sans Bold"],
          "text-size": 14,
          // LineString/Point anchor below what they label, not centered -- "center" would draw text directly on top of the line/marker.
          "text-anchor": ["match", ["get", "geometryType"], "LineString", "top", "Point", "top", "center"],
          "text-offset": [
            "match",
            ["get", "geometryType"],
            "LineString",
            ["literal", [0, 0.6]],
            "Point",
            ["literal", [0, 0.6]],
            ["literal", [0, 0]],
          ],
          // Falls back to MapLibre's default (10ems) for a too-small shape or a Point, where computeMaxWidthEms has no value to give.
          "text-max-width": ["coalesce", ["get", "maxWidthEms"], 10],
        },
        // Falls back to DEFAULT_SHAPE_COLOR for an area with no custom style, matching what an unstyled shape actually renders in.
        paint: {
          "text-color": ["coalesce", ["get", "color"], DEFAULT_SHAPE_COLOR],
          "text-halo-color": "#ffffff",
          "text-halo-width": 1.5,
        },
      });

      // Terra Draw's MapLibre adapter must be created after the map's style has loaded, so the instance is built here, not right after the Map.
      const draw = new TerraDraw({
        adapter: new TerraDrawMapLibreGLAdapter({ map }),
        // Terra Draw's default id strategy only accepts UUID-shaped ids, which area identifiers like "LI" aren't;
        // this lets an identifier double as the feature id directly.
        idStrategy: {
          isValidId: (id): id is string => typeof id === "string" && id.length > 0,
          getId: () => uuidv4(),
        },
        modes: [
          new TerraDrawSelectMode({
            flags: selectModeFlags(false),
            // Terra Draw's own Delete key bypasses this app's state entirely (removes the feature without the delete API/confirm
            // modal, ignoring the locked flags below), so it's disabled in favor of handleDeleteConfirmed.
            keyEvents: { deselect: null, delete: null, rotate: null, scale: null },
            // Selected-state colors are separate style keys from the base mode's own; without overriding these too,
            // a custom color would flip to Terra Draw's default on selection.
            styles: {
              selectedPolygonColor: featureFillColor,
              selectedPolygonOutlineColor: featureStrokeColor,
              selectedLineStringColor: featureStrokeColor,
              selectedPointColor: featureMarkerColor,
            },
          }),
          new TerraDrawPolygonMode({
            styles: { fillColor: featureFillColor, outlineColor: featureStrokeColor },
          }),
          new TerraDrawLineStringMode({
            styles: { lineStringColor: featureStrokeColor },
          }),
          new TerraDrawPointMode({
            styles: { pointColor: featureMarkerColor },
          }),
        ],
      });
      drawRef.current = draw;
      draw.start();
      // Terra Draw's start() adds its own layers above "area-labels", burying it; move it to the top once here (layers are only ever added once).
      map.moveLayer("area-labels");
      draw.setMode("select");

      draw.on("finish", (id, context) => {
        // "finish" also fires for completed drags; only a brand-new shape (action "draw") should prompt for a name.
        if (context.action === "draw") handleDrawFinishRef.current(String(id));
      });
      // "finish" doesn't reliably fire for a whole-feature drag (confirmed empirically), so guides could get stuck visible.
      // mouseup/touchend on the canvas is a catch-all for "drag ended" regardless of Terra Draw's event semantics; a no-op if already empty.
      const clearGuideLines = () => setGuideLines([]);
      map.getCanvasContainer().addEventListener("mouseup", clearGuideLines);
      map.getCanvasContainer().addEventListener("touchend", clearGuideLines);
      draw.on("change", (ids, type, context) => {
        // A properties-only update (e.g. the color picker) also fires type "update"; context.target distinguishes it from an actual geometry drag.
        if (type === "update" && context?.target === "geometry") {
          handleDrawChangeRef.current(ids.map(String));
        }
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
              properties: { mode: geometryToModeName(area.geometry.type), name: area.name, ...pickStyleFields(area) },
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

  // Keeps the label source in sync with the saved area list; in-progress edits are covered separately by handleDrawChangeRef.
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
    });
  }

  function startDrawing(type: "polygon" | "linestring" | "point") {
    requestSwitch(() => {
      setDraft(null);
      setOriginal(null);
      drawRef.current?.setMode(type);
    });
  }

  // Clones the selected shape's geometry with a visible offset, then reuses the same naming-modal -> create-area flow as a fresh draw.
  function duplicateArea() {
    if (!draft) return;
    const sourceGeometry = draft.geometry;
    const sourceName = draft.name;
    const sourceStyle = pickStyleFields(draft);
    requestSwitch(() => {
      const draw = drawRef.current;
      if (!draw) return;
      const tempId = uuidv4();
      draw.addFeatures([
        {
          id: tempId,
          type: "Feature",
          properties: { mode: geometryToModeName(sourceGeometry.type), name: sourceName, ...sourceStyle },
          geometry: offsetGeometry(sourceGeometry),
        },
      ]);
      setDraft(null);
      setOriginal(null);
      setPendingNameSuggestion(sourceName ? `${sourceName} copy` : "");
      setPendingGeometryType(sourceGeometry.type);
      setPendingLocked(false);
      setPendingStyle(sourceStyle);
      setPendingDrawFeatureId(tempId);
    });
  }

  // Client-side only; `areas` is already the full in-memory list, kept in sync on create/update/delete.
  function exportAllAreas() {
    downloadGeoJson(
      { type: "FeatureCollection", features: areas.map(areaToFeature) },
      "areas.geojson",
    );
  }

  // Exports from draft, not the saved list, so an in-progress unsaved edit is reflected.
  function exportSelectedArea() {
    if (!draft) return;
    downloadGeoJson(
      { type: "FeatureCollection", features: [areaToFeature(draft)] },
      `${draft.identifier}.geojson`,
    );
  }

  // Shared tail end of "pending draw-map feature becomes a saved Area", used by handleNameConfirm and handleImportFeature's direct-create path.
  // `locked` is a parameter (not always false) so imports can preserve properties.locked. `suppressToast`/the return value support the
  // batch import loop's single summary toast.
  async function createAreaFromPendingFeature(
    tempId: string,
    identifier: string,
    name: string,
    locked: boolean,
    style: StyleFields,
    opts?: { suppressToast?: boolean },
  ): Promise<boolean> {
    const draw = drawRef.current;
    if (!draw) return false;

    const feature = draw.getSnapshotFeature(tempId);
    if (!feature) {
      // Never made it into the store -- addFeatures() rejected it during validation. Nothing to clean up or save.
      if (!opts?.suppressToast) {
        showToast("error", "That shape could not be created -- its geometry was rejected.");
      }
      return false;
    }
    if (!isAreaGeometryType(feature.geometry.type)) {
      removeFeatureIfPresent(draw, tempId);
      return false;
    }

    setSaving(true);
    try {
      const saved = await createArea({
        identifier,
        name,
        geometry: feature.geometry as Area["geometry"],
        locked,
        ...style,
      });
      removeFeatureIfPresent(draw, tempId);
      draw.addFeatures([
        {
          id: saved.identifier,
          type: "Feature",
          properties: { mode: geometryToModeName(saved.geometry.type), name: saved.name, ...pickStyleFields(saved) },
          geometry: saved.geometry,
        },
      ]);
      draw.setMode("select");
      setAreas((current) => [...current, saved]);
      setDraft(clone(saved));
      setOriginal(clone(saved));
      setSelectDraggable(saved.locked);
      if (!opts?.suppressToast) {
        showToast("success", `${geometryDisplayNoun(saved.geometry.type)} '${saved.identifier}' created.`);
      }
      return true;
    } catch (err) {
      removeFeatureIfPresent(draw, tempId);
      if (!opts?.suppressToast) {
        showToast("error", err instanceof ApiError ? err.message : "Failed to create area.");
      }
      return false;
    } finally {
      setSaving(false);
    }
  }

  async function handleNameConfirm(identifier: string, name: string) {
    const tempId = pendingDrawFeatureId;
    const locked = pendingLocked;
    const style = pendingStyle;
    setPendingDrawFeatureId(null);
    setPendingNameSuggestion(null);
    setPendingLocked(false);
    setPendingStyle({});
    if (!tempId) return;
    await createAreaFromPendingFeature(tempId, identifier, name, locked, style);
  }

  // Entry point from ImportAreaModal. A single feature keeps the original single-conflict behavior; multiple features check for
  // identifier collisions first and show ImportConflictModal if any exist, otherwise the batch runs straight through.
  function handleImportFeatures(features: ImportedFeature[]) {
    if (features.length === 1) {
      handleImportFeature(features[0]);
      return;
    }
    requestSwitch(() => {
      const colliding = collidingIdentifiers(features, areas.map((a) => a.identifier));
      if (colliding.length > 0) {
        setPendingImportFeatures(features);
        setConflictIdentifiers(colliding);
      } else {
        void importFeaturesBatch(features, new Set());
      }
    });
  }

  async function importFeaturesBatch(features: ImportedFeature[], skipIdentifiers: ReadonlySet<string>) {
    const draw = drawRef.current;
    if (!draw) return;
    setDraft(null);
    setOriginal(null);

    const result = await importAreasBatch(
      features,
      areas.map((a) => a.identifier),
      async (identity, feature) => {
        const style = extractStyleFields(feature.properties ?? {});
        const tempId = uuidv4();
        draw.addFeatures([
          {
            id: tempId,
            type: "Feature",
            properties: { mode: geometryToModeName(feature.geometry.type), name: identity.name, ...style },
            geometry: roundGeometryPrecision(feature.geometry),
          },
        ]);
        return createAreaFromPendingFeature(
          tempId,
          identity.identifier,
          identity.name,
          identity.locked,
          style,
          { suppressToast: true },
        );
      },
      skipIdentifiers,
    );

    const total = features.length;
    const parts = [`${result.created.length} of ${total} areas imported`];
    if (skipIdentifiers.size > 0) {
      parts.push(`${skipIdentifiers.size} skipped (already exists)`);
    }
    if (result.failed.length > 0) {
      parts.push(`${result.failed.length} failed`);
    }
    showToast(result.failed.length === 0 ? "success" : "error", `${parts.join("; ")}.`);

    // Matches the initial-load fit-bounds behavior, so imported areas outside the current viewport are visible without a refresh.
    if (result.created.length > 0) {
      const bounds = computeBounds(areasRef.current);
      if (bounds) mapRef.current?.fitBounds(bounds, { padding: 40 });
    }
  }

  // Recomputes the rename preview via the real batch resolver, so it can never diverge from what importAreasBatch would actually produce.
  function computeAreaConflictPreview(choices: Map<string, ConflictChoice>): Map<string, string> {
    if (!pendingImportFeatures) return new Map();
    const skipIdentifiers = new Set(
      [...choices].filter(([, choice]) => choice === "skip").map(([identifier]) => identifier),
    );
    const entries = resolveImportIdentities(
      pendingImportFeatures,
      areas.map((a) => a.identifier),
      skipIdentifiers,
    );
    const preview = new Map<string, string>();
    for (const { feature, identity } of entries) {
      const props = feature.properties ?? {};
      const original = typeof props.identifier === "string" ? props.identifier.trim() : "";
      if (original && !preview.has(original)) preview.set(original, identity.identifier);
    }
    return preview;
  }

  function handleAreaConflictConfirm(choices: Map<string, ConflictChoice>) {
    const features = pendingImportFeatures;
    setPendingImportFeatures(null);
    setConflictIdentifiers([]);
    if (!features) return;
    const skipIdentifiers = new Set(
      [...choices].filter(([, choice]) => choice === "skip").map(([identifier]) => identifier),
    );
    void importFeaturesBatch(features, skipIdentifiers);
  }

  function handleAreaConflictCancel() {
    setPendingImportFeatures(null);
    setConflictIdentifiers([]);
  }

  // Creates immediately if the feature has a usable name + non-duplicate identifier; otherwise falls through to AreaNameModal to reuse its validation.
  function handleImportFeature(feature: ImportedFeature) {
    requestSwitch(() => {
      const draw = drawRef.current;
      if (!draw) return;

      const props = feature.properties ?? {};
      const rawName = typeof props.name === "string" ? props.name : "";
      const rawIdentifier = typeof props.identifier === "string" ? props.identifier : "";
      const rawLocked = typeof props.locked === "boolean" ? props.locked : false;
      const rawStyle = extractStyleFields(props);
      const identifierUsable =
        rawIdentifier.trim() !== "" &&
        IDENTIFIER_PATTERN.test(rawIdentifier) &&
        !areas.some((a) => a.identifier === rawIdentifier);

      const tempId = uuidv4();
      draw.addFeatures([
        {
          id: tempId,
          type: "Feature",
          properties: { mode: geometryToModeName(feature.geometry.type), name: rawName, ...rawStyle },
          geometry: roundGeometryPrecision(feature.geometry),
        },
      ]);
      setDraft(null);
      setOriginal(null);

      if (rawName.trim() && identifierUsable) {
        void createAreaFromPendingFeature(tempId, rawIdentifier, rawName.trim(), rawLocked, rawStyle);
      } else {
        setPendingNameSuggestion(rawName || null);
        setPendingGeometryType(feature.geometry.type);
        setPendingLocked(rawLocked);
        setPendingStyle(rawStyle);
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
    setPendingStyle({});
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

  // Saves immediately rather than through the dirty/Save flow -- a direct state flip, not an in-progress edit to discard.
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
    setGuideLines([]);
    // Clears every style key back to original's value (or undefined), so a live color-picker preview fully reverts on Discard.
    const revertedStyle: Record<string, string | number | undefined> = {};
    for (const key of STYLE_KEYS) revertedStyle[key] = original[key];
    drawRef.current?.updateFeatureProperties(original.identifier, revertedStyle);
  }

  async function handleDeleteConfirmed() {
    if (!deleteTarget) return;
    setDeleting(true);
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
      setDeleting(false);
      setDeleteTarget(null);
    }
  }

  const toolbar = (
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
  );

  // Renders exactly once regardless of breakpoint; repositioned via CSS but never duplicated/remounted, since MapLibre is attached to mapContainerRef.
  const mapPane = (
    <div className="relative h-[400px] overflow-hidden rounded-md border border-slate-200 md:order-2 md:h-auto md:flex-1 dark:border-slate-700">
      {/* h-full/w-full, not absolute+inset-0: MapLibre's own `maplibregl-map` class sets position:relative and lands later in the
          built CSS than Tailwind's .absolute, silently overriding it and collapsing this div to near-zero height. */}
      <div ref={mapContainerRef} className="h-full w-full" />
      {/* Screen-space SVG overlay matches map.project()'s pixel origin directly, simpler than round-tripping through a MapLibre GeoJSON layer. */}
      {guideLines.length > 0 && (
        <svg className="pointer-events-none absolute inset-0 h-full w-full" aria-hidden="true">
          {guideLines.map((g, i) => (
            <line
              key={i}
              x1={g.axis === "x" ? g.pos : g.from}
              y1={g.axis === "x" ? g.from : g.pos}
              x2={g.axis === "x" ? g.pos : g.to}
              y2={g.axis === "x" ? g.to : g.pos}
              stroke="#ec4899"
              strokeWidth={1.5}
              strokeDasharray="4 4"
            />
          ))}
        </svg>
      )}
    </div>
  );

  return (
    <div className="flex flex-col gap-4 md:h-full md:flex-row md:gap-6">
      {/* Mobile: sticky toolbar+map header above a scrolling list. `md:contents` removes this wrapper's own box at desktop width, so its
          children become ordinary flex items reordered by the `order` utilities on the list column and mapPane. */}
      <div className="sticky top-0 z-10 flex flex-col gap-2 bg-slate-50 pb-3 dark:bg-slate-900 md:contents">
        <div className="md:hidden">{toolbar}</div>
        {mapPane}
      </div>

      <div className="flex flex-col gap-2 md:order-1 md:w-72 md:shrink-0">
        <div className="hidden md:block">{toolbar}</div>

        {loading ? (
          <p className="text-slate-400">Loading areas...</p>
        ) : (
          <ul className="flex flex-col gap-1">
            {[...areas]
              .sort((a, b) =>
                (a.name || a.identifier).localeCompare(b.name || b.identifier),
              )
              .map((area) => {
              const isSelected = draft?.identifier === area.identifier;
              return (
                <li
                  key={area.identifier}
                  className={`mb-2 overflow-hidden rounded-md border-l-4 ${
                    isSelected
                      ? "border-sky-600 bg-slate-100 dark:border-sky-400 dark:bg-slate-800"
                      : "border-transparent bg-slate-50 hover:bg-slate-100 dark:bg-slate-800/40 dark:hover:bg-slate-800"
                  }`}
                >
                  {isSelected && draft ? (
                    <div className="flex flex-col gap-3 px-3 py-3">
                      <input
                        type="text"
                        value={draft.name}
                        onChange={(e) => setDraft({ ...draft, name: e.target.value })}
                        placeholder="Display name"
                        className="rounded-md border border-slate-300 px-2 py-1 text-sm dark:border-slate-600 dark:bg-slate-900"
                      />
                      {/* Color control(s) matching geometry type; live-previewed on the map via updateFeatureProperties, not just on Save. */}
                      {draft.geometry.type === "Polygon" && (
                        <div className="flex gap-2">
                          <label className="flex flex-1 items-center gap-2 text-xs font-medium text-slate-600 dark:text-slate-300">
                            Fill
                            <input
                              type="color"
                              value={draft.fill ?? DEFAULT_SHAPE_COLOR}
                              onChange={(e) => {
                                setDraft({ ...draft, fill: e.target.value });
                                drawRef.current?.updateFeatureProperties(draft.identifier, { fill: e.target.value });
                              }}
                              className="h-7 flex-1 cursor-pointer rounded border border-slate-300 dark:border-slate-600"
                            />
                          </label>
                          <label className="flex flex-1 items-center gap-2 text-xs font-medium text-slate-600 dark:text-slate-300">
                            Stroke
                            <input
                              type="color"
                              value={draft.stroke ?? DEFAULT_SHAPE_COLOR}
                              onChange={(e) => {
                                setDraft({ ...draft, stroke: e.target.value });
                                drawRef.current?.updateFeatureProperties(draft.identifier, { stroke: e.target.value });
                              }}
                              className="h-7 flex-1 cursor-pointer rounded border border-slate-300 dark:border-slate-600"
                            />
                          </label>
                        </div>
                      )}
                      {draft.geometry.type === "LineString" && (
                        <label className="flex items-center gap-2 text-xs font-medium text-slate-600 dark:text-slate-300">
                          Stroke
                          <input
                            type="color"
                            value={draft.stroke ?? DEFAULT_SHAPE_COLOR}
                            onChange={(e) => {
                              setDraft({ ...draft, stroke: e.target.value });
                              drawRef.current?.updateFeatureProperties(draft.identifier, { stroke: e.target.value });
                            }}
                            className="h-7 flex-1 cursor-pointer rounded border border-slate-300 dark:border-slate-600"
                          />
                        </label>
                      )}
                      {draft.geometry.type === "Point" && (
                        <label className="flex items-center gap-2 text-xs font-medium text-slate-600 dark:text-slate-300">
                          Marker color
                          <input
                            type="color"
                            value={draft["marker-color"] ?? DEFAULT_SHAPE_COLOR}
                            onChange={(e) => {
                              setDraft({ ...draft, "marker-color": e.target.value });
                              drawRef.current?.updateFeatureProperties(draft.identifier, { "marker-color": e.target.value });
                            }}
                            className="h-7 flex-1 cursor-pointer rounded border border-slate-300 dark:border-slate-600"
                          />
                        </label>
                      )}
                      <button
                        type="button"
                        onClick={handleSave}
                        disabled={!dirty || saving}
                        className="w-full rounded-md bg-sky-600 px-4 py-2.5 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-40"
                      >
                        Save
                      </button>
                      <div className="grid grid-cols-2 gap-2">
                        <button
                          type="button"
                          onClick={handleDiscard}
                          disabled={!dirty}
                          className="rounded-md border border-slate-300 px-3 py-2.5 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
                        >
                          Discard
                        </button>
                        <button
                          type="button"
                          onClick={duplicateArea}
                          className="rounded-md border border-slate-300 px-3 py-2.5 text-sm font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
                        >
                          Duplicate
                        </button>
                      </div>
                      <div className="grid grid-cols-2 gap-2">
                        <button
                          type="button"
                          onClick={toggleLock}
                          disabled={saving}
                          className="flex items-center justify-center gap-1 rounded-md border border-slate-300 px-3 py-2.5 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
                        >
                          {draft.locked ? <Unlock size={16} /> : <Lock size={16} />}
                          {draft.locked ? "Unlock" : "Lock"}
                        </button>
                        <button
                          type="button"
                          onClick={exportSelectedArea}
                          className="flex items-center justify-center gap-1 rounded-md border border-slate-300 px-3 py-2.5 text-sm font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
                        >
                          <MdiIcon path={mdiExportVariant} size={16} />
                          Export
                        </button>
                      </div>
                      <button
                        type="button"
                        onClick={() => setDeleteTarget(area)}
                        className="w-full rounded-md border border-red-300 px-4 py-2.5 text-sm font-medium text-red-600 hover:bg-red-50 dark:border-red-800 dark:hover:bg-red-950"
                      >
                        Delete
                      </button>
                    </div>
                  ) : (
                    <div className="flex items-center">
                      <button
                        type="button"
                        onClick={() => selectArea(area)}
                        title={area.identifier}
                        className="flex-1 truncate px-3 py-3 text-left text-sm text-slate-700 dark:text-slate-200"
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
        confirmLoading={deleting}
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
        onImport={(features) => {
          setImportModalOpen(false);
          handleImportFeatures(features);
        }}
        onCancel={() => setImportModalOpen(false)}
      />

      <ImportConflictModal
        open={conflictIdentifiers.length > 0}
        noun="area"
        identifiers={conflictIdentifiers}
        computePreview={computeAreaConflictPreview}
        onConfirm={handleAreaConflictConfirm}
        onCancel={handleAreaConflictCancel}
      />
    </div>
  );
}
