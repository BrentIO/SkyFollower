import type { AreaGeometry } from "../api/areas";
import { IDENTIFIER_PATTERN, sanitizeIdentifier } from "../components/AreaNameModal";

// A single feature pulled out of an imported GeoJSON FeatureCollection,
// already structurally validated (geometry is one of the supported types).
// The parent view resolves identifier/name/locked/style from `properties`.
export interface ImportedFeature {
  type: "Feature";
  geometry: AreaGeometry;
  properties: Record<string, unknown>;
}

export interface ParseResult {
  // Non-null means the whole file is rejected -- nothing is imported, not
  // even the features that were individually fine. Set for malformed JSON,
  // a non-FeatureCollection root, an empty feature list, or any one feature
  // with an unsupported/missing geometry type.
  error: string | null;
  features: ImportedFeature[];
}

// Polygon, LineString, Point -- the geometry types AreasView can actually
// render and store. A MultiPolygon, GeometryCollection, etc. is rejected.
export const SUPPORTED_GEOMETRY_TYPES = new Set<string>(["Polygon", "LineString", "Point"]);

// Structural validation of the pasted / dropped GeoJSON. This is a
// whole-file hard gate: it either returns every feature (all with a
// supported geometry type) or an error and nothing. It is exactly as
// strict as the original single-feature importer -- the only thing removed
// is the "features.length must be 1" restriction. Blank input is the
// pristine no-op state (no error, no features), not an error, so a modal
// the user hasn't touched yet doesn't show a red banner.
export function parseAndValidate(text: string): ParseResult {
  if (!text.trim()) return { error: null, features: [] };

  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { error: "Not valid GeoJSON.", features: [] };
  }

  if (
    typeof parsed !== "object" ||
    parsed === null ||
    (parsed as { type?: unknown }).type !== "FeatureCollection" ||
    !Array.isArray((parsed as { features?: unknown }).features)
  ) {
    return { error: "Not valid GeoJSON.", features: [] };
  }

  const rawFeatures = (parsed as { features: unknown[] }).features;
  if (rawFeatures.length === 0) {
    return { error: "The FeatureCollection has no features.", features: [] };
  }

  const features: ImportedFeature[] = [];
  for (let i = 0; i < rawFeatures.length; i++) {
    const f = rawFeatures[i] as {
      geometry?: { type?: string };
      properties?: Record<string, unknown> | null;
    };
    const geometryType = f.geometry?.type;
    if (!geometryType || !SUPPORTED_GEOMETRY_TYPES.has(geometryType)) {
      return {
        error: `Feature ${i + 1}: unsupported geometry type "${geometryType ?? "unknown"}".`,
        features: [],
      };
    }
    features.push({
      type: "Feature",
      geometry: f.geometry as AreaGeometry,
      properties: f.properties ?? {},
    });
  }

  return { error: null, features };
}

// Precision (decimal places) every imported coordinate is rounded to
// before the feature reaches Terra Draw's draw.addFeatures(). Mapping tools
// routinely export 15-decimal-place coordinates; Terra Draw silently drops
// (does not throw for) any feature whose coordinates exceed its
// coordinatePrecision ceiling, which defaults to 9 -- the same rejection
// AreasView's offsetGeometry already rounds to avoid for duplicated areas.
// 5 places (~1.1 m at mid latitudes) matches shared/models.py's
// Position._cap_coordinate_precision, the cap the pipeline imposes on every
// ingested ADS-B position -- one coordinate-precision convention app-wide.
export const IMPORT_COORDINATE_PRECISION = 5;

function roundToPrecision(value: number, precision: number): number {
  const factor = 10 ** precision;
  return Math.round(value * factor) / factor;
}

// Returns a copy of `geometry` with every coordinate's longitude and
// latitude rounded to IMPORT_COORDINATE_PRECISION. Any third ordinate
// (elevation) is preserved untouched. Pure -- does not mutate the input.
// Applied by both of AreasView's import entry points (single-feature and
// batch) before the feature is handed to Terra Draw.
export function roundGeometryPrecision(geometry: AreaGeometry): AreaGeometry {
  const round = (c: number[]): number[] => [
    roundToPrecision(c[0], IMPORT_COORDINATE_PRECISION),
    roundToPrecision(c[1], IMPORT_COORDINATE_PRECISION),
    ...c.slice(2),
  ];
  switch (geometry.type) {
    case "Polygon":
      return { ...geometry, coordinates: geometry.coordinates.map((ring) => ring.map(round)) };
    case "LineString":
      return { ...geometry, coordinates: geometry.coordinates.map(round) };
    case "Point":
      return { ...geometry, coordinates: round(geometry.coordinates) };
  }
}

// Lower-case noun used only when synthesising a name/identifier for a
// feature that carries neither -- e.g. "Imported polygon 1". Distinct from
// api/areas' geometryDisplayNoun ("Area"/"Line"/"Point"), which is the
// user-facing label language for toasts and the naming modal's title.
export function geometryImportNoun(type: AreaGeometry["type"]): string {
  switch (type) {
    case "Polygon":
      return "polygon";
    case "LineString":
      return "line";
    case "Point":
      return "point";
  }
}

export interface ResolvedFeatureIdentity {
  identifier: string;
  name: string;
  locked: boolean;
}

// Auto-resolves the identifier/name for one feature in a multi-feature
// import, so bulk import never has to stop and prompt (the single-feature
// path still falls through to AreaNameModal -- that stays unchanged).
//
// - `index` is the feature's 1-based position in the file, used only for a
//   synthesised name.
// - `taken` is every identifier already claimed: existing areas plus every
//   feature resolved earlier in this same batch. The returned identifier is
//   added to it, so two colliding entries inside one file don't collide
//   with each other either.
//
// A missing, pattern-invalid, or already-taken identifier gets an
// incrementing `_2` / `_3` suffix; the paired name gets the matching
// `(2)` / `(3)`. A feature with neither name nor identifier gets a fully
// synthesised `"Imported {geometry} {index}"` pair.
export function resolveFeatureIdentity(
  feature: ImportedFeature,
  index: number,
  taken: Set<string>,
): ResolvedFeatureIdentity {
  const props = feature.properties ?? {};
  const rawName = typeof props.name === "string" ? props.name.trim() : "";
  const rawIdentifier = typeof props.identifier === "string" ? props.identifier.trim() : "";
  const locked = typeof props.locked === "boolean" ? props.locked : false;

  const synthesized = `Imported ${geometryImportNoun(feature.geometry.type)} ${index}`;
  const identifierValid = rawIdentifier !== "" && IDENTIFIER_PATTERN.test(rawIdentifier);

  let baseName: string;
  let baseIdentifier: string;
  if (rawName === "" && !identifierValid) {
    baseName = synthesized;
    baseIdentifier = sanitizeIdentifier(synthesized);
  } else {
    baseName = rawName !== "" ? rawName : rawIdentifier !== "" ? rawIdentifier : synthesized;
    baseIdentifier = identifierValid ? sanitizeIdentifier(rawIdentifier) : sanitizeIdentifier(baseName);
  }

  let identifier = baseIdentifier;
  let name = baseName;
  if (taken.has(identifier)) {
    let n = 2;
    while (taken.has(`${baseIdentifier}_${n}`)) n++;
    identifier = `${baseIdentifier}_${n}`;
    name = `${baseName} (${n})`;
  }

  taken.add(identifier);
  return { identifier, name, locked };
}

export interface BatchImportResult {
  created: { identifier: string; name: string }[];
  failed: { identifier: string }[];
}

// Drives a multi-feature import: resolve each feature's identity in file
// order (so earlier resolutions feed the collision set for later ones),
// then create it. `createOne` returns false (or throws) for a backend
// rejection or network error on that one feature -- best-effort applies
// here and only here: the rest of the batch still gets created, and the
// caller reports a summary. Structural validity was already a whole-file
// gate in parseAndValidate; this step never relaxes that.
export async function importAreasBatch(
  features: ImportedFeature[],
  existingIdentifiers: string[],
  createOne: (identity: ResolvedFeatureIdentity, feature: ImportedFeature) => Promise<boolean>,
): Promise<BatchImportResult> {
  const taken = new Set(existingIdentifiers);
  const created: BatchImportResult["created"] = [];
  const failed: BatchImportResult["failed"] = [];

  for (let i = 0; i < features.length; i++) {
    const identity = resolveFeatureIdentity(features[i], i + 1, taken);
    let ok = false;
    try {
      ok = await createOne(identity, features[i]);
    } catch {
      ok = false;
    }
    if (ok) {
      created.push({ identifier: identity.identifier, name: identity.name });
    } else {
      failed.push({ identifier: identity.identifier });
    }
  }

  return { created, failed };
}
