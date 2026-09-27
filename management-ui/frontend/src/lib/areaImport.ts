import type { AreaGeometry } from "../api/areas";
import { IDENTIFIER_PATTERN, sanitizeIdentifier } from "../components/AreaNameModal";

// Structurally validated (geometry is a supported type); the parent view
// resolves identifier/name/locked/style from `properties`.
export interface ImportedFeature {
  type: "Feature";
  geometry: AreaGeometry;
  properties: Record<string, unknown>;
}

export interface ParseResult {
  // Non-null rejects the whole file -- nothing is imported, even features
  // that were individually valid.
  error: string | null;
  features: ImportedFeature[];
}

// Geometry types AreasView can render and store; anything else is rejected.
export const SUPPORTED_GEOMETRY_TYPES = new Set<string>(["Polygon", "LineString", "Point"]);

// Whole-file hard gate: returns every feature or an error and nothing. Blank
// input is the pristine no-op state (no error, no features), not an error.
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

// Terra Draw silently drops features exceeding its coordinatePrecision ceiling
// (default 9), and mapping tools routinely export 15-decimal coordinates. 5
// places matches the pipeline's own coordinate-precision cap app-wide.
export const IMPORT_COORDINATE_PRECISION = 5;

function roundToPrecision(value: number, precision: number): number {
  const factor = 10 ** precision;
  return Math.round(value * factor) / factor;
}

// Pure; preserves any third ordinate (elevation) untouched.
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

// Used only to synthesize a name/identifier, e.g. "Imported polygon 1".
// Distinct from api/areas' user-facing geometryDisplayNoun.
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

// Per-identifier choice for an import conflict: skip the duplicate, or keep
// both via an auto-suffixed rename. Drives ImportConflictModal.
export type ImportConflictChoice = "skip" | "rename";

// Distinct imported identifiers that collide with `existingIdentifiers`, in file
// order. Feeds ImportConflictModal; an empty result skips the modal entirely.
export function collidingIdentifiers(
  features: ImportedFeature[],
  existingIdentifiers: string[],
): string[] {
  const existing = new Set(existingIdentifiers);
  const seen = new Set<string>();
  const out: string[] = [];
  for (const feature of features) {
    const props = feature.properties ?? {};
    const raw = typeof props.identifier === "string" ? props.identifier.trim() : "";
    if (raw && existing.has(raw) && !seen.has(raw)) {
      seen.add(raw);
      out.push(raw);
    }
  }
  return out;
}

// Auto-resolves one feature's identifier/name in a multi-feature import so bulk
// import never has to prompt. `taken` covers existing areas plus features already
// resolved in this batch, and gains this result too, so in-file collisions also get
// an incrementing `_2`/`(2)` suffix.
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

export interface ResolvedFeatureImportEntry {
  feature: ImportedFeature;
  identity: ResolvedFeatureIdentity;
}

// A feature whose identifier is in `skipIdentifiers` is left out entirely, as if
// absent from the file. Shared by importAreasBatch and ImportConflictModal's
// live preview so the preview can never diverge from the real import.
export function resolveImportIdentities(
  features: ImportedFeature[],
  existingIdentifiers: string[],
  skipIdentifiers: ReadonlySet<string> = new Set(),
): ResolvedFeatureImportEntry[] {
  const taken = new Set(existingIdentifiers);
  const resolved: ResolvedFeatureImportEntry[] = [];
  for (let i = 0; i < features.length; i++) {
    const props = features[i].properties ?? {};
    const raw = typeof props.identifier === "string" ? props.identifier.trim() : "";
    if (raw && skipIdentifiers.has(raw)) continue;
    resolved.push({ feature: features[i], identity: resolveFeatureIdentity(features[i], i + 1, taken) });
  }
  return resolved;
}

export interface BatchImportResult {
  created: { identifier: string; name: string }[];
  failed: { identifier: string }[];
}

// Best-effort: a `createOne` failure for one feature doesn't stop the rest of
// the batch; the caller gets a created/failed summary.
export async function importAreasBatch(
  features: ImportedFeature[],
  existingIdentifiers: string[],
  createOne: (identity: ResolvedFeatureIdentity, feature: ImportedFeature) => Promise<boolean>,
  skipIdentifiers: ReadonlySet<string> = new Set(),
): Promise<BatchImportResult> {
  const created: BatchImportResult["created"] = [];
  const failed: BatchImportResult["failed"] = [];

  const entries = resolveImportIdentities(features, existingIdentifiers, skipIdentifiers);
  for (const { feature, identity } of entries) {
    let ok = false;
    try {
      ok = await createOne(identity, feature);
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
