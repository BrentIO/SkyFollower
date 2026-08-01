import { useEffect, useMemo, useRef, useState } from "react";
import type { AreaGeometry } from "../api/areas";

export interface ImportedFeature {
  type: "Feature";
  geometry: AreaGeometry;
  properties: Record<string, unknown>;
}

interface ImportAreaModalProps {
  open: boolean;
  onImport: (feature: ImportedFeature) => void;
  onCancel: () => void;
}

const SUPPORTED_GEOMETRY_TYPES = new Set(["Polygon", "LineString", "Point"]);

interface ParseResult {
  error: string | null;
  feature: ImportedFeature | null;
}

// Funnels both the drop zone (via a pretty-printed textbox population) and
// direct textbox edits/pastes through the same validation, per the issue's
// "both funnel through this" requirement. Blank input is a pristine/no-op
// state (Import disabled, no banner) rather than a "Not valid GeoJSON"
// error -- showing an error on a modal the user hasn't touched yet would be
// jarring, and isn't what the acceptance criteria are actually enumerating.
function parseAndValidate(text: string): ParseResult {
  if (!text.trim()) return { error: null, feature: null };

  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { error: "Not valid GeoJSON.", feature: null };
  }

  if (
    typeof parsed !== "object" ||
    parsed === null ||
    (parsed as { type?: unknown }).type !== "FeatureCollection" ||
    !Array.isArray((parsed as { features?: unknown }).features)
  ) {
    return { error: "Not valid GeoJSON.", feature: null };
  }

  const features = (parsed as { features: unknown[] }).features;
  if (features.length !== 1) {
    return { error: "Only one GeoJSON feature can be imported at a time.", feature: null };
  }

  const feature = features[0] as {
    geometry?: { type?: string };
    properties?: Record<string, unknown> | null;
  };
  const geometryType = feature.geometry?.type;
  if (!geometryType || !SUPPORTED_GEOMETRY_TYPES.has(geometryType)) {
    return { error: `Unsupported geometry type: ${geometryType ?? "unknown"}.`, feature: null };
  }

  return {
    error: null,
    feature: {
      type: "Feature",
      geometry: feature.geometry as AreaGeometry,
      properties: feature.properties ?? {},
    },
  };
}

// Counterpart to AreasView.tsx's own Export/"Export all" actions -- imports
// a single area from a GeoJSON FeatureCollection (drag-drop,
// click/tap-to-browse, or direct paste into the textbox). Name/identifier/
// locked resolution from the parsed feature's properties, and the
// missing/duplicate-identifier fallback to AreaNameModal, both live in
// AreasView.tsx (the parent) -- this component's only job is getting a
// validated single Feature out to its onImport callback.
export function ImportAreaModal({ open, onImport, onCancel }: ImportAreaModalProps) {
  const [text, setText] = useState("");
  const [fileError, setFileError] = useState<string | null>(null);
  const [dragOver, setDragOver] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (open) {
      setText("");
      setFileError(null);
      setDragOver(false);
    }
  }, [open]);

  const { error: contentError, feature } = useMemo(() => parseAndValidate(text), [text]);
  const bannerError = fileError ?? contentError;

  if (!open) return null;

  function readFile(file: File) {
    if (!/\.geojson$/i.test(file.name)) {
      setFileError("File must have a .geojson extension.");
      return;
    }
    setFileError(null);
    const reader = new FileReader();
    reader.onload = () => {
      if (typeof reader.result !== "string") return;
      // Pretty-print only applies to file-derived population -- a direct
      // edit/paste into the textbox is re-validated as-is, not reformatted
      // out from under the user while they're typing.
      try {
        setText(JSON.stringify(JSON.parse(reader.result), null, 2));
      } catch {
        setText(reader.result);
      }
    };
    reader.readAsText(file);
  }

  function handleDrop(e: React.DragEvent<HTMLDivElement>) {
    e.preventDefault();
    setDragOver(false);
    const file = e.dataTransfer.files[0];
    if (file) readFile(file);
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="flex max-h-[85vh] w-full max-w-lg flex-col rounded-lg bg-white p-6 shadow-xl dark:bg-slate-800">
        <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">Import area</h2>

        <div
          onClick={() => fileInputRef.current?.click()}
          onDragOver={(e) => {
            e.preventDefault();
            setDragOver(true);
          }}
          onDragLeave={() => setDragOver(false)}
          onDrop={handleDrop}
          role="button"
          tabIndex={0}
          className={`mt-4 flex cursor-pointer flex-col items-center justify-center rounded-md border-2 border-dashed px-4 py-6 text-center text-sm ${
            dragOver
              ? "border-sky-500 bg-sky-50 dark:bg-sky-950"
              : "border-slate-300 text-slate-500 dark:border-slate-600 dark:text-slate-400"
          }`}
        >
          Drag and drop a .geojson file here, or click to browse
          <input
            ref={fileInputRef}
            type="file"
            accept=".geojson"
            className="hidden"
            onChange={(e) => {
              const file = e.target.files?.[0];
              if (file) readFile(file);
              e.target.value = "";
            }}
          />
        </div>

        <label className="mt-4 flex min-h-0 flex-1 flex-col text-sm font-medium text-slate-700 dark:text-slate-200">
          GeoJSON
          <textarea
            value={text}
            onChange={(e) => setText(e.target.value)}
            placeholder='{"type": "FeatureCollection", "features": [...]}'
            spellCheck={false}
            className="mt-1 min-h-40 flex-1 rounded-md border border-slate-300 px-3 py-1.5 font-mono text-xs dark:border-slate-600 dark:bg-slate-900"
          />
        </label>
        {bannerError && <p className="mt-2 text-sm text-red-600 dark:text-red-400">{bannerError}</p>}

        <div className="mt-6 flex justify-end gap-3">
          <button
            type="button"
            onClick={onCancel}
            className="rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={feature === null}
            onClick={() => {
              if (feature) onImport(feature);
            }}
            className="rounded-md bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-40"
          >
            Import
          </button>
        </div>
      </div>
    </div>
  );
}
