import { useEffect, useMemo, useRef, useState } from "react";
import { parseAndValidate, type ImportedFeature } from "../lib/areaImport";

interface ImportAreaModalProps {
  open: boolean;
  onImport: (features: ImportedFeature[]) => void;
  onCancel: () => void;
}

// Counterpart to AreasView.tsx's own Export/"Export all" actions -- imports
// one or more areas from a GeoJSON FeatureCollection (drag-drop,
// click/tap-to-browse, or direct paste into the textbox). Structural
// validation (parseAndValidate) is a whole-file hard gate shared with the
// batch importer. Per-feature identifier/name resolution -- the
// single-feature fallback to AreaNameModal, and the multi-feature
// auto-suffix -- lives in AreasView.tsx (the parent); this component's only
// job is getting a validated feature array out to its onImport callback.
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

  const { error: contentError, features } = useMemo(() => parseAndValidate(text), [text]);
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
            disabled={features.length === 0}
            onClick={() => {
              if (features.length > 0) onImport(features);
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
