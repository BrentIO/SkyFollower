import { useEffect, useState } from "react";

interface NewSearchModalProps {
  open: boolean;
  onConfirm: (name: string, whereClause: string) => void;
  onCancel: () => void;
  // Pre-fills the form -- used to resubmit a failed/aborted search without
  // retyping its WHERE clause from scratch. Omitted (or "") for a blank
  // "+ New Search" open.
  initialName?: string;
  initialWhereClause?: string;
  title?: string;
}

// Queryable columns and their types, from specs/data-dictionary.yaml's
// archive_parquet_index record -- this is the whole point of the legend:
// writing SQL against exact column names from memory, so it stays visible
// the entire time (not a placeholder that vanishes once typing starts).
// Example values are all the same aircraft (N659DL / A8AE7F), matching
// the example used elsewhere in this app (e.g. LookupView.tsx's
// placeholder text) rather than a different one per row.
const COLUMN_REFERENCE: [string, string, string][] = [
  ["icao_hex", "string", "A8AE7F"],
  ["registration", "string", "N659DL"],
  ["type_designator", "string", "B752"],
  ["military", "boolean", "false"],
  ["operator_designator", "string", "DAL"],
  ["ident", "string", "DAL2"],
  ["first_message", "timestamp", "2026-07-31 12:00:00"],
  ["last_message", "timestamp", "2026-07-31 13:45:00"],
];

// Shown once, when the user clicks "+ New Search" -- collects the two
// fields ArchiveSearchCreate requires (see api/archiveSearch.ts): `name`
// and a raw SQL `where_clause`. Matches AreaNameModal.tsx's structure
// (open/onConfirm/onCancel, reset-on-close, disabled-until-valid Save).
export function NewSearchModal({
  open,
  onConfirm,
  onCancel,
  initialName = "",
  initialWhereClause = "",
  title = "New Search",
}: NewSearchModalProps) {
  const [name, setName] = useState(initialName);
  const [whereClause, setWhereClause] = useState(initialWhereClause);

  useEffect(() => {
    if (open) {
      setName(initialName);
      setWhereClause(initialWhereClause);
    }
  }, [open, initialName, initialWhereClause]);

  if (!open) return null;

  const trimmedName = name.trim();
  const trimmedWhereClause = whereClause.trim();
  const canSubmit = trimmedName !== "" && trimmedWhereClause !== "";

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="w-full max-w-xl rounded-lg bg-white p-6 shadow-xl dark:bg-slate-800">
        <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">{title}</h2>

        <label className="mt-4 block text-sm font-medium text-slate-700 dark:text-slate-200">
          Name
          <input
            type="text"
            autoFocus
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="Display name for this search"
            className="mt-1 block w-full rounded-md border border-slate-300 px-3 py-1.5 text-sm dark:border-slate-600 dark:bg-slate-900"
          />
        </label>

        <label className="mt-3 block text-sm font-medium text-slate-700 dark:text-slate-200">
          WHERE clause
          <textarea
            value={whereClause}
            onChange={(e) => setWhereClause(e.target.value)}
            placeholder="icao_hex = 'A8AE7F'"
            rows={4}
            className="mt-1 block w-full rounded-md border border-slate-300 px-3 py-1.5 font-mono text-sm dark:border-slate-600 dark:bg-slate-900"
          />
        </label>

        <div className="mt-2 rounded-md bg-slate-50 p-3 text-xs dark:bg-slate-900">
          <p className="mb-2 font-semibold text-slate-600 dark:text-slate-300">Available Fields</p>
          <div className="grid grid-cols-[auto_auto_auto] gap-x-4 gap-y-0.5 font-mono text-slate-500 dark:text-slate-400">
            <span className="whitespace-nowrap font-sans font-semibold text-slate-600 dark:text-slate-300">Field</span>
            <span className="whitespace-nowrap font-sans font-semibold text-slate-600 dark:text-slate-300">Type</span>
            <span className="whitespace-nowrap font-sans font-semibold text-slate-600 dark:text-slate-300">Example</span>
            {COLUMN_REFERENCE.map(([column, type, example]) => (
              <div key={column} className="contents">
                <span className="whitespace-nowrap">{column}</span>
                <span className="whitespace-nowrap">{type}</span>
                <span className="whitespace-nowrap">{example}</span>
              </div>
            ))}
          </div>
          <p className="mt-2 text-slate-500 dark:text-slate-400">
            Timestamp literal: <code className="font-mono">first_message &gt;= TIMESTAMP &apos;2024-01-01 00:00:00&apos;</code>
          </p>
        </div>

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
            disabled={!canSubmit}
            onClick={() => onConfirm(trimmedName, trimmedWhereClause)}
            className="rounded-md bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-40"
          >
            Search
          </button>
        </div>
      </div>
    </div>
  );
}
