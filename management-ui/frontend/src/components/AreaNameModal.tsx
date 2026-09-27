import { useEffect, useState } from "react";
import { geometryDisplayNoun, type AreaGeometry } from "../api/areas";

interface AreaNameModalProps {
  open: boolean;
  existingIdentifiers: string[];
  // Pre-fills Name/Identifier; used by "Duplicate" to suggest "<name> copy"
  // instead of starting blank like a freshly drawn shape.
  initialName?: string;
  // Drives the modal's title ("Name this area"/"line"/"point").
  geometryType: AreaGeometry["type"];
  onConfirm: (identifier: string, name: string) => void;
  onCancel: () => void;
}

// Non-empty, no whitespace -- matches the backend's _IDENTIFIER_PATTERN.
// Exported so AreasView.tsx can apply the same rule to imported identifiers.
export const IDENTIFIER_PATTERN = /^\S+$/;

// Exported so lib/areaImport.ts's bulk-import resolver derives identifiers
// from names the same way, without reimplementing the rule.
export function sanitizeIdentifier(raw: string): string {
  return raw.replace(/\s/g, "_");
}

// Collects the two fields Area requires beyond geometry: `identifier`
// (routing key, no spaces, immutable) and `name` (editable display label).
export function AreaNameModal({
  open,
  existingIdentifiers,
  initialName,
  geometryType,
  onConfirm,
  onCancel,
}: AreaNameModalProps) {
  const [name, setName] = useState("");
  const [identifier, setIdentifier] = useState("");
  const [identifierManuallyEdited, setIdentifierManuallyEdited] = useState(false);

  // Seeds Name/Identifier fresh each time the modal opens.
  useEffect(() => {
    if (open) {
      setName(initialName ?? "");
      setIdentifier(initialName ? sanitizeIdentifier(initialName) : "");
      setIdentifierManuallyEdited(false);
    }
  }, [open, initialName]);

  if (!open) return null;

  function handleNameChange(value: string) {
    setName(value);
    if (!identifierManuallyEdited) {
      setIdentifier(sanitizeIdentifier(value));
    }
  }

  const trimmedIdentifier = identifier.trim();
  const error = !trimmedIdentifier
    ? "Identifier is required."
    : !IDENTIFIER_PATTERN.test(trimmedIdentifier)
      ? "Identifier may not contain spaces."
      : existingIdentifiers.includes(trimmedIdentifier)
        ? `Identifier '${trimmedIdentifier}' is already used by another area.`
        : null;

  function reset() {
    setName("");
    setIdentifier("");
    setIdentifierManuallyEdited(false);
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="w-full max-w-sm rounded-lg bg-white p-6 shadow-xl dark:bg-slate-800">
        <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
          Name this {geometryDisplayNoun(geometryType).toLowerCase()}
        </h2>

        <label className="mt-4 block text-sm font-medium text-slate-700 dark:text-slate-200">
          Name
          <input
            type="text"
            autoFocus
            value={name}
            onChange={(e) => handleNameChange(e.target.value)}
            placeholder="Display name"
            className="mt-1 block w-full rounded-md border border-slate-300 px-3 py-1.5 text-sm dark:border-slate-600 dark:bg-slate-900"
          />
        </label>

        <label className="mt-3 block text-sm font-medium text-slate-700 dark:text-slate-200">
          Identifier
          <input
            type="text"
            value={identifier}
            onChange={(e) => {
              setIdentifierManuallyEdited(true);
              setIdentifier(sanitizeIdentifier(e.target.value));
            }}
            placeholder="Unique identifier (no spaces)"
            className="mt-1 block w-full rounded-md border border-slate-300 px-3 py-1.5 font-mono text-sm dark:border-slate-600 dark:bg-slate-900"
          />
        </label>
        {error && <p className="mt-2 text-sm text-red-600 dark:text-red-400">{error}</p>}

        <div className="mt-6 flex justify-end gap-3">
          <button
            type="button"
            onClick={() => {
              reset();
              onCancel();
            }}
            className="rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
          >
            Cancel
          </button>
          <button
            type="button"
            disabled={error !== null}
            onClick={() => {
              onConfirm(trimmedIdentifier, name.trim());
              reset();
            }}
            className="rounded-md bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-40"
          >
            Save
          </button>
        </div>
      </div>
    </div>
  );
}
