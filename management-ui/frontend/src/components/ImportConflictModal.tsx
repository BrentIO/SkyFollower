import { useEffect, useMemo, useState } from "react";

export type ConflictChoice = "skip" | "rename";

interface ImportConflictModalProps {
  open: boolean;
  // Singular noun for the thing being imported -- "rule" or "area" --
  // used only for display copy.
  noun: "rule" | "area";
  // Every colliding identifier, in file order, deduplicated. Ignored while
  // `open` is false.
  identifiers: string[];
  // Computes the live rename preview for the current choice set, using the
  // real resolver (resolveImportIdentifiers / resolveImportIdentities) over
  // the whole import batch -- never a hardcoded guess. Only identifiers
  // whose current choice is "rename" need an entry in the result; a "skip"
  // row shows no preview.
  computePreview: (choices: Map<string, ConflictChoice>) => Map<string, string>;
  onConfirm: (choices: Map<string, ConflictChoice>) => void;
  onCancel: () => void;
}

// Shown before a rule/area import batch runs, whenever one or more imported
// identifiers already exist. One row per colliding identifier, each with
// its own Skip/Rename toggle -- not one global choice for every conflict.
// Defaults every row to Skip: confirming with nothing touched leaves every
// existing rule/area with a colliding identifier completely untouched, and
// excludes only the colliding imported entries -- everything else in the
// file still imports normally. Rename keeps both, auto-suffixing the
// imported one via the same resolver the real import uses (ruleImport's
// resolveImportIdentifiers / areaImport's resolveImportIdentities), so the
// live preview shown here can never diverge from what actually gets
// created. Shared, noun-parameterized component for both RulesView and
// AreasView's batch import flows -- same shape/style as ConfirmModal.
export function ImportConflictModal({
  open,
  noun,
  identifiers,
  computePreview,
  onConfirm,
  onCancel,
}: ImportConflictModalProps) {
  const [choices, setChoices] = useState<Map<string, ConflictChoice>>(new Map());

  useEffect(() => {
    if (open) {
      setChoices(new Map(identifiers.map((id) => [id, "skip" as ConflictChoice])));
    }
  }, [open, identifiers]);

  const previews = useMemo(() => computePreview(choices), [choices, computePreview]);

  if (!open) return null;

  function setChoice(identifier: string, choice: ConflictChoice) {
    setChoices((current) => {
      const next = new Map(current);
      next.set(identifier, choice);
      return next;
    });
  }

  function setAll(choice: ConflictChoice) {
    setChoices(new Map(identifiers.map((id) => [id, choice])));
  }

  const renamedCount = [...choices.values()].filter((c) => c === "rename").length;
  const skippedCount = choices.size - renamedCount;
  const nounPlural = `${noun}${identifiers.length === 1 ? "" : "s"}`;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="flex max-h-[85vh] w-full max-w-lg flex-col rounded-lg bg-white p-6 shadow-xl dark:bg-slate-800">
        <h2 className="text-lg font-semibold text-slate-900 dark:text-slate-100">
          {identifiers.length} colliding {nounPlural}
        </h2>
        <p className="mt-2 text-sm text-slate-600 dark:text-slate-300">
          These identifiers already exist. Skip keeps the existing {noun} untouched and drops the imported
          duplicate; Rename keeps both, importing the duplicate under a new identifier.
        </p>

        <div className="mt-3 flex gap-4 text-sm">
          <button
            type="button"
            onClick={() => setAll("skip")}
            className="font-medium text-sky-600 hover:underline dark:text-sky-400"
          >
            Skip all
          </button>
          <button
            type="button"
            onClick={() => setAll("rename")}
            className="font-medium text-sky-600 hover:underline dark:text-sky-400"
          >
            Rename all
          </button>
        </div>

        <ul className="mt-3 min-h-0 flex-1 divide-y divide-slate-200 overflow-y-auto dark:divide-slate-700">
          {identifiers.map((identifier) => {
            const choice = choices.get(identifier) ?? "skip";
            const preview = previews.get(identifier);
            return (
              <li key={identifier} className="flex items-center justify-between gap-3 py-2 text-sm">
                <div className="min-w-0">
                  <code className="truncate rounded bg-slate-100 px-1 py-0.5 font-mono text-[0.85em] dark:bg-slate-900">
                    {identifier}
                  </code>
                  {choice === "rename" && preview && (
                    <span className="ml-2 text-xs text-slate-500 dark:text-slate-400">&rarr; {preview}</span>
                  )}
                </div>
                <div className="flex shrink-0 gap-1 rounded-md border border-slate-300 p-0.5 dark:border-slate-600">
                  <button
                    type="button"
                    onClick={() => setChoice(identifier, "skip")}
                    aria-pressed={choice === "skip"}
                    className={`rounded px-2 py-1 text-xs font-medium ${
                      choice === "skip"
                        ? "bg-slate-700 text-white dark:bg-slate-500"
                        : "text-slate-600 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-slate-700"
                    }`}
                  >
                    Skip
                  </button>
                  <button
                    type="button"
                    onClick={() => setChoice(identifier, "rename")}
                    aria-pressed={choice === "rename"}
                    className={`rounded px-2 py-1 text-xs font-medium ${
                      choice === "rename"
                        ? "bg-sky-600 text-white"
                        : "text-slate-600 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-slate-700"
                    }`}
                  >
                    Rename
                  </button>
                </div>
              </li>
            );
          })}
        </ul>

        <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
          <p className="text-sm text-slate-500 dark:text-slate-400">
            {skippedCount} skipped, {renamedCount} renamed
          </p>
          <div className="flex gap-3">
            <button
              type="button"
              onClick={onCancel}
              className="rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
            >
              Cancel import
            </button>
            <button
              type="button"
              onClick={() => onConfirm(choices)}
              className="rounded-md bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700"
            >
              OK
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
