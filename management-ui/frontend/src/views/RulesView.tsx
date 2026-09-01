import { mdiExportVariant, mdiFileImportOutline } from "@mdi/js";
import { ChevronDown, ChevronUp } from "lucide-react";
import { useEffect, useState } from "react";
import { ConfirmModal } from "../components/ConfirmModal";
import { ImportConflictModal, type ConflictChoice } from "../components/ImportConflictModal";
import { ImportRuleModal } from "../components/ImportRuleModal";
import { MdiIcon } from "../components/MdiIcon";
import { RuleForm } from "../components/RuleForm";
import { apiClient, ApiError } from "../api/client";
import {
  createRule,
  deleteRule,
  emptyRule,
  listRules,
  updateRule,
  type Condition,
  type Rule,
} from "../api/rules";
import { useToast } from "../hooks/useToast";
import { downloadTextFile } from "../lib/csv";
import { sortConditions } from "../lib/ruleConditions";
import {
  collidingIdentifiers,
  importRulesBatch,
  resolveImportIdentifiers,
  type ImportedRule,
} from "../lib/ruleImport";

// The /api/areas response carries full Area objects; `geometry.type` is
// read here only to filter the rule editor's `area`-condition dropdown to
// Polygon areas -- per the backend Area model, a LineString/Point area is
// never usable as an `area` condition value (rules_engine.py skips it), so
// offering one would silently produce a rule that never matches.
interface AreaOption {
  identifier: string;
  name: string;
  geometry: { type: string };
}

const clone = (rule: Rule): Rule => JSON.parse(JSON.stringify(rule));

// Canonical condition order, applied at every point a rule is (re)seeded
// into the editor or sent to the backend -- so draft and original are
// always sorted identically (no spurious `dirty` from order alone) and
// the persisted rule is diff-friendly. Never called mid-edit; see
// lib/ruleConditions.ts.
const withSortedConditions = (rule: Rule): Rule => ({
  ...rule,
  conditions: sortConditions(rule.conditions),
});

// Mirrors message-processor/rules_engine.py's _eval_date/_compare_ordered:
// a date-only value compares at UTC day granularity, a datetime value at
// UTC minute granularity (seconds/microseconds zeroed on both sides).
function isDateConditionActiveNow(condition: Condition): boolean {
  const raw = condition.value as string;
  const target = new Date(raw);
  if (Number.isNaN(target.getTime())) return true; // can't evaluate -- don't flag bad data as inactive

  const now = new Date();
  let current: number;
  let threshold: number;
  if (raw.includes("T")) {
    current = Math.floor(now.getTime() / 60000) * 60000;
    threshold = Math.floor(target.getTime() / 60000) * 60000;
  } else {
    current = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
    threshold = Date.UTC(target.getUTCFullYear(), target.getUTCMonth(), target.getUTCDate());
  }

  switch (condition.operator) {
    case "equals":
      return current === threshold;
    case "minimum":
      return current >= threshold;
    case "maximum":
      return current <= threshold;
    default:
      return true;
  }
}

// null = rule has no date condition at all (no pill); false = at least one
// date condition isn't satisfied right now (conditions AND together, so
// the rule can't currently match) -- "Inactive" pill.
function isRuleDateActive(rule: Rule): boolean | null {
  const dateConditions = rule.conditions.filter((c) => c.type === "date");
  if (dateConditions.length === 0) return null;
  return dateConditions.every(isDateConditionActiveNow);
}

function StatusPill({ label, tone }: { label: string; tone: "danger" | "neutral" }) {
  const toneClasses =
    tone === "danger"
      ? "bg-red-600 text-white"
      : "border border-slate-400 text-slate-500 dark:border-slate-500 dark:text-slate-400";
  return (
    <span className={`shrink-0 whitespace-nowrap rounded-full px-2 py-0.5 text-xs font-medium ${toneClasses}`}>
      {label}
    </span>
  );
}

// Renders the identifier in monospace so it reads unambiguously as "the
// literal value", distinct from the free-text display Name beside it.
function DeleteRuleMessage({ rule }: { rule: Rule }) {
  const idCode = (
    <code className="rounded bg-slate-100 px-1 py-0.5 font-mono text-[0.85em] dark:bg-slate-800">
      {rule.identifier}
    </code>
  );
  return rule.name ? (
    <>
      This will permanently delete '{rule.name}' ({idCode}).
    </>
  ) : (
    <>This will permanently delete {idCode}.</>
  );
}

export function RulesView() {
  const { showToast } = useToast();
  const [rules, setRules] = useState<Rule[]>([]);
  const [areas, setAreas] = useState<AreaOption[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);

  const [original, setOriginal] = useState<Rule | null>(null);
  const [draft, setDraft] = useState<Rule | null>(null);
  const [isNew, setIsNew] = useState(false);

  // Mobile-only accordion state for the rule list -- ignored at the md+
  // breakpoint, where the list is always visible regardless (see the
  // className on the <ul> below).
  const [mobileListOpen, setMobileListOpen] = useState(false);

  const [pendingSwitch, setPendingSwitch] = useState<(() => void) | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<Rule | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [importModalOpen, setImportModalOpen] = useState(false);
  // Set together, right before ImportConflictModal opens: the raw imported
  // batch (needed to actually run the import once the operator confirms
  // their skip/rename choices) and the colliding identifiers it contains.
  // Both null/empty = the modal is closed.
  const [pendingImportRules, setPendingImportRules] = useState<ImportedRule[] | null>(null);
  const [conflictIdentifiers, setConflictIdentifiers] = useState<string[]>([]);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [loadedRules, loadedAreas] = await Promise.all([
          listRules(),
          apiClient.get<AreaOption[]>("/api/areas"),
        ]);
        if (cancelled) return;
        setRules(loadedRules);
        setAreas(
          loadedAreas
            .filter((a) => a.geometry?.type === "Polygon")
            .sort((a, b) => (a.name || a.identifier).localeCompare(b.name || b.identifier)),
        );
      } catch (err) {
        if (!cancelled) showToast("error", err instanceof Error ? err.message : "Failed to load");
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, [showToast]);

  const dirty = draft !== null && original !== null && JSON.stringify(draft) !== JSON.stringify(original);

  function requestSwitch(action: () => void) {
    if (dirty) {
      setPendingSwitch(() => action);
    } else {
      action();
    }
  }

  function selectRule(rule: Rule) {
    requestSwitch(() => {
      const sorted = withSortedConditions(rule);
      setDraft(clone(sorted));
      setOriginal(clone(sorted));
      setIsNew(false);
      setMobileListOpen(false);
    });
  }

  function startNewRule() {
    requestSwitch(() => {
      const fresh = withSortedConditions(emptyRule());
      setDraft(clone(fresh));
      setOriginal(clone(fresh));
      setIsNew(true);
      setMobileListOpen(false);
    });
  }

  async function handleSave() {
    if (!draft) return;
    setSaving(true);
    try {
      const toSave = withSortedConditions(draft);
      const saved = isNew
        ? await createRule(toSave)
        : await updateRule(toSave.identifier, toSave);
      const sortedSaved = withSortedConditions(saved);
      setRules((current) => {
        const idx = current.findIndex(
          (r) => r.identifier === (isNew ? sortedSaved.identifier : toSave.identifier),
        );
        if (idx === -1) return [...current, sortedSaved];
        const next = current.slice();
        next[idx] = sortedSaved;
        return next;
      });
      setDraft(clone(sortedSaved));
      setOriginal(clone(sortedSaved));
      setIsNew(false);
      showToast("success", `Rule '${saved.identifier}' saved.`);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to save rule.");
    } finally {
      setSaving(false);
    }
  }

  function handleDiscard() {
    if (!original) return;
    setDraft(clone(original));
  }

  async function handleDeleteConfirmed() {
    if (!deleteTarget) return;
    setDeleting(true);
    try {
      await deleteRule(deleteTarget.identifier);
      setRules((current) => current.filter((r) => r.identifier !== deleteTarget.identifier));
      if (draft?.identifier === deleteTarget.identifier) {
        setDraft(null);
        setOriginal(null);
      }
      showToast("success", `Rule '${deleteTarget.identifier}' deleted.`);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to delete rule.");
    } finally {
      setDeleting(false);
      setDeleteTarget(null);
    }
  }

  function exportAllRules() {
    // triggered_lifetime/triggered_last_30_days are read-time-computed
    // display stats the backend adds to GET /api/rules responses -- never
    // part of what's stored in config:rules -- so they're stripped before
    // export rather than round-tripped through a saved file.
    const exportable = rules.map(
      ({ triggered_lifetime: _triggeredLifetime, triggered_last_30_days: _triggeredLast30Days, ...rule }) => rule,
    );
    downloadTextFile("rules.json", JSON.stringify(exportable, null, 2), "application/json");
  }

  // Entry point from ImportRuleModal. An imported identifier already used
  // by an existing rule stops here and shows ImportConflictModal instead of
  // importing immediately; an import with no collisions runs straight
  // through exactly as before that modal existed.
  function handleImportRules(imported: ImportedRule[]) {
    setImportModalOpen(false);
    const colliding = collidingIdentifiers(imported, rules.map((r) => r.identifier));
    if (colliding.length > 0) {
      setPendingImportRules(imported);
      setConflictIdentifiers(colliding);
    } else {
      void runRulesImport(imported, new Set());
    }
  }

  async function runRulesImport(imported: ImportedRule[], skipIdentifiers: ReadonlySet<string>) {
    const result = await importRulesBatch(
      imported,
      rules.map((r) => r.identifier),
      areas.map((a) => a.identifier),
      (payload) => createRule(payload as unknown as Rule).then(() => undefined),
      skipIdentifiers,
    );

    try {
      setRules(await listRules());
    } catch {
      /* the toast below still reports what happened; a manual refresh recovers the list */
    }

    const total = imported.length;
    const summaryParts = [`${result.created.length} of ${total} rules imported`];
    if (skipIdentifiers.size > 0) {
      summaryParts.push(`${skipIdentifiers.size} skipped (already exists)`);
    }
    const summaryText = `${summaryParts.join(". ")}.`;
    const hasIssues = result.rejected.length > 0 || result.failed.length > 0;

    if (!hasIssues) {
      showToast("success", summaryText);
      return;
    }

    showToast(
      "error",
      <div>
        <p className="font-semibold">{summaryText}</p>
        <ul className="mt-1 list-disc space-y-0.5 pl-4">
          {result.rejected.map((r) => (
            <li key={`rejected-${r.identifier}`}>
              <span className="font-mono">{r.identifier}</span> rejected: {r.reason}
            </li>
          ))}
          {result.failed.map((f) => (
            <li key={`failed-${f.identifier}`}>
              <span className="font-mono">{f.identifier}</span> failed: {f.reason}
            </li>
          ))}
        </ul>
      </div>,
    );
  }

  // Recomputes every conflict row's rename preview via the real batch
  // resolver, over the whole pending import, so it can never diverge from
  // what importRulesBatch would actually produce for the same choices.
  function computeRuleConflictPreview(choices: Map<string, ConflictChoice>): Map<string, string> {
    if (!pendingImportRules) return new Map();
    const skipIdentifiers = new Set(
      [...choices].filter(([, choice]) => choice === "skip").map(([identifier]) => identifier),
    );
    const entries = resolveImportIdentifiers(
      pendingImportRules,
      rules.map((r) => r.identifier),
      skipIdentifiers,
    );
    const preview = new Map<string, string>();
    for (const { rule, identifier } of entries) {
      const original = typeof rule.identifier === "string" ? rule.identifier.trim() : "";
      if (original && !preview.has(original)) preview.set(original, identifier);
    }
    return preview;
  }

  function handleConflictConfirm(choices: Map<string, ConflictChoice>) {
    const imported = pendingImportRules;
    setPendingImportRules(null);
    setConflictIdentifiers([]);
    if (!imported) return;
    const skipIdentifiers = new Set(
      [...choices].filter(([, choice]) => choice === "skip").map(([identifier]) => identifier),
    );
    void runRulesImport(imported, skipIdentifiers);
  }

  function handleConflictCancel() {
    setPendingImportRules(null);
    setConflictIdentifiers([]);
  }

  if (loading) {
    return <p className="text-slate-400">Loading rules...</p>;
  }

  return (
    <div className="flex flex-col gap-4 md:h-full md:flex-row md:gap-6">
      <div className="flex flex-col gap-2 md:w-72 md:shrink-0">
        <button
          type="button"
          onClick={startNewRule}
          className="rounded-md border border-sky-600 px-3 py-2 text-sm font-medium text-sky-600 hover:bg-sky-50 dark:border-sky-400 dark:text-sky-400 dark:hover:bg-sky-950"
        >
          Add Rule
        </button>

        <div className="flex gap-2">
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
            onClick={exportAllRules}
            disabled={rules.length === 0}
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
          <span>Rules</span>
          {mobileListOpen ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
        </button>

        <ul
          className={`${mobileListOpen ? "flex" : "hidden"} max-h-64 flex-col gap-1 overflow-y-auto md:flex md:max-h-none`}
        >
          {[...rules]
            .sort((a, b) =>
              (a.name || a.identifier).localeCompare(b.name || b.identifier),
            )
            .map((rule) => {
            const dateActive = isRuleDateActive(rule);
            const isSelected = draft?.identifier === rule.identifier && !isNew;
            return (
              <li
                key={rule.identifier}
                className={`rounded-r-md border-l-4 ${
                  isSelected
                    ? "border-sky-600 bg-slate-100 dark:border-sky-400 dark:bg-slate-800"
                    : "border-transparent hover:bg-slate-100 dark:hover:bg-slate-800"
                }`}
              >
                <button
                  type="button"
                  className={`flex w-full items-center justify-between gap-2 px-3 py-2 text-left text-sm ${
                    isSelected
                      ? "font-semibold text-sky-700 dark:text-sky-400"
                      : "text-slate-700 dark:text-slate-200"
                  }`}
                  onClick={() => selectRule(rule)}
                  title={rule.identifier}
                >
                  <span className="truncate">{rule.name || rule.identifier}</span>
                  {!rule.enabled ? (
                    <StatusPill label="Not Enabled" tone="danger" />
                  ) : dateActive === false ? (
                    <StatusPill label="Inactive" tone="neutral" />
                  ) : null}
                </button>
              </li>
            );
          })}
          {rules.length === 0 && <li className="px-3 py-2 text-sm text-slate-400">No rules yet.</li>}
        </ul>
      </div>

      <hr className="border-slate-200 dark:border-slate-700 md:hidden" />

      <div className="flex-1 overflow-y-auto md:min-h-0 md:overflow-hidden">
        {draft ? (
          <RuleForm
            key={isNew ? "__new__" : original?.identifier ?? "__new__"}
            rule={draft}
            isNew={isNew}
            otherRules={rules.filter((r) => r.identifier !== original?.identifier || isNew)}
            areaOptions={areas}
            onChange={setDraft}
            onSave={handleSave}
            onDiscard={handleDiscard}
            onDelete={() => setDeleteTarget(draft)}
            saving={saving}
            dirty={dirty}
          />
        ) : (
          <p className="text-slate-400">Select a rule, or add a new one.</p>
        )}
      </div>

      <ConfirmModal
        open={pendingSwitch !== null}
        title="Discard unsaved changes?"
        message="You have unsaved changes to this rule. Switching now will discard them."
        confirmLabel="Discard"
        onConfirm={() => {
          pendingSwitch?.();
          setPendingSwitch(null);
        }}
        onCancel={() => setPendingSwitch(null)}
      />

      <ConfirmModal
        open={deleteTarget !== null}
        title="Delete rule?"
        message={deleteTarget ? <DeleteRuleMessage rule={deleteTarget} /> : ""}
        confirmLabel="Delete"
        confirmLoading={deleting}
        onConfirm={handleDeleteConfirmed}
        onCancel={() => setDeleteTarget(null)}
      />

      <ImportRuleModal
        open={importModalOpen}
        onImport={handleImportRules}
        onCancel={() => setImportModalOpen(false)}
      />

      <ImportConflictModal
        open={conflictIdentifiers.length > 0}
        noun="rule"
        identifiers={conflictIdentifiers}
        computePreview={computeRuleConflictPreview}
        onConfirm={handleConflictConfirm}
        onCancel={handleConflictCancel}
      />
    </div>
  );
}
