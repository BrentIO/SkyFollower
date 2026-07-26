import { useEffect, useState } from "react";
import { ConfirmModal } from "../components/ConfirmModal";
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

interface AreaOption {
  identifier: string;
  name: string;
}

const clone = (rule: Rule): Rule => JSON.parse(JSON.stringify(rule));

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

export function RulesView() {
  const { showToast } = useToast();
  const [rules, setRules] = useState<Rule[]>([]);
  const [areas, setAreas] = useState<AreaOption[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);

  const [original, setOriginal] = useState<Rule | null>(null);
  const [draft, setDraft] = useState<Rule | null>(null);
  const [isNew, setIsNew] = useState(false);

  const [pendingSwitch, setPendingSwitch] = useState<(() => void) | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);

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
        setAreas(loadedAreas);
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
      setDraft(clone(rule));
      setOriginal(clone(rule));
      setIsNew(false);
    });
  }

  function startNewRule() {
    requestSwitch(() => {
      const fresh = emptyRule();
      setDraft(clone(fresh));
      setOriginal(clone(fresh));
      setIsNew(true);
    });
  }

  async function handleSave() {
    if (!draft) return;
    setSaving(true);
    try {
      const saved = isNew ? await createRule(draft) : await updateRule(draft.identifier, draft);
      setRules((current) => {
        const idx = current.findIndex((r) => r.identifier === (isNew ? saved.identifier : draft.identifier));
        if (idx === -1) return [...current, saved];
        const next = current.slice();
        next[idx] = saved;
        return next;
      });
      setDraft(clone(saved));
      setOriginal(clone(saved));
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
    try {
      await deleteRule(deleteTarget);
      setRules((current) => current.filter((r) => r.identifier !== deleteTarget));
      if (draft?.identifier === deleteTarget) {
        setDraft(null);
        setOriginal(null);
      }
      showToast("success", `Rule '${deleteTarget}' deleted.`);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to delete rule.");
    } finally {
      setDeleteTarget(null);
    }
  }

  if (loading) {
    return <p className="text-slate-400">Loading rules...</p>;
  }

  return (
    <div className="flex h-full gap-6">
      <div className="flex w-72 shrink-0 flex-col gap-2">
        <button
          type="button"
          onClick={startNewRule}
          className="rounded-md bg-sky-600 px-3 py-2 text-sm font-medium text-white hover:bg-sky-700"
        >
          Add Rule
        </button>

        <ul className="flex flex-col gap-1 overflow-y-auto">
          {rules.map((rule) => {
            const dateActive = isRuleDateActive(rule);
            return (
              <li
                key={rule.identifier}
                className={`rounded-md ${
                  draft?.identifier === rule.identifier && !isNew
                    ? "bg-sky-100 dark:bg-sky-900"
                    : "hover:bg-slate-100 dark:hover:bg-slate-800"
                }`}
              >
                <button
                  type="button"
                  className="flex w-full items-center justify-between gap-2 px-3 py-2 text-left text-sm"
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

      <div className="flex-1 overflow-y-auto">
        {draft ? (
          <RuleForm
            rule={draft}
            isNew={isNew}
            otherRules={rules.filter((r) => r.identifier !== original?.identifier || isNew)}
            areaOptions={areas}
            onChange={setDraft}
            onSave={handleSave}
            onDiscard={handleDiscard}
            onDelete={() => setDeleteTarget(draft.identifier)}
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
        message={`This will permanently delete '${deleteTarget}'.`}
        confirmLabel="Delete"
        onConfirm={handleDeleteConfirmed}
        onCancel={() => setDeleteTarget(null)}
      />
    </div>
  );
}
