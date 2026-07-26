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
  type Rule,
} from "../api/rules";
import { useToast } from "../hooks/useToast";

interface AreaOption {
  identifier: string;
  name: string;
}

const clone = (rule: Rule): Rule => JSON.parse(JSON.stringify(rule));

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

  async function toggleEnabled(rule: Rule) {
    try {
      const saved = await updateRule(rule.identifier, { ...rule, enabled: !rule.enabled });
      setRules((current) => current.map((r) => (r.identifier === saved.identifier ? saved : r)));
      if (draft?.identifier === saved.identifier && original?.identifier === saved.identifier) {
        setDraft(clone(saved));
        setOriginal(clone(saved));
      }
      showToast("success", `Rule '${saved.identifier}' ${saved.enabled ? "enabled" : "disabled"}.`);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to update rule.");
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
          {rules.map((rule) => (
            <li
              key={rule.identifier}
              className={`flex items-center justify-between gap-2 rounded-md px-3 py-2 text-sm ${
                draft?.identifier === rule.identifier && !isNew
                  ? "bg-sky-100 dark:bg-sky-900"
                  : "hover:bg-slate-100 dark:hover:bg-slate-800"
              }`}
            >
              <button
                type="button"
                className="flex-1 truncate text-left"
                onClick={() => selectRule(rule)}
                title={rule.identifier}
              >
                {rule.name || rule.identifier}
              </button>
              <input
                type="checkbox"
                checked={rule.enabled}
                onChange={() => toggleEnabled(rule)}
                title="Enabled"
              />
            </li>
          ))}
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
