import { useState } from "react";
import { ConditionForm } from "./ConditionForm";
import type { Condition, ConditionType, Rule, WakeTurbulenceCategory } from "../api/rules";
import { OPERATORS_BY_TYPE, WAKE_TURBULENCE_CATEGORIES } from "../api/rules";

interface AreaOption {
  identifier: string;
  name: string;
}

interface RuleFormProps {
  rule: Rule;
  isNew: boolean;
  otherRules: Rule[];
  areaOptions: AreaOption[];
  onChange: (rule: Rule) => void;
  onSave: () => void;
  onDiscard: () => void;
  onDelete: () => void;
  saving: boolean;
  dirty: boolean;
}

function newCondition(): Condition {
  const type: ConditionType = "altitude";
  return { type, operator: OPERATORS_BY_TYPE[type][0], value: "" };
}

// Returns the first validation problem found, or null if the rule is
// save-worthy. Mirrors message-processor/rules_engine.py's per-type
// validators as a fast-fail UX nicety -- the server's 400 is still the
// source of truth, this just avoids a round trip for the common mistakes.
export function validateRule(rule: Rule, otherRules: Rule[]): string | null {
  if (!rule.identifier || !/^[A-Za-z0-9_-]+$/.test(rule.identifier)) {
    return "Identifier is required and may only contain letters, numbers, hyphens, and underscores.";
  }
  if (otherRules.some((r) => r.identifier === rule.identifier)) {
    return `Identifier '${rule.identifier}' is already used by another rule.`;
  }
  if (rule.conditions.length === 0) {
    return "At least one condition is required.";
  }
  for (let i = 0; i < rule.conditions.length; i++) {
    const err = validateCondition(rule.conditions[i]);
    if (err) return `Condition #${i + 1}: ${err}`;
  }
  return null;
}

function validateCondition(condition: Condition): string | null {
  const value = condition.value;
  switch (condition.type) {
    case "altitude":
    case "velocity":
    case "aircraft_powerplant_count":
      if (!/^\d+$/.test(String(value))) return "must be a non-negative integer";
      return null;

    case "vertical_speed":
      if (!/^-?\d+$/.test(String(value))) return "must be an integer";
      return null;

    case "heading": {
      const parts = String(value).split(",").map(Number);
      if (parts.length !== 2 || parts.some((n) => Number.isNaN(n) || n < 0 || n > 359)) {
        return "min and max must each be between 0 and 359";
      }
      return null;
    }

    case "squawk":
      if (!/^\d{4}$/.test(String(value))) return "must be exactly 4 digits";
      return null;

    case "aircraft_icao_hex":
      if (String(value).length !== 6) return "must be exactly 6 characters";
      return null;

    case "aircraft_type_designator":
      if (String(value).length !== 4) return "must be exactly 4 characters";
      return null;

    case "operator_airline_designator":
      if (String(value).length !== 3) return "must be exactly 3 characters";
      return null;

    case "aircraft_registration":
      if (String(value).length < 2) return "must be at least 2 characters";
      return null;

    case "ident":
      if (!String(value).trim()) return "must not be empty";
      return null;

    case "wake_turbulence_category":
      if (!WAKE_TURBULENCE_CATEGORIES.includes(value as WakeTurbulenceCategory)) {
        return "must be selected";
      }
      return null;

    case "area":
      if (!String(value).trim()) return "must be selected";
      return null;

    case "date":
      if (!String(value).trim()) return "must not be empty";
      return null;

    case "matched_rules":
      if (!Array.isArray(value) || value.length === 0) return "at least one rule must be selected";
      return null;

    case "military":
      return null;

    default:
      return null;
  }
}

export function RuleForm({
  rule,
  isNew,
  otherRules,
  areaOptions,
  onChange,
  onSave,
  onDiscard,
  onDelete,
  saving,
  dirty,
}: RuleFormProps) {
  const [validationError, setValidationError] = useState<string | null>(null);

  function updateCondition(index: number, next: Condition) {
    const conditions = rule.conditions.slice();
    conditions[index] = next;
    onChange({ ...rule, conditions });
  }

  function removeCondition(index: number) {
    onChange({ ...rule, conditions: rule.conditions.filter((_, i) => i !== index) });
  }

  function addCondition() {
    onChange({ ...rule, conditions: [...rule.conditions, newCondition()] });
  }

  function handleSave() {
    const error = validateRule(rule, otherRules);
    setValidationError(error);
    if (!error) onSave();
  }

  const otherRuleIdentifiers = otherRules.map((r) => r.identifier).filter(Boolean);

  return (
    <div className="flex flex-col gap-6">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h1 className="text-xl font-semibold">
          {isNew ? "New Rule" : rule.name || rule.identifier}
        </h1>
        <div className="flex gap-2">
          {!isNew && (
            <button
              type="button"
              onClick={onDelete}
              className="rounded-md border border-red-300 px-3 py-1.5 text-sm font-medium text-red-600 hover:bg-red-50 dark:border-red-800 dark:hover:bg-red-950"
            >
              Delete
            </button>
          )}
          <button
            type="button"
            onClick={onDiscard}
            disabled={!dirty}
            className="rounded-md border border-slate-300 px-3 py-1.5 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-40 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
          >
            Discard
          </button>
          <button
            type="button"
            onClick={handleSave}
            disabled={saving}
            className="rounded-md bg-sky-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-40"
          >
            {saving ? "Saving..." : "Save"}
          </button>
        </div>
      </div>

      {validationError && (
        <div className="rounded-md bg-red-50 px-4 py-2 text-sm text-red-700 dark:bg-red-950 dark:text-red-300">
          {validationError}
        </div>
      )}

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <label className="flex flex-col gap-1 text-sm font-medium text-slate-600 dark:text-slate-300">
          Name
          <input
            type="text"
            className="input"
            value={rule.name}
            onChange={(e) => onChange({ ...rule, name: e.target.value })}
          />
        </label>

        <label className="flex flex-col gap-1 text-sm font-medium text-slate-600 dark:text-slate-300">
          Identifier
          <input
            type="text"
            className="input"
            value={rule.identifier}
            disabled={!isNew}
            onChange={(e) =>
              onChange({ ...rule, identifier: e.target.value.replace(/[^A-Za-z0-9_-]/g, "") })
            }
          />
        </label>

        <label className="col-span-1 flex flex-col gap-1 text-sm font-medium text-slate-600 dark:text-slate-300 sm:col-span-2">
          Description
          <textarea
            className="input"
            rows={2}
            value={rule.description}
            onChange={(e) => onChange({ ...rule, description: e.target.value })}
          />
        </label>

        <label className="flex items-center gap-2 text-sm font-medium text-slate-600 dark:text-slate-300">
          <input
            type="checkbox"
            checked={rule.enabled}
            onChange={(e) => onChange({ ...rule, enabled: e.target.checked })}
          />
          Enabled
        </label>

        <label className="flex items-center gap-2 text-sm font-medium text-slate-600 dark:text-slate-300">
          <input
            type="checkbox"
            checked={rule.force_archive}
            onChange={(e) => onChange({ ...rule, force_archive: e.target.checked })}
          />
          Force Archive
        </label>
      </div>

      <div className="flex flex-col gap-3">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold uppercase tracking-wide text-slate-400">Conditions</h2>
          <button
            type="button"
            onClick={addCondition}
            className="rounded-md border border-slate-300 px-3 py-1 text-sm font-medium hover:bg-slate-50 dark:border-slate-600 dark:hover:bg-slate-800"
          >
            Add Condition
          </button>
        </div>

        {rule.conditions.length === 0 && (
          <p className="text-sm text-slate-400">No conditions yet -- add at least one.</p>
        )}

        {rule.conditions.map((condition, index) => (
          <ConditionForm
            key={index}
            condition={condition}
            onChange={(next) => updateCondition(index, next)}
            onRemove={() => removeCondition(index)}
            otherRuleIdentifiers={otherRuleIdentifiers}
            areaOptions={areaOptions}
          />
        ))}
      </div>
    </div>
  );
}
