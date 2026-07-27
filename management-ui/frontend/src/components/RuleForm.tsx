import { useEffect, useState } from "react";
import { ConditionForm } from "./ConditionForm";
import type { Condition, Rule, WakeTurbulenceCategory } from "../api/rules";
import { WAKE_TURBULENCE_CATEGORIES } from "../api/rules";

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

const NAME_MAX_LENGTH = 64;
const IDENTIFIER_MAX_LENGTH = 64;
const DESCRIPTION_MAX_LENGTH = 2000;

// No default type -- the user has to pick one (see ConditionForm's "Select
// a type..." placeholder option). `operator`/`value` here are throwaway:
// picking a real type calls retypeCondition(), which replaces both anyway.
function newCondition(): Condition {
  return { type: "", operator: "equals", value: "" };
}

// Replaces each literal space with an underscore, then drops every other
// character outside the identifier's allowed charset -- used both for the
// auto-fill-from-Name behavior below and to sanitise direct edits to the
// Identifier field itself.
function sanitizeIdentifier(raw: string): string {
  return raw.replace(/ /g, "_").replace(/[^A-Za-z0-9_-]/g, "").slice(0, IDENTIFIER_MAX_LENGTH);
}

// Returns the first validation problem found, or null if the rule is
// save-worthy. Mirrors message-processor/rules_engine.py's per-type
// validators as a fast-fail UX nicety -- the server's 400 is still the
// source of truth, this just avoids a round trip for the common mistakes.
export function validateRule(rule: Rule, otherRules: Rule[]): string | null {
  if (!rule.identifier || !/^[A-Za-z0-9_-]+$/.test(rule.identifier)) {
    return "Identifier is required and may only contain letters, numbers, hyphens, and underscores.";
  }
  if (rule.identifier.length > IDENTIFIER_MAX_LENGTH) {
    return `Identifier must be ${IDENTIFIER_MAX_LENGTH} characters or fewer.`;
  }
  if (rule.name.length > NAME_MAX_LENGTH) {
    return `Name must be ${NAME_MAX_LENGTH} characters or fewer.`;
  }
  if (rule.description.length > DESCRIPTION_MAX_LENGTH) {
    return `Description must be ${DESCRIPTION_MAX_LENGTH} characters or fewer.`;
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
    case "":
      return "select a condition type";

    case "altitude":
      return validateBoundedInt(value, 0, 65000);

    case "velocity":
      return validateBoundedInt(value, 0, 1334);

    case "aircraft_powerplant_count":
      return validateBoundedInt(value, 0, 99);

    case "vertical_speed":
      return validateBoundedInt(value, -10000, 10000);

    case "heading": {
      const parts = String(value).split(",").map(Number);
      if (parts.length !== 2 || parts.some((n) => Number.isNaN(n) || n < 0 || n > 359)) {
        return "min and max must each be between 0 and 359";
      }
      return null;
    }

    case "squawk":
      // Octal, not decimal -- a real transponder never sends 8 or 9 in
      // any position (mirrors message-processor/rules_engine.py's
      // _validate_squawk).
      if (!/^[0-7]{4}$/.test(String(value))) return "must be exactly 4 digits, each 0-7";
      return null;

    case "aircraft_icao_hex":
      if (!/^[0-9A-Fa-f]{6}$/.test(String(value))) return "must be exactly 6 hexadecimal characters";
      return null;

    case "aircraft_type_designator":
      if (String(value).length !== 4) return "must be exactly 4 characters";
      return null;

    case "operator_airline_designator":
      if (String(value).length !== 3) return "must be exactly 3 characters";
      return null;

    case "aircraft_registration":
      if (!/^[0-9A-Z][0-9A-Z-]*[0-9A-Z]$/.test(String(value))) {
        return "must be at least 2 characters, contain only letters, numbers, and hyphens, and not start or end with a hyphen";
      }
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

function validateBoundedInt(value: unknown, min: number, max: number): string | null {
  if (!/^-?\d+$/.test(String(value))) return "must be an integer";
  const n = Number(value);
  if (n < min || n > max) return `must be between ${min.toLocaleString()} and ${max.toLocaleString()}`;
  return null;
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
  const [focusNewConditionAt, setFocusNewConditionAt] = useState<number | null>(null);
  // Tracks whether the user has typed into Identifier directly (as opposed
  // to it merely holding a value auto-derived from Name) -- gates
  // handleNameChange below. Checking `rule.identifier === ""` instead of
  // this flag was the original approach, but that only holds true for the
  // very first character typed into Name: deriving a non-empty identifier
  // on keystroke 1 made keystroke 2 see a non-empty identifier and stop
  // deriving, even though the user never touched Identifier themselves.
  const [identifierManuallyEdited, setIdentifierManuallyEdited] = useState(false);

  // The autoFocus attribute only matters at mount time, so this flag only
  // needs to survive one render past addCondition() -- reset immediately
  // after so a later re-render (e.g. editing an unrelated field) doesn't
  // keep re-focusing the same row.
  useEffect(() => {
    if (focusNewConditionAt !== null) setFocusNewConditionAt(null);
  }, [focusNewConditionAt]);

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
    setFocusNewConditionAt(rule.conditions.length);
  }

  function handleSave() {
    const error = validateRule(rule, otherRules);
    setValidationError(error);
    if (!error) onSave();
  }

  function handleNameChange(rawName: string) {
    const name = rawName.slice(0, NAME_MAX_LENGTH);
    // Keeps deriving Identifier from every keystroke of Name for a new
    // rule, until the user edits Identifier directly (see
    // handleIdentifierChange) -- existing rules never hit this, since
    // their Identifier field is disabled.
    const identifier = isNew && !identifierManuallyEdited ? sanitizeIdentifier(name) : rule.identifier;
    onChange({ ...rule, name, identifier });
  }

  function handleIdentifierChange(rawIdentifier: string) {
    setIdentifierManuallyEdited(true);
    onChange({ ...rule, identifier: sanitizeIdentifier(rawIdentifier) });
  }

  const otherRuleOptions = otherRules
    .filter((r) => r.identifier)
    .map((r) => ({ identifier: r.identifier, name: r.name }));

  return (
    <div className="flex flex-col gap-6 md:h-full md:min-h-0">
      <div className="flex shrink-0 flex-col gap-6">
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
              placeholder="Rule name"
              maxLength={NAME_MAX_LENGTH}
              className="input"
              value={rule.name}
              onChange={(e) => handleNameChange(e.target.value)}
            />
          </label>

          <label className="flex flex-col gap-1 text-sm font-medium text-slate-600 dark:text-slate-300">
            Identifier
            <input
              type="text"
              placeholder="Rule unique identifier"
              maxLength={IDENTIFIER_MAX_LENGTH}
              className="input"
              value={rule.identifier}
              disabled={!isNew}
              onChange={(e) => handleIdentifierChange(e.target.value)}
            />
          </label>

          <label className="col-span-1 flex flex-col gap-1 text-sm font-medium text-slate-600 dark:text-slate-300 sm:col-span-2">
            Description
            <textarea
              className="input"
              rows={2}
              placeholder="Describe the rule..."
              maxLength={DESCRIPTION_MAX_LENGTH}
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

          <label
            className="flex items-center gap-2 text-sm font-medium text-slate-600 dark:text-slate-300"
            title="Archives MLAT-only flights that match this rule. MLAT-only flights are otherwise skipped."
          >
            <input
              type="checkbox"
              checked={rule.force_archive}
              onChange={(e) => onChange({ ...rule, force_archive: e.target.checked })}
            />
            Force Archive
          </label>
        </div>

        <hr className="border-slate-200 dark:border-slate-700" />
      </div>

      <div className="flex flex-col gap-3 md:min-h-0 md:flex-1">
        <div className="flex shrink-0 items-start justify-between">
          <div>
            <h2 className="text-sm font-semibold text-slate-500 dark:text-slate-400">
              Conditions{rule.conditions.length > 0 ? ` (${rule.conditions.length})` : ""}
            </h2>
            {rule.conditions.length > 0 && (
              <p className="text-xs text-slate-400">All conditions must be met to trigger this rule.</p>
            )}
          </div>
          <button
            type="button"
            onClick={addCondition}
            className="shrink-0 rounded-md border border-slate-300 px-3 py-1 text-sm font-medium hover:bg-slate-50 dark:border-slate-600 dark:hover:bg-slate-800"
          >
            Add Condition
          </button>
        </div>

        {rule.conditions.length === 0 && (
          <p className="shrink-0 text-sm text-slate-400">No conditions yet. Add at least one.</p>
        )}

        <div className="flex flex-col gap-3 md:min-h-0 md:flex-1 md:overflow-y-auto">
          {rule.conditions.map((condition, index) => (
            <ConditionForm
              key={index}
              condition={condition}
              onChange={(next) => updateCondition(index, next)}
              onRemove={() => removeCondition(index)}
              otherRuleOptions={otherRuleOptions}
              areaOptions={areaOptions}
              autoFocusType={index === focusNewConditionAt}
            />
          ))}
        </div>
      </div>
    </div>
  );
}
