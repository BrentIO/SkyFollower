import { useEffect, useState } from "react";
import {
  CONDITION_TYPES,
  OPERATOR_LABELS,
  OPERATORS_BY_TYPE,
  WAKE_TURBULENCE_CATEGORIES,
  type Condition,
  type ConditionType,
  type Operator,
  type WakeTurbulenceCategory,
} from "../api/rules";

interface AreaOption {
  identifier: string;
  name: string;
}

interface ConditionFormProps {
  condition: Condition;
  onChange: (next: Condition) => void;
  onRemove: () => void;
  otherRuleIdentifiers: string[];
  areaOptions: AreaOption[];
}

const TYPE_LABELS: Record<ConditionType, string> = {
  altitude: "Altitude",
  heading: "Heading",
  velocity: "Velocity",
  vertical_speed: "Vertical Speed",
  area: "Area",
  date: "Date",
  ident: "Ident",
  squawk: "Squawk",
  military: "Military",
  operator_airline_designator: "Operator Airline Designator",
  aircraft_type_designator: "Aircraft Type Designator",
  aircraft_registration: "Aircraft Registration",
  aircraft_icao_hex: "Aircraft ICAO Hex",
  aircraft_powerplant_count: "Aircraft Powerplant Count",
  wake_turbulence_category: "Wake Turbulence Category",
  matched_rules: "Matched Rules",
};

function defaultValueFor(type: ConditionType): string | string[] {
  return type === "matched_rules" ? [] : "";
}

// WAKE_TURBULENCE_CATEGORIES stores the exact lowercase strings
// message-processor/rules_engine.py validates against (e.g. "medium 1",
// "high vortex aircraft") -- this only formats the dropdown's visible
// text; the submitted `value` is always the original lowercase form.
function titleCase(category: WakeTurbulenceCategory): string {
  return category.replace(/\b\w/g, (c) => c.toUpperCase());
}

const SORTED_WAKE_TURBULENCE_CATEGORIES = [...WAKE_TURBULENCE_CATEGORIES].sort((a, b) =>
  titleCase(a).localeCompare(titleCase(b)),
);

// Units shown alongside the "Value" label for the condition types where a
// bare number would otherwise be ambiguous. Types not listed here (text,
// dropdown, or otherwise self-explanatory values) get no suffix.
const UNIT_LABELS: Partial<Record<ConditionType, string>> = {
  altitude: "Feet",
  heading: "Degrees",
  velocity: "Knots",
  vertical_speed: "Feet/min",
};

// Small text appended to the right of the value input itself (not the
// "Value" label) for the condition types where a bare number would
// otherwise be ambiguous.
function UnitSuffix({ type }: { type: ConditionType }) {
  const unit = UNIT_LABELS[type];
  if (!unit) return null;
  return <span className="whitespace-nowrap text-sm text-slate-500 dark:text-slate-400">{unit}</span>;
}

// Dropdown order is alphabetical by display label, independent of
// CONDITION_TYPES' declaration order (which mirrors CLAUDE.md's Conditions
// table and message-processor/rules_engine.py's evaluation-priority
// grouping -- neither is meant to dictate UI ordering).
const SORTED_CONDITION_TYPES = [...CONDITION_TYPES].sort((a, b) =>
  TYPE_LABELS[a].localeCompare(TYPE_LABELS[b]),
);

// When the condition type changes, the operator must still be valid for the
// new type (e.g. switching from `heading` (equals-only) to `altitude`
// (minimum/maximum-only) would otherwise leave an operator the new type
// rejects), and the stale value from the old type rarely makes sense
// under the new one either.
function retypeCondition(type: ConditionType): Condition {
  return { type, operator: OPERATORS_BY_TYPE[type][0], value: defaultValueFor(type) };
}

export function ConditionForm({
  condition,
  onChange,
  onRemove,
  otherRuleIdentifiers,
  areaOptions,
}: ConditionFormProps) {
  const validOperators = OPERATORS_BY_TYPE[condition.type];

  function setValue(value: string | string[]) {
    onChange({ ...condition, value });
  }

  function setOperator(operator: Operator) {
    onChange({ ...condition, operator });
  }

  return (
    <div className="flex flex-wrap items-start gap-3 rounded-md border border-slate-200 p-3 dark:border-slate-700">
      <label className="flex flex-col gap-1 text-xs font-medium text-slate-500">
        Type
        <select
          className="input"
          value={condition.type}
          onChange={(e) => onChange(retypeCondition(e.target.value as ConditionType))}
        >
          {SORTED_CONDITION_TYPES.map((type) => (
            <option key={type} value={type}>
              {TYPE_LABELS[type]}
            </option>
          ))}
        </select>
      </label>

      <label className="flex flex-col gap-1 text-xs font-medium text-slate-500">
        Operator
        <select
          className="input"
          value={condition.operator}
          onChange={(e) => setOperator(e.target.value as Operator)}
        >
          {validOperators.map((op) => (
            <option key={op} value={op}>
              {OPERATOR_LABELS[op]}
            </option>
          ))}
        </select>
      </label>

      <div className="flex min-w-64 flex-1 flex-col gap-1 text-xs font-medium text-slate-500">
        Value
        <ConditionValueInput
          condition={condition}
          onValueChange={setValue}
          otherRuleIdentifiers={otherRuleIdentifiers}
          areaOptions={areaOptions}
        />
      </div>

      <button
        type="button"
        onClick={onRemove}
        className="mt-5 rounded-md px-2 py-1.5 text-sm text-red-600 hover:bg-red-50 dark:hover:bg-red-950"
        aria-label="Remove condition"
      >
        Remove
      </button>
    </div>
  );
}

function ConditionValueInput({
  condition,
  onValueChange,
  otherRuleIdentifiers,
  areaOptions,
}: {
  condition: Condition;
  onValueChange: (value: string | string[]) => void;
  otherRuleIdentifiers: string[];
  areaOptions: AreaOption[];
}) {
  const value = condition.value;

  switch (condition.type) {
    case "altitude":
    case "velocity":
    case "aircraft_powerplant_count":
      return (
        <div className="flex items-center gap-2">
          <input
            type="number"
            min={0}
            step={1}
            className="input"
            value={value as string}
            onChange={(e) => onValueChange(e.target.value)}
          />
          <UnitSuffix type={condition.type} />
        </div>
      );

    case "vertical_speed":
      return (
        <div className="flex items-center gap-2">
          <input
            type="number"
            step={1}
            className="input"
            value={value as string}
            onChange={(e) => onValueChange(e.target.value)}
          />
          <UnitSuffix type={condition.type} />
        </div>
      );

    case "heading":
      return (
        <div className="flex items-center gap-2">
          <HeadingInput value={value as string} onValueChange={onValueChange} />
          <UnitSuffix type={condition.type} />
        </div>
      );

    case "date":
      return <DateConditionInput value={value as string} onValueChange={onValueChange} />;

    case "squawk":
      return (
        <input
          type="text"
          inputMode="numeric"
          maxLength={4}
          placeholder="0000"
          className="input"
          value={value as string}
          onChange={(e) => onValueChange(e.target.value.replace(/[^0-9]/g, "").slice(0, 4))}
        />
      );

    case "aircraft_icao_hex":
      return (
        <input
          type="text"
          maxLength={6}
          placeholder="A8AE7F"
          className="input uppercase"
          value={value as string}
          onChange={(e) => onValueChange(e.target.value.toUpperCase().slice(0, 6))}
        />
      );

    case "aircraft_type_designator":
      return (
        <input
          type="text"
          maxLength={4}
          placeholder="B752"
          className="input uppercase"
          value={value as string}
          onChange={(e) => onValueChange(e.target.value.toUpperCase().slice(0, 4))}
        />
      );

    case "operator_airline_designator":
      return (
        <input
          type="text"
          maxLength={3}
          className="input uppercase"
          value={value as string}
          onChange={(e) =>
            onValueChange(e.target.value.replace(/[^A-Za-z0-9]/g, "").toUpperCase().slice(0, 3))
          }
        />
      );

    case "military":
      return (
        <select
          className="input"
          value={value === "true" ? "true" : "false"}
          onChange={(e) => onValueChange(e.target.value)}
        >
          <option value="true">True</option>
          <option value="false">False</option>
        </select>
      );

    case "wake_turbulence_category":
      return (
        <select className="input" value={value as string} onChange={(e) => onValueChange(e.target.value)}>
          <option value="" disabled>
            Select...
          </option>
          {SORTED_WAKE_TURBULENCE_CATEGORIES.map((category) => (
            <option key={category} value={category}>
              {titleCase(category)}
            </option>
          ))}
        </select>
      );

    case "area":
      return (
        <select className="input" value={value as string} onChange={(e) => onValueChange(e.target.value)}>
          <option value="" disabled>
            Select an area...
          </option>
          {areaOptions.map((area) => (
            <option key={area.identifier} value={area.identifier}>
              {area.name || area.identifier}
            </option>
          ))}
        </select>
      );

    case "matched_rules": {
      const selected = Array.isArray(value) ? value : [];

      function toggle(identifier: string) {
        onValueChange(
          selected.includes(identifier)
            ? selected.filter((id) => id !== identifier)
            : [...selected, identifier],
        );
      }

      return (
        <div className="input flex max-h-32 flex-col gap-1 overflow-y-auto">
          {otherRuleIdentifiers.length === 0 && (
            <span className="text-sm text-slate-400">No other rules to match against yet.</span>
          )}
          {otherRuleIdentifiers.map((identifier) => (
            <label key={identifier} className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={selected.includes(identifier)}
                onChange={() => toggle(identifier)}
              />
              {identifier}
            </label>
          ))}
        </div>
      );
    }

    case "ident":
    case "aircraft_registration":
    default:
      return (
        <input
          type="text"
          className="input"
          value={value as string}
          onChange={(e) => onValueChange(e.target.value)}
        />
      );
  }
}

function HeadingInput({
  value,
  onValueChange,
}: {
  value: string;
  onValueChange: (value: string) => void;
}) {
  const [min = "", max = ""] = value.split(",");

  function update(nextMin: string, nextMax: string) {
    onValueChange(`${nextMin},${nextMax}`);
  }

  return (
    <div className="flex items-center gap-2">
      <input
        type="number"
        min={0}
        max={359}
        placeholder="min"
        className="input w-24"
        value={min}
        onChange={(e) => update(e.target.value, max)}
      />
      <span className="text-slate-400">to</span>
      <input
        type="number"
        min={0}
        max={359}
        placeholder="max"
        className="input w-24"
        value={max}
        onChange={(e) => update(min, e.target.value)}
      />
    </div>
  );
}

// A native <input type="datetime-local"> renders in whatever numeric
// mm/dd vs. dd/mm order the browser's own locale prefers, and has no way
// to force a specific display format -- so the display format below
// (dd-MMM-yyyy HH:mm, e.g. "24-Dec-2026 22:00") is a plain text input with
// manual parse/format instead, always in the browser's local timezone and
// converted to `YYYY-MM-DDTHH:MMZ` on save, per the spec's "UI converts
// local time to UTC (Z) before saving."
const MONTHS = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];
const DATE_DISPLAY_PATTERN = /^(\d{1,2})-([A-Za-z]{3})-(\d{4})[ T](\d{1,2}):(\d{2})$/;

function pad2(n: number): string {
  return String(n).padStart(2, "0");
}

function localToUtcZ(local: Date): string {
  return (
    `${local.getUTCFullYear()}-${pad2(local.getUTCMonth() + 1)}-${pad2(local.getUTCDate())}` +
    `T${pad2(local.getUTCHours())}:${pad2(local.getUTCMinutes())}Z`
  );
}

// Reverse direction, for populating the display input when editing an
// existing condition (any ISO 8601 offset parses fine via the Date
// constructor, not just Z, and so does a bare date-only value).
function isoToDisplay(iso: string): string {
  const asDate = new Date(iso);
  if (Number.isNaN(asDate.getTime())) return "";
  return (
    `${pad2(asDate.getDate())}-${MONTHS[asDate.getMonth()]}-${asDate.getFullYear()} ` +
    `${pad2(asDate.getHours())}:${pad2(asDate.getMinutes())}`
  );
}

// Parses "dd-MMM-yyyy HH:mm" as a local-timezone Date, or null if the text
// doesn't match or names an out-of-range day (e.g. "31-Feb-2026" -- the
// Date constructor would otherwise silently roll that over into March).
function parseDisplay(text: string): Date | null {
  const match = DATE_DISPLAY_PATTERN.exec(text.trim());
  if (!match) return null;
  const [, day, monthName, year, hour, minute] = match;
  const monthIndex = MONTHS.findIndex((m) => m.toLowerCase() === monthName.toLowerCase());
  if (monthIndex === -1) return null;

  const parsed = new Date(Number(year), monthIndex, Number(day), Number(hour), Number(minute));
  if (parsed.getMonth() !== monthIndex || parsed.getDate() !== Number(day)) return null;
  return parsed;
}

const browserTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;

function DateConditionInput({
  value,
  onValueChange,
}: {
  value: string;
  onValueChange: (value: string) => void;
}) {
  const [text, setText] = useState(() => isoToDisplay(value));
  const [invalid, setInvalid] = useState(false);

  // Re-sync the visible text when switching to a different condition
  // (e.g. selecting a different row), not on every keystroke of our own.
  useEffect(() => {
    setText(isoToDisplay(value));
    setInvalid(false);
  }, [value]);

  function handleChange(next: string) {
    setText(next);
    const parsed = parseDisplay(next);
    setInvalid(parsed === null);
    if (parsed) onValueChange(localToUtcZ(parsed));
  }

  return (
    <div className="flex flex-col gap-1">
      <input
        type="text"
        placeholder="dd-MMM-yyyy HH:mm"
        className={`input ${invalid ? "border-red-500 dark:border-red-500" : ""}`}
        value={text}
        onChange={(e) => handleChange(e.target.value)}
      />
      <p className="text-xs text-slate-400">
        Format: dd-MMM-yyyy HH:mm, in your local timezone ({browserTimeZone}); stored as UTC.
      </p>
    </div>
  );
}
