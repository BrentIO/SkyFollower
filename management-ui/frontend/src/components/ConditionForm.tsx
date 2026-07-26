import {
  CONDITION_TYPES,
  OPERATORS_BY_TYPE,
  WAKE_TURBULENCE_CATEGORIES,
  type Condition,
  type ConditionType,
  type Operator,
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
          className="rounded-md border border-slate-300 bg-white px-2 py-1.5 text-sm dark:border-slate-600 dark:bg-slate-800"
          value={condition.type}
          onChange={(e) => onChange(retypeCondition(e.target.value as ConditionType))}
        >
          {CONDITION_TYPES.map((type) => (
            <option key={type} value={type}>
              {TYPE_LABELS[type]}
            </option>
          ))}
        </select>
      </label>

      <label className="flex flex-col gap-1 text-xs font-medium text-slate-500">
        Operator
        <select
          className="rounded-md border border-slate-300 bg-white px-2 py-1.5 text-sm dark:border-slate-600 dark:bg-slate-800"
          value={condition.operator}
          onChange={(e) => setOperator(e.target.value as Operator)}
        >
          {validOperators.map((op) => (
            <option key={op} value={op}>
              {op}
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
        <input
          type="number"
          min={0}
          step={1}
          className="input"
          value={value as string}
          onChange={(e) => onValueChange(e.target.value)}
        />
      );

    case "vertical_speed":
      return (
        <input
          type="number"
          step={1}
          className="input"
          value={value as string}
          onChange={(e) => onValueChange(e.target.value)}
        />
      );

    case "heading":
      return <HeadingInput value={value as string} onValueChange={onValueChange} />;

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
          placeholder="DAL"
          className="input uppercase"
          value={value as string}
          onChange={(e) => onValueChange(e.target.value.toUpperCase().slice(0, 3))}
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
          {WAKE_TURBULENCE_CATEGORIES.map((category) => (
            <option key={category} value={category}>
              {category}
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
      return (
        <select
          multiple
          className="input h-24"
          value={selected}
          onChange={(e) =>
            onValueChange(Array.from(e.target.selectedOptions).map((o) => o.value))
          }
        >
          {otherRuleIdentifiers.map((identifier) => (
            <option key={identifier} value={identifier}>
              {identifier}
            </option>
          ))}
        </select>
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

const DATE_ONLY = /^\d{4}-\d{2}-\d{2}$/;

// Converts a `datetime-local` input value (always in the browser's local
// timezone, no offset of its own) to `YYYY-MM-DDTHH:MMZ`, per the spec's
// "UI converts local time to UTC (Z) before saving."
function localToUtcZ(local: string): string {
  const asDate = new Date(local);
  const pad = (n: number) => String(n).padStart(2, "0");
  return (
    `${asDate.getUTCFullYear()}-${pad(asDate.getUTCMonth() + 1)}-${pad(asDate.getUTCDate())}` +
    `T${pad(asDate.getUTCHours())}:${pad(asDate.getUTCMinutes())}Z`
  );
}

// Reverse of localToUtcZ, for populating the datetime-local input when
// editing an existing datetime condition (any ISO 8601 offset parses fine
// via the Date constructor, not just Z).
function isoToLocalInput(iso: string): string {
  const asDate = new Date(iso);
  if (Number.isNaN(asDate.getTime())) return "";
  const pad = (n: number) => String(n).padStart(2, "0");
  return (
    `${asDate.getFullYear()}-${pad(asDate.getMonth() + 1)}-${pad(asDate.getDate())}` +
    `T${pad(asDate.getHours())}:${pad(asDate.getMinutes())}`
  );
}

const browserTimeZone = Intl.DateTimeFormat().resolvedOptions().timeZone;

function DateConditionInput({
  value,
  onValueChange,
}: {
  value: string;
  onValueChange: (value: string) => void;
}) {
  const isDateOnly = value === "" || DATE_ONLY.test(value);

  function setFormat(format: "date" | "datetime") {
    const now = new Date().toISOString();
    onValueChange(format === "date" ? now.slice(0, 10) : `${now.slice(0, 16)}Z`);
  }

  return (
    <div className="flex flex-col gap-1">
      <select
        className="input"
        value={isDateOnly ? "date" : "datetime"}
        onChange={(e) => setFormat(e.target.value as "date" | "datetime")}
      >
        <option value="date">Date</option>
        <option value="datetime">Date and time</option>
      </select>

      {isDateOnly ? (
        <input
          type="date"
          className="input"
          value={value}
          onChange={(e) => onValueChange(e.target.value)}
        />
      ) : (
        <>
          <input
            type="datetime-local"
            className="input"
            value={isoToLocalInput(value)}
            onChange={(e) => onValueChange(localToUtcZ(e.target.value))}
          />
          <p className="text-xs text-slate-400">
            Entered in your local timezone ({browserTimeZone}), stored as UTC.
          </p>
        </>
      )}
    </div>
  );
}
