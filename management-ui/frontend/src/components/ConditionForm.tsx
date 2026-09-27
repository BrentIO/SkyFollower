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

interface RuleOption {
  identifier: string;
  name: string;
}

interface ConditionFormProps {
  condition: Condition;
  onChange: (next: Condition) => void;
  onRemove: () => void;
  otherRuleOptions: RuleOption[];
  areaOptions: AreaOption[];
  autoFocusType?: boolean;
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
  receiver_source: "Receiver Source",
  operator_airline_designator: "Airline Designator",
  aircraft_type_designator: "Aircraft Type Designator",
  aircraft_registration: "Aircraft Registration",
  aircraft_icao_hex: "Aircraft ICAO Hex",
  aircraft_powerplant_count: "Aircraft Powerplant Count",
  wake_turbulence_category: "Wake Turbulence Category",
  matched_rules: "Matched Rules",
};

function defaultValueFor(type: ConditionType): string | string[] {
  return type === "matched_rules" || type === "receiver_source" ? [] : "";
}

// Fixed 3-row list for receiver_source, unlike matched_rules' dynamic list
// of other rule identifiers -- these are the only 3 valid wire values.
const RECEIVER_SOURCE_OPTIONS: readonly { value: string; label: string }[] = [
  { value: "1090", label: "1090MHz ADS-B" },
  { value: "978", label: "978 UAT" },
  { value: "EXTERNAL", label: "External" },
];

// Formats the dropdown's visible text; the submitted `value` stays the
// original lowercase form WAKE_TURBULENCE_CATEGORIES validates against.
function titleCase(category: WakeTurbulenceCategory): string {
  return category.replace(/\b\w/g, (c) => c.toUpperCase());
}

// Light-to-heavy by weight, not alphabetical. Super, rotorcraft, and high
// performance are dropped: Super has no code point in the live category
// data this field is sourced from, and the other two are an emitter-type
// axis, not a weight class.
const WAKE_TURBULENCE_ORDER: readonly WakeTurbulenceCategory[] = ["light", "medium", "heavy"];

// Catches drift if WAKE_TURBULENCE_CATEGORIES changes without this
// hand-written order being updated to match.
if (WAKE_TURBULENCE_ORDER.length !== WAKE_TURBULENCE_CATEGORIES.length) {
  throw new Error("WAKE_TURBULENCE_ORDER is out of sync with WAKE_TURBULENCE_CATEGORIES");
}

// Units for condition types where a bare number would be ambiguous.
const UNIT_LABELS: Partial<Record<ConditionType, string>> = {
  altitude: "Feet",
  heading: "Degrees",
  velocity: "Knots",
  vertical_speed: "Feet/min",
};

// Client-side min/max mirroring real-world flight envelope limits, purely
// a UI fast-fail nicety -- the backend enforces no upper bound.
const NUMERIC_BOUNDS: Partial<Record<ConditionType, { min: number; max: number }>> = {
  altitude: { min: 0, max: 65000 },
  velocity: { min: 0, max: 1334 },
  aircraft_powerplant_count: { min: 0, max: 99 },
  vertical_speed: { min: -10000, max: 10000 },
};

function UnitSuffix({ type }: { type: ConditionType }) {
  const unit = UNIT_LABELS[type];
  if (!unit) return null;
  return <span className="whitespace-nowrap text-sm text-slate-500 dark:text-slate-400">{unit}</span>;
}

// Dropdown order is alphabetical by display label, independent of
// CONDITION_TYPES' declaration order (which isn't meant to dictate UI order).
const SORTED_CONDITION_TYPES = [...CONDITION_TYPES].sort((a, b) =>
  TYPE_LABELS[a].localeCompare(TYPE_LABELS[b]),
);

// Resets operator and value on type change, since neither is guaranteed
// valid under the new type (e.g. heading's equals-only vs. altitude's
// minimum/maximum-only operators).
function retypeCondition(type: ConditionType): Condition {
  return { type, operator: OPERATORS_BY_TYPE[type][0], value: defaultValueFor(type) };
}

export function ConditionForm({
  condition,
  onChange,
  onRemove,
  otherRuleOptions,
  areaOptions,
  autoFocusType,
}: ConditionFormProps) {
  // condition.type is "" for a freshly-added row with no type chosen yet.
  const validOperators = condition.type ? OPERATORS_BY_TYPE[condition.type] : [];

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
          autoFocus={autoFocusType}
          onChange={(e) => onChange(retypeCondition(e.target.value as ConditionType))}
        >
          <option value="" disabled>
            Select a type...
          </option>
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
          disabled={validOperators.length <= 1}
          onChange={(e) => setOperator(e.target.value as Operator)}
        >
          {validOperators.map((op) => (
            <option key={op} value={op}>
              {OPERATOR_LABELS[op]}
            </option>
          ))}
        </select>
      </label>

      <div className="flex min-w-80 flex-1 flex-col gap-1 text-xs font-medium text-slate-500">
        Value
        <ConditionValueInput
          condition={condition}
          onValueChange={setValue}
          otherRuleOptions={otherRuleOptions}
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
  otherRuleOptions,
  areaOptions,
}: {
  condition: Condition;
  onValueChange: (value: string | string[]) => void;
  otherRuleOptions: RuleOption[];
  areaOptions: AreaOption[];
}) {
  const value = condition.value;

  switch (condition.type) {
    case "":
      return <span className="input flex items-center text-slate-400">Select a type first</span>;

    case "altitude":
    case "velocity":
    case "aircraft_powerplant_count": {
      const bounds = NUMERIC_BOUNDS[condition.type];
      return (
        <div className="flex items-center gap-2">
          <input
            type="number"
            min={bounds?.min}
            max={bounds?.max}
            step={1}
            placeholder={condition.type === "aircraft_powerplant_count" ? "1" : undefined}
            className="input"
            value={value as string}
            onChange={(e) => onValueChange(e.target.value)}
          />
          <UnitSuffix type={condition.type} />
        </div>
      );
    }

    case "vertical_speed": {
      const bounds = NUMERIC_BOUNDS.vertical_speed;
      return (
        <div className="flex items-center gap-2">
          <input
            type="number"
            min={bounds?.min}
            max={bounds?.max}
            step={1}
            className="input"
            value={value as string}
            onChange={(e) => onValueChange(e.target.value)}
          />
          <UnitSuffix type={condition.type} />
        </div>
      );
    }

    case "heading":
      return (
        <div className="flex flex-wrap items-center gap-2">
          <HeadingInput value={value as string} onValueChange={onValueChange} />
          <UnitSuffix type={condition.type} />
          <HeadingCompass value={value as string} />
        </div>
      );

    case "date":
      return <DateConditionInput value={value as string} onValueChange={onValueChange} />;

    case "squawk":
      // Octal digits only -- a transponder never sends 8 or 9 in any position.
      return (
        <input
          type="text"
          inputMode="numeric"
          maxLength={4}
          placeholder="1200"
          className="input"
          value={value as string}
          onChange={(e) => onValueChange(e.target.value.replace(/[^0-7]/g, "").slice(0, 4))}
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
          onChange={(e) =>
            onValueChange(e.target.value.replace(/[^0-9A-Fa-f]/g, "").toUpperCase().slice(0, 6))
          }
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

    case "receiver_source": {
      const selected = Array.isArray(value) ? value : [];

      function toggle(source: string) {
        onValueChange(
          selected.includes(source) ? selected.filter((s) => s !== source) : [...selected, source],
        );
      }

      return (
        <div className="input flex max-h-32 flex-col gap-1 overflow-y-auto">
          {RECEIVER_SOURCE_OPTIONS.map((option) => {
            const checked = selected.includes(option.value);
            // Capped at 2: all 3 selected is equivalent to no filter, which
            // the backend rejects, so disable the last box pre-emptively.
            const disabled = !checked && selected.length >= 2;
            return (
              <label
                key={option.value}
                className={`flex items-center gap-2 text-sm ${disabled ? "opacity-40" : ""}`}
              >
                <input
                  type="checkbox"
                  checked={checked}
                  disabled={disabled}
                  onChange={() => toggle(option.value)}
                />
                {option.label}
              </label>
            );
          })}
        </div>
      );
    }

    case "wake_turbulence_category":
      return (
        <select className="input" value={value as string} onChange={(e) => onValueChange(e.target.value)}>
          <option value="" disabled>
            Select...
          </option>
          {WAKE_TURBULENCE_ORDER.map((category) => (
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

      // Checked items float to the top, alphabetical within each group.
      const sortedOptions = [...otherRuleOptions].sort((a, b) => {
        const aChecked = selected.includes(a.identifier);
        const bChecked = selected.includes(b.identifier);
        if (aChecked !== bChecked) return aChecked ? -1 : 1;
        return (a.name || a.identifier).localeCompare(b.name || b.identifier);
      });

      return (
        <div className="input flex max-h-32 flex-col gap-1 overflow-y-auto">
          {otherRuleOptions.length === 0 && (
            <span className="text-sm text-slate-400">No other rules to match against yet.</span>
          )}
          {sortedOptions.map((option) => (
            <label key={option.identifier} className="flex items-center gap-2 text-sm">
              <input
                type="checkbox"
                checked={selected.includes(option.identifier)}
                onChange={() => toggle(option.identifier)}
              />
              {option.name || option.identifier}
            </label>
          ))}
        </div>
      );
    }

    case "aircraft_registration":
      return (
        <input
          type="text"
          placeholder="N659DL"
          className="input uppercase"
          value={value as string}
          onChange={(e) =>
            onValueChange(e.target.value.replace(/[^0-9A-Za-z-]/g, "").toUpperCase())
          }
        />
      );

    case "ident":
      return (
        <input
          type="text"
          placeholder="DAL2"
          className="input"
          value={value as string}
          onChange={(e) => onValueChange(e.target.value)}
        />
      );

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

// Min/max is the arc going clockwise from min to max, so min > max wraps
// through 0/360 (e.g. 340-020 is northbound). Shades the arc that will
// actually be matched, since entering the pair backwards silently selects
// the other, usually much larger, arc instead.
function HeadingCompass({ value }: { value: string }) {
  const [min = "", max = ""] = value.split(",");
  const size = 40;
  const radius = 13;
  const center = size / 2;

  function pointAt(degrees: number, atRadius: number = radius) {
    const rad = (degrees * Math.PI) / 180;
    return {
      x: center + atRadius * Math.sin(rad),
      y: center - atRadius * Math.cos(rad),
    };
  }

  const minDeg = Number(min);
  const maxDeg = Number(max);
  const hasRange = min !== "" && max !== "" && !Number.isNaN(minDeg) && !Number.isNaN(maxDeg);

  let wedgePath: string | null = null;
  if (hasRange) {
    const sweep = (maxDeg - minDeg + 360) % 360;
    const largeArc = sweep > 180 ? 1 : 0;
    const start = pointAt(minDeg);
    const end = pointAt(maxDeg);
    wedgePath = `M ${center} ${center} L ${start.x} ${start.y} A ${radius} ${radius} 0 ${largeArc} 1 ${end.x} ${end.y} Z`;
  }

  return (
    <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} className="shrink-0" aria-hidden="true">
      <circle
        cx={center}
        cy={center}
        r={radius}
        className="fill-none stroke-slate-300 dark:stroke-slate-600"
        strokeWidth={1}
      />
      {wedgePath && (
        <path
          d={wedgePath}
          className="fill-sky-500/40 stroke-sky-600 dark:fill-sky-400/30 dark:stroke-sky-400"
          strokeWidth={1}
        />
      )}
      {(["N", "E", "S", "W"] as const).map((label, i) => {
        const p = pointAt(i * 90, radius + 5);
        return (
          <text
            key={label}
            x={p.x}
            y={p.y}
            dy="0.3em"
            textAnchor="middle"
            className="fill-slate-400 text-[6px] dark:fill-slate-500"
          >
            {label}
          </text>
        );
      })}
    </svg>
  );
}

// Converts a `datetime-local` value (browser's local timezone, no offset
// of its own) to `YYYY-MM-DDTHH:MMZ` before saving.
function localToUtcZ(local: string): string {
  const asDate = new Date(local);
  const pad = (n: number) => String(n).padStart(2, "0");
  return (
    `${asDate.getUTCFullYear()}-${pad(asDate.getUTCMonth() + 1)}-${pad(asDate.getUTCDate())}` +
    `T${pad(asDate.getUTCHours())}:${pad(asDate.getUTCMinutes())}Z`
  );
}

// Reverse of localToUtcZ, for populating the input when editing an existing
// condition. Any ISO 8601 offset (or a bare date) parses via Date, not just Z.
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
  return (
    <div className="flex flex-col gap-1">
      <input
        type="datetime-local"
        className="input"
        value={isoToLocalInput(value)}
        onChange={(e) => onValueChange(localToUtcZ(e.target.value))}
      />
      <p className="text-xs text-slate-400">
        Entered in your local timezone ({browserTimeZone}), stored as UTC.
      </p>
    </div>
  );
}
