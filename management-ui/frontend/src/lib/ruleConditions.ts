import type { Condition } from "../api/rules";

// Arrays (matched_rules, receiver_source) sort by their comma-joined form.
function valueKey(value: string | string[]): string {
  return Array.isArray(value) ? value.join(",") : value;
}

// Applied on save/load only, not while editing -- re-sorting on every keystroke
// would move a row (keyed by array index) out from under the input being typed into.
export function sortConditions(conditions: Condition[]): Condition[] {
  return [...conditions].sort((a, b) => {
    if (a.type !== b.type) return a.type.localeCompare(b.type);
    if (a.operator !== b.operator) return a.operator.localeCompare(b.operator);
    return valueKey(a.value).localeCompare(valueKey(b.value));
  });
}
