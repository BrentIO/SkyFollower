import type { Condition } from "../api/rules";

// A condition's `value` is either a scalar string or a string[] (the two
// array-valued types, matched_rules and receiver_source). Compare arrays
// by their comma-joined form -- the same convention this app already uses
// for a condition's compound value elsewhere (e.g. heading's "min,max").
function valueKey(value: string | string[]): string {
  return Array.isArray(value) ? value.join(",") : value;
}

// Canonical order for a rule's `conditions`: type, then operator, then
// value. Applied when a rule is saved and when it is (re)loaded into the
// editor -- NOT continuously while editing: `value` is part of the sort
// key, so re-sorting on every keystroke would make a row jump position
// while it is being typed into (and rows are keyed by array index, so
// React would drop focus). Array.prototype.sort is stable, so genuinely
// equal (type, operator, value) tuples keep their existing relative order.
//
// Shared (not private to RulesView) so the rules importer can canonicalize
// imported rules' conditions the same way.
export function sortConditions(conditions: Condition[]): Condition[] {
  return [...conditions].sort((a, b) => {
    if (a.type !== b.type) return a.type.localeCompare(b.type);
    if (a.operator !== b.operator) return a.operator.localeCompare(b.operator);
    return valueKey(a.value).localeCompare(valueKey(b.value));
  });
}
