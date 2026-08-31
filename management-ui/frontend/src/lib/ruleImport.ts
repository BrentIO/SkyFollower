import type { Condition } from "../api/rules";
import { sortConditions } from "./ruleConditions";

// The identifier charset the backend enforces on POST /api/rules.
const IDENTIFIER_PATTERN = /^[A-Za-z0-9_-]+$/;

// A rule pulled from an imported JSON array. Only `identifier` is checked
// structurally here -- the backend's own POST /api/rules 400 is the
// authority on everything deeper (condition types, operators, values), and
// a second client copy of that validation would just be two things to keep
// in sync. Everything else passes through untouched.
export interface ImportedRule {
  identifier: string;
  [key: string]: unknown;
}

export interface RuleParseResult {
  // Non-null means the whole file is rejected -- nothing is imported.
  error: string | null;
  rules: ImportedRule[];
}

// Whole-file hard gate: valid JSON, array root, non-empty, every element a
// plain object with a non-empty `identifier` string. Blank input is the
// pristine no-op state, not an error.
export function parseAndValidate(text: string): RuleParseResult {
  if (!text.trim()) return { error: null, rules: [] };

  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { error: "Not valid JSON.", rules: [] };
  }
  if (!Array.isArray(parsed)) {
    return { error: "Expected a JSON array of rules.", rules: [] };
  }
  if (parsed.length === 0) {
    return { error: "The file contains no rules.", rules: [] };
  }

  const rules: ImportedRule[] = [];
  for (let i = 0; i < parsed.length; i++) {
    const r = parsed[i];
    if (typeof r !== "object" || r === null || Array.isArray(r)) {
      return { error: `Rule ${i + 1}: not a JSON object.`, rules: [] };
    }
    const identifier = (r as { identifier?: unknown }).identifier;
    if (typeof identifier !== "string" || identifier.trim() === "") {
      return { error: `Rule ${i + 1}: missing or empty "identifier".`, rules: [] };
    }
    rules.push(r as ImportedRule);
  }
  return { error: null, rules };
}

// Auto-suffix collision strategy, mirroring areaImport's
// resolveFeatureIdentity: a missing / pattern-invalid / already-taken
// identifier gets an incrementing `_2` / `_3`. `taken` accumulates across
// the batch (existing rules plus everything resolved earlier), so two
// colliding entries in one file don't collide with each other either.
// Rules have no name to resolve, only the identifier.
export function resolveRuleIdentifier(
  rule: ImportedRule,
  index: number,
  taken: Set<string>,
): string {
  const raw = typeof rule.identifier === "string" ? rule.identifier.trim() : "";
  const base = raw !== "" && IDENTIFIER_PATTERN.test(raw) ? raw : `imported_rule_${index}`;
  let identifier = base;
  if (taken.has(identifier)) {
    let n = 2;
    while (taken.has(`${base}_${n}`)) n++;
    identifier = `${base}_${n}`;
  }
  taken.add(identifier);
  return identifier;
}

function conditionsOf(rule: ImportedRule): Record<string, unknown>[] {
  const c = rule.conditions;
  if (!Array.isArray(c)) return [];
  return c.filter((x): x is Record<string, unknown> => !!x && typeof x === "object");
}

function stringArrayValue(condition: Record<string, unknown>): string[] {
  const v = condition.value;
  return Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];
}

// Every `area` condition value in the rule not present in the current
// areas list. Empty = all references resolve. Mirrors the backend's own
// save-time existence check exactly: identifier presence, any geometry
// type (Polygon-only is a separate usability concern, not existence).
export function missingAreaReferences(
  rule: ImportedRule,
  existingAreaIdentifiers: Set<string>,
): string[] {
  const values = conditionsOf(rule)
    .filter((c) => c.type === "area" && typeof c.value === "string")
    .map((c) => c.value as string);
  return [...new Set(values.filter((v) => !existingAreaIdentifiers.has(v)))];
}

// matched_rules condition values in the rule, remapped through the
// batch's original->resolved identifier map so a self-consistent file
// still links up even when an identifier was auto-suffixed on import.
function remappedMatchedRuleRefs(
  rule: ImportedRule,
  remap: Map<string, string>,
): string[] {
  const out: string[] = [];
  for (const c of conditionsOf(rule)) {
    if (c.type === "matched_rules") {
      for (const ref of stringArrayValue(c)) out.push(remap.get(ref) ?? ref);
    }
  }
  return out;
}

// Produces the exact payload sent to the backend: identifier replaced with
// the resolved one, matched_rules references remapped, conditions
// canonically sorted (same helper the editor uses).
function buildPayload(
  rule: ImportedRule,
  identifier: string,
  remap: Map<string, string>,
): Record<string, unknown> {
  const conditions: Record<string, unknown>[] = conditionsOf(rule).map((c) =>
    c.type === "matched_rules"
      ? { ...c, value: stringArrayValue(c).map((ref) => remap.get(ref) ?? ref) }
      : { ...c },
  );
  const sortable =
    conditions.length > 0 &&
    conditions.every((c) => typeof c.type === "string" && typeof c.operator === "string");
  return {
    ...rule,
    identifier,
    conditions: sortable ? sortConditions(conditions as unknown as Condition[]) : conditions,
  };
}

export interface RuleImportResult {
  created: string[];
  failed: { identifier: string; reason: string }[];
  skipped: { identifier: string; missingAreas: string[] }[];
}

// Best-effort batch import with three outcome buckets:
//
// - `skipped`: an `area` condition references an area not present here. The
//   backend already rejects this on save; pre-checking gives it a known,
//   explainable bucket (with the missing identifier) instead of a generic
//   failure, and skips a doomed round trip. Never attempted.
// - `created`: reached the backend and it accepted the rule.
// - `failed`: a backend 400 for any other reason, OR ejected in the
//   post-pass below.
//
// matched_rules can't be pre-checked the way `area` can -- the backend
// places no existence constraint on it (that's what makes a legit cyclic
// pair possible). So it's create-then-verify: create everything that
// passed the area check regardless of what its matched_rules point at,
// then verify referential integrity against what's actually there and
// eject (delete) anything dangling, looping until a full pass ejects
// nothing (ejecting C can leave E -- which referenced C -- newly dangling).
export async function importRulesBatch(
  rules: ImportedRule[],
  existingRuleIdentifiers: string[],
  existingAreaIdentifiers: string[],
  createOne: (payload: Record<string, unknown>) => Promise<void>,
  deleteOne: (identifier: string) => Promise<void>,
): Promise<RuleImportResult> {
  const areaSet = new Set(existingAreaIdentifiers);
  const taken = new Set(existingRuleIdentifiers);
  const remap = new Map<string, string>();

  // Pass 1: resolve every identifier (so the remap is complete before any
  // payload is built) and run the area pre-check.
  const identifiers: string[] = [];
  const skipped: RuleImportResult["skipped"] = [];
  for (let i = 0; i < rules.length; i++) {
    const original = typeof rules[i].identifier === "string" ? rules[i].identifier.trim() : "";
    const identifier = resolveRuleIdentifier(rules[i], i + 1, taken);
    if (original) remap.set(original, identifier);
    identifiers.push(identifier);
  }

  const toCreate: { payload: Record<string, unknown>; identifier: string }[] = [];
  for (let i = 0; i < rules.length; i++) {
    const missing = missingAreaReferences(rules[i], areaSet);
    if (missing.length > 0) {
      skipped.push({ identifier: identifiers[i], missingAreas: missing });
      continue;
    }
    toCreate.push({ payload: buildPayload(rules[i], identifiers[i], remap), identifier: identifiers[i] });
  }

  // Pass 2: create, regardless of matched_rules references.
  const created: string[] = [];
  const failed: RuleImportResult["failed"] = [];
  const payloadByIdentifier = new Map<string, Record<string, unknown>>();
  for (const { payload, identifier } of toCreate) {
    try {
      await createOne(payload);
      created.push(identifier);
      payloadByIdentifier.set(identifier, payload);
    } catch (err) {
      failed.push({ identifier, reason: err instanceof Error ? err.message : "backend rejected the rule" });
    }
  }

  // Pass 3: eject rules whose matched_rules references don't resolve,
  // transitively, until stable.
  const known = new Set<string>([...existingRuleIdentifiers, ...created]);
  let changed = true;
  while (changed) {
    changed = false;
    for (const identifier of [...created]) {
      const payload = payloadByIdentifier.get(identifier);
      if (!payload) continue;
      const refs = remappedMatchedRuleRefs(payload as ImportedRule, remap);
      const dangling = [...new Set(refs.filter((r) => !known.has(r)))];
      if (dangling.length === 0) continue;

      try {
        await deleteOne(identifier);
      } catch {
        // Even a failed eject-delete: still report the rule as rejected so
        // the operator knows to clean it up.
      }
      created.splice(created.indexOf(identifier), 1);
      known.delete(identifier);
      failed.push({ identifier, reason: `references missing rule(s): ${dangling.join(", ")}` });
      changed = true;
    }
  }

  return { created, failed, skipped };
}
