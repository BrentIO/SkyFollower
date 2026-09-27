import type { Condition } from "../api/rules";
import { sortConditions } from "./ruleConditions";

// The identifier charset the backend enforces on POST /api/rules.
const IDENTIFIER_PATTERN = /^[A-Za-z0-9_-]+$/;

// Only `identifier` is checked structurally; deeper validation (condition
// types/operators/values) is left to the backend's own POST /api/rules 400.
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

// Missing/invalid/already-taken identifiers get an incrementing `_2` suffix;
// `taken` accumulates across the batch so in-file collisions resolve too.
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

// Per-identifier conflict choice: skip the duplicate, or keep both via rename.
export type ImportConflictChoice = "skip" | "rename";

// Distinct imported identifiers colliding with `existingIdentifiers`, in file
// order. An empty result skips ImportConflictModal entirely.
export function collidingIdentifiers(rules: ImportedRule[], existingIdentifiers: string[]): string[] {
  const existing = new Set(existingIdentifiers);
  const seen = new Set<string>();
  const out: string[] = [];
  for (const rule of rules) {
    const raw = typeof rule.identifier === "string" ? rule.identifier.trim() : "";
    if (raw && existing.has(raw) && !seen.has(raw)) {
      seen.add(raw);
      out.push(raw);
    }
  }
  return out;
}

export interface ResolvedRuleImportEntry {
  rule: ImportedRule;
  identifier: string;
}

// A rule whose identifier is in `skipIdentifiers` is left out entirely, as if
// absent from the file. Shared by importRulesBatch and ImportConflictModal's
// live preview so the preview can never diverge from the real import.
export function resolveImportIdentifiers(
  rules: ImportedRule[],
  existingRuleIdentifiers: string[],
  skipIdentifiers: ReadonlySet<string> = new Set(),
): ResolvedRuleImportEntry[] {
  const taken = new Set(existingRuleIdentifiers);
  const resolved: ResolvedRuleImportEntry[] = [];
  for (let i = 0; i < rules.length; i++) {
    const original = typeof rules[i].identifier === "string" ? rules[i].identifier.trim() : "";
    if (original && skipIdentifiers.has(original)) continue;
    resolved.push({ rule: rules[i], identifier: resolveRuleIdentifier(rules[i], i + 1, taken) });
  }
  return resolved;
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

// Mirrors the backend's save-time existence check (identifier presence only,
// any geometry type). Empty result means all area references resolve.
export function missingAreaReferences(
  rule: ImportedRule,
  existingAreaIdentifiers: Set<string>,
): string[] {
  const values = conditionsOf(rule)
    .filter((c) => c.type === "area" && typeof c.value === "string")
    .map((c) => c.value as string);
  return [...new Set(values.filter((v) => !existingAreaIdentifiers.has(v)))];
}

// Remapped through the batch's original->resolved map so a self-consistent
// file still links up even after an auto-suffixed identifier.
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
  // These are read-time-computed display stats the backend adds to GET responses;
  // an imported file may carry them, but they must never be sent back on create.
  const { triggered_lifetime: _triggeredLifetime, triggered_last_30_days: _triggeredLast30Days, ...rest } = rule;
  return {
    ...rest,
    identifier,
    conditions: sortable ? sortConditions(conditions as unknown as Condition[]) : conditions,
  };
}

export interface RuleImportResult {
  created: string[];
  // Rejected in-memory before any backend write: referential integrity within
  // the batch can't be satisfied (missing area / missing rule ref).
  rejected: { identifier: string; reason: string }[];
  // Passed validation but the backend still refused it on create. Expected rare.
  failed: { identifier: string; reason: string }[];
}

// Phase 1 (in-memory, no backend writes) resolves identifiers, rejects rules with
// missing area or matched_rules references (as a transitive closure, so a
// legitimate cyclic pair A->B/B->A can still both import), then phase 2 creates
// only the survivors -- nothing is ever created and then deleted.
export async function importRulesBatch(
  rules: ImportedRule[],
  existingRuleIdentifiers: string[],
  existingAreaIdentifiers: string[],
  createOne: (payload: Record<string, unknown>) => Promise<void>,
  skipIdentifiers: ReadonlySet<string> = new Set(),
): Promise<RuleImportResult> {
  const areaSet = new Set(existingAreaIdentifiers);
  const remap = new Map<string, string>();

  const entries = resolveImportIdentifiers(rules, existingRuleIdentifiers, skipIdentifiers);
  const identifiers = entries.map((e) => e.identifier);
  for (const { rule, identifier } of entries) {
    const original = typeof rule.identifier === "string" ? rule.identifier.trim() : "";
    if (original) remap.set(original, identifier);
  }

  // index (into `entries`) -> rejection reason; absent means still a candidate.
  const rejectionReason = new Map<number, string>();

  for (let i = 0; i < entries.length; i++) {
    const missing = missingAreaReferences(entries[i].rule, areaSet);
    if (missing.length > 0) {
      rejectionReason.set(i, `references missing area(s): ${missing.join(", ")}`);
    }
  }

  // Rejecting a rule can strand another that referenced it, so loop until a
  // pass changes nothing (a fixed point) -- this also lets a valid cyclic
  // pair (A->B, B->A) import together.
  let changed = true;
  while (changed) {
    changed = false;
    const valid = new Set(existingRuleIdentifiers);
    for (let i = 0; i < entries.length; i++) {
      if (!rejectionReason.has(i)) valid.add(identifiers[i]);
    }
    for (let i = 0; i < entries.length; i++) {
      if (rejectionReason.has(i)) continue;
      const refs = remappedMatchedRuleRefs(entries[i].rule, remap);
      const dangling = [...new Set(refs.filter((r) => !valid.has(r)))];
      if (dangling.length > 0) {
        rejectionReason.set(i, `references missing rule(s): ${dangling.join(", ")}`);
        changed = true;
      }
    }
  }

  const rejected: RuleImportResult["rejected"] = [];
  const toCreate: { payload: Record<string, unknown>; identifier: string }[] = [];
  for (let i = 0; i < entries.length; i++) {
    const reason = rejectionReason.get(i);
    if (reason) {
      rejected.push({ identifier: identifiers[i], reason });
    } else {
      toCreate.push({
        payload: buildPayload(entries[i].rule, identifiers[i], remap),
        identifier: identifiers[i],
      });
    }
  }

  const created: string[] = [];
  const failed: RuleImportResult["failed"] = [];
  for (const { payload, identifier } of toCreate) {
    try {
      await createOne(payload);
      created.push(identifier);
    } catch (err) {
      failed.push({ identifier, reason: err instanceof Error ? err.message : "backend rejected the rule" });
    }
  }

  return { created, rejected, failed };
}
