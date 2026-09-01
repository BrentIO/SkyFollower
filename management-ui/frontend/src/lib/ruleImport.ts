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

// Per-identifier operator choice for an import conflict: keep only the
// existing rule (skip the imported duplicate entirely) or keep both (auto
// suffix the imported one). Drives ImportConflictModal.
export type ImportConflictChoice = "skip" | "rename";

// The distinct imported identifiers that already exist among
// `existingIdentifiers`, in file order, deduplicated. Feeds
// ImportConflictModal's row list -- an import with an empty result here
// proceeds straight through with no modal.
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

// Resolves every non-skipped rule's final identifier for the batch, honoring
// per-identifier skip choices for entries that collide with an existing
// rule. A rule whose trimmed identifier is in `skipIdentifiers` is left out
// of the result entirely -- resolveRuleIdentifier never sees it, and it
// never occupies a slot in `taken` -- exactly as if it were absent from the
// file. Every other rule resolves via resolveRuleIdentifier exactly as
// before skip/rename existed. Shared by importRulesBatch (the real import)
// and ImportConflictModal's live rename preview (identical inputs, identical
// output), so the preview can never diverge from what actually gets created.
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
  // Reached the backend and it accepted the rule.
  created: string[];
  // Decided up front, before any backend write: referential integrity
  // within the batch can't be satisfied (missing area / missing rule ref).
  rejected: { identifier: string; reason: string }[];
  // Passed phase-1 validation but the backend still refused it on create --
  // a genuine backend-only rejection client validation can't anticipate.
  // Expected to be rare.
  failed: { identifier: string; reason: string }[];
}

// Validate-then-commit batch import.
//
// Phase 1 (in-memory, zero backend writes) decides the entire outcome:
//   - Resolve every non-skipped rule's identifier for the whole batch (via
//     resolveImportIdentifiers), so the original->resolved remap is
//     complete before anything else looks at references. An identifier in
//     `skipIdentifiers` (the operator's per-conflict "Skip" choice from
//     ImportConflictModal) is excluded here -- resolveRuleIdentifier never
//     sees it, and it is never counted against created/failed/rejected.
//   - Area references: an `area` condition value must already exist in
//     `existingAreaIdentifiers` (areas are never part of a rules import).
//     A rule referencing a missing area is rejected with a reason.
//   - matched_rules references: each remapped reference must resolve to an
//     existing rule identifier or the resolved identifier of another
//     still-valid rule in this same batch. Computed as a transitive
//     closure -- rejecting a rule can leave a rule that referenced it
//     newly dangling -- iterating until stable. This is what lets a
//     legitimate cyclic pair (A->B, B->A) import: both are in the batch's
//     valid set, so neither dangles.
//
// Phase 2 creates only the survivors. Nothing is ever created and then
// deleted. A create that still fails is a backend-only rejection -> `failed`.
export async function importRulesBatch(
  rules: ImportedRule[],
  existingRuleIdentifiers: string[],
  existingAreaIdentifiers: string[],
  createOne: (payload: Record<string, unknown>) => Promise<void>,
  skipIdentifiers: ReadonlySet<string> = new Set(),
): Promise<RuleImportResult> {
  const areaSet = new Set(existingAreaIdentifiers);
  const remap = new Map<string, string>();

  // Phase 1a: resolve every non-skipped rule's identifier (so the remap is
  // complete before any reference or payload is evaluated).
  const entries = resolveImportIdentifiers(rules, existingRuleIdentifiers, skipIdentifiers);
  const identifiers = entries.map((e) => e.identifier);
  for (const { rule, identifier } of entries) {
    const original = typeof rule.identifier === "string" ? rule.identifier.trim() : "";
    if (original) remap.set(original, identifier);
  }

  // index (into `entries`) -> rejection reason. Absent = still a candidate
  // for creation.
  const rejectionReason = new Map<number, string>();

  // Phase 1b: area references.
  for (let i = 0; i < entries.length; i++) {
    const missing = missingAreaReferences(entries[i].rule, areaSet);
    if (missing.length > 0) {
      rejectionReason.set(i, `references missing area(s): ${missing.join(", ")}`);
    }
  }

  // Phase 1c: matched_rules transitive closure. A reference resolves if it
  // points at an existing rule or a still-valid batch rule; rejecting a
  // rule shrinks the valid set, so loop until a full pass changes nothing.
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

  // Phase 1 outcome, fully determined before any backend write.
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

  // Phase 2: commit the survivors.
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
