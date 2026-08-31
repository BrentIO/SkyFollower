import { describe, expect, it } from "vitest";
import {
  importRulesBatch,
  missingAreaReferences,
  parseAndValidate,
  resolveRuleIdentifier,
  type ImportedRule,
} from "./ruleImport";

function rule(identifier: string, conditions: unknown[] = []): ImportedRule {
  return { identifier, enabled: true, name: "", description: "", conditions } as ImportedRule;
}

describe("parseAndValidate", () => {
  it("blank input is the pristine no-op state", () => {
    expect(parseAndValidate("")).toEqual({ error: null, rules: [] });
  });

  it("rejects non-JSON, non-array root, empty array, and a non-object element", () => {
    expect(parseAndValidate("{oops").error).toBe("Not valid JSON.");
    expect(parseAndValidate('{"identifier":"a"}').error).toBe("Expected a JSON array of rules.");
    expect(parseAndValidate("[]").error).toBe("The file contains no rules.");
    expect(parseAndValidate("[1]").error).toBe("Rule 1: not a JSON object.");
    expect(parseAndValidate('[{"name":"x"}]').error).toBe('Rule 1: missing or empty "identifier".');
  });

  it("accepts an array of objects each with an identifier", () => {
    const result = parseAndValidate('[{"identifier":"a"},{"identifier":"b"}]');
    expect(result.error).toBeNull();
    expect(result.rules.map((r) => r.identifier)).toEqual(["a", "b"]);
  });
});

describe("resolveRuleIdentifier", () => {
  it("passes a clean unused identifier through", () => {
    expect(resolveRuleIdentifier(rule("ramp"), 1, new Set())).toBe("ramp");
  });

  it("auto-suffixes a collision (existing or earlier in the batch)", () => {
    const taken = new Set(["ramp"]);
    expect(resolveRuleIdentifier(rule("ramp"), 1, taken)).toBe("ramp_2");
    expect(resolveRuleIdentifier(rule("ramp"), 2, taken)).toBe("ramp_3");
  });

  it("synthesizes for a missing/invalid identifier", () => {
    expect(resolveRuleIdentifier({ identifier: "has space" } as ImportedRule, 4, new Set())).toBe(
      "imported_rule_4",
    );
  });
});

describe("missingAreaReferences", () => {
  it("returns area condition values not in the existing set", () => {
    const r = rule("r", [
      { type: "area", operator: "equals", value: "zone_a" },
      { type: "area", operator: "equals", value: "zone_missing" },
      { type: "altitude", operator: "minimum", value: "1000" },
    ]);
    expect(missingAreaReferences(r, new Set(["zone_a"]))).toEqual(["zone_missing"]);
    expect(missingAreaReferences(r, new Set(["zone_a", "zone_missing"]))).toEqual([]);
  });
});

describe("importRulesBatch", () => {
  const noDelete = async () => {};

  it("creates every rule, auto-suffixing identifier collisions", async () => {
    const created: string[] = [];
    const result = await importRulesBatch(
      [rule("a"), rule("a"), rule("b")],
      ["b"],
      [],
      async (p) => {
        created.push(p.identifier as string);
      },
      noDelete,
    );
    expect(created).toEqual(["a", "a_2", "b_2"]);
    expect(result.created).toEqual(["a", "a_2", "b_2"]);
    expect(result.failed).toEqual([]);
  });

  it("skips (never attempts) a rule referencing a missing area", async () => {
    const attempted: string[] = [];
    const result = await importRulesBatch(
      [rule("r", [{ type: "area", operator: "equals", value: "nope" }])],
      [],
      ["zone_a"],
      async (p) => {
        attempted.push(p.identifier as string);
      },
      noDelete,
    );
    expect(attempted).toEqual([]);
    expect(result.skipped).toEqual([{ identifier: "r", missingAreas: ["nope"] }]);
    expect(result.created).toEqual([]);
  });

  it("a per-rule backend rejection doesn't stop the batch", async () => {
    const result = await importRulesBatch(
      [rule("a"), rule("b"), rule("c")],
      [],
      [],
      async (p) => {
        if (p.identifier === "b") throw new Error("400 bad condition");
      },
      noDelete,
    );
    expect(result.created).toEqual(["a", "c"]);
    expect(result.failed.map((f) => f.identifier)).toEqual(["b"]);
  });

  it("keeps a matched_rules cycle within the batch (neither ejected)", async () => {
    const deleted: string[] = [];
    const result = await importRulesBatch(
      [
        rule("a", [{ type: "matched_rules", operator: "in_list", value: ["b"] }]),
        rule("b", [{ type: "matched_rules", operator: "in_list", value: ["a"] }]),
      ],
      [],
      [],
      async () => {},
      async (id) => {
        deleted.push(id);
      },
    );
    expect(result.created.sort()).toEqual(["a", "b"]);
    expect(deleted).toEqual([]);
  });

  it("ejects a rule whose matched_rules reference is missing, transitively", async () => {
    const deleted: string[] = [];
    // c -> d (d missing/skipped), e -> c. Ejecting c must then eject e.
    const result = await importRulesBatch(
      [
        rule("c", [{ type: "matched_rules", operator: "in_list", value: ["d"] }]),
        rule("e", [{ type: "matched_rules", operator: "in_list", value: ["c"] }]),
      ],
      [],
      [],
      async () => {},
      async (id) => {
        deleted.push(id);
      },
    );
    expect(result.created).toEqual([]);
    expect(deleted.sort()).toEqual(["c", "e"]);
    expect(result.failed.map((f) => f.identifier).sort()).toEqual(["c", "e"]);
  });

  it("remaps matched_rules references through auto-suffixed identifiers", async () => {
    const payloads: Record<string, unknown>[] = [];
    // Both identifiers collide with existing rules -> a_2, b_2. The
    // in-file references must follow.
    await importRulesBatch(
      [
        rule("a", [{ type: "matched_rules", operator: "in_list", value: ["b"] }]),
        rule("b", [{ type: "matched_rules", operator: "in_list", value: ["a"] }]),
      ],
      ["a", "b"],
      [],
      async (p) => {
        payloads.push(p);
      },
      async () => {},
    );
    const a2 = payloads.find((p) => p.identifier === "a_2")!;
    expect((a2.conditions as { value: string[] }[])[0].value).toEqual(["b_2"]);
  });
});
