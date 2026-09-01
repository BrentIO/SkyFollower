import { describe, expect, it } from "vitest";
import {
  collidingIdentifiers,
  importRulesBatch,
  missingAreaReferences,
  parseAndValidate,
  resolveImportIdentifiers,
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

describe("collidingIdentifiers", () => {
  it("returns the distinct imported identifiers that already exist, in file order", () => {
    expect(collidingIdentifiers([rule("a"), rule("b"), rule("c")], ["b", "c"])).toEqual(["b", "c"]);
  });

  it("dedupes a repeated colliding identifier", () => {
    expect(collidingIdentifiers([rule("a"), rule("a")], ["a"])).toEqual(["a"]);
  });

  it("returns nothing when no imported identifier collides", () => {
    expect(collidingIdentifiers([rule("a"), rule("b")], ["z"])).toEqual([]);
  });
});

describe("resolveImportIdentifiers", () => {
  it("excludes a skipped rule entirely -- never resolved, never occupies a taken slot", () => {
    const entries = resolveImportIdentifiers([rule("a"), rule("b")], ["a"], new Set(["a"]));
    expect(entries.map((e) => e.identifier)).toEqual(["b"]);
  });

  it("still auto-suffixes a non-skipped collision", () => {
    const entries = resolveImportIdentifiers([rule("a")], ["a"], new Set());
    expect(entries.map((e) => e.identifier)).toEqual(["a_2"]);
  });
});

describe("importRulesBatch", () => {
  it("creates every rule, auto-suffixing identifier collisions", async () => {
    const created: string[] = [];
    const result = await importRulesBatch(
      [rule("a"), rule("a"), rule("b")],
      ["b"],
      [],
      async (p) => {
        created.push(p.identifier as string);
      },
    );
    expect(created).toEqual(["a", "a_2", "b_2"]);
    expect(result.created).toEqual(["a", "a_2", "b_2"]);
    expect(result.rejected).toEqual([]);
    expect(result.failed).toEqual([]);
  });

  it("rejects a rule referencing a missing area before any backend write", async () => {
    const attempted: string[] = [];
    const result = await importRulesBatch(
      [rule("r", [{ type: "area", operator: "equals", value: "nope" }])],
      [],
      ["zone_a"],
      async (p) => {
        attempted.push(p.identifier as string);
      },
    );
    expect(attempted).toEqual([]);
    expect(result.rejected).toEqual([
      { identifier: "r", reason: "references missing area(s): nope" },
    ]);
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
    );
    expect(result.created).toEqual(["a", "c"]);
    expect(result.failed.map((f) => f.identifier)).toEqual(["b"]);
    expect(result.rejected).toEqual([]);
  });

  it("imports a batch-internal cyclic matched_rules pair with no create/delete round trip", async () => {
    const calls: string[] = [];
    const result = await importRulesBatch(
      [
        rule("a", [{ type: "matched_rules", operator: "in_list", value: ["b"] }]),
        rule("b", [{ type: "matched_rules", operator: "not_in_list", value: ["a"] }]),
      ],
      [],
      [],
      async (p) => {
        calls.push(`create:${p.identifier as string}`);
      },
    );
    expect(result.created.sort()).toEqual(["a", "b"]);
    expect(result.rejected).toEqual([]);
    expect(result.failed).toEqual([]);
    // Only creates, exactly one per rule -- nothing created then removed.
    expect(calls.sort()).toEqual(["create:a", "create:b"]);
  });

  it("rejects a rule referencing another rule that failed its own area check, transitively", async () => {
    const attempted: string[] = [];
    // b references a missing area -> rejected. a references b -> now dangling.
    const result = await importRulesBatch(
      [
        rule("a", [{ type: "matched_rules", operator: "in_list", value: ["b"] }]),
        rule("b", [{ type: "area", operator: "equals", value: "nope" }]),
      ],
      [],
      ["zone_a"],
      async (p) => {
        attempted.push(p.identifier as string);
      },
    );
    expect(attempted).toEqual([]);
    expect(result.created).toEqual([]);
    expect(result.rejected).toEqual([
      { identifier: "a", reason: "references missing rule(s): b" },
      { identifier: "b", reason: "references missing area(s): nope" },
    ]);
  });

  it("rejects a reference to an identifier absent from both existing rules and the batch", async () => {
    const attempted: string[] = [];
    const result = await importRulesBatch(
      [rule("c", [{ type: "matched_rules", operator: "in_list", value: ["ghost"] }])],
      ["existing_1"],
      [],
      async (p) => {
        attempted.push(p.identifier as string);
      },
    );
    expect(attempted).toEqual([]);
    expect(result.created).toEqual([]);
    expect(result.rejected).toEqual([
      { identifier: "c", reason: "references missing rule(s): ghost" },
    ]);
  });

  it("rejects a dangling matched_rules reference transitively, before any backend write", async () => {
    const attempted: string[] = [];
    // c -> d (absent), e -> c. Rejecting c must then reject e. Neither created.
    const result = await importRulesBatch(
      [
        rule("c", [{ type: "matched_rules", operator: "in_list", value: ["d"] }]),
        rule("e", [{ type: "matched_rules", operator: "in_list", value: ["c"] }]),
      ],
      [],
      [],
      async (p) => {
        attempted.push(p.identifier as string);
      },
    );
    expect(attempted).toEqual([]);
    expect(result.created).toEqual([]);
    expect(result.rejected.map((r) => r.identifier).sort()).toEqual(["c", "e"]);
    expect(result.rejected.find((r) => r.identifier === "c")!.reason).toBe(
      "references missing rule(s): d",
    );
    expect(result.rejected.find((r) => r.identifier === "e")!.reason).toBe(
      "references missing rule(s): c",
    );
  });

  it("a batch with a mix of skipped and renamed collisions: skipped never reach the create/resolve step", async () => {
    const attempted: string[] = [];
    const resolvedForSkipped: string[] = [];
    // "a" and "b" both collide with existing rules; "c" doesn't collide at
    // all. "a" is skipped, "b" is renamed.
    const result = await importRulesBatch(
      [rule("a"), rule("b"), rule("c")],
      ["a", "b"],
      [],
      async (p) => {
        attempted.push(p.identifier as string);
        if (p.identifier === "a" || p.identifier === "a_2") resolvedForSkipped.push(p.identifier as string);
      },
      new Set(["a"]),
    );
    // "a" is excluded entirely -- never attempted, under any identifier.
    expect(resolvedForSkipped).toEqual([]);
    expect(attempted).toEqual(["b_2", "c"]);
    expect(result.created).toEqual(["b_2", "c"]);
    expect(result.rejected).toEqual([]);
    expect(result.failed).toEqual([]);
  });

  it("skipping every colliding identifier is a no-op on those identifiers, non-colliding entries still import", async () => {
    const attempted: string[] = [];
    const result = await importRulesBatch(
      [rule("a"), rule("b"), rule("fresh")],
      ["a", "b"],
      [],
      async (p) => {
        attempted.push(p.identifier as string);
      },
      new Set(["a", "b"]),
    );
    expect(attempted).toEqual(["fresh"]);
    expect(result.created).toEqual(["fresh"]);
    expect(result.rejected).toEqual([]);
    expect(result.failed).toEqual([]);
  });

  it("remaps matched_rules references through auto-suffixed identifiers", async () => {
    const payloads: Record<string, unknown>[] = [];
    // Both identifiers collide with existing rules -> a_2, b_2. The
    // in-file references must follow, and the pair still validates.
    const result = await importRulesBatch(
      [
        rule("a", [{ type: "matched_rules", operator: "in_list", value: ["b"] }]),
        rule("b", [{ type: "matched_rules", operator: "in_list", value: ["a"] }]),
      ],
      ["a", "b"],
      [],
      async (p) => {
        payloads.push(p);
      },
    );
    expect(result.created.sort()).toEqual(["a_2", "b_2"]);
    expect(result.rejected).toEqual([]);
    const a2 = payloads.find((p) => p.identifier === "a_2")!;
    expect((a2.conditions as { value: string[] }[])[0].value).toEqual(["b_2"]);
  });

  it("never sends triggered_lifetime/triggered_last_30_days, even when the source rule carries them", async () => {
    const payloads: Record<string, unknown>[] = [];
    const withCounts = {
      ...rule("a"),
      triggered_lifetime: 42,
      triggered_last_30_days: 7,
    } as ImportedRule;
    const result = await importRulesBatch([withCounts], [], [], async (p) => {
      payloads.push(p);
    });
    expect(result.created).toEqual(["a"]);
    const payload = payloads[0];
    expect(payload).not.toHaveProperty("triggered_lifetime");
    expect(payload).not.toHaveProperty("triggered_last_30_days");
  });
});
