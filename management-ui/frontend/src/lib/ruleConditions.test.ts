import { describe, expect, it } from "vitest";
import type { Condition } from "../api/rules";
import { sortConditions } from "./ruleConditions";

function c(type: string, operator: string, value: string | string[]): Condition {
  return { type, operator, value } as Condition;
}

describe("sortConditions", () => {
  it("sorts by type, then operator, then value", () => {
    const input = [
      c("velocity", "maximum", "500"),
      c("altitude", "minimum", "1000"),
      c("altitude", "maximum", "5000"),
      c("altitude", "minimum", "500"),
    ];
    // value is a plain string compare, not numeric -- "1000" < "500".
    expect(sortConditions(input).map((x) => [x.type, x.operator, x.value])).toEqual([
      ["altitude", "maximum", "5000"],
      ["altitude", "minimum", "1000"],
      ["altitude", "minimum", "500"],
      ["velocity", "maximum", "500"],
    ]);
  });

  it("compares array-valued types (matched_rules, receiver_source) by comma-joined value", () => {
    const input = [
      c("receiver_source", "equals", ["978", "1090"]),
      c("receiver_source", "equals", ["1090", "978"]),
      c("matched_rules", "in_list", ["b", "a"]),
      c("matched_rules", "in_list", ["a", "b"]),
    ];
    expect(sortConditions(input).map((x) => x.value)).toEqual([
      ["a", "b"],
      ["b", "a"],
      ["1090", "978"],
      ["978", "1090"],
    ]);
  });

  it("is stable for identical (type, operator, value) tuples", () => {
    const first = c("ident", "equals", "DAL1");
    const second = c("ident", "equals", "DAL1");
    const sorted = sortConditions([first, second]);
    expect(sorted[0]).toBe(first);
    expect(sorted[1]).toBe(second);
  });

  it("does not mutate the input array", () => {
    const input = [c("velocity", "minimum", "100"), c("altitude", "minimum", "100")];
    sortConditions(input);
    expect(input[0].type).toBe("velocity");
  });

  it("handles an empty array", () => {
    expect(sortConditions([])).toEqual([]);
  });
});
