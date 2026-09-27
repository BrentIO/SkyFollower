import { apiClient } from "./client";

// Mirrors management-ui/backend/main.py's Condition/Rule Pydantic models (see
// CLAUDE.md's Conditions table). `value` is a string for every type except
// `matched_rules` and `receiver_source`, which are string lists.
export const CONDITION_TYPES = [
  "altitude",
  "heading",
  "velocity",
  "vertical_speed",
  "area",
  "date",
  "ident",
  "squawk",
  "military",
  "receiver_source",
  "operator_airline_designator",
  "aircraft_type_designator",
  "aircraft_registration",
  "aircraft_icao_hex",
  "aircraft_powerplant_count",
  "wake_turbulence_category",
  "matched_rules",
] as const;

export type ConditionType = (typeof CONDITION_TYPES)[number];

export const OPERATORS = ["equals", "minimum", "maximum", "in_list", "not_in_list"] as const;

export type Operator = (typeof OPERATORS)[number];

// `in_list`/`not_in_list` only ever apply to `matched_rules`, where
// "includes"/"excludes" reads better than the raw wire values.
export const OPERATOR_LABELS: Record<Operator, string> = {
  equals: "equals",
  minimum: "minimum",
  maximum: "maximum",
  in_list: "includes",
  not_in_list: "excludes",
};

// Mirrors CLAUDE.md's Conditions table; drives the operator dropdown only --
// the backend's 400 is still authoritative. aircraft_powerplant_count allows
// `equals` too, unlike the other numeric range fields.
export const OPERATORS_BY_TYPE: Record<ConditionType, readonly Operator[]> = {
  altitude: ["minimum", "maximum"],
  velocity: ["minimum", "maximum"],
  vertical_speed: ["minimum", "maximum"],
  aircraft_powerplant_count: ["equals", "minimum", "maximum"],
  heading: ["equals"],
  date: ["minimum", "maximum"],
  ident: ["equals"],
  squawk: ["equals"],
  military: ["equals"],
  receiver_source: ["equals"],
  operator_airline_designator: ["equals"],
  aircraft_type_designator: ["equals"],
  aircraft_registration: ["equals"],
  aircraft_icao_hex: ["equals"],
  wake_turbulence_category: ["equals"],
  area: ["equals"],
  matched_rules: ["in_list", "not_in_list"],
};

export const WAKE_TURBULENCE_CATEGORIES = ["light", "medium", "heavy"] as const;

export type WakeTurbulenceCategory = (typeof WAKE_TURBULENCE_CATEGORIES)[number];

export interface Condition {
  // "" is a transient client-side state for a newly-added row with no type
  // chosen yet; validateRule() rejects it before a save reaches the API.
  type: ConditionType | "";
  operator: Operator;
  value: string | string[];
}

export interface Rule {
  name: string;
  description: string;
  identifier: string;
  enabled: boolean;
  force_archive: boolean;
  conditions: Condition[];
  // Response-only, computed fresh on every GET; never sent on create/update.
  triggered_lifetime?: number;
  triggered_last_30_days?: number;
}

export function emptyRule(): Rule {
  return {
    name: "",
    description: "",
    identifier: "",
    enabled: true,
    force_archive: false,
    conditions: [],
  };
}

export function listRules(): Promise<Rule[]> {
  return apiClient.get<Rule[]>("/api/rules");
}

export function getRule(identifier: string): Promise<Rule> {
  return apiClient.get<Rule>(`/api/rules/${encodeURIComponent(identifier)}`);
}

export function createRule(rule: Rule): Promise<Rule> {
  return apiClient.post<Rule>("/api/rules", rule);
}

export function updateRule(identifier: string, rule: Rule): Promise<Rule> {
  return apiClient.put<Rule>(`/api/rules/${encodeURIComponent(identifier)}`, rule);
}

export function deleteRule(identifier: string): Promise<void> {
  return apiClient.delete(`/api/rules/${encodeURIComponent(identifier)}`);
}
