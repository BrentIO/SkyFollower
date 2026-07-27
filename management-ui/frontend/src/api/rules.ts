import { apiClient } from "./client";

// Mirrors management-ui/backend/main.py's Condition/Rule Pydantic models
// (see also CLAUDE.md's Conditions table). `value` is a string for every
// type except `matched_rules` (a list of rule identifiers) and
// `receiver_source` (a list of 1-2 of "1090"/"978"/"MLAT") -- matching
// SkyFollower-legacy's convention (altitude "10000", military "true",
// heading "340,020" for min,max wrap-around).
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

// Display labels for the operator dropdown -- `in_list`/`not_in_list` only
// ever apply to `matched_rules` (see OPERATORS_BY_TYPE below), where
// "includes"/"excludes" reads far better than the raw wire values.
export const OPERATOR_LABELS: Record<Operator, string> = {
  equals: "equals",
  minimum: "minimum",
  maximum: "maximum",
  in_list: "includes",
  not_in_list: "excludes",
};

// Which operators are valid for each condition type -- mirrors CLAUDE.md's
// Conditions table (aircraft_powerplant_count allows `equals` too, unlike
// the other numeric range fields -- message-processor/rules_engine.py's
// _validate_aircraft_powerplant_count imposes no operator restriction of
// its own). Server-side `400` is still authoritative; this only drives
// which choices the operator dropdown offers.
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

export const WAKE_TURBULENCE_CATEGORIES = [
  "light",
  "medium",
  "medium 1",
  "medium 2",
  "high vortex aircraft",
  "heavy",
  "super",
  "rotorcraft",
  "high performance",
] as const;

export type WakeTurbulenceCategory = (typeof WAKE_TURBULENCE_CATEGORIES)[number];

export interface Condition {
  // "" only ever appears transiently client-side, for a newly-added
  // condition row that hasn't had a type chosen yet (see RuleForm.tsx's
  // newCondition()) -- validateRule() rejects it before a save can reach
  // the API, so a Condition actually sent over the wire always has a real
  // ConditionType.
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
