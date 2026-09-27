// Classifies a /lookup query into which reference-data categories it could plausibly
// belong to, so LookupView fires only the matching backend lookups. Each predicate is
// shape-based, not a real data lookup; a shape match that 404s is handled by the caller.

// 6 hex digits -> icao_hex.
const HEX_PATTERN = /^[0-9A-Fa-f]{6}$/;

// The only registration prefixes in this repo with no hyphen: US (N), South
// Korea (HL), Japan (JA). Any other no-hyphen convention won't be offered.
const NO_HYPHEN_REGISTRATION_PREFIX = /^(N|HL|JA)/i;

const HAS_DIGIT = /\d/;

// 2-3 chars, at least one letter -- real IATA codes often carry a digit
// ("5X", "9E"), but a bare number is never a plausible designator.
const OPERATOR_PATTERN = /^(?=.*[A-Za-z])[A-Za-z0-9]{2,3}$/;

// 3 (IATA) or 4 (ICAO) alphanumeric chars -- FAA-LID codes can carry digits
// ("KX14", "0S9"), so no alpha-only restriction.
const AIRPORT_PATTERN = /^[A-Za-z0-9]{3,4}$/;

// e.g. "DAL2", "AA100", "VIR92MC" -- matches shared/redis_keys.py's
// _FLIGHT_IDENT_PATTERN so frontend and backend agree.
const ROUTE_PATTERN = /^[A-Za-z]+\d+[A-Za-z]*$/;

export function isHex(value: string): boolean {
  return HEX_PATTERN.test(value);
}

// The digit requirement matters: without it, a bare alpha string like "JAX"
// would collide with the alpha-only operator/airport categories below.
export function isRegistration(value: string): boolean {
  if (value.includes("-")) return true;
  return NO_HYPHEN_REGISTRATION_PREFIX.test(value) && HAS_DIGIT.test(value);
}

export function isOperator(value: string): boolean {
  return OPERATOR_PATTERN.test(value);
}

export function isAirport(value: string): boolean {
  return AIRPORT_PATTERN.test(value);
}

export function isRoute(value: string): boolean {
  return ROUTE_PATTERN.test(value);
}

// "aircraft-hex" and "aircraft-registration" both resolve to /api/aircraft with a
// different query param; they're mutually exclusive by construction.
export type LookupCategory =
  | "aircraft-hex"
  | "aircraft-registration"
  | "operator"
  | "airport"
  | "route";

// Stable order: aircraft, operator, airport, route. Empty means nothing matched.
export function classifyLookup(raw: string): LookupCategory[] {
  const value = raw.trim();
  if (!value) return [];

  const categories: LookupCategory[] = [];

  const registration = !isHex(value) && isRegistration(value);
  if (isHex(value)) categories.push("aircraft-hex");
  else if (registration) categories.push("aircraft-registration");

  if (isOperator(value)) categories.push("operator");
  if (isAirport(value)) categories.push("airport");
  // Excluded to avoid also matching a no-hyphen registration like "N659DL".
  if (!registration && isRoute(value)) categories.push("route");

  return categories;
}

// Falls back to a route-only query when nothing matched, so an unanticipated
// shape still gets one cheap attempt instead of no network call.
export function categoriesToQuery(raw: string): LookupCategory[] {
  if (!raw.trim()) return [];
  const matched = classifyLookup(raw);
  return matched.length > 0 ? matched : ["route"];
}
