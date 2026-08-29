// Client-side classification for the /lookup search field. Given a single
// free-text query, decide which of the four reference-data categories
// (aircraft-by-hex, aircraft-by-registration, operator, airport, route) it
// could plausibly belong to, so LookupView fires only the matching backend
// lookups instead of making the operator pick a category first, or blindly
// trying all four.
//
// Each predicate is deliberately shape-based, not a lookup against real
// data: a string either looks like an ICAO hex / a registration / a
// designator / an airport code / a flight ident, or it doesn't. A shape
// match that then 404s at the endpoint just means "not in Redis," and is
// handled by the caller, not here.

// 6 hex digits -> icao_hex. Unchanged from the previous single-field logic.
const HEX_PATTERN = /^[0-9A-Fa-f]{6}$/;

// Registration prefixes that, unlike the other ~44 country registry formats
// in this repo, carry no hyphen: the US (N), South Korea (HL), and Japan
// (JA -- resolvable via mictronics' global coverage even though no
// dedicated country runner backs it here). Any other undiscovered
// no-hyphen convention simply won't be offered as a registration
// candidate -- a documented limitation, not a wrong answer.
const NO_HYPHEN_REGISTRATION_PREFIX = /^(N|HL|JA)/i;

const HAS_DIGIT = /\d/;

// Alpha-only, 2-3 characters -- the codebase's existing precedent for
// "looks like an ICAO/IATA-style airline designator."
const OPERATOR_PATTERN = /^[A-Za-z]{2,3}$/;

// Alpha-only, exactly 3 (IATA) or exactly 4 (ICAO) characters. Alpha-only
// is stricter than the backend's own /api/airports/{code}, deliberately:
// it keeps a route ident like "DAL2" (letters + a digit) from also being
// tried as a 4-character airport code.
const AIRPORT_PATTERN = /^[A-Za-z]{3,4}$/;

// 3 letters followed by one or more digits, e.g. "DAL2".
const ROUTE_PATTERN = /^[A-Za-z]{3}\d+$/;

export function isHex(value: string): boolean {
  return HEX_PATTERN.test(value);
}

// Contains a hyphen, OR starts with N/HL/JA (case-insensitive) and contains
// at least one digit. The digit clause is load-bearing: without it, a bare
// alpha string like "JAX" would match here and collide with the alpha-only
// operator/airport categories below.
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

// The category tags LookupView acts on. "aircraft-hex" and
// "aircraft-registration" both resolve to the same /api/aircraft endpoint
// but with a different query parameter; they are mutually exclusive by
// construction (no string is both 6 hex-only characters and a
// hyphen/N/HL/JA+digit match).
export type LookupCategory =
  | "aircraft-hex"
  | "aircraft-registration"
  | "operator"
  | "airport"
  | "route";

// Returns every category whose shape the trimmed input matches, in a
// stable order (aircraft, operator, airport, route). An empty array means
// the input matches nothing and no network call should be made at all.
export function classifyLookup(raw: string): LookupCategory[] {
  const value = raw.trim();
  if (!value) return [];

  const categories: LookupCategory[] = [];

  if (isHex(value)) categories.push("aircraft-hex");
  else if (isRegistration(value)) categories.push("aircraft-registration");

  if (isOperator(value)) categories.push("operator");
  if (isAirport(value)) categories.push("airport");
  if (isRoute(value)) categories.push("route");

  return categories;
}
