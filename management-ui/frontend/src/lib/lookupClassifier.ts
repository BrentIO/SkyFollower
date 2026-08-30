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

// 2-3 characters, at least one a letter -- ICAO/IATA-style airline
// designator. /api/operators/{designator} does no shape check of its own,
// so this only needs to be loose enough not to miss real codes: real
// 2-char IATA codes routinely carry a digit ("5X", "9E", "0B"). The
// "at least one letter" clause keeps a bare 2-3 digit number, implausible
// as any designator, from triggering an operator lookup on every numeric
// guess.
const OPERATOR_PATTERN = /^(?=.*[A-Za-z])[A-Za-z0-9]{2,3}$/;

// 3 (IATA) or 4 (ICAO) alphanumeric characters. /api/airports/{code}
// branches purely on length -- 4 tries a direct ICAO-keyed lookup, 3 an
// IATA search -- with no alpha restriction, so real FAA-LID-derived codes
// with digits ("KX14", "0S9") must be allowed here too.
const AIRPORT_PATTERN = /^[A-Za-z0-9]{3,4}$/;

// One or more letters, one or more digits, then an optional trailing
// letter suffix -- e.g. "DAL2", "AA100", "VIR92MC". This is exactly
// shared/redis_keys.py's _FLIGHT_IDENT_PATTERN, the backend's own
// authoritative flight-ident shape, so the frontend guess and the
// backend's parsing agree.
const ROUTE_PATTERN = /^[A-Za-z]+\d+[A-Za-z]*$/;

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
// nothing matched -- the caller still tries a route lookup anyway (a
// flight ident is the shape most likely to have a form nobody anticipated,
// and the query is cheap and 404s silently if wrong).
export function classifyLookup(raw: string): LookupCategory[] {
  const value = raw.trim();
  if (!value) return [];

  const categories: LookupCategory[] = [];

  const registration = !isHex(value) && isRegistration(value);
  if (isHex(value)) categories.push("aircraft-hex");
  else if (registration) categories.push("aircraft-registration");

  if (isOperator(value)) categories.push("operator");
  if (isAirport(value)) categories.push("airport");
  // The relaxed route shape (letters + digits + optional trailing letters)
  // would otherwise also match a no-hyphen registration like "N659DL" or
  // "HL7771". A string that already looks like a registration isn't
  // plausibly a flight ident, so don't spend a lookup on it.
  if (!registration && isRoute(value)) categories.push("route");

  return categories;
}

// The categories LookupView actually queries for a non-empty input: every
// category classifyLookup matched, or -- when it matched nothing -- a
// route-only fallback, so a query with an unanticipated shape still gets
// one cheap, silently-404ing attempt instead of no network call at all.
// Empty/whitespace input returns [] (the caller guards against it anyway).
export function categoriesToQuery(raw: string): LookupCategory[] {
  if (!raw.trim()) return [];
  const matched = classifyLookup(raw);
  return matched.length > 0 ? matched : ["route"];
}
