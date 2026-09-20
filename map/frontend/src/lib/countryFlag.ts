// Ported verbatim (same 3-line regional-indicator-symbol trick, no lookup
// table) from shared/country_flags.py's country_flag() -- see #1848. The
// map's aircraft list panel renders the flag client-side from
// AircraftInfo.country_code so the WebSocket/REST payload never carries
// extra bytes for it.
//
// A country's flag emoji is simply its two-letter ISO 3166-1 alpha-2 code
// re-encoded as a pair of Unicode Regional Indicator Symbols, so no lookup
// table is needed -- any syntactically valid alpha-2 code works, including
// one not actually assigned to a country.

const REGIONAL_INDICATOR_OFFSET = "🇦".codePointAt(0)! - "A".codePointAt(0)!;

/** Returns the flag emoji for a 2-letter ISO 3166-1 alpha-2 country code,
 * or null if `isoCode` isn't a syntactically valid 2-letter alpha code
 * (unlike the Python original, which raises -- this port is called from
 * render code that would rather omit the flag than throw). */
export function countryFlag(isoCode: string): string | null {
  const code = isoCode.toUpperCase();
  if (code.length !== 2 || !/^[A-Z]{2}$/.test(code)) {
    return null;
  }
  return Array.from(code)
    .map((c) => String.fromCodePoint(c.codePointAt(0)! + REGIONAL_INDICATOR_OFFSET))
    .join("");
}
