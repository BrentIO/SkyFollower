// A country's flag emoji is its ISO 3166-1 alpha-2 code re-encoded as a pair
// of Unicode Regional Indicator Symbols, so no lookup table is needed.

const REGIONAL_INDICATOR_OFFSET = "🇦".codePointAt(0)! - "A".codePointAt(0)!;

/** Flag emoji for a 2-letter ISO 3166-1 alpha-2 code, or null if invalid
 * (callers prefer omitting the flag over throwing). */
export function countryFlag(isoCode: string): string | null {
  const code = isoCode.toUpperCase();
  if (code.length !== 2 || !/^[A-Z]{2}$/.test(code)) {
    return null;
  }
  return Array.from(code)
    .map((c) => String.fromCodePoint(c.codePointAt(0)! + REGIONAL_INDICATOR_OFFSET))
    .join("");
}
