// Pure helpers for reflecting the selected aircraft in the address bar.
// Kept separate from the actual window.location/window.history calls so
// the query-string logic is unit-testable. No routing library is used -- a
// single optional query param is simple enough for a minimal
// URLSearchParams read/write.

const AIRCRAFT_PARAM = "aircraft";

// Reads the deep-linked aircraft's icao_hex out of a URL query string, or
// null if absent. Deliberately does no shape validation -- a bogus
// identifier is handled uniformly downstream by simply never appearing in
// tracked state.
export function readSelectionFromSearch(search: string): string | null {
  return new URLSearchParams(search).get(AIRCRAFT_PARAM);
}

// Returns the `search` string (including its leading "?", or "" when
// there are no params left) that reflects `icaoHex` being selected, or
// nothing selected for `null` -- starting from whatever other query
// params `currentSearch` already carries, so unrelated params round-trip
// untouched.
export function searchWithSelection(currentSearch: string, icaoHex: string | null): string {
  const params = new URLSearchParams(currentSearch);
  if (icaoHex) {
    params.set(AIRCRAFT_PARAM, icaoHex);
  } else {
    params.delete(AIRCRAFT_PARAM);
  }
  const next = params.toString();
  return next ? `?${next}` : "";
}
