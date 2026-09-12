// Pure helpers for reflecting the selected aircraft in the address bar
// (App.tsx/components/MapView.tsx's shareable-URL feature). Kept separate
// from the actual window.location/window.history calls so the
// query-string logic is covered by plain unit tests -- same pattern as
// lib/selection.ts and lib/followTarget.ts (this project has no jsdom/
// component-render test setup, see lib/config.test.ts's own note).
//
// No routing library is used -- a single optional query param is simple
// enough that a minimal URLSearchParams read/write covers it without
// pulling in react-router for one value.

const AIRCRAFT_PARAM = "aircraft";

// Reads the deep-linked aircraft's icao_hex out of a URL query string
// (e.g. `location.search`), or null if absent. Deliberately does no shape
// validation (hex-ness, length) -- a bogus identifier is handled uniformly
// downstream by simply never appearing in tracked state, per the issue's
// "fail silently" rule, so there's nothing extra to check for here.
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
