// Formats an aircraft's manufactured_date (ISO 8601 UTC datetime, e.g.
// "2019-01-01T00:00:00Z") for display on /lookup and /history.
//
// Most contributing registries only publish a 4-digit year, and the
// runner that ingests them synthesizes "{year}-01-01T00:00:00Z" -- so a
// January 1st value almost always means "year precision only" rather than
// an aircraft that genuinely rolled out of the factory on New Year's Day.
// Any time-of-day on January 1st counts as year-only (hours/minutes/
// seconds are ignored); this is a deliberate widening of "midnight exactly"
// to "any time that day", since the synthesized value's clock component
// carries no information either way.
//
// Computed in UTC (not local time) so the Jan-1 classification doesn't
// shift for viewers in a negative UTC offset, where a bare `Date.getMonth()`
// could roll a UTC Jan-1 timestamp back into "December 31" locally.
export function formatManufacturedDate(iso: string): string | undefined {
  if (!iso) return undefined;

  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return undefined;

  const year = date.getUTCFullYear();
  const month = date.getUTCMonth(); // 0-indexed
  const day = date.getUTCDate();

  if (month === 0 && day === 1) {
    return String(year);
  }

  const mm = String(month + 1).padStart(2, "0");
  const dd = String(day).padStart(2, "0");
  return `${year}-${mm}-${dd}`;
}
