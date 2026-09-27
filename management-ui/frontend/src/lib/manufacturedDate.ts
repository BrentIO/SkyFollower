// Most registries only publish a year; ingestion synthesizes "{year}-01-01T00:00:00Z",
// so any Jan-1 timestamp is treated as year-only precision. Computed in UTC so this
// classification doesn't shift for viewers in a negative UTC offset.
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
