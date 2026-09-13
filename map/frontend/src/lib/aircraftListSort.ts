// Pure sort-state/comparator logic for the aircraft list flyout (see
// components/AircraftListPanel.tsx). Mirrors management-ui/frontend's
// lib/resultsSort.ts (same column-click toggle convention -- see
// HistoryView.tsx's SortableColumnHeader), but this is a plain client-side
// array sort over the live in-memory AircraftMap rather than a
// server-paginated one, so there's no "unsorted server order" null state
// to preserve between clicks -- callers seed this with a real default
// (Distance ascending) rather than null.

export type SortDirection = "asc" | "desc";

export interface AircraftListSortState {
  columnKey: string;
  dir: SortDirection;
}

// Clicking a column header: the same column toggles direction, a
// different column switches to it ascending.
export function nextAircraftListSortState(
  current: AircraftListSortState,
  columnKey: string,
): AircraftListSortState {
  if (current.columnKey === columnKey) {
    return { columnKey, dir: current.dir === "asc" ? "desc" : "asc" };
  }
  return { columnKey, dir: "asc" };
}

export type SortValue = string | number | null;

// A field with no known value (blank -- this codebase's "omit, never
// N/A/?" convention, see lib/infoBox.ts's doc comments) always sorts last,
// regardless of direction, rather than flipping to the front on a
// descending sort.
function compareSortValues(a: SortValue, b: SortValue, dir: SortDirection): number {
  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;
  const cmp = typeof a === "number" && typeof b === "number" ? a - b : String(a).localeCompare(String(b));
  return dir === "asc" ? cmp : -cmp;
}

// Stable sort of `rows` by `sortKey(row)`, in `dir` order -- a fresh array,
// never mutating `rows` in place (callers hold the unsorted row list as
// the source of truth and re-derive this on every sort-state/data change).
export function sortAircraftListRows<T>(rows: T[], sortKey: (row: T) => SortValue, dir: SortDirection): T[] {
  return [...rows].sort((a, b) => compareSortValues(sortKey(a), sortKey(b), dir));
}
