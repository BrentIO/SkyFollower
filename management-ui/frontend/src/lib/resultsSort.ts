import type { ArchiveSearchSortColumn, ArchiveSearchSortDir } from "../api/archiveSearch";

export interface ResultsSortState {
  column: ArchiveSearchSortColumn;
  dir: ArchiveSearchSortDir;
}

// Clicking a column header: the same column toggles direction, a different
// column switches to it ascending. `current` is null when no sort has been
// applied yet (the table's default, unsorted server order).
export function nextSortState(current: ResultsSortState | null, clicked: ArchiveSearchSortColumn): ResultsSortState {
  if (current?.column === clicked) {
    return { column: clicked, dir: current.dir === "asc" ? "desc" : "asc" };
  }
  return { column: clicked, dir: "asc" };
}
