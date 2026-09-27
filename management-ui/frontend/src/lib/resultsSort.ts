import type { ArchiveSearchSortColumn, ArchiveSearchSortDir } from "../api/archiveSearch";

export interface ResultsSortState {
  column: ArchiveSearchSortColumn;
  dir: ArchiveSearchSortDir;
}

// `current` is null when unsorted (default server order).
export function nextSortState(current: ResultsSortState | null, clicked: ArchiveSearchSortColumn): ResultsSortState {
  if (current?.column === clicked) {
    return { column: clicked, dir: current.dir === "asc" ? "desc" : "asc" };
  }
  return { column: clicked, dir: "asc" };
}
