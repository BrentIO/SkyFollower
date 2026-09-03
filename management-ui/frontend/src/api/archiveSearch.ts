import { apiClient } from "./client";

// Mirrors management-ui/backend/main.py's Archive* Pydantic models.
export type ArchiveSearchStatus = "RUNNING" | "COMPLETE" | "FAILED" | "ABORTED";

export interface ArchiveSearchSummary {
  uuid: string;
  name: string;
  status: ArchiveSearchStatus;
  submitted_at: string;
  expires_at: string;
  // Only ever set for FAILED (Athena's own reason) or ABORTED (this
  // backend's own deadline/restart message).
  error: string | null;
}

export interface ArchiveSearchDetail extends ArchiveSearchSummary {
  where_clause: string;
  // The RESOLVED range actually queried (explicit input intersected with
  // whatever the WHERE clause's own predicates could prove) -- UTC
  // calendar dates, YYYY-MM-DD. Null for a record written before this
  // field existed.
  start_date: string | null;
  end_date: string | null;
}

export interface ArchiveSearchResultRow {
  uuid: string;
  icao_hex: string;
  registration: string;
  type_designator: string;
  military: boolean;
  operator_designator: string;
  ident: string;
  first_message: string;
  last_message: string;
  // Opaque, encrypted -- never the real S3 key. Only valid for the
  // lifetime of the backend process that minted it (see
  // downloadArchiveFlight below).
  token: string;
}

export interface ArchiveSearchResultsPage {
  rows: ArchiveSearchResultRow[];
  // Exact whenever `truncated` is false; when true, this is the backend's
  // cache cap (see `truncated`), not the real (unread) match count.
  total_rows: number;
  // True when more than the cached window actually matched -- the exact
  // count beyond that is never computed. Use Download for the full set.
  truncated: boolean;
}

// Every column the results table lets a user sort by -- mirrors main.py's
// _SORTABLE_COLUMNS (everything ArchiveSearchResultRow exposes except the
// server-derived uuid/token).
export type ArchiveSearchSortColumn =
  | "icao_hex"
  | "registration"
  | "type_designator"
  | "military"
  | "operator_designator"
  | "ident"
  | "first_message"
  | "last_message";

export type ArchiveSearchSortDir = "asc" | "desc";

// startDate/endDate are UTC calendar dates (YYYY-MM-DD), or undefined/""
// for "all time" on that side -- the backend resolves an omitted bound to
// the full archive range (see ArchiveSearchDetail's start_date/end_date).
export function createArchiveSearch(
  name: string,
  whereClause: string,
  startDate?: string,
  endDate?: string,
): Promise<{ uuid: string }> {
  return apiClient.post<{ uuid: string }>("/api/archive/search", {
    name,
    where_clause: whereClause,
    start_date: startDate || null,
    end_date: endDate || null,
  });
}

export function listArchiveSearches(): Promise<ArchiveSearchSummary[]> {
  return apiClient.get<ArchiveSearchSummary[]>("/api/archive/search");
}

export function getArchiveSearchDetail(uuid: string): Promise<ArchiveSearchDetail> {
  return apiClient.get<ArchiveSearchDetail>(`/api/archive/search/${encodeURIComponent(uuid)}`);
}

export function getArchiveSearchResults(
  uuid: string,
  page: number,
  pageSize?: number,
  sortBy?: ArchiveSearchSortColumn,
  sortDir?: ArchiveSearchSortDir,
): Promise<ArchiveSearchResultsPage> {
  const params = new URLSearchParams({ page: String(page) });
  if (pageSize !== undefined) params.set("page_size", String(pageSize));
  if (sortBy !== undefined) {
    params.set("sort_by", sortBy);
    params.set("sort_dir", sortDir ?? "asc");
  }
  return apiClient.get<ArchiveSearchResultsPage>(
    `/api/archive/search/${encodeURIComponent(uuid)}/results?${params.toString()}`,
  );
}

export function deleteArchiveSearch(uuid: string): Promise<void> {
  return apiClient.delete(`/api/archive/search/${encodeURIComponent(uuid)}`);
}

// A plain URL, not a fetch() helper -- the endpoint 307s to a presigned S3
// URL, and following that via a real browser navigation (an <a href>/
// window.open, not fetch()) avoids the S3 bucket needing its own CORS
// policy just for this: fetch() would need to read a cross-origin
// response's body, a plain navigation doesn't. Works for every result
// size, no threshold -- the backend never reads the bytes either way.
export function archiveSearchDownloadUrl(uuid: string): string {
  return `/api/archive/search/${encodeURIComponent(uuid)}/download`;
}

// Downloads via fetch + Blob rather than a plain navigation/window.open --
// an expired/invalid token 400s (see main.py's get_archive_flight), and
// only a real fetch() lets the caller catch that and surface it as a toast
// instead of the browser just showing a blank/JSON error page.
export async function downloadArchiveFlight(token: string): Promise<void> {
  const { blob, filename } = await apiClient.download(`/api/archive/flights/${encodeURIComponent(token)}`);
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename ?? "flight.json.gz";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
