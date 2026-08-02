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
  total_rows: number;
}

export function createArchiveSearch(name: string, whereClause: string): Promise<{ uuid: string }> {
  return apiClient.post<{ uuid: string }>("/api/archive/search", { name, where_clause: whereClause });
}

export function listArchiveSearches(): Promise<ArchiveSearchSummary[]> {
  return apiClient.get<ArchiveSearchSummary[]>("/api/archive/search");
}

export function getArchiveSearchResults(uuid: string, page: number): Promise<ArchiveSearchResultsPage> {
  return apiClient.get<ArchiveSearchResultsPage>(
    `/api/archive/search/${encodeURIComponent(uuid)}/results?page=${page}`,
  );
}

export function deleteArchiveSearch(uuid: string): Promise<void> {
  return apiClient.delete(`/api/archive/search/${encodeURIComponent(uuid)}`);
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
