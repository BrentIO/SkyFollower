import { apiClient } from "./client";

// Mirrors management-ui/backend/main.py's Archive* Pydantic models.
export type ArchiveSearchStatus = "RUNNING" | "COMPLETE" | "FAILED" | "ABORTED";

export interface ArchiveSearchSummary {
  uuid: string;
  name: string;
  status: ArchiveSearchStatus;
  submitted_at: string;
  expires_at: string;
  // Set only for FAILED (Athena's reason) or ABORTED (backend deadline/restart).
  error: string | null;
}

export interface ArchiveSearchDetail extends ArchiveSearchSummary {
  where_clause: string;
  // The resolved range actually queried, UTC YYYY-MM-DD. Null if unset or
  // written before this field existed.
  start_date: string | null;
  end_date: string | null;
  // What the operator typed, before backend substitution; null if blank or
  // written before this field existed.
  requested_start_date: string | null;
  requested_end_date: string | null;
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
  // Opaque, encrypted, never the real S3 key; valid only for the lifetime of
  // the backend process that minted it.
  token: string;
}

// Mirrors main.py's FlightView response model. A missing optional field means
// "omit this", not "show a placeholder".
export interface FlightView {
  ident?: string;
  registration?: string;
  icao_hex: string;
  country?: string;
  country_code?: string;
  squawk?: string;
  military?: boolean;
  type_designator?: string;
  manufacturer_model?: string;
  description_code?: string;
  category?: string;
  aircraft_type?: string;
  model?: string;
  serial_number?: string;
  manufactured_date?: string;
  seats?: number;
  powerplant?: { type?: string; count?: number; manufacturer?: string; model?: string; power_type?: string };
  operator?: { name?: string; callsign?: string; airline_designator?: string; country?: string };
  registrant?: {
    names?: string[];
    street?: string[];
    city?: string;
    administrative_area?: string;
    postal_code?: string;
    country?: string;
  };
  origin?: FlightViewAirport;
  destination?: FlightViewAirport;
  first_message: string;
  last_message: string;
  total_messages: number;
  matched_rules: string[];
  receiver_sources: string[];
  // A GeoJSON LineString Feature, or absent when the flight has fewer than
  // 2 positions.
  flight_path?: {
    type: "Feature";
    geometry: { type: "LineString"; coordinates: (number[])[] };
    // coordTimes: epoch seconds per coordinate. coordSpeeds: knots,
    // nearest-matched/interpolated; absent on archives older than this field.
    properties: { coordTimes?: (number | null)[]; coordSpeeds?: (number | null)[] };
  };
}

export interface FlightViewAirport {
  icao_code: string;
  iata_code?: string;
  name?: string;
  city?: string;
  region?: string;
  country?: string;
}

export interface ArchiveSearchResultsPage {
  rows: ArchiveSearchResultRow[];
  // Exact when `truncated` is false; otherwise the backend's cache cap, not
  // the real match count.
  total_rows: number;
  // True when more matched than the cached window; use Download for the full set.
  truncated: boolean;
}

// Mirrors main.py's _SORTABLE_COLUMNS.
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

// startDate/endDate are UTC YYYY-MM-DD, or undefined/"" for "all time" --
// the backend resolves an omitted bound to the full archive range.
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

// A plain URL, not a fetch() helper: the endpoint 307s to a presigned S3 URL,
// and a real browser navigation follows it without needing the bucket to have
// its own CORS policy (fetch() would need to read the cross-origin body).
export function archiveSearchDownloadUrl(uuid: string): string {
  return `/api/archive/search/${encodeURIComponent(uuid)}/download`;
}

// Uses fetch + Blob rather than plain navigation so an expired/invalid token's
// 400 can be caught and surfaced as a toast, not a blank browser error page.
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

export function getFlightView(token: string): Promise<FlightView> {
  return apiClient.get<FlightView>(`/api/archive/flights/${encodeURIComponent(token)}/view`);
}

export async function downloadFlightPath(token: string): Promise<void> {
  const { blob, filename } = await apiClient.download(
    `/api/archive/flights/${encodeURIComponent(token)}/flight-path`,
  );
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename ?? "flight-path.geojson";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}
