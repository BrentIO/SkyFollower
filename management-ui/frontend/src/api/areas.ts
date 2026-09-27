import { apiClient } from "./client";

// Mirrors management-ui/backend/main.py's Area/AreaGeometry Pydantic models.
export interface PolygonGeometry {
  type: "Polygon";
  coordinates: number[][][];
}

export interface LineStringGeometry {
  type: "LineString";
  coordinates: number[][];
}

export interface PointGeometry {
  type: "Point";
  coordinates: number[];
}

export type AreaGeometry = PolygonGeometry | LineStringGeometry | PointGeometry;

export interface Area {
  identifier: string;
  name: string;
  geometry: AreaGeometry;
  // Prevents drag/vertex editing on the map only; doesn't restrict name edits
  // or deletion. Toggling this saves immediately, bypassing the dirty/Save flow.
  locked: boolean;
  // simplestyle-spec keys (hyphenated, matching the backend's field aliases).
  // fill/fill-opacity: Polygon. stroke*: Polygon and LineString. marker-*: Point.
  fill?: string;
  "fill-opacity"?: number;
  stroke?: string;
  "stroke-width"?: number;
  "stroke-opacity"?: number;
  "marker-color"?: string;
  "marker-size"?: "small" | "medium" | "large";
  "marker-symbol"?: string;
}

// Shared by the naming modal and success toasts for consistent language.
export function geometryDisplayNoun(type: AreaGeometry["type"]): "Area" | "Line" | "Point" {
  switch (type) {
    case "Polygon":
      return "Area";
    case "LineString":
      return "Line";
    case "Point":
      return "Point";
  }
}

export function listAreas(): Promise<Area[]> {
  return apiClient.get<Area[]>("/api/areas");
}

export function createArea(area: Area): Promise<Area> {
  return apiClient.post<Area>("/api/areas", area);
}

export function updateArea(identifier: string, area: Area): Promise<Area> {
  return apiClient.put<Area>(`/api/areas/${encodeURIComponent(identifier)}`, area);
}

export function deleteArea(identifier: string): Promise<void> {
  return apiClient.delete(`/api/areas/${encodeURIComponent(identifier)}`);
}
