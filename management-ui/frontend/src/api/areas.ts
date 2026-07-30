import { apiClient } from "./client";

// Mirrors management-ui/backend/main.py's Area/AreaGeometry Pydantic models.
// `identifier` is the routing key (no spaces, used by /api/areas/{identifier}
// and matched against by a rule's `area` condition -- Polygon areas only,
// see PolygonGeometry below); `name` is a separate, optional free-text
// display label that can contain spaces.
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
  // Prevents drag/vertex editing on the map while true; does not restrict
  // name edits or deletion. Toggling this saves immediately rather than
  // going through the dirty/Save flow -- see AreasView.tsx's toggleLock.
  locked: boolean;
  // simplestyle-spec (https://github.com/mapbox/simplestyle-spec) style
  // properties, matching management-ui/backend/main.py's Area field
  // aliases exactly (hyphenated key names, not the backend's underscored
  // Python attribute names). All optional -- an area with none of them
  // set renders with AreasView.tsx's default color scheme. fill/
  // fill-opacity apply to Polygon; stroke/stroke-width/stroke-opacity to
  // Polygon and LineString; marker-* to Point.
  fill?: string;
  "fill-opacity"?: number;
  stroke?: string;
  "stroke-width"?: number;
  "stroke-opacity"?: number;
  "marker-color"?: string;
  "marker-size"?: "small" | "medium" | "large";
  "marker-symbol"?: string;
}

// Display noun for a geometry type -- shared by the naming modal's title
// and AreasView.tsx's success toasts, so "Area"/"Line"/"Point" language
// stays consistent with whichever shape a user actually drew.
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
