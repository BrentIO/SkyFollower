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
