// METAR flight-category dots: colours, expiry, zoom thinning and popup text.

import type { FeatureCollection, Point } from "geojson";
import { DIVIDER, PILL, SECTION_BAR } from "../components/AircraftDetailPanel";

export interface MetarCloud {
  cover: string;
  base_ft?: number | null;
}

export interface MetarStation {
  station_id: string;
  name?: string;
  /** The station file's display priority; lower is more important. */
  station_priority?: number;
  lat: number;
  lon: number;
  /** Epoch seconds. */
  observed_at: number;
  flight_category?: string;
  raw: string;
  wind_variable?: boolean;
  wind_dir_deg?: number;
  wind_speed_kt?: number;
  wind_gust_kt?: number;
  visibility_sm?: string;
  weather?: string;
  clouds?: MetarCloud[];
  sky_clear?: string;
  temperature_c?: number;
  dewpoint_c?: number;
  altimeter_inhg?: number;
}

export interface MetarResponse {
  max_age_seconds: number;
  stations: MetarStation[];
}

export interface FlightCategoryStyle {
  label: string;
  color: string;
  /** Readable on a `color` background. */
  textColor: string;
  /** Higher draws on top: worse conditions win an overlap. */
  priority: number;
}

const UNKNOWN_CATEGORY: FlightCategoryStyle = { label: "Unknown", color: "#9ca3af", textColor: "#111827", priority: 0 };

const CATEGORIES: Record<string, FlightCategoryStyle> = {
  VFR: { label: "VFR", color: "#16a34a", textColor: "#ffffff", priority: 1 },
  MVFR: { label: "Marginal VFR", color: "#2563eb", textColor: "#ffffff", priority: 2 },
  IFR: { label: "IFR", color: "#dc2626", textColor: "#ffffff", priority: 3 },
  LIFR: { label: "Low IFR", color: "#d946ef", textColor: "#ffffff", priority: 4 },
};

export const METAR_DOT_RADIUS_PX = 4.5;
/** At this zoom and closer every station is shown. */
export const METAR_FULL_DETAIL_ZOOM = 8;
/** Below it, same-colour dots closer together than this are thinned. */
export const METAR_MIN_SPACING_PX = 28;

export function flightCategoryStyle(category: string | undefined): FlightCategoryStyle {
  return (category !== undefined && Object.hasOwn(CATEGORIES, category) && CATEGORIES[category]) || UNKNOWN_CATEGORY;
}

export function currentStations(response: MetarResponse | null, nowMs: number): MetarStation[] {
  if (!response) return [];
  const oldest = nowMs / 1000 - response.max_age_seconds;
  return response.stations.filter((s) => s.observed_at >= oldest);
}

// Web Mercator pixel position at `zoom`. Independent of the camera centre, so
// panning never changes which dots are thinned.
export function worldPixel(lat: number, lon: number, zoom: number): [number, number] {
  const size = 512 * 2 ** zoom;
  const sin = Math.sin((lat * Math.PI) / 180);
  return [((lon + 180) / 360) * size, (0.5 - Math.log((1 + sin) / (1 - sin)) / (4 * Math.PI)) * size];
}

function byImportance(a: MetarStation, b: MetarStation): number {
  const rank = (s: MetarStation) => s.station_priority ?? Number.MAX_SAFE_INTEGER;
  return rank(a) - rank(b) || a.station_id.localeCompare(b.station_id);
}

/** A station is dropped only if a kept station of the same colour is within
 * METAR_MIN_SPACING_PX of it. More important stations are placed first, so
 * the one kept is never less important than the one it displaces. */
export function thinStations(stations: readonly MetarStation[], zoom: number): MetarStation[] {
  if (zoom >= METAR_FULL_DETAIL_ZOOM) return [...stations];

  const spacing = METAR_MIN_SPACING_PX;
  const grid = new Map<string, [number, number][]>();
  const kept: MetarStation[] = [];
  const ordered = [...stations].sort(byImportance);

  for (const station of ordered) {
    const color = flightCategoryStyle(station.flight_category).color;
    const [x, y] = worldPixel(station.lat, station.lon, zoom);
    const column = Math.floor(x / spacing);
    const row = Math.floor(y / spacing);
    let crowded = false;
    for (let dx = -1; dx <= 1 && !crowded; dx++) {
      for (let dy = -1; dy <= 1 && !crowded; dy++) {
        const cell = grid.get(`${color}:${column + dx}:${row + dy}`);
        crowded = !!cell?.some(([px, py]) => Math.hypot(px - x, py - y) < spacing);
      }
    }
    if (crowded) continue;
    const key = `${color}:${column}:${row}`;
    grid.set(key, [...(grid.get(key) ?? []), [x, y]]);
    kept.push(station);
  }
  return kept;
}

export function metarFeatureCollection(stations: readonly MetarStation[]): FeatureCollection<Point> {
  return {
    type: "FeatureCollection",
    features: stations.map((s) => {
      const style = flightCategoryStyle(s.flight_category);
      return {
        type: "Feature",
        geometry: { type: "Point", coordinates: [s.lon, s.lat] },
        properties: { station_id: s.station_id, color: style.color, priority: style.priority },
      };
    }),
  };
}

function formatWind(s: MetarStation): string | null {
  const speed = s.wind_speed_kt;
  if (speed === undefined) return null;
  const gust = s.wind_gust_kt === undefined ? "" : `, gusting ${s.wind_gust_kt} kt`;
  if (s.wind_variable) return `Variable at ${speed} kt${gust}`;
  if (speed === 0 && s.wind_gust_kt === undefined) return "Calm";
  if (s.wind_dir_deg === undefined) return `${speed} kt${gust}`;
  return `${String(s.wind_dir_deg).padStart(3, "0")}° at ${speed} kt${gust}`;
}

const SKY_CLEAR_TEXT: Record<string, string> = { CLR: "Clear", SKC: "Clear", CAVOK: "CAVOK" };

function formatClouds(s: MetarStation): string | null {
  if (!s.clouds?.length) return (s.sky_clear && SKY_CLEAR_TEXT[s.sky_clear]) || null;
  return s.clouds
    .map((c) => {
      // OVX: the sky is obscured and the base is a vertical visibility.
      const cover = c.cover === "OVX" ? "Vertical visibility" : c.cover;
      return c.base_ft === undefined || c.base_ft === null ? cover : `${cover} ${c.base_ft.toLocaleString("en-US")} ft`;
    })
    .join(", ");
}

export function formatReportTime(observedAt: number, nowMs: number): string {
  const minutes = Math.max(0, Math.floor((nowMs / 1000 - observedAt) / 60));
  const utc = new Date(observedAt * 1000).toISOString().slice(11, 16);
  return `${minutes} min ago (${utc} UTC)`;
}

/** The popup's labelled rows, omitting anything the report doesn't carry. */
export function metarPopupRows(s: MetarStation, nowMs: number): [string, string][] {
  const rows: [string, string | null | undefined][] = [
    ["Report", formatReportTime(s.observed_at, nowMs)],
    ["Wind", formatWind(s)],
    ["Visibility", s.visibility_sm === undefined ? null : `${s.visibility_sm} SM`],
    ["Weather", s.weather],
    ["Clouds", formatClouds(s)],
    ["Temperature", s.temperature_c === undefined ? null : `${s.temperature_c} °C`],
    ["Dew point", s.dewpoint_c === undefined ? null : `${s.dewpoint_c} °C`],
    ["Altimeter", s.altimeter_inhg === undefined ? null : `${s.altimeter_inhg.toFixed(2)} inHg`],
  ];
  return rows.filter((row): row is [string, string] => !!row[1]);
}

function element<K extends keyof HTMLElementTagNameMap>(tag: K, className: string, text?: string) {
  const node = document.createElement(tag);
  node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

// Styled like AircraftDetailPanel. Built with textContent only: report and
// station text comes from outside.
export function buildMetarPopupContent(s: MetarStation, nowMs: number): HTMLElement {
  const style = flightCategoryStyle(s.flight_category);
  const root = element("div", "sf-metar");

  const header = element("div", "p-3 pr-10");
  const code = element("span", `${PILL} inline-block px-3 py-1 text-xl leading-tight font-bold`, s.station_id);
  code.style.backgroundColor = style.color;
  code.style.color = style.textColor;
  code.title = style.label;
  header.append(code);
  if (s.name) header.append(element("div", "mt-1 text-sm", s.name));
  root.append(header);

  const list = element("dl", DIVIDER);
  metarPopupRows(s, nowMs).forEach(([label, value], index) => {
    const row = element("div", `flex items-baseline justify-between gap-3 px-4 py-1.5 ${index ? DIVIDER : ""}`);
    row.append(
      element("dt", "shrink-0 text-xs text-slate-500 dark:text-slate-400", label),
      element("dd", "text-right text-sm", value),
    );
    list.append(row);
  });
  root.append(list);

  const raw = element("div", DIVIDER);
  raw.append(
    element("div", SECTION_BAR, "Raw METAR"),
    element("div", "px-4 py-2.5 font-mono text-xs break-words", s.raw),
  );
  root.append(raw);
  return root;
}
