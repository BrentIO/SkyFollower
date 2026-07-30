import * as maplibregl from "maplibre-gl";
import { mdiMapMarker } from "@mdi/js";
import { type FormEvent, type ReactNode, useEffect, useRef, useState } from "react";
import {
  getAircraft,
  getAirport,
  getOperator,
  getRoute,
  type AircraftRecord,
  type AirportRecord,
  type OperatorRecord,
  type RouteLookup,
} from "../api/reference";
import { ApiError } from "../api/client";
import { useToast } from "../hooks/useToast";
import { MAP_STYLE } from "../lib/maplibreSetup";

type TabKey = "aircraft" | "operator" | "airport" | "route";

const TABS: { key: TabKey; label: string; placeholder: string }[] = [
  { key: "aircraft", label: "Aircraft", placeholder: "ICAO hex (A8AE7F) or registration (N659DL)" },
  { key: "operator", label: "Operator", placeholder: "ICAO airline designator (DAL)" },
  { key: "airport", label: "Airport", placeholder: "ICAO (KJFK) or IATA (JFK) code" },
  { key: "route", label: "Route", placeholder: "Flight ident (DAL2)" },
];

// Every lookup type across all four tabs is alphanumeric plus, at most,
// a space or hyphen (registrations like "VP-CKA", idents, designators) --
// strips anything else as it's typed/pasted rather than merely flagging it
// invalid after the fact.
function sanitizeQuery(raw: string): string {
  return raw.replace(/[^A-Za-z0-9 -]/g, "");
}

// 6 hex digits -> icao_hex; anything else -> registration. Registration
// formats vary too much by country to validate further client-side (see
// #558) -- non-hex input is just passed through and a 404 means "not found."
//
// This guess is only a starting point, not a guarantee: plenty of real
// registrations (e.g. "CA7116", "AEE326") happen to use only [0-9A-F]
// characters too, so a 404 on the first guess doesn't necessarily mean the
// aircraft isn't in Redis -- it may just mean the guess was wrong. See
// getAircraftGuessing below.
const HEX_PATTERN = /^[0-9A-Fa-f]{6}$/;

// Tries the guessed lookup type first; on a 404 (and only a 404 -- any
// other error propagates immediately), retries as the other type before
// giving up. Only shows "No data found" once both interpretations of the
// input have actually missed.
async function getAircraftGuessing(trimmed: string): Promise<AircraftRecord> {
  const guessedHex = HEX_PATTERN.test(trimmed);
  try {
    return await getAircraft(guessedHex ? { icaoHex: trimmed } : { registration: trimmed });
  } catch (err) {
    if (!(err instanceof ApiError) || err.status !== 404) throw err;
    return await getAircraft(guessedHex ? { registration: trimmed } : { icaoHex: trimmed });
  }
}

type LookupResult =
  | { tab: "aircraft"; data: AircraftRecord }
  | { tab: "operator"; data: OperatorRecord }
  | { tab: "airport"; data: AirportRecord }
  | { tab: "route"; data: RouteLookup };

// ---------------------------------------------------------------------------
// Loose-value display helpers -- these endpoints return whatever's really in
// Redis (see api/reference.ts), not a fixed schema, so every field read
// below is optional/unverified at the type level.
// ---------------------------------------------------------------------------

function displayStr(v: unknown): string | undefined {
  if (typeof v === "string") return v.trim() !== "" ? v : undefined;
  if (typeof v === "number") return String(v);
  return undefined;
}

function displayBool(v: unknown): boolean | undefined {
  return typeof v === "boolean" ? v : undefined;
}

function displayArray(v: unknown): string[] | undefined {
  if (!Array.isArray(v)) return undefined;
  const out = v.map(displayStr).filter((x): x is string => !!x);
  return out.length > 0 ? out : undefined;
}

function displayObj(v: unknown): Record<string, unknown> | undefined {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : undefined;
}

function joinParts(parts: (string | undefined)[], sep = " "): string | undefined {
  const filtered = parts.filter((p): p is string => !!p);
  return filtered.length > 0 ? filtered.join(sep) : undefined;
}

// ---------------------------------------------------------------------------
// Shared field-name styling -- distinct color from the value it labels,
// consistently across all four tabs.
// ---------------------------------------------------------------------------

function Label({ children }: { children: ReactNode }) {
  return <span className="text-sm text-slate-500 dark:text-slate-400">{children}</span>;
}

function SectionLabel({ children }: { children: ReactNode }) {
  return <div className="text-sm font-semibold text-slate-500 dark:text-slate-400">{children}</div>;
}

// A raw code (icao_hex, airline/IATA designator) shown as-is rather than in
// "(parens)" -- monospace distinguishes it as a code from surrounding prose.
function Mono({ children }: { children: ReactNode }) {
  return <span className="font-mono text-sm text-slate-700 dark:text-slate-300">{children}</span>;
}

type BadgeColor = "yellow" | "green" | "blue" | "red";

const BADGE_CLASSES: Record<BadgeColor, string> = {
  yellow: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
  green: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
  blue: "bg-sky-100 text-sky-800 dark:bg-sky-900 dark:text-sky-200",
  red: "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200",
};

function Badge({ color, children }: { color: BadgeColor; children: ReactNode }) {
  return <span className={`rounded px-2 py-0.5 text-xs font-semibold ${BADGE_CLASSES[color]}`}>{children}</span>;
}

// ---------------------------------------------------------------------------
// Aircraft
// ---------------------------------------------------------------------------

function AircraftResultView({ data }: { data: AircraftRecord }) {
  const registration = displayStr(data.registration);
  const icaoHex = displayStr(data.icao_hex);
  const military = displayBool(data.military);
  const specialLivery = displayStr(data.special_livery);

  const registrant = displayObj(data.registrant);
  const names = displayArray(registrant?.names);
  const street = displayArray(registrant?.street);
  const city = displayStr(registrant?.city);
  const adminLine = joinParts([
    displayStr(registrant?.administrative_area),
    displayStr(registrant?.country),
    displayStr(registrant?.postal_code),
  ]);
  const hasRegistrant = !!(names || street || city || adminLine);

  const category = displayStr(data.category);
  const type = displayStr(data.type);
  const manufacturerModel = displayStr(data.manufacturer_model);
  const typeDesignator = displayStr(data.type_designator);
  const manufacturerModelLine =
    manufacturerModel || typeDesignator
      ? joinParts([manufacturerModel, typeDesignator ? `(${typeDesignator})` : undefined])
      : undefined;
  const serialNumber = displayStr(data.serial_number);
  const seats = displayStr(data.seats);

  const powerplant = displayObj(data.powerplant);
  const ppCountType = joinParts([displayStr(powerplant?.count), displayStr(powerplant?.type)], " x ");
  const ppManufacturerModel = joinParts([displayStr(powerplant?.manufacturer), displayStr(powerplant?.model)]);
  const hasPowerplant = !!(ppCountType || ppManufacturerModel);

  const hasAircraftSection = !!(category || type || manufacturerModelLine || serialNumber || seats || hasPowerplant);

  const dataSources = displayArray(data.data_sources);

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-baseline gap-2">
        {registration && (
          <span className="text-2xl font-semibold text-slate-900 dark:text-slate-100">{registration}</span>
        )}
        {icaoHex && <Mono>{icaoHex}</Mono>}
        {military && <Badge color="green">Military</Badge>}
        {specialLivery && <Badge color="yellow">{specialLivery}</Badge>}
      </div>

      {hasRegistrant && (
        <div>
          <SectionLabel>Registrant</SectionLabel>
          <div className="pl-4 text-sm text-slate-900 dark:text-slate-100">
            {names?.map((n, i) => <div key={`name-${i}`}>{n}</div>)}
            {street?.map((s, i) => <div key={`street-${i}`}>{s}</div>)}
            {city && <div>{city}</div>}
            {adminLine && <div>{adminLine}</div>}
          </div>
        </div>
      )}

      {hasAircraftSection && (
        <div>
          <SectionLabel>Aircraft</SectionLabel>
          <div className="flex flex-col gap-1 pl-4 text-sm text-slate-900 dark:text-slate-100">
            {category && (
              <div>
                <Label>Category</Label> {category}
              </div>
            )}
            {type && (
              <div>
                <Label>Type</Label> {type}
              </div>
            )}
            {manufacturerModelLine && (
              <div>
                <Label>Manufacturer/Model</Label> {manufacturerModelLine}
              </div>
            )}
            {serialNumber && (
              <div>
                <Label>Serial Number</Label> {serialNumber}
              </div>
            )}
            {seats && (
              <div>
                <Label>Seats</Label> {seats}
              </div>
            )}
            {hasPowerplant && (
              <div>
                <Label>Powerplant</Label>
                <div className="pl-4">
                  {ppCountType && <div>{ppCountType}</div>}
                  {ppManufacturerModel && <div>{ppManufacturerModel}</div>}
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {dataSources && (
        <div>
          <SectionLabel>Sources</SectionLabel>
          <div className="pl-4 text-sm text-slate-900 dark:text-slate-100">
            {dataSources.map((s, i) => <div key={i}>{s}</div>)}
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Operator
// ---------------------------------------------------------------------------

function OperatorResultView({ data }: { data: OperatorRecord }) {
  const name = displayStr(data.name);
  const designator = displayStr(data.airline_designator) ?? "";
  const callsign = displayStr(data.callsign);
  const country = displayStr(data.country);

  return (
    <div className="flex flex-col gap-1">
      <div className="flex flex-wrap items-baseline gap-2">
        {name ? (
          <>
            <span className="text-2xl font-semibold text-slate-900 dark:text-slate-100">{name}</span>
            <Mono>{designator}</Mono>
          </>
        ) : (
          <span className="text-2xl font-semibold text-slate-900 dark:text-slate-100">{designator}</span>
        )}
      </div>
      {callsign && (
        <div className="text-sm text-slate-900 dark:text-slate-100">
          <Label>Callsign</Label> {callsign}
        </div>
      )}
      {country && (
        <div className="text-sm text-slate-900 dark:text-slate-100">
          <Label>Country</Label> {country}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Airport
// ---------------------------------------------------------------------------

function AirportMap({ latitude, longitude }: { latitude: number; longitude: number }) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    const map = new maplibregl.Map({
      container: containerRef.current,
      style: MAP_STYLE,
      center: [longitude, latitude],
      zoom: 11,
      // Pan/zoom only -- matches the "I can pan and zoom with the map, but
      // that's it" ask. Same lock pattern as AreasView.tsx's map: maxPitch
      // is the hard guarantee against tilt, the rest plus the two
      // disableRotation() calls below block every gesture path (mouse,
      // touch, keyboard) that could otherwise rotate or tilt it.
      maxPitch: 0,
      pitchWithRotate: false,
      dragRotate: false,
      touchPitch: false,
    });
    map.touchZoomRotate.disableRotation();
    map.keyboard.disableRotation();
    // showCompass: false -- rotation is locked above, so a reset-bearing
    // compass button has nothing to do.
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    new maplibregl.Marker().setLngLat([longitude, latitude]).addTo(map);
    return () => {
      map.remove();
    };
  }, [latitude, longitude]);

  return (
    <div
      ref={containerRef}
      className="h-80 w-full rounded-md border border-slate-200 dark:border-slate-700"
    />
  );
}

function AirportResultView({ data }: { data: AirportRecord }) {
  const icaoCode = displayStr(data.icao_code) ?? "";
  const name = displayStr(data.name);
  const locLine = joinParts([displayStr(data.city), displayStr(data.region), displayStr(data.country)]);
  const phonic = displayStr(data.phonic);
  const latitude = typeof data.latitude === "number" ? data.latitude : undefined;
  const longitude = typeof data.longitude === "number" ? data.longitude : undefined;

  return (
    <div className="flex flex-col gap-1">
      <span className="text-2xl font-semibold text-slate-900 dark:text-slate-100">{icaoCode}</span>
      {name && <div className="text-sm text-slate-900 dark:text-slate-100">{name}</div>}
      {locLine && <div className="text-sm text-slate-900 dark:text-slate-100">{locLine}</div>}
      {phonic && <div className="text-sm italic text-slate-900 dark:text-slate-100">"{phonic}"</div>}
      {latitude !== undefined && longitude !== undefined && (
        <div className="mt-2">
          <AirportMap latitude={latitude} longitude={longitude} />
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Route
// ---------------------------------------------------------------------------

type RouteRole = "origin" | "stop" | "destination";

const ROLE_COLOR: Record<RouteRole, string> = {
  origin: "#16a34a", // green-600
  stop: "#0284c7", // sky-600
  destination: "#dc2626", // red-600
};

const ROLE_BADGE_COLOR: Record<RouteRole, BadgeColor> = {
  origin: "green",
  stop: "blue",
  destination: "red",
};

const ROLE_LABEL: Record<RouteRole, string> = {
  origin: "Origin",
  stop: "Stop",
  destination: "Destination",
};

function roleFor(index: number, total: number): RouteRole {
  if (index === 0) return "origin";
  if (index === total - 1) return "destination";
  return "stop";
}

function toRad(deg: number): number {
  return (deg * Math.PI) / 180;
}

function toDeg(rad: number): number {
  return (rad * 180) / Math.PI;
}

// Spherical linear interpolation between two [lon, lat] points along the
// great-circle arc connecting them -- straight rhumb-line segments would
// visibly cut corners on anything but a very short hop. Each interpolated
// point's longitude comes from its own atan2(), independently wrapped to
// (-180, 180] -- a path that actually crosses the antimeridian (e.g.
// ZSPD -> PANC, whose shortest path runs over the Bering Sea) produces a
// point sequence that jumps from just under +180 to just over -180 (or vice
// versa) partway through. unwrapLongitudes() below straightens that back
// into a continuous sequence before it's ever handed to MapLibre.
function greatCircleSegment(
  [lon1, lat1]: [number, number],
  [lon2, lat2]: [number, number],
  steps: number,
): [number, number][] {
  const phi1 = toRad(lat1);
  const lambda1 = toRad(lon1);
  const phi2 = toRad(lat2);
  const lambda2 = toRad(lon2);
  const d =
    2 *
    Math.asin(
      Math.sqrt(
        Math.sin((phi2 - phi1) / 2) ** 2 + Math.cos(phi1) * Math.cos(phi2) * Math.sin((lambda2 - lambda1) / 2) ** 2,
      ),
    );
  if (d === 0) return [[lon1, lat1]];

  const points: [number, number][] = [];
  for (let i = 0; i <= steps; i++) {
    const f = i / steps;
    const A = Math.sin((1 - f) * d) / Math.sin(d);
    const B = Math.sin(f * d) / Math.sin(d);
    const x = A * Math.cos(phi1) * Math.cos(lambda1) + B * Math.cos(phi2) * Math.cos(lambda2);
    const y = A * Math.cos(phi1) * Math.sin(lambda1) + B * Math.cos(phi2) * Math.sin(lambda2);
    const z = A * Math.sin(phi1) + B * Math.sin(phi2);
    const phi = Math.atan2(z, Math.sqrt(x * x + y * y));
    const lambda = Math.atan2(y, x);
    points.push([toDeg(lambda), toDeg(phi)]);
  }
  return points;
}

// Walks the coordinate sequence and adds/subtracts multiples of 360 to keep
// each point's longitude within 180 degrees of the one before it -- turns a
// sequence that jumps across +/-180 into a continuous one that may run
// outside the standard [-180, 180] range (e.g. 190 instead of -170).
// MapLibre's Mercator projection renders that correctly with
// renderWorldCopies (the default): a longitude outside the standard range
// simply lands in the adjacent world copy, visually continuous with the
// rest of the line.
function unwrapLongitudes(coords: [number, number][]): [number, number][] {
  if (coords.length === 0) return coords;
  const out: [number, number][] = [coords[0]];
  let offset = 0;
  for (let i = 1; i < coords.length; i++) {
    let lon = coords[i][0] + offset;
    const prevLon = out[i - 1][0];
    while (lon - prevLon > 180) {
      lon -= 360;
      offset -= 360;
    }
    while (lon - prevLon < -180) {
      lon += 360;
      offset += 360;
    }
    out.push([lon, coords[i][1]]);
  }
  return out;
}

function buildGreatCircleLine(waypoints: [number, number][]): [number, number][] {
  const coords: [number, number][] = [];
  for (let i = 0; i < waypoints.length - 1; i++) {
    const segment = greatCircleSegment(waypoints[i], waypoints[i + 1], 64);
    coords.push(...(i === 0 ? segment : segment.slice(1)));
  }
  return unwrapLongitudes(coords);
}

function iconMarkerElement(color: string): HTMLDivElement {
  const el = document.createElement("div");
  el.innerHTML = `<svg width="28" height="28" viewBox="0 0 24 24" fill="${color}"><path d="${mdiMapMarker}"/></svg>`;
  return el;
}

function labelMarkerElement(color: string, text: string): HTMLDivElement {
  const el = document.createElement("div");
  el.textContent = text;
  el.style.fontWeight = "700";
  el.style.fontSize = "14px";
  el.style.color = color;
  el.style.whiteSpace = "nowrap";
  el.style.textShadow = "0 1px 3px rgba(255,255,255,0.9), 0 1px 3px rgba(255,255,255,0.9)";
  return el;
}

function stopCoordinates(stops: AirportRecord[]): [number, number][] {
  return stops
    .map((s) => {
      const lat = typeof s.latitude === "number" ? s.latitude : undefined;
      const lon = typeof s.longitude === "number" ? s.longitude : undefined;
      return lat !== undefined && lon !== undefined ? ([lon, lat] as [number, number]) : null;
    })
    .filter((p): p is [number, number] => p !== null);
}

function RouteMap({ stops }: { stops: AirportRecord[] }) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    const points = stopCoordinates(stops);
    if (points.length === 0) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: MAP_STYLE,
      center: points[0],
      zoom: 3,
    });
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");

    map.on("load", () => {
      if (points.length > 1) {
        map.addSource("route-line", {
          type: "geojson",
          data: {
            type: "Feature",
            properties: {},
            geometry: { type: "LineString", coordinates: buildGreatCircleLine(points) },
          },
        });
        map.addLayer({
          id: "route-line",
          type: "line",
          source: "route-line",
          paint: { "line-color": "#0284c7", "line-width": 3 },
        });
      }

      stops.forEach((stop, i) => {
        const lat = typeof stop.latitude === "number" ? stop.latitude : undefined;
        const lon = typeof stop.longitude === "number" ? stop.longitude : undefined;
        if (lat === undefined || lon === undefined) return;
        const color = ROLE_COLOR[roleFor(i, stops.length)];
        new maplibregl.Marker({ element: iconMarkerElement(color), anchor: "bottom" })
          .setLngLat([lon, lat])
          .addTo(map);
        new maplibregl.Marker({
          element: labelMarkerElement(color, displayStr(stop.icao_code) ?? ""),
          anchor: "top",
          offset: [0, 2],
        })
          .setLngLat([lon, lat])
          .addTo(map);
      });

      const bounds = points.reduce((b, p) => b.extend(p), new maplibregl.LngLatBounds(points[0], points[0]));
      map.fitBounds(bounds, { padding: 60, animate: false });

      // fitBounds centers on the bounding box's own centroid, which for a
      // long-haul route (e.g. Sydney -> Chicago) lands somewhere over land
      // between them rather than on the great-circle path itself. Keep the
      // zoom level fitBounds picked (still scaled to show the whole route)
      // but re-center on the actual midpoint of the origin/destination
      // great-circle arc -- for that Sydney/Chicago example, the Pacific
      // crossing near the Bering Sea, not a naive lat/lon average of the
      // two endpoints.
      const midpoint = greatCircleSegment(points[0], points[points.length - 1], 2)[1];
      map.setCenter(midpoint);
    });

    return () => {
      map.remove();
    };
  }, [stops]);

  return (
    <div
      ref={containerRef}
      className="h-96 w-full rounded-md border border-slate-200 dark:border-slate-700"
    />
  );
}

function RouteResultView({ data }: { data: RouteLookup }) {
  const operatorName = displayStr(data.operator?.name);
  const operatorCallsign = displayStr(data.operator?.callsign);

  return (
    <div className="flex flex-col gap-4">
      <RouteMap stops={data.stops} />
      <span className="text-2xl font-semibold text-slate-900 dark:text-slate-100">{data.ident}</span>
      {operatorName && (
        <div className="flex flex-wrap items-baseline gap-2">
          <span className="text-slate-900 dark:text-slate-100">{operatorName}</span>
          {operatorCallsign && <Mono>{operatorCallsign}</Mono>}
        </div>
      )}
      <div className="flex flex-col gap-2">
        {data.stops.map((stop, i) => {
          const role = roleFor(i, data.stops.length);
          const icaoCode = displayStr(stop.icao_code) ?? "";
          const iataCode = displayStr(stop.iata_code);
          const detailLine = joinParts([displayStr(stop.phonic), displayStr(stop.city), displayStr(stop.region)], ", ");
          return (
            <div key={i} className="flex flex-wrap items-baseline gap-2 text-sm">
              <Badge color={ROLE_BADGE_COLOR[role]}>{ROLE_LABEL[role]}</Badge>
              <span className="text-lg font-semibold text-slate-900 dark:text-slate-100">{icaoCode}</span>
              {iataCode && <Mono>{iataCode}</Mono>}
              {detailLine && <span className="text-slate-900 dark:text-slate-100">{detailLine}</span>}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// LookupView
// ---------------------------------------------------------------------------

export function LookupView() {
  const { showToast } = useToast();
  const [tab, setTab] = useState<TabKey>("aircraft");
  const [query, setQuery] = useState("");
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<LookupResult | null>(null);
  const [notFound, setNotFound] = useState(false);

  function selectTab(next: TabKey) {
    setTab(next);
    setQuery("");
    setResult(null);
    setNotFound(false);
  }

  async function handleSearch(e: FormEvent) {
    e.preventDefault();
    const trimmed = query.trim();
    if (!trimmed) return;

    setLoading(true);
    setResult(null);
    setNotFound(false);
    try {
      switch (tab) {
        case "aircraft":
          setResult({ tab: "aircraft", data: await getAircraftGuessing(trimmed) });
          break;
        case "operator":
          setResult({ tab: "operator", data: await getOperator(trimmed) });
          break;
        case "airport":
          setResult({ tab: "airport", data: await getAirport(trimmed) });
          break;
        case "route":
          setResult({ tab: "route", data: await getRoute(trimmed) });
          break;
      }
    } catch (err) {
      if (err instanceof ApiError && err.status === 404) {
        // Redis can't distinguish "never seen," "TTL expired," or "not yet
        // enriched" -- one generic message for all three, per #558.
        setNotFound(true);
      } else {
        showToast("error", err instanceof ApiError ? err.message : "Lookup failed.");
      }
    } finally {
      setLoading(false);
    }
  }

  const activeTabInfo = TABS.find((t) => t.key === tab)!;

  return (
    <div className="flex flex-col gap-4">
      <div className="flex gap-2">
        {TABS.map((t) => (
          <button
            key={t.key}
            type="button"
            onClick={() => selectTab(t.key)}
            className={`rounded-md px-3 py-2 text-sm font-medium ${
              tab === t.key
                ? "bg-sky-600 text-white"
                : "bg-slate-100 text-slate-700 hover:bg-slate-200 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-700"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>

      <form onSubmit={handleSearch} className="flex max-w-lg gap-2">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(sanitizeQuery(e.target.value))}
          placeholder={activeTabInfo.placeholder}
          className="flex-1 rounded-md border border-slate-300 px-3 py-1.5 text-sm dark:border-slate-600 dark:bg-slate-900"
        />
        <button
          type="submit"
          disabled={loading || !query.trim()}
          className="rounded-md bg-sky-600 px-4 py-1.5 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-40"
        >
          Search
        </button>
      </form>

      <div className="max-w-3xl">
        {loading && <p className="text-slate-400">Searching...</p>}
        {!loading && notFound && <p className="text-slate-500 dark:text-slate-400">No data found.</p>}
        {!loading && result?.tab === "aircraft" && <AircraftResultView data={result.data} />}
        {!loading && result?.tab === "operator" && <OperatorResultView data={result.data} />}
        {!loading && result?.tab === "airport" && <AirportResultView data={result.data} />}
        {!loading && result?.tab === "route" && <RouteResultView data={result.data} />}
      </div>
    </div>
  );
}
