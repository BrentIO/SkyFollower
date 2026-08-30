import * as maplibregl from "maplibre-gl";
import { mdiMapMarker } from "@mdi/js";
import { Fragment, type FormEvent, type ReactNode, useEffect, useRef, useState } from "react";
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
import { classifyLookup, type LookupCategory } from "../lib/lookupClassifier";

// Every lookup type the single search field accepts is alphanumeric plus, at most,
// a space or hyphen (registrations like "VP-CKA", idents, designators) --
// strips anything else as it's typed/pasted rather than merely flagging it
// invalid after the fact.
function sanitizeQuery(raw: string): string {
  return raw.replace(/[^A-Za-z0-9 -]/g, "");
}

type LookupResult =
  | { tab: "aircraft"; data: AircraftRecord }
  | { tab: "operator"; data: OperatorRecord }
  | { tab: "airport"; data: AirportRecord }
  | { tab: "route"; data: RouteLookup };

const RESULT_LABEL: Record<LookupResult["tab"], string> = {
  aircraft: "Aircraft",
  operator: "Operator",
  airport: "Airport",
  route: "Route",
};

// Runs the one backend lookup a classified category maps to, tagging the
// payload with its result-panel kind. "aircraft-hex" and
// "aircraft-registration" hit the same endpoint with a different query
// parameter; every other category is 1:1 with an endpoint.
function lookupForCategory(category: LookupCategory, query: string): Promise<LookupResult> {
  switch (category) {
    case "aircraft-hex":
      return getAircraft({ icaoHex: query }).then((data) => ({ tab: "aircraft", data }));
    case "aircraft-registration":
      return getAircraft({ registration: query }).then((data) => ({ tab: "aircraft", data }));
    case "operator":
      return getOperator(query).then((data) => ({ tab: "operator", data }));
    case "airport":
      return getAirport(query).then((data) => ({ tab: "airport", data }));
    case "route":
      return getRoute(query).then((data) => ({ tab: "route", data }));
  }
}

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
  // The manufacturer/model line above is the generic ICAO type (from
  // Mictronics). `model` is the specific designation a national registry
  // published (e.g. "737-8H4" from the FAA) -- shown separately when present,
  // absent for aircraft covered only by Mictronics.
  const model = displayStr(data.model);
  const serialNumber = displayStr(data.serial_number);
  const seats = displayStr(data.seats);

  const powerplant = displayObj(data.powerplant);
  const ppCountType = joinParts([displayStr(powerplant?.count), displayStr(powerplant?.type)], " x ");
  const ppManufacturerModel = joinParts([displayStr(powerplant?.manufacturer), displayStr(powerplant?.model)]);
  const hasPowerplant = !!(ppCountType || ppManufacturerModel);

  const hasAircraftSection = !!(
    category || type || manufacturerModelLine || model || serialNumber || seats || hasPowerplant
  );

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
            {model && (
              <div>
                <Label>Model</Label> {model}
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

// Mirrors AirportResultView's shape: the code leads in large text, and
// self-evident fields (name, callsign, country) drop their labels. The
// name + quoted-italic callsign row reuses RouteResultView's operator
// header pattern verbatim; country is the muted trailing "where" line
// shared by all three panels.
function OperatorResultView({ data }: { data: OperatorRecord }) {
  const name = displayStr(data.name);
  const designator = displayStr(data.airline_designator) ?? "";
  const callsign = displayStr(data.callsign);
  const country = displayStr(data.country);

  return (
    <div className="flex flex-col gap-1">
      <span className="text-2xl font-semibold text-slate-900 dark:text-slate-100">{designator}</span>
      {(name || callsign) && (
        <div className="flex flex-wrap items-baseline gap-2 text-sm text-slate-900 dark:text-slate-100">
          {name && <span>{name}</span>}
          {callsign && <span className="italic">"{callsign}"</span>}
        </div>
      )}
      {country && <div className="text-sm text-slate-500 dark:text-slate-400">{country}</div>}
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
  const iataCode = displayStr(data.iata_code);
  const name = displayStr(data.name);
  const locLine = joinParts([displayStr(data.city), displayStr(data.region), displayStr(data.country)]);
  const phonic = displayStr(data.phonic);
  const latitude = typeof data.latitude === "number" ? data.latitude : undefined;
  const longitude = typeof data.longitude === "number" ? data.longitude : undefined;

  return (
    <div className="flex flex-col gap-1">
      <div className="flex flex-wrap items-baseline gap-2">
        <span className="text-2xl font-semibold text-slate-900 dark:text-slate-100">{icaoCode}</span>
        {iataCode && <Mono>{iataCode}</Mono>}
      </div>
      {name && (
        <div className="text-sm text-slate-900 dark:text-slate-100">
          {name} {phonic && <span className="italic">"{phonic}"</span>}
        </div>
      )}
      {locLine && <div className="text-sm text-slate-500 dark:text-slate-400">{locLine}</div>}
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
      // Points along the actual rendered great-circle path (already
      // longitude-unwrapped for antimeridian crossings), not just the
      // stops themselves -- for a long-haul route the path's northward
      // bulge or antimeridian crossing can reach well outside the
      // stops-only bounding box (e.g. Sydney -> Chicago passing near the
      // Bering Sea). Fitting bounds to the path directly, in one
      // fitBounds() call, frames the whole visible route and every stop
      // together -- a previous version fit only the stops' bounding box
      // and then separately re-centered on the great-circle midpoint,
      // which kept fitBounds's narrower zoom level but moved the
      // endpoints toward or past the edge of the now-differently-centered
      // viewport.
      const lineCoords = points.length > 1 ? buildGreatCircleLine(points) : [];

      if (lineCoords.length > 0) {
        map.addSource("route-line", {
          type: "geojson",
          data: {
            type: "Feature",
            properties: {},
            geometry: { type: "LineString", coordinates: lineCoords },
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

      const boundsPoints = lineCoords.length > 0 ? lineCoords : points;
      const bounds = boundsPoints.reduce(
        (b, p) => b.extend(p),
        new maplibregl.LngLatBounds(boundsPoints[0], boundsPoints[0]),
      );
      map.fitBounds(bounds, { padding: 60, animate: false });
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
  const operatorCountry = displayStr(data.operator?.country);

  return (
    <div className="flex flex-col gap-4">
      <RouteMap stops={data.stops} />
      <span className="text-2xl font-semibold text-slate-900 dark:text-slate-100">{data.ident}</span>
      {operatorName && (
        <div className="flex flex-col gap-1">
          <div className="flex flex-wrap items-baseline gap-2">
            <span className="text-slate-900 dark:text-slate-100">{operatorName}</span>
            {operatorCallsign && (
              <span className="text-sm italic text-slate-900 dark:text-slate-100">"{operatorCallsign}"</span>
            )}
          </div>
          {operatorCountry && (
            <div className="text-sm text-slate-500 dark:text-slate-400">{operatorCountry}</div>
          )}
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

function ResultPanelBody({ result }: { result: LookupResult }) {
  switch (result.tab) {
    case "aircraft":
      return <AircraftResultView data={result.data} />;
    case "operator":
      return <OperatorResultView data={result.data} />;
    case "airport":
      return <AirportResultView data={result.data} />;
    case "route":
      return <RouteResultView data={result.data} />;
  }
}

// ---------------------------------------------------------------------------
// LookupView
// ---------------------------------------------------------------------------

type SearchState =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "results"; results: LookupResult[] }
  // Every category the input matched was actually queried and came back empty.
  | { status: "not-found" }
  // The input matched no category's shape -- nothing was queried at all.
  | { status: "no-match" };

export function LookupView() {
  const { showToast } = useToast();
  const [query, setQuery] = useState("");
  const [state, setState] = useState<SearchState>({ status: "idle" });

  async function handleSearch(e: FormEvent) {
    e.preventDefault();
    const trimmed = query.trim();
    if (!trimmed) return;

    const categories = classifyLookup(trimmed);
    if (categories.length === 0) {
      // Nothing looks like a hex, registration, designator, airport code,
      // or flight ident -- say so immediately without touching the network.
      setState({ status: "no-match" });
      return;
    }

    setState({ status: "loading" });

    // Every matching category is queried in parallel; an ambiguous input
    // (e.g. "FFT" -> operator + airport, "ABC123" -> hex + route) shows one
    // labeled panel per category that actually resolves.
    const settled = await Promise.allSettled(
      categories.map((category) => lookupForCategory(category, trimmed)),
    );

    const results: LookupResult[] = [];
    let otherError: unknown = null;
    for (const outcome of settled) {
      if (outcome.status === "fulfilled") {
        results.push(outcome.value);
        continue;
      }
      const err = outcome.reason;
      // A 404 just means that one category isn't in Redis ("never seen,"
      // "TTL expired," or "not yet enriched" are indistinguishable) -- drop
      // it silently. Anything else is a real failure worth surfacing.
      if (err instanceof ApiError && err.status === 404) continue;
      otherError = err;
    }

    if (otherError) {
      showToast("error", otherError instanceof ApiError ? otherError.message : "Lookup failed.");
    }

    if (results.length > 0) {
      setState({ status: "results", results });
    } else if (otherError) {
      setState({ status: "idle" });
    } else {
      setState({ status: "not-found" });
    }
  }

  const loading = state.status === "loading";

  return (
    <div className="flex flex-col gap-4">
      <form onSubmit={handleSearch} className="flex max-w-lg gap-2">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(sanitizeQuery(e.target.value))}
          placeholder="ICAO hex, registration, operator, airport, or flight ident"
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

      <div className="flex max-w-3xl flex-col gap-8">
        {loading && <p className="text-slate-400">Searching...</p>}
        {state.status === "no-match" && (
          <p className="text-slate-500 dark:text-slate-400">
            That doesn't look like an ICAO hex, registration, operator designator, airport code, or flight ident.
          </p>
        )}
        {state.status === "not-found" && <p className="text-slate-500 dark:text-slate-400">No data found.</p>}
        {state.status === "results" &&
          state.results.map((result, i) => (
            <Fragment key={`${result.tab}-${i}`}>
              {i > 0 && <hr className="border-slate-200 dark:border-slate-700" />}
              <div className="flex flex-col gap-3">
                <SectionLabel>{RESULT_LABEL[result.tab]}</SectionLabel>
                <ResultPanelBody result={result} />
              </div>
            </Fragment>
          ))}
      </div>
    </div>
  );
}
