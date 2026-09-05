import { useEffect, useRef, useState, type ReactNode } from "react";
import * as maplibregl from "maplibre-gl";
import { Download, Loader2, MapPinOff, X } from "lucide-react";
import { MAP_STYLE } from "../lib/maplibreSetup";
import {
  EMERGENCY_SQUAWKS,
  VFR_SQUAWK,
  airportLocation,
  boundsOf,
  flightPathFeature,
  formatDuration,
  lineGradientExpression,
  tracePointsFeatureCollection,
  type Coord,
} from "../lib/flightView";
import {
  downloadArchiveFlight,
  downloadFlightPath,
  getFlightView,
  type FlightView,
  type FlightViewAirport,
} from "../api/archiveSearch";
import { ApiError } from "../api/client";
import { useToast } from "../hooks/useToast";

interface FlightViewModalProps {
  // null means closed -- rendered unconditionally by the parent view rather
  // than gated on an `open` boolean, since the token is also the fetch key.
  token: string | null;
  onClose: () => void;
}

const PILL = "rounded px-2 py-0.5 text-xs font-semibold";
const PILL_GREEN = "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200";
const PILL_RED = "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200";

const TRACE_POINTS_SOURCE = "trace-points";
const TRACE_POINTS_CIRCLE_LAYER = "trace-points-circle";
const TRACE_POINTS_LABEL_LAYER = "trace-points-label";

// Lucide's `Waypoints` glyph -- reads as "individual sample points along a
// path" at a glance, distinct from NavigationControl's zoom/compass glyphs
// above it. This control isn't a React component (MapLibre controls render
// outside React's tree), so the icon is inlined as raw markup rather than
// mounted via lucide-react; path data copied from lucide-react's Waypoints
// icon node (stroke icon, default stroke-width 2) so geometry matches the
// rest of the app. `display:block; margin:auto` keeps it optically centered
// in the button regardless of the button's own text-align/line-height.
const TRACE_POINTS_ICON_SVG = `
<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" xmlns="http://www.w3.org/2000/svg" style="display:block;margin:auto">
  <path d="m10.586 5.414-5.172 5.172" />
  <path d="m18.586 13.414-5.172 5.172" />
  <path d="M6 12h12" />
  <circle cx="12" cy="20" r="2" />
  <circle cx="12" cy="4" r="2" />
  <circle cx="20" cy="12" r="2" />
  <circle cx="4" cy="12" r="2" />
</svg>`;

const TRACE_POINTS_COLOR = "#0ea5e9"; // Tailwind sky-500

// The app's first custom maplibregl.IControl -- every map elsewhere uses
// only the stock NavigationControl. Plain DOM (not React) since MapLibre
// controls render outside React's tree; toggle state lives entirely inside
// this instance rather than component state, so it resets for free every
// time the modal reopens and rebuilds the map (see the map-creation effect
// below) instead of needing an explicit reset.
class TracePointsControl implements maplibregl.IControl {
  private _map: maplibregl.Map | undefined;
  private _button: HTMLButtonElement | undefined;
  private _active = false;

  onAdd(map: maplibregl.Map): HTMLElement {
    this._map = map;
    const container = document.createElement("div");
    container.className = "maplibregl-ctrl maplibregl-ctrl-group";
    this._button = document.createElement("button");
    this._button.type = "button";
    this._button.title = "Toggle trace points";
    this._button.setAttribute("aria-label", "Toggle trace points");
    // Flex-center the glyph in the button's content box (matches the
    // stacked zoom +/- buttons above it) rather than relying on the SVG's
    // own margin:auto against the button's default inline-block layout.
    this._button.style.display = "flex";
    this._button.style.alignItems = "center";
    this._button.style.justifyContent = "center";
    this._button.style.color = TRACE_POINTS_COLOR;
    this._button.innerHTML = TRACE_POINTS_ICON_SVG;
    this._button.addEventListener("click", this._onClick);
    container.appendChild(this._button);
    return container;
  }

  onRemove(): void {
    this._button?.removeEventListener("click", this._onClick);
    this._button?.parentElement?.remove();
    this._map = undefined;
    this._button = undefined;
  }

  private _onClick = () => {
    const map = this._map;
    const button = this._button;
    if (!map || !button) return;
    this._active = !this._active;
    // Inline style rather than a new CSS class/file -- this is the only
    // custom control in the app, so a stylesheet just for its active state
    // isn't worth it. Inactive = blue glyph on the stock maplibre white;
    // active inverts to a solid blue button with a white glyph, so the
    // on/off state reads at a glance. maplibre-gl.css's own :hover tint
    // (a semi-transparent overlay) still applies on top of either background.
    button.style.color = this._active ? "#fff" : TRACE_POINTS_COLOR;
    button.style.background = this._active ? TRACE_POINTS_COLOR : "";
    const visibility = this._active ? "visible" : "none";
    map.setLayoutProperty(TRACE_POINTS_CIRCLE_LAYER, "visibility", visibility);
    map.setLayoutProperty(TRACE_POINTS_LABEL_LAYER, "visibility", visibility);
  };
}

// Short display form for receiver_sources -- distinct from ConditionForm.tsx's
// verbose rule-builder labels ("1090MHz ADS-B", "978 UAT"); 1090/978 render as-is.
function receiverSourceLabel(source: string): string {
  return source === "EXTERNAL" ? "External" : source;
}

function SectionLabel({ children }: { children: ReactNode }) {
  return <div className="text-sm font-semibold text-slate-500 dark:text-slate-400">{children}</div>;
}

// Matches /lookup's field-name styling (LookupView.tsx's own Label) so the
// Aircraft section below reads the same way in both places.
function Label({ children }: { children: ReactNode }) {
  return <span className="text-sm text-slate-500 dark:text-slate-400">{children}</span>;
}

// Origin/destination ICAO code carries the same green/red pill used for
// Origin/Destination in /lookup's route view, and matches this modal's own
// map start (green) / end (red) markers.
function AirportBlock({ airport, role }: { airport: FlightViewAirport; role: "origin" | "destination" }) {
  const location = airportLocation(airport);
  return (
    <div>
      <div className="flex flex-wrap items-baseline gap-2">
        <span className={`${PILL} px-3 py-1 text-base ${role === "origin" ? PILL_GREEN : PILL_RED}`}>
          {airport.icao_code}
        </span>
        {airport.iata_code && (
          <span className="font-mono text-sm text-slate-700 dark:text-slate-300">{airport.iata_code}</span>
        )}
      </div>
      {airport.name && <div className="mt-1 text-sm text-slate-900 dark:text-slate-100">{airport.name}</div>}
      {location && <div className="text-sm text-slate-500 dark:text-slate-400">{location}</div>}
    </div>
  );
}

export function FlightViewModal({ token, onClose }: FlightViewModalProps) {
  const [view, setView] = useState<FlightView | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [downloading, setDownloading] = useState<"raw" | "geojson" | null>(null);
  const mapContainerRef = useRef<HTMLDivElement>(null);
  const { showToast } = useToast();

  // Resets and re-fetches on every new token -- the modal opens immediately
  // (loading state below) rather than waiting for this to resolve.
  useEffect(() => {
    if (!token) return;
    setView(null);
    setError(null);
    let cancelled = false;
    getFlightView(token)
      .then((result) => {
        if (!cancelled) setView(result);
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof ApiError ? err.message : "Failed to load flight.");
      });
    return () => {
      cancelled = true;
    };
  }, [token]);

  useEffect(() => {
    if (!view?.flight_path || !mapContainerRef.current) return;
    const coordinates = view.flight_path.geometry.coordinates as Coord[];

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: MAP_STYLE,
      // Pan/zoom only -- same lock pattern as AreasView.tsx/LookupView.tsx's maps.
      maxPitch: 0,
      pitchWithRotate: false,
      dragRotate: false,
      touchPitch: false,
    });
    map.touchZoomRotate.disableRotation();
    map.keyboard.disableRotation();
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    // Added after NavigationControl so MapLibre stacks it directly below
    // the zoom control at the same corner, per the toggle's placement spec.
    map.addControl(new TracePointsControl(), "top-right");

    map.on("load", () => {
      // lineMetrics is required for the line-gradient paint property below
      // -- it's what lets MapLibre resolve ["line-progress"] per-vertex.
      map.addSource("flight-path", {
        type: "geojson",
        lineMetrics: true,
        data: flightPathFeature(coordinates),
      });
      map.addLayer({
        id: "flight-path-line",
        type: "line",
        source: "flight-path",
        layout: { "line-cap": "round", "line-join": "round" },
        paint: { "line-gradient": lineGradientExpression(coordinates), "line-width": 3.5 },
      });
      new maplibregl.Marker({ color: "#16a34a" })
        .setLngLat(coordinates[0].slice(0, 2) as [number, number])
        .addTo(map);
      new maplibregl.Marker({ color: "#dc2626" })
        .setLngLat(coordinates[coordinates.length - 1].slice(0, 2) as [number, number])
        .addTo(map);
      map.fitBounds(boundsOf(coordinates), { padding: 32, animate: false });

      // Trace points -- off by default (TracePointsControl flips
      // visibility on click); the source/layers exist from the start
      // rather than being added/removed per toggle, since MapLibre layers
      // are cheap to hide and this avoids re-adding on every click.
      const traceProps = (view.flight_path?.properties ?? {}) as {
        coordTimes?: (number | null)[];
        coordSpeeds?: (number | null)[];
      };
      map.addSource(TRACE_POINTS_SOURCE, {
        type: "geojson",
        data: tracePointsFeatureCollection(coordinates, traceProps.coordTimes ?? [], traceProps.coordSpeeds ?? []),
      });
      map.addLayer({
        id: TRACE_POINTS_CIRCLE_LAYER,
        type: "circle",
        source: TRACE_POINTS_SOURCE,
        layout: { visibility: "none" },
        paint: {
          "circle-color": ["get", "color"],
          "circle-radius": 4,
          "circle-stroke-width": 1,
          "circle-stroke-color": ["get", "strokeColor"],
        },
      });
      map.addLayer({
        id: TRACE_POINTS_LABEL_LAYER,
        type: "symbol",
        source: TRACE_POINTS_SOURCE,
        layout: {
          visibility: "none",
          "text-field": ["get", "label"],
          "text-size": 11,
          "text-anchor": "bottom-left",
          "text-offset": [0.6, -0.6],
          "text-justify": "left",
          // false is MapLibre's own default -- explicit here since this
          // collision behavior *is* the decluttering mechanism (see
          // symbol-sort-key below), not an incidental setting.
          "text-allow-overlap": false,
          "text-ignore-placement": false,
          "symbol-sort-key": ["get", "sortKey"],
        },
        paint: {
          "text-color": "#0f172a",
          "text-halo-color": "#ffffff",
          "text-halo-width": 1.5,
        },
      });
    });

    return () => {
      map.remove();
    };
  }, [view]);

  if (!token) return null;

  async function handleDownload() {
    if (!token) return;
    setDownloading("raw");
    try {
      await downloadArchiveFlight(token);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to download flight.");
    } finally {
      setDownloading(null);
    }
  }

  async function handleDownloadGeoJson() {
    if (!token) return;
    setDownloading("geojson");
    try {
      await downloadFlightPath(token);
    } catch (err) {
      showToast("error", err instanceof ApiError ? err.message : "Failed to download flight path.");
    } finally {
      setDownloading(null);
    }
  }

  const loading = !view && !error;
  const hasSquawkAlert = !!view?.squawk && EMERGENCY_SQUAWKS.has(view.squawk);
  const isVfr = view?.squawk === VFR_SQUAWK;
  const hasRoute = !!(view?.origin || view?.destination);
  const hasRegistrantOrOperator = !!(view?.registrant || view?.operator);
  const powerplant = view?.powerplant;
  const hasPowerplant = !!(powerplant?.count || powerplant?.type || powerplant?.manufacturer || powerplant?.model);
  const hasAircraftSection = !!(
    view?.category ||
    view?.aircraft_type ||
    view?.manufacturer_model ||
    view?.type_designator ||
    view?.model ||
    view?.serial_number ||
    view?.seats != null ||
    hasPowerplant
  );
  const hasMatchedRules = !!view?.matched_rules && view.matched_rules.length > 0;
  const hasReceiverSources = !!view?.receiver_sources && view.receiver_sources.length > 0;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="flex h-[80dvh] w-[80vw] flex-col overflow-hidden rounded-lg bg-white shadow-xl dark:bg-slate-800">
        <div className="flex shrink-0 items-start justify-between gap-4 p-5 pb-3">
          <div className="min-w-0">
            {loading ? (
              <div className="h-7 w-40 animate-pulse rounded bg-slate-200 dark:bg-slate-700" />
            ) : error ? (
              <span className="text-lg font-semibold text-slate-900 dark:text-slate-100">Flight</span>
            ) : (
              <>
                <div className="flex flex-wrap items-baseline gap-2">
                  <span className="text-xl font-semibold text-slate-900 dark:text-slate-100">
                    {view!.ident ?? view!.icao_hex}
                  </span>
                  {hasSquawkAlert && <span className={`${PILL} ${PILL_RED}`}>{view!.squawk}</span>}
                  {isVfr && <span className={`${PILL} ${PILL_GREEN}`}>VFR</span>}
                  {view!.military && <span className={`${PILL} ${PILL_GREEN}`}>Military</span>}
                </div>
                <div className="text-sm text-slate-500 dark:text-slate-400">
                  {new Date(view!.first_message).toLocaleString()} ·{" "}
                  {formatDuration(view!.first_message, view!.last_message)} ·{" "}
                  {view!.total_messages.toLocaleString()} messages
                </div>
              </>
            )}
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md border border-slate-200 text-slate-500 hover:bg-slate-50 dark:border-slate-700 dark:text-slate-400 dark:hover:bg-slate-700"
          >
            <X size={16} />
          </button>
        </div>

        <div className="mx-5 min-h-[16rem] flex-1 overflow-hidden rounded-md border border-slate-200 dark:border-slate-700">
          {loading ? (
            <div className="flex h-full flex-col items-center justify-center gap-2 text-slate-400">
              <Loader2 className="h-6 w-6 animate-spin" />
              <span className="text-sm">Fetching flight record&hellip;</span>
            </div>
          ) : error ? (
            <div className="flex h-full items-center justify-center px-6 text-center text-sm text-slate-400">
              {error}
            </div>
          ) : !view!.flight_path ? (
            <div className="flex h-full flex-col items-center justify-center gap-2 px-6 text-center text-slate-400">
              <MapPinOff className="h-6 w-6" />
              <span className="text-sm">Not enough position data to show a flight path</span>
            </div>
          ) : (
            <div ref={mapContainerRef} className="h-full w-full" />
          )}
        </div>

        {!loading && !error && view && (
          <div className="flex min-h-0 flex-col gap-4 overflow-y-auto p-5 pt-4">
            {hasRoute && (
              <>
                <div>
                  <SectionLabel>Route</SectionLabel>
                  <div className="grid grid-cols-1 gap-4 pl-4 sm:grid-cols-2">
                    {view.origin && <AirportBlock airport={view.origin} role="origin" />}
                    {view.destination && <AirportBlock airport={view.destination} role="destination" />}
                  </div>
                </div>
                {(hasRegistrantOrOperator || hasAircraftSection || hasMatchedRules || hasReceiverSources) && (
                  <hr className="border-slate-200 dark:border-slate-700" />
                )}
              </>
            )}

            {hasRegistrantOrOperator && (
              <>
                <div className={`grid gap-4 ${view.registrant && view.operator ? "sm:grid-cols-2" : ""}`}>
                  {view.registrant && (
                    <div>
                      <SectionLabel>Registrant</SectionLabel>
                      <div className="pl-4 text-sm text-slate-900 dark:text-slate-100">
                        {view.registrant.names?.map((name, i) => <div key={`name-${i}`}>{name}</div>)}
                        {view.registrant.street?.map((line, i) => <div key={`street-${i}`}>{line}</div>)}
                        {view.registrant.city && <div>{view.registrant.city}</div>}
                        {[view.registrant.administrative_area, view.registrant.country, view.registrant.postal_code]
                          .filter(Boolean).length > 0 && (
                          <div>
                            {[
                              view.registrant.administrative_area,
                              view.registrant.country,
                              view.registrant.postal_code,
                            ]
                              .filter(Boolean)
                              .join(" ")}
                          </div>
                        )}
                      </div>
                    </div>
                  )}
                  {view.operator && (
                    <div>
                      <SectionLabel>Operator</SectionLabel>
                      <div className="flex flex-wrap items-baseline gap-2 pl-4 text-sm text-slate-900 dark:text-slate-100">
                        {view.operator.name && <span>{view.operator.name}</span>}
                        {view.operator.callsign && <span className="italic">"{view.operator.callsign}"</span>}
                      </div>
                    </div>
                  )}
                </div>
                {(hasAircraftSection || hasMatchedRules || hasReceiverSources) && (
                  <hr className="border-slate-200 dark:border-slate-700" />
                )}
              </>
            )}

            {(hasAircraftSection || view.registration) && (
              <>
                <div>
                  <SectionLabel>Aircraft</SectionLabel>
                  <div className="pl-4">
                    <div className="flex flex-wrap items-baseline gap-2">
                      {view.registration && (
                        <span className="text-2xl font-semibold text-slate-900 dark:text-slate-100">
                          {view.registration}
                        </span>
                      )}
                      <span className="font-mono text-sm text-slate-700 dark:text-slate-300">{view.icao_hex}</span>
                    </div>
                    <div className="mt-1 flex flex-col gap-1 text-sm text-slate-900 dark:text-slate-100">
                      {view.category && (
                        <div>
                          <Label>Category</Label> {view.category}
                        </div>
                      )}
                      {view.aircraft_type && (
                        <div>
                          <Label>Type</Label> {view.aircraft_type}
                        </div>
                      )}
                      {(view.manufacturer_model || view.type_designator) && (
                        <div>
                          <Label>Manufacturer/Model</Label>{" "}
                          {[view.manufacturer_model, view.type_designator ? `(${view.type_designator})` : undefined]
                            .filter(Boolean)
                            .join(" ")}
                        </div>
                      )}
                      {view.model && (
                        <div>
                          <Label>Model</Label> {view.model}
                        </div>
                      )}
                      {view.serial_number && (
                        <div>
                          <Label>Serial Number</Label> {view.serial_number}
                        </div>
                      )}
                      {view.seats != null && (
                        <div>
                          <Label>Seats</Label> {view.seats}
                        </div>
                      )}
                      {hasPowerplant && (
                        <div>
                          <Label>Powerplant</Label>
                          <div className="pl-4">
                            {(powerplant?.count || powerplant?.type) && (
                              <div>{[powerplant?.count, powerplant?.type].filter(Boolean).join(" × ")}</div>
                            )}
                            {(powerplant?.manufacturer || powerplant?.model) && (
                              <div>{[powerplant?.manufacturer, powerplant?.model].filter(Boolean).join(" ")}</div>
                            )}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
                {(hasMatchedRules || hasReceiverSources) && <hr className="border-slate-200 dark:border-slate-700" />}
              </>
            )}

            {(hasMatchedRules || hasReceiverSources) && (
              <div className={`grid gap-4 ${hasMatchedRules && hasReceiverSources ? "sm:grid-cols-2" : ""}`}>
                {hasMatchedRules && (
                  <div>
                    <SectionLabel>Matched Rules</SectionLabel>
                    <div className="flex flex-wrap gap-1.5 pl-4">
                      {view.matched_rules.map((rule) => (
                        <span
                          key={rule}
                          className="rounded bg-slate-100 px-2 py-0.5 font-mono text-xs text-slate-700 dark:bg-slate-900 dark:text-slate-300"
                        >
                          {rule}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
                {hasReceiverSources && (
                  <div>
                    <SectionLabel>Receiver Sources</SectionLabel>
                    <div className="flex flex-wrap gap-1.5 pl-4">
                      {view.receiver_sources.map((source) => (
                        <span
                          key={source}
                          className="rounded bg-slate-100 px-2 py-0.5 font-mono text-xs text-slate-700 dark:bg-slate-900 dark:text-slate-300"
                        >
                          {receiverSourceLabel(source)}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {!loading && !error && (
          <div className="hidden shrink-0 justify-end gap-1.5 border-t border-slate-200 bg-slate-50 px-5 py-2.5 dark:border-slate-700 dark:bg-slate-900 md:flex">
            <button
              type="button"
              onClick={handleDownload}
              disabled={downloading !== null}
              className="flex items-center gap-1 rounded-md border border-slate-300 px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
            >
              {downloading === "raw" ? <Loader2 size={12} className="animate-spin" /> : <Download size={12} />}
              Download
            </button>
            <button
              type="button"
              onClick={handleDownloadGeoJson}
              disabled={downloading !== null || !view?.flight_path}
              className="flex items-center gap-1 rounded-md border border-slate-300 px-2 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
            >
              {downloading === "geojson" ? <Loader2 size={12} className="animate-spin" /> : <Download size={12} />}
              GeoJSON
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
