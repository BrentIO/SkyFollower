import { useEffect, useRef, useState, type ReactNode } from "react";
import * as maplibregl from "maplibre-gl";
import { Download, Loader2, MapPinOff, X } from "lucide-react";
import { MAP_STYLE } from "../lib/maplibreSetup";
import {
  EMERGENCY_SQUAWKS,
  VFR_SQUAWK,
  airportLocation,
  boundsOf,
  formatDuration,
  segmentFeatures,
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

function SectionLabel({ children }: { children: ReactNode }) {
  return <div className="text-sm font-semibold text-slate-500 dark:text-slate-400">{children}</div>;
}

// Origin/destination ICAO code carries the same green/red pill used for
// Origin/Destination in /lookup's route view, and matches this modal's own
// map start (green) / end (red) markers.
function AirportBlock({ airport, role }: { airport: FlightViewAirport; role: "origin" | "destination" }) {
  const location = airportLocation(airport);
  return (
    <div>
      <div className="flex flex-wrap items-baseline gap-2">
        <span className={`${PILL} px-2.5 py-1 text-sm ${role === "origin" ? PILL_GREEN : PILL_RED}`}>
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

    map.on("load", () => {
      map.addSource("flight-path", { type: "geojson", data: segmentFeatures(coordinates) });
      // A dark, translucent halo underneath lifts the colored line off the
      // basemap regardless of what's beneath it, same idea as tar1090's own
      // track rendering.
      map.addLayer({
        id: "flight-path-halo",
        type: "line",
        source: "flight-path",
        layout: { "line-cap": "round", "line-join": "round" },
        paint: { "line-color": "#0f172a", "line-width": 6, "line-opacity": 0.25 },
      });
      map.addLayer({
        id: "flight-path-line",
        type: "line",
        source: "flight-path",
        layout: { "line-cap": "round", "line-join": "round" },
        paint: { "line-color": ["get", "color"], "line-width": 3.5 },
      });
      new maplibregl.Marker({ color: "#16a34a" })
        .setLngLat(coordinates[0].slice(0, 2) as [number, number])
        .addTo(map);
      new maplibregl.Marker({ color: "#dc2626" })
        .setLngLat(coordinates[coordinates.length - 1].slice(0, 2) as [number, number])
        .addTo(map);
      map.fitBounds(boundsOf(coordinates), { padding: 32, animate: false });
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
  const hasMatchedRules = !!view?.matched_rules && view.matched_rules.length > 0;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="flex max-h-[90vh] w-full max-w-2xl flex-col overflow-y-auto rounded-lg bg-white shadow-xl dark:bg-slate-800">
        <div className="flex items-start justify-between gap-4 p-5 pb-3">
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
                  {view!.registration && (
                    <span className="text-sm text-slate-600 dark:text-slate-300">{view!.registration}</span>
                  )}
                  <span className="font-mono text-sm text-slate-700 dark:text-slate-300">{view!.icao_hex}</span>
                  {hasSquawkAlert && <span className={`${PILL} ${PILL_RED}`}>{view!.squawk}</span>}
                  {isVfr && <span className={`${PILL} ${PILL_GREEN}`}>VFR</span>}
                  {view!.military && <span className={`${PILL} ${PILL_GREEN}`}>Military</span>}
                </div>
                {(view!.manufacturer_model || view!.type_designator) && (
                  <div className="mt-0.5 text-sm text-slate-500 dark:text-slate-400">
                    {[view!.manufacturer_model, view!.type_designator].filter(Boolean).join(" · ")}
                  </div>
                )}
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

        <div className="mx-5 h-64 overflow-hidden rounded-md border border-slate-200 dark:border-slate-700">
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
          <div className="flex flex-col gap-4 p-5 pt-4">
            {hasRoute && (
              <>
                <div>
                  <SectionLabel>Route</SectionLabel>
                  <div className="grid grid-cols-1 gap-4 pl-4 sm:grid-cols-2">
                    {view.origin && <AirportBlock airport={view.origin} role="origin" />}
                    {view.destination && <AirportBlock airport={view.destination} role="destination" />}
                  </div>
                </div>
                {(hasRegistrantOrOperator || hasMatchedRules) && (
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
                {hasMatchedRules && <hr className="border-slate-200 dark:border-slate-700" />}
              </>
            )}

            {hasMatchedRules && (
              <div>
                <SectionLabel>Matched rules</SectionLabel>
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
          </div>
        )}

        {!loading && !error && (
          <div className="flex justify-end gap-1.5 border-t border-slate-200 bg-slate-50 px-5 py-2.5 dark:border-slate-700 dark:bg-slate-900">
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
