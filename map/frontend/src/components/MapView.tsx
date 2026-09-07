import * as maplibregl from "maplibre-gl";
import type { Feature, FeatureCollection } from "geojson";
import { useEffect, useRef, useState } from "react";
import { MAP_STYLE } from "../lib/maplibreSetup";
import { altitudeColor } from "../lib/altitudeColor";
import { AIRCRAFT_ICON_ID, buildAircraftIconImageData } from "../lib/aircraftIcon";
import { crosshairSvgMarkup, MUTED_GRAY } from "../lib/crosshairIcon";
import { loadConfig, type AppConfig } from "../lib/config";
import { useMapFlights } from "../hooks/useMapFlights";
import type { AircraftRecord } from "../lib/aircraftState";
import { ControlsPanel } from "./ControlsPanel";
import { InfoBoxLayer, type InfoBoxLayerItem } from "./InfoBoxLayer";

const AIRCRAFT_SOURCE_ID = "sf-aircraft";
const AIRCRAFT_LAYER_ID = "sf-aircraft-icons";
const TRAIL_SOURCE_ID = "sf-trails";
const TRAIL_LAYER_ID = "sf-trails-line";

const EMPTY_FEATURE_COLLECTION: FeatureCollection = { type: "FeatureCollection", features: [] };

function hasPosition(a: AircraftRecord): a is AircraftRecord & { latitude: number; longitude: number } {
  return a.latitude != null && a.longitude != null;
}

function aircraftFeatureCollection(
  aircraft: Record<string, AircraftRecord>,
  selected: Set<string>,
): FeatureCollection {
  const features: Feature[] = Object.values(aircraft)
    .filter(hasPosition)
    .map((a) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [a.longitude, a.latitude] },
      properties: {
        icao_hex: a.icao_hex,
        heading: a.heading ?? 0,
        color: altitudeColor(a.altitude ?? null),
        selected: selected.has(a.icao_hex),
        stale: a.stale,
      },
    }));
  return { type: "FeatureCollection", features };
}

function trailFeatureCollection(
  aircraft: Record<string, AircraftRecord>,
  visibleIds: Set<string>,
): FeatureCollection {
  const features: Feature[] = [];
  for (const a of Object.values(aircraft)) {
    if (!visibleIds.has(a.icao_hex) || a.trail.length < 2) continue;
    features.push({
      type: "Feature",
      geometry: {
        type: "LineString",
        coordinates: a.trail.map((p) => [p.longitude, p.latitude]),
      },
      properties: {
        icao_hex: a.icao_hex,
        // Current altitude, not per-point -- the trail is a single flat
        // color per aircraft, same mechanism as the icon fill (see the
        // issue's Live trail section), not a per-point gradient.
        color: altitudeColor(a.altitude ?? null),
      },
    });
  }
  return { type: "FeatureCollection", features };
}

// Top-level export: fetches runtime config (GET /api/config -- see
// map/lib/config.ts's loadConfig) once before the actual map ever mounts,
// since the map's initial center/zoom and home marker depend on it. A
// brief loading state is expected and fine here; the fetch is one small
// same-origin round-trip made once per page load.
export function MapView() {
  const [config, setConfig] = useState<AppConfig | null>(null);

  useEffect(() => {
    let cancelled = false;
    loadConfig().then((cfg) => {
      if (!cancelled) setConfig(cfg);
    });
    return () => {
      cancelled = true;
    };
  }, []);

  if (!config) {
    return (
      <div className="flex h-full w-full items-center justify-center text-sm text-gray-400">
        Loading map…
      </div>
    );
  }

  return <MapViewInner config={config} />;
}

// Full-viewport MapLibre live map: aircraft icon layer (heading rotation,
// altitude-colored fill), live trails, floating info boxes with
// collision-avoidance placement, floating top-right controls, and a
// "home" reference-point marker/recenter. See the issue this implements
// for the full design spec. Only ever mounted once `config` has resolved
// (see MapView above), so every `config.home` read below is a plain,
// already-loaded value -- no further async handling needed in here.
function MapViewInner({ config }: { config: AppConfig }) {
  const { aircraft, connected } = useMapFlights(config.wsUrl, config.restFlightsUrl);

  const mapContainerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [mapLoaded, setMapLoaded] = useState(false);

  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [historyAll, setHistoryAll] = useState(false);
  const [screenPositions, setScreenPositions] = useState<Record<string, { x: number; y: number }>>({});

  // Kept in a ref so the map's 'move' listener (attached once, on mount)
  // always reads current aircraft positions rather than closing over a
  // stale snapshot from whenever that listener was attached.
  const aircraftRef = useRef(aircraft);
  aircraftRef.current = aircraft;

  // --- Map construction (once) ---------------------------------------
  useEffect(() => {
    if (!mapContainerRef.current) return;

    const map = new maplibregl.Map({
      container: mapContainerRef.current,
      style: MAP_STYLE,
      center: config.home ? [config.home.longitude, config.home.latitude] : [0, 0],
      zoom: config.home ? 9 : 1,
      // Locked north-up -- the aircraft icon rotates via icon-rotate,
      // never the map itself. Same lock pattern as every existing
      // MapLibre view in management-ui/frontend.
      pitchWithRotate: false,
      dragRotate: false,
    });
    mapRef.current = map;
    map.touchZoomRotate.disableRotation();
    map.keyboard.disableRotation();
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-left");

    function syncScreenPositions() {
      const current = aircraftRef.current;
      const positions: Record<string, { x: number; y: number }> = {};
      for (const a of Object.values(current)) {
        if (!hasPosition(a)) continue;
        const p = map.project([a.longitude, a.latitude]);
        positions[a.icao_hex] = { x: p.x, y: p.y };
      }
      setScreenPositions(positions);
    }

    map.on("load", () => {
      map.addImage(AIRCRAFT_ICON_ID, buildAircraftIconImageData(), { sdf: true });

      map.addSource(TRAIL_SOURCE_ID, { type: "geojson", data: EMPTY_FEATURE_COLLECTION });
      map.addLayer({
        id: TRAIL_LAYER_ID,
        type: "line",
        source: TRAIL_SOURCE_ID,
        layout: { "line-cap": "round", "line-join": "round" },
        paint: { "line-color": ["get", "color"], "line-width": 2.5, "line-opacity": 0.85 },
      });

      map.addSource(AIRCRAFT_SOURCE_ID, { type: "geojson", data: EMPTY_FEATURE_COLLECTION });
      map.addLayer({
        id: AIRCRAFT_LAYER_ID,
        type: "symbol",
        source: AIRCRAFT_SOURCE_ID,
        layout: {
          "icon-image": AIRCRAFT_ICON_ID,
          "icon-rotate": ["get", "heading"],
          "icon-rotation-alignment": "map",
          "icon-allow-overlap": true,
          "icon-ignore-placement": true,
          "icon-size": 0.4,
        },
        paint: {
          // Icon fill is altitude-based; it never changes on selection --
          // selection is shown only via the halo below.
          "icon-color": ["get", "color"],
          "icon-halo-color": "#ffffff",
          "icon-halo-width": ["case", ["boolean", ["get", "selected"], false], 3, 0],
          "icon-halo-blur": ["case", ["boolean", ["get", "selected"], false], 0.5, 0],
          "icon-opacity": ["case", ["boolean", ["get", "stale"], false], 0.4, 1],
        },
      });

      map.on("click", AIRCRAFT_LAYER_ID, (e) => {
        const icaoHex = e.features?.[0]?.properties?.icao_hex as string | undefined;
        if (!icaoHex) return;
        setSelected((prev) => {
          const next = new Set(prev);
          if (next.has(icaoHex)) next.delete(icaoHex);
          else next.add(icaoHex);
          return next;
        });
      });
      map.on("mouseenter", AIRCRAFT_LAYER_ID, () => {
        map.getCanvas().style.cursor = "pointer";
      });
      map.on("mouseleave", AIRCRAFT_LAYER_ID, () => {
        map.getCanvas().style.cursor = "";
      });

      // Home / centered reference point -- a fixed marker from config,
      // never derived from received data.
      if (config.home) {
        const el = document.createElement("div");
        el.style.display = "flex";
        el.style.flexDirection = "column";
        el.style.alignItems = "center";
        el.style.gap = "2px";
        el.style.pointerEvents = "none";
        el.innerHTML =
          crosshairSvgMarkup(28, MUTED_GRAY) +
          `<span style="font-size:9px;font-weight:600;letter-spacing:0.05em;color:${MUTED_GRAY};text-shadow:0 1px 2px rgba(255,255,255,0.8);">HOME</span>`;
        new maplibregl.Marker({ element: el, anchor: "center" })
          .setLngLat([config.home.longitude, config.home.latitude])
          .addTo(map);
      }

      map.on("move", syncScreenPositions);
      syncScreenPositions();
      setMapLoaded(true);
    });

    return () => {
      map.off("move", syncScreenPositions);
      map.remove();
      mapRef.current = null;
    };
    // Intentionally created once -- `config` (this component's own prop)
    // is only ever set once per page load by MapView above; it can change
    // between page loads (it's now a runtime GET /api/config fetch, not a
    // build-time constant), but never while this component is mounted.
  }, []);

  // --- Keep the aircraft/trail sources and screen positions in sync ---
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded) return;

    const visibleTrailIds = historyAll ? new Set(Object.keys(aircraft)) : selected;

    (map.getSource(AIRCRAFT_SOURCE_ID) as maplibregl.GeoJSONSource | undefined)?.setData(
      aircraftFeatureCollection(aircraft, selected),
    );
    (map.getSource(TRAIL_SOURCE_ID) as maplibregl.GeoJSONSource | undefined)?.setData(
      trailFeatureCollection(aircraft, visibleTrailIds),
    );

    const positions: Record<string, { x: number; y: number }> = {};
    for (const a of Object.values(aircraft)) {
      if (!hasPosition(a)) continue;
      const p = map.project([a.longitude, a.latitude]);
      positions[a.icao_hex] = { x: p.x, y: p.y };
    }
    setScreenPositions(positions);
  }, [aircraft, selected, historyAll, mapLoaded]);

  function handleRecenter() {
    const map = mapRef.current;
    if (!map || !config.home) return;
    // Preserves whatever zoom level the user is already at -- only the
    // center changes.
    map.easeTo({ center: [config.home.longitude, config.home.latitude] });
  }

  const infoBoxItems: InfoBoxLayerItem[] = Object.values(aircraft)
    .filter(hasPosition)
    .filter((a) => screenPositions[a.icao_hex] !== undefined)
    .map((a) => ({
      id: a.icao_hex,
      x: screenPositions[a.icao_hex].x,
      y: screenPositions[a.icao_hex].y,
      aircraft: a,
    }));

  return (
    <div className="relative h-full w-full">
      <div ref={mapContainerRef} className="h-full w-full" />
      {mapLoaded && <InfoBoxLayer items={infoBoxItems} />}
      <ControlsPanel
        connected={connected}
        aircraftCount={Object.keys(aircraft).length}
        historyAll={historyAll}
        onToggleHistoryAll={() => setHistoryAll((prev) => !prev)}
        onRecenter={handleRecenter}
        recenterDisabled={!config.home}
      />
    </div>
  );
}
