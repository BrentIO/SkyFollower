// Runtime configuration, fetched from the backend's GET /api/config at
// startup (see map/main.py's get_config) -- Vite's build-time
// import.meta.env.VITE_* mechanism only ever produces one baked-in bundle,
// and the published ghcr.io/brentio/skyfollower-map image is built with
// none of these set, so a per-deployment value (the "center" reference
// point) has to come from the network instead. VITE_MAP_API_BASE_URL is
// still resolved at build time -- see resolveApiBaseUrl() below -- since
// same-origin is the correct default for this project's bundled
// single-container deployment and there's no bootstrapping problem there
// (unlike center, which has no sane build-time default at all).

export interface CenterPoint {
  latitude: number;
  longitude: number;
}

export interface AppConfig {
  /** "" means same-origin (this frontend's own host:port). */
  apiBaseUrl: string;
  restFlightsUrl: string;
  restProcessorsUrl: string;
  wsUrl: string;
  /** null when no center reference point is configured on the backend (or,
   * in `npm run dev`, when VITE_CENTER_LATITUDE/VITE_CENTER_LONGITUDE are
   * also unset/invalid) -- center marker and recenter are simply not
   * shown. */
  center: CenterPoint | null;
}

interface ApiConfigResponse {
  center: CenterPoint | null;
}

function resolveApiBaseUrl(): string {
  const raw = import.meta.env.VITE_MAP_API_BASE_URL;
  if (raw && raw.trim() !== "") return raw.replace(/\/+$/, "");
  return "";
}

function resolveWsUrl(apiBaseUrl: string): string {
  if (apiBaseUrl === "") {
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    return `${proto}//${window.location.host}/ws`;
  }
  return `${apiBaseUrl.replace(/^http/, "ws")}/ws`;
}

function isValidCenter(center: CenterPoint | null | undefined): center is CenterPoint {
  return (
    !!center &&
    Number.isFinite(center.latitude) &&
    Number.isFinite(center.longitude)
  );
}

// Dev-only fallback: `npm run dev` runs Vite's own dev server, which has
// no backend at the same origin to serve GET /api/config unless a proxy is
// set up -- VITE_CENTER_LATITUDE/VITE_CENTER_LONGITUDE (map/frontend/.env.example)
// still work as a local override there. Gated on import.meta.env.DEV so a
// production build can never fall back to a value baked in at build time.
function resolveDevCenter(): CenterPoint | null {
  if (!import.meta.env.DEV) return null;
  const latRaw = import.meta.env.VITE_CENTER_LATITUDE;
  const lonRaw = import.meta.env.VITE_CENTER_LONGITUDE;
  if (latRaw === undefined || lonRaw === undefined) return null;
  const center = { latitude: Number(latRaw), longitude: Number(lonRaw) };
  return isValidCenter(center) ? center : null;
}

async function resolveCenter(apiBaseUrl: string): Promise<CenterPoint | null> {
  const devCenter = resolveDevCenter();
  if (devCenter) return devCenter;

  try {
    const response = await fetch(`${apiBaseUrl}/api/config`);
    if (!response.ok) {
      throw new Error(`GET /api/config -> HTTP ${response.status}`);
    }
    const data = (await response.json()) as ApiConfigResponse;
    return isValidCenter(data.center) ? data.center : null;
  } catch (err) {
    console.warn("Failed to load /api/config -- center marker/recenter will be unavailable:", err);
    return null;
  }
}

// Fetched once at page load, same as the old build-time-constant model in
// practice -- but now a real network call rather than a bundle-time
// constant, so a config change on the backend takes effect on the next
// page load with no frontend rebuild required.
export async function loadConfig(): Promise<AppConfig> {
  const apiBaseUrl = resolveApiBaseUrl();
  const center = await resolveCenter(apiBaseUrl);
  if (!center) {
    // eslint has no presence in this project; a plain console.warn is the
    // simplest way to surface a misconfigured deployment without
    // crashing the map itself.
    console.warn(
      "No center reference point configured (MAP_CENTER_LATITUDE/MAP_CENTER_LONGITUDE unset on the " +
        "backend, or -- in `npm run dev` only -- VITE_CENTER_LATITUDE/VITE_CENTER_LONGITUDE unset/" +
        "invalid) -- the center marker and recenter button will be unavailable.",
    );
  }
  return {
    apiBaseUrl,
    restFlightsUrl: `${apiBaseUrl}/api/flights`,
    restProcessorsUrl: `${apiBaseUrl}/api/processors`,
    wsUrl: resolveWsUrl(apiBaseUrl),
    center,
  };
}
