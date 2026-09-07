// Build-time configuration, read the same way management-ui/frontend
// reads its own build-time values (Vite's standard import.meta.env.VITE_*
// mechanism -- management-ui/frontend has no *runtime* frontend env-var
// convention to match; its only VITE_* values, VITE_VERSION/VITE_COMMIT,
// are likewise baked in at build time by its Dockerfile). See
// map/frontend/.env.example for the full list and defaults/fallback
// behavior.

export interface HomePoint {
  latitude: number;
  longitude: number;
}

export interface AppConfig {
  /** "" means same-origin (this frontend's own host:port). */
  apiBaseUrl: string;
  restFlightsUrl: string;
  wsUrl: string;
  /** null when VITE_HOME_LATITUDE/VITE_HOME_LONGITUDE are unset/invalid -- home marker and recenter are simply not shown. */
  home: HomePoint | null;
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

function resolveHome(): HomePoint | null {
  const latRaw = import.meta.env.VITE_HOME_LATITUDE;
  const lonRaw = import.meta.env.VITE_HOME_LONGITUDE;
  if (latRaw === undefined || lonRaw === undefined) return null;
  const latitude = Number(latRaw);
  const longitude = Number(lonRaw);
  if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
  return { latitude, longitude };
}

export function loadConfig(): AppConfig {
  const apiBaseUrl = resolveApiBaseUrl();
  const home = resolveHome();
  if (!home) {
    // eslint has no presence in this project; a plain console.warn is the
    // simplest way to surface a misconfigured deployment without
    // crashing the map itself.
    console.warn(
      "VITE_HOME_LATITUDE/VITE_HOME_LONGITUDE not set (or not valid numbers) -- " +
        "the home marker and recenter button will be unavailable.",
    );
  }
  return {
    apiBaseUrl,
    restFlightsUrl: `${apiBaseUrl}/api/flights`,
    wsUrl: resolveWsUrl(apiBaseUrl),
    home,
  };
}
