/// <reference types="vite/client" />

interface ImportMetaEnv {
  // Base URL of the map backend's REST/WebSocket API, e.g.
  // "http://map-host:80". Leave unset to use same-origin (the
  // frontend served by the same host as the API) -- see src/lib/config.ts.
  readonly VITE_MAP_API_BASE_URL?: string;
  // DEV-ONLY fallback for the centered reference point ("center"). A real
  // build/deployment gets this at runtime from the backend's
  // GET /api/config instead -- see src/lib/config.ts.
  readonly VITE_CENTER_LATITUDE?: string;
  readonly VITE_CENTER_LONGITUDE?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
