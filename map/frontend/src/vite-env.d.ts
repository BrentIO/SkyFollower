/// <reference types="vite/client" />

interface ImportMetaEnv {
  // Base URL of the map backend's REST/WebSocket API, e.g.
  // "http://map-host:8090". Leave unset to use same-origin (the
  // frontend served by the same host as the API) -- see src/lib/config.ts.
  readonly VITE_MAP_API_BASE_URL?: string;
  // Centered reference point ("home") for the on-map marker and the
  // initial camera position -- see src/lib/config.ts.
  readonly VITE_HOME_LATITUDE?: string;
  readonly VITE_HOME_LONGITUDE?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
