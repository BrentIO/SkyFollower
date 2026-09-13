// Persists the aircraft list drawer's operator-resized width (#1784) to
// this browser's localStorage -- same pattern as controlsPersistence.ts
// (versioned blob, safe parse/fallback on anything missing or malformed,
// silently swallow write failures), a separate small module rather than
// folding into controlsPersistence.ts, which is explicitly scoped to
// ControlsPanel's own four toggles. Scoped to this browser only --
// localStorage never syncs across devices/accounts.

const STORAGE_KEY = "skyfollower-map:aircraft-list-panel:v1";
const STORAGE_VERSION = 1;

// Matches AircraftListPanel.tsx's original fixed PANEL_WIDTH_PX -- "wide
// enough for all six columns without wrapping at a normal viewport width".
export const DEFAULT_PANEL_WIDTH_PX = 720;

// A static floor/ceiling independent of the live viewport -- see
// AircraftListPanel.tsx's own additional viewport-aware clamp during an
// active drag, which keeps the map area from being squeezed to nothing on
// a narrow browser window. MIN keeps the six columns from wrapping/
// overlapping; MAX is just "wide enough that going further stops being
// useful", not a hard technical limit.
export const MIN_PANEL_WIDTH_PX = 480;
export const MAX_PANEL_WIDTH_PX = 1200;

interface StoredShape {
  version: number;
  widthPx: number;
}

export function clampPanelWidth(widthPx: number): number {
  return Math.min(MAX_PANEL_WIDTH_PX, Math.max(MIN_PANEL_WIDTH_PX, widthPx));
}

// Reads and parses the persisted width, falling back to
// DEFAULT_PANEL_WIDTH_PX for anything missing, malformed, under a
// different version, or if localStorage itself throws (private browsing,
// blocked site data, quota weirdness on read). Never throws. The
// fallback's own result is still run through clampPanelWidth, in case a
// future change lowers MIN/MAX below what an older DEFAULT_PANEL_WIDTH_PX
// used to be.
export function loadPersistedPanelWidth(): number {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return clampPanelWidth(DEFAULT_PANEL_WIDTH_PX);

    const parsed = JSON.parse(raw) as Partial<StoredShape> | null;
    if (!parsed || typeof parsed !== "object") return clampPanelWidth(DEFAULT_PANEL_WIDTH_PX);
    if (parsed.version !== STORAGE_VERSION) return clampPanelWidth(DEFAULT_PANEL_WIDTH_PX);
    if (typeof parsed.widthPx !== "number" || !Number.isFinite(parsed.widthPx)) {
      return clampPanelWidth(DEFAULT_PANEL_WIDTH_PX);
    }

    return clampPanelWidth(parsed.widthPx);
  } catch {
    return clampPanelWidth(DEFAULT_PANEL_WIDTH_PX);
  }
}

// Serializes and writes the width, silently swallowing any failure (quota
// exceeded, blocked storage) rather than surfacing it to the operator --
// matches controlsPersistence.ts's savePersistedControls convention.
export function savePersistedPanelWidth(widthPx: number): void {
  try {
    const stored: StoredShape = { version: STORAGE_VERSION, widthPx: clampPanelWidth(widthPx) };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(stored));
  } catch {
    // Intentionally ignored -- see comment above.
  }
}
