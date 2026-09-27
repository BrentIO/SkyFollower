// Persists the four control-panel toggles (History, Labels, Map Labels,
// Range Outline -- see components/ControlsPanel.tsx) to this browser's
// localStorage, so a reload restores whatever the operator last set rather
// than resetting to hardcoded defaults every time. Scoped to this browser
// only -- localStorage never syncs across devices/accounts, which matches
// what's being asked (respect the operator's choice on reload in the same
// browser).
//
// The key is versioned so a future change to this shape can be detected
// (mismatched version) and ignored rather than silently misapplied -- see
// loadPersistedControls() below.

const STORAGE_KEY = "skyfollower-map:controls:v1";
// Bumped whenever PersistedControls' shape changes -- a stored payload
// missing new fields fails isPersistedControls below even without the
// version check, but bumping the version is this file's own documented
// mechanism for a shape change and costs nothing extra.
const STORAGE_VERSION = 4;

export interface PersistedControls {
  historyAll: boolean;
  labelsAll: boolean;
  mapLabelsOn: boolean;
  rangeOutlineVisible: boolean;
  /** The static 100/150/200nmi rings (lib/rangeRings.ts). Defaults true so
   * an operator who never touches the Settings-panel toggle sees no
   * change from the previous always-on behavior. */
  rangeRingsVisible: boolean;
  radarOn: boolean;
  radarOpacity: number;
  /** Multiplies the aircraft icon-size expression and the info box's
   * rendered size (see MapView.tsx/InfoBoxLayer.tsx). 1 is the no-op
   * default. */
  displayScale: number;
}

// Today's hardcoded defaults, matching MapView.tsx's own literal
// useState() defaults. These apply only when no stored payload exists at
// all; an existing stored value is never overwritten just because it
// matches an old default.
const DEFAULTS: PersistedControls = {
  historyAll: false,
  labelsAll: false,
  mapLabelsOn: false,
  rangeOutlineVisible: false,
  rangeRingsVisible: true,
  radarOn: false,
  radarOpacity: 0.5,
  displayScale: 1,
};

interface StoredShape {
  version: number;
  controls: PersistedControls;
}

function isBoolean(value: unknown): value is boolean {
  return typeof value === "boolean";
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function isPersistedControls(value: unknown): value is PersistedControls {
  if (!value || typeof value !== "object") return false;
  const v = value as Record<string, unknown>;
  return (
    isBoolean(v.historyAll) &&
    isBoolean(v.labelsAll) &&
    isBoolean(v.mapLabelsOn) &&
    isBoolean(v.rangeOutlineVisible) &&
    isBoolean(v.rangeRingsVisible) &&
    isBoolean(v.radarOn) &&
    isFiniteNumber(v.radarOpacity) &&
    isFiniteNumber(v.displayScale)
  );
}

// Reads and parses the persisted controls, falling back to DEFAULTS for
// anything missing, malformed, under a different version, or if
// localStorage itself throws (private browsing, blocked site data, quota
// weirdness on read). Never throws.
export function loadPersistedControls(): PersistedControls {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return { ...DEFAULTS };

    const parsed = JSON.parse(raw) as Partial<StoredShape> | null;
    if (!parsed || typeof parsed !== "object") return { ...DEFAULTS };
    if (parsed.version !== STORAGE_VERSION) return { ...DEFAULTS };
    if (!isPersistedControls(parsed.controls)) return { ...DEFAULTS };

    return { ...parsed.controls };
  } catch {
    return { ...DEFAULTS };
  }
}

// Serializes and writes the controls, silently swallowing any failure
// (quota exceeded, blocked storage) rather than surfacing it to the
// operator -- a failed save just means the next reload falls back to
// defaults/whatever was last successfully saved, same as if this were
// never called.
export function savePersistedControls(controls: PersistedControls): void {
  try {
    const stored: StoredShape = { version: STORAGE_VERSION, controls };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(stored));
  } catch {
    // Intentionally ignored -- see comment above.
  }
}
