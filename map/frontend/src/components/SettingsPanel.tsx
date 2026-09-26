import { DIVIDER, SECTION_BAR } from "./AircraftDetailPanel";

// #2012: consolidates the rarely-touched controls that used to be their own
// standalone icon-column buttons/popovers in ControlsPanel.tsx -- Map
// Labels, Display Scale (relabeled "Text & Icon Size" here), Range Outline,
// and a brand-new Range Rings toggle -- plus Radar's opacity slider (its
// on/off/Play-Pause controls stay in ControlsPanel's own Radar popover for
// now; see the separate radar-button issue this panel's docstring points
// at). Opened from one new "Settings" IconButton in ControlsPanel's column,
// mirroring that column's existing popover-behind-a-button pattern (Radar,
// Display Scale before this change) but sized/styled like
// AircraftDetailPanel instead of a small popover -- headed sections with
// dividers, reusing that component's own SECTION_BAR/DIVIDER constants
// rather than a second, driftable copy of the same classes.
export interface SettingsPanelProps {
  onClose: () => void;
  /** Basemap's own text labels -- see ControlsPanel's own prop doc for the
   * full history; unchanged behavior, just a switch instead of a button. */
  mapLabelsOn: boolean;
  onToggleMapLabels: () => void;
  /** #2000, relabeled "Text & Icon Size" for clarity -- same 0.5-1.5 range
   * and behavior as the popover it replaces. */
  displayScale: number;
  onDisplayScaleChange: (value: number) => void;
  /** Daily reception range outline overlay -- same disabled-when-no-center
   * convention as ControlsPanel's other center-dependent controls. */
  rangeOutlineVisible: boolean;
  onToggleRangeOutline: () => void;
  rangeOutlineDisabled: boolean;
  /** New in #2012: the static 100/150/200nmi rings (lib/rangeRings.ts)
   * previously rendered unconditionally whenever a center was configured,
   * with no on/off control at all. Same disabled-when-no-center convention
   * as rangeOutlineDisabled -- no center means nothing to draw rings
   * around. */
  rangeRingsVisible: boolean;
  onToggleRangeRings: () => void;
  rangeRingsDisabled: boolean;
  /** 0-1, applied live via raster-opacity -- see lib/controlsPersistence.ts
   * for why 0.5 is the default. The on/off switch and Play/Pause button
   * stay in ControlsPanel's own Radar popover; only the slider moved. */
  radarOpacity: number;
  onRadarOpacityChange: (value: number) => void;
}

const ROW_LABEL = "text-sm text-slate-700 dark:text-slate-200";

export function SettingsPanel({
  onClose,
  mapLabelsOn,
  onToggleMapLabels,
  displayScale,
  onDisplayScaleChange,
  rangeOutlineVisible,
  onToggleRangeOutline,
  rangeOutlineDisabled,
  rangeRingsVisible,
  onToggleRangeRings,
  rangeRingsDisabled,
  radarOpacity,
  onRadarOpacityChange,
}: SettingsPanelProps) {
  return (
    // Positioned relative to the Settings button beside it (right-full/mr-2,
    // same idiom as ControlsPanel's existing Radar/Display Scale popovers)
    // rather than a fixed top-4/left-4 like AircraftDetailPanel -- the two
    // panels can plausibly be open at once (this one from the icon column,
    // that one from selecting an aircraft), so anchoring here instead of
    // reusing AircraftDetailPanel's own corner avoids any overlap. No
    // explicit zIndex needed -- ControlsPanel's outer column wrapper already
    // pins every popover above InfoBoxLayer's labels (#1953).
    <div className="absolute top-0 right-full mr-2 w-72 max-h-[70vh] overflow-y-auto rounded-md bg-white text-slate-900 shadow-md dark:bg-slate-900 dark:text-slate-100">
      <div className="flex items-center justify-between gap-3 p-3">
        <div className="text-sm font-bold">Settings</div>
        <button
          type="button"
          onClick={onClose}
          aria-label="Close"
          className="flex h-6 w-6 shrink-0 items-center justify-center rounded text-slate-500 hover:bg-slate-100 dark:text-slate-400 dark:hover:bg-slate-800"
        >
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M4 4L12 12M12 4L4 12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
          </svg>
        </button>
      </div>

      <div className={DIVIDER}>
        <div className={SECTION_BAR}>Map Labels</div>
        <div className="flex items-center justify-between gap-3 px-4 py-2.5">
          <span className={ROW_LABEL}>Basemap Labels</span>
          <ToggleSwitch checked={mapLabelsOn} onChange={onToggleMapLabels} feature="map labels" />
        </div>
      </div>

      <div className={DIVIDER}>
        <div className={SECTION_BAR}>Text &amp; Icon Size</div>
        <div className="px-4 py-3">
          <label className="flex flex-col gap-1 text-xs text-slate-700 dark:text-slate-200">
            <span>Text &amp; Icon Size ({Math.round(displayScale * 100)}%)</span>
            <input
              type="range"
              min={0.5}
              max={1.5}
              step={0.05}
              value={displayScale}
              onChange={(e) => onDisplayScaleChange(Number(e.target.value))}
              aria-label="Text and icon size"
            />
          </label>
        </div>
      </div>

      <div className={DIVIDER}>
        <div className={SECTION_BAR}>Range</div>
        <div className="flex flex-col gap-2.5 px-4 py-2.5">
          <div className="flex items-center justify-between gap-3">
            <span className={ROW_LABEL}>Reception Outline</span>
            <ToggleSwitch
              checked={rangeOutlineVisible}
              onChange={onToggleRangeOutline}
              disabled={rangeOutlineDisabled}
              feature="reception outline"
            />
          </div>
          <div className="flex items-center justify-between gap-3">
            <span className={ROW_LABEL}>Distance Rings</span>
            <ToggleSwitch
              checked={rangeRingsVisible}
              onChange={onToggleRangeRings}
              disabled={rangeRingsDisabled}
              feature="distance rings"
            />
          </div>
        </div>
      </div>

      <div className={DIVIDER}>
        <div className={SECTION_BAR}>Radar</div>
        <div className="px-4 py-3">
          <label className="flex flex-col gap-1 text-xs text-slate-700 dark:text-slate-200">
            <span>Opacity</span>
            <input
              type="range"
              min={0}
              max={1}
              step={0.05}
              value={radarOpacity}
              onChange={(e) => onRadarOpacityChange(Number(e.target.value))}
              aria-label="Radar opacity"
            />
          </label>
        </div>
      </div>
    </div>
  );
}

// Phone-style toggle switch -- copied verbatim (role="switch"/aria-checked,
// sliding thumb via translate-x, bg-blue-600 when on) from ControlsPanel's
// pre-existing Radar on/off switch (#1911), extracted here since this panel
// needs the same shape three times (Map Labels, Reception Outline, Distance
// Rings) instead of once.
function ToggleSwitch({
  checked,
  onChange,
  feature,
  disabled = false,
}: {
  checked: boolean;
  onChange: () => void;
  /** Lowercase feature name, e.g. "map labels" -- interpolated into the
   * aria-label as "Turn {feature} on/off", matching the Radar switch's own
   * aria-label convention. */
  feature: string;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onChange}
      disabled={disabled}
      role="switch"
      aria-checked={checked}
      aria-label={checked ? `Turn ${feature} off` : `Turn ${feature} on`}
      className={`relative inline-flex h-5 w-9 shrink-0 items-center rounded-full transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${
        checked ? "bg-blue-600" : "bg-slate-300 dark:bg-slate-600"
      }`}
    >
      <span
        className={`inline-block h-4 w-4 rounded-full bg-white shadow transition-transform ${
          checked ? "translate-x-4" : "translate-x-0.5"
        }`}
      />
    </button>
  );
}
