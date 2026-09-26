import { useState } from "react";
import { PAUSE_ICON, PLAY_ICON, ROUTE_ICON, SETTINGS_ICON, TAGS_ICON, WEATHER_RADAR_ICON } from "../lib/actionIcons";
import { crosshairSvgMarkup } from "../lib/crosshairIcon";
import { fullscreenIcon } from "../lib/fullscreen";
import { MAX_LABEL_Z_INDEX } from "../lib/labelStackOrder";
import { toggleButtonClass } from "../lib/toggleButtonStyle";
import { IconButton } from "./IconButton";
import { SettingsPanel } from "./SettingsPanel";

export interface ControlsPanelProps {
  historyAll: boolean;
  onToggleHistoryAll: () => void;
  labelsAll: boolean;
  onToggleLabelsAll: () => void;
  /** Basemap's own text labels (place names, road names/shields, water
   * names, airport labels) -- distinct from `labelsAll`, which is about
   * aircraft info boxes. Defaults on (basemap unchanged out of the box);
   * turning it off is what hides the basemap's text. #2012 moved this from
   * its own standalone icon button into a phone-style switch inside the new
   * Settings panel -- the prop itself is unchanged. */
  mapLabelsOn: boolean;
  onToggleMapLabels: () => void;
  /** Daily reception range outline overlay (envelope band only, see
   * lib/mapLayerIds.ts's RANGE_OUTLINE_* ids). Disabled -- not just
   * unchecked -- when no center is configured, matching `recenterDisabled`:
   * the backend always returns an empty FeatureCollection in that case, so
   * there's nothing to show. #2012 moved this from its own standalone icon
   * button into a switch inside the Settings panel, under its "Range"
   * heading -- the prop itself is unchanged. */
  rangeOutlineVisible: boolean;
  onToggleRangeOutline: () => void;
  rangeOutlineDisabled: boolean;
  /** #2012: the static 100/150/200nmi rings (lib/rangeRings.ts) -- rendered
   * unconditionally whenever a center was configured before this issue, with
   * no on/off control anywhere. New prop, new persisted key
   * (lib/controlsPersistence.ts), same disabled-when-no-center convention as
   * rangeOutlineDisabled. Lives in the Settings panel's "Range" heading,
   * alongside rangeOutlineVisible above. */
  rangeRingsVisible: boolean;
  onToggleRangeRings: () => void;
  rangeRingsDisabled: boolean;
  onRecenter: () => void;
  recenterDisabled: boolean;
  /** True whenever the camera is currently centered on the configured
   * center point -- see lib/mapCentered.ts's isWithinCenterTolerance, kept
   * live via MapView.tsx's `load`/`moveend` listeners (#1847). Drives the
   * button's toggle-active styling via `toggleButtonClass()`, same
   * convention as every other toggle in this panel; irrelevant (and never
   * computed) while `recenterDisabled` is true. */
  recenterActive: boolean;
  /** Whole-page Fullscreen API toggle -- true once
   * `document.fullscreenElement` is set, kept in sync via a
   * `fullscreenchange` listener in MapView.tsx rather than only optimistic
   * state, since Esc/F11/OS gestures exit fullscreen without going through
   * `onToggleFullscreen`. */
  fullscreen: boolean;
  onToggleFullscreen: () => void;
  /** Mirrors `rangeOutlineDisabled`'s convention: the button always
   * renders, just disabled, rather than being hidden outright, when
   * `document.fullscreenEnabled` is false (some embedded/iframe contexts
   * and older Safari versions). */
  fullscreenDisabled: boolean;
  /** Live weather radar overlay (#1896, see lib/radar.ts). `radarOn` is
   * the toggle-active state driving the icon's own coloring, distinct from
   * this component's local `radarExpanded` state (whether the popover
   * showing the toggle/opacity/play controls is open) -- opening the
   * popover doesn't itself turn the layer on, matching every other
   * IconButton's `active` meaning "the feature is on," not "its controls
   * are visible." */
  radarOn: boolean;
  onToggleRadar: () => void;
  /** 0-1, applied live via `raster-opacity` -- see lib/controlsPersistence.ts
   * for why 0.2 is the default. #2012 moved the slider itself into the
   * Settings panel's "Radar" heading; the on/off switch and Play/Pause
   * button below stay in this popover for now (see the separate
   * radar-button issue this panel's docstring points at). */
  radarOpacity: number;
  onRadarOpacityChange: (value: number) => void;
  /** Whether the last-30-minutes playback loop is currently animating.
   * Disabled (not hidden) whenever `radarOn` is false, matching
   * `rangeOutlineDisabled`'s convention -- play/pause is meaningless with
   * no radar layer to animate. */
  radarPlaying: boolean;
  onToggleRadarPlaying: () => void;
  /** #1910: true only while playback's frame-prefetch phase is in
   * progress -- shows a spinner on the Play button instead of the
   * play/pause icon, and disables it, so the pause before the loop
   * visibly starts reads as "loading," not a stalled click. */
  radarPlaybackLoading: boolean;
  /** #2000: multiplies both the aircraft icon-size expression (MapView.tsx)
   * and the info box's rendered size (InfoBoxLayer.tsx, via a CSS
   * transform) -- an operator-facing fix for a display where the fixed
   * CSS-pixel defaults render too large. A display's true physical pixel
   * density can't be read from the browser (see the issue's own research),
   * so this is a manual control rather than an automatic one, persisted
   * per-browser the same way radarOpacity is. 1 is the no-op default. #2012
   * moved this from its own standalone popover into the Settings panel,
   * relabeled "Text & Icon Size" -- same range/behavior, prop unchanged. */
  displayScale: number;
  onDisplayScaleChange: (value: number) => void;
}

// Top-right floating controls: one unified vertically stacked column mixing
// the recenter button (its click, `onRecenter`, is still a momentary
// one-shot action, not an on/off toggle -- but its *appearance* now follows
// this panel's shared toggle-button convention, reflecting whether the
// camera happens to already be centered; see `recenterActive`, #1847) with
// the icon buttons for the "Fullscreen", "Labels", "Trails", "Radar", and
// "Settings" toggles, in that top-to-bottom order. Icon buttons share the
// exact rendering mechanism (IconButton, toggleButtonClass coloring) as
// AircraftDetailPanel's action row, sized via IconButton's "md" size prop to
// match the recenter button's own h-9 w-9 -- AircraftDetailPanel's row keeps
// IconButton's default size and its own horizontal layout, unrelated to
// this column. The recenter button can't use IconButton itself -- its
// crosshair icon is rendered via `dangerouslySetInnerHTML`
// (lib/crosshairIcon.ts's dashed-circle markup, not expressible as an
// IconButton `IconSpec`) -- so it imports `toggleButtonClass()` directly
// instead, applying the exact same active/inactive classes IconButton does.
//
// #2012: Range Outline, Map Labels, and Display Scale used to each be their
// own standalone button (the latter two behind small popovers) -- all three
// moved into the new Settings panel below (components/SettingsPanel.tsx),
// along with a brand-new Range Rings toggle and Radar's opacity slider. This
// column now ends in Radar (its on/off switch + opacity + Play/Pause popover
// left as-is for this issue -- see the separate radar-button issue for that
// rework) followed by the new Settings button.
//
// The connection-status dot that used to float here (its own top-2/right-2
// wrapper, independent of this column's top-4/right-4 inset) has moved into
// AircraftListPanel's header, next to the aircraft count -- see the issue
// that relocated it after this corner needed repeated z-index/position
// fixes (#1768, #1789) as the icon column below kept changing shape.
export function ControlsPanel({
  historyAll,
  onToggleHistoryAll,
  labelsAll,
  onToggleLabelsAll,
  mapLabelsOn,
  onToggleMapLabels,
  rangeOutlineVisible,
  onToggleRangeOutline,
  rangeOutlineDisabled,
  rangeRingsVisible,
  onToggleRangeRings,
  rangeRingsDisabled,
  onRecenter,
  recenterDisabled,
  recenterActive,
  fullscreen,
  onToggleFullscreen,
  fullscreenDisabled,
  radarOn,
  onToggleRadar,
  radarOpacity,
  onRadarOpacityChange,
  radarPlaying,
  onToggleRadarPlaying,
  radarPlaybackLoading,
  displayScale,
  onDisplayScaleChange,
}: ControlsPanelProps) {
  // Purely local, transient UI state -- whether the radar popover is open.
  // Not lifted to MapView/persisted: unlike radarOn/radarOpacity, this
  // isn't an operator preference worth restoring on reload (matching
  // AircraftListPanel's own open/closed drawer state, which also resets
  // fresh each load).
  const [radarExpanded, setRadarExpanded] = useState(false);
  // Same rationale as radarExpanded above -- only whether the Settings panel
  // is open is transient/local; every value it shows/edits is itself already
  // lifted to MapView and persisted there.
  const [settingsExpanded, setSettingsExpanded] = useState(false);

  return (
    <div
      className="pointer-events-none absolute top-4 right-4 flex flex-col items-end gap-2"
      // #1953: pinned above every InfoBoxLayer label box, same
      // MAX_LABEL_Z_INDEX + 1 convention AircraftDetailPanel/
      // AircraftListPanel use -- without this, an info box with a high
      // altitude-derived z-index (up to MAX_LABEL_Z_INDEX itself) paints
      // over whichever button in this column it happens to overlap.
      // #1909 fixed only the radar popover this way; this covers the
      // whole column (including the popover, whose own zIndex below is
      // now redundant but harmless).
      style={{ zIndex: MAX_LABEL_Z_INDEX + 1 }}
    >
      <div className="pointer-events-auto flex flex-col gap-2">
        <IconButton
          label={fullscreen ? "Exit Fullscreen" : "Fullscreen"}
          icon={fullscreenIcon(fullscreen)}
          active={fullscreen}
          onClick={onToggleFullscreen}
          disabled={fullscreenDisabled}
          size="md"
        />
        <button
          type="button"
          onClick={onRecenter}
          disabled={recenterDisabled}
          title="Return to center"
          aria-label="Return to center"
          aria-pressed={recenterActive}
          className={`flex h-9 w-9 items-center justify-center rounded border transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${toggleButtonClass(recenterActive)}`}
          // Same crosshair markup as the on-map center marker -- see
          // lib/crosshairIcon.ts's docstring for why they must stay
          // visually identical. "currentColor" lets the button's own
          // text-color classes (toggleButtonClass's active/inactive
          // variants, light/dark) drive the icon color, unlike the center
          // marker which passes a fixed color of its own.
          dangerouslySetInnerHTML={{ __html: crosshairSvgMarkup(20, "currentColor") }}
        />
        <IconButton label="Labels" icon={TAGS_ICON} active={labelsAll} onClick={onToggleLabelsAll} size="md" />
        <IconButton
          label="Trails"
          icon={ROUTE_ICON}
          active={historyAll}
          onClick={onToggleHistoryAll}
          size="md"
        />
        <div className="relative">
          <IconButton
            label="Radar"
            icon={WEATHER_RADAR_ICON}
            active={radarOn}
            onClick={() => setRadarExpanded((prev) => !prev)}
            size="md"
          />
          {radarExpanded && (
            <div
              className="absolute top-0 right-full mr-2 flex w-48 flex-col gap-3 rounded-md border border-slate-200 bg-white p-3 shadow-md dark:border-slate-700 dark:bg-slate-900"
              // Popover content is its own click surface, independent of
              // the toggle button beside it -- no outside-click-to-close
              // handling here, matching this codebase's other disclosure
              // (AircraftListPanel's drawer also only closes on its own
              // explicit toggle). No zIndex needed here (#1953) -- the
              // outer wrapper above already covers this popover.
            >
              <div className="flex items-center justify-between">
                <span className="text-sm text-slate-700 dark:text-slate-200">Radar</span>
                {/* Phone-style toggle switch, replacing the bordered On/Off
                    text button -- radar-specific, not a shared component
                    (a deliberate scope decision, not an oversight).
                    role="switch"/aria-checked is the correct ARIA pattern
                    for this control shape, matching (and improving on) the
                    aria-pressed convention every other on/off button here
                    still uses. */}
                <button
                  type="button"
                  onClick={onToggleRadar}
                  role="switch"
                  aria-checked={radarOn}
                  aria-label={radarOn ? "Turn radar off" : "Turn radar on"}
                  className={`relative inline-flex h-5 w-9 shrink-0 items-center rounded-full transition-colors ${
                    radarOn ? "bg-blue-600" : "bg-slate-300 dark:bg-slate-600"
                  }`}
                >
                  <span
                    className={`inline-block h-4 w-4 rounded-full bg-white shadow transition-transform ${
                      radarOn ? "translate-x-4" : "translate-x-0.5"
                    }`}
                  />
                </button>
              </div>
              {/* #2012: the opacity slider that used to live here moved into
                  the Settings panel's "Radar" heading -- this popover now
                  holds only the on/off switch above and the Play/Pause
                  button below. */}
              <IconButton
                label={radarPlaybackLoading ? "Loading radar frames" : radarPlaying ? "Pause" : "Play"}
                icon={radarPlaying ? PAUSE_ICON : PLAY_ICON}
                active={radarPlaying}
                onClick={onToggleRadarPlaying}
                disabled={!radarOn}
                loading={radarPlaybackLoading}
                size="md"
              />
            </div>
          )}
        </div>
        {/* #2012: consolidates Map Labels, Text & Icon Size, Range Outline,
            the new Range Rings toggle, and Radar's opacity slider into one
            panel -- same popover-behind-an-icon-button shape as Radar
            above, just styled/sized like AircraftDetailPanel instead of a
            small popover (see SettingsPanel.tsx). The icon reads "active"
            while the panel is open, matching a disclosure button rather
            than an on/off feature (Settings itself has no on/off state of
            its own -- everything it holds is its own independent toggle). */}
        <div className="relative">
          <IconButton
            label="Settings"
            icon={SETTINGS_ICON}
            active={settingsExpanded}
            onClick={() => setSettingsExpanded((prev) => !prev)}
            size="md"
          />
          {settingsExpanded && (
            <SettingsPanel
              onClose={() => setSettingsExpanded(false)}
              mapLabelsOn={mapLabelsOn}
              onToggleMapLabels={onToggleMapLabels}
              displayScale={displayScale}
              onDisplayScaleChange={onDisplayScaleChange}
              rangeOutlineVisible={rangeOutlineVisible}
              onToggleRangeOutline={onToggleRangeOutline}
              rangeOutlineDisabled={rangeOutlineDisabled}
              rangeRingsVisible={rangeRingsVisible}
              onToggleRangeRings={onToggleRangeRings}
              rangeRingsDisabled={rangeRingsDisabled}
              radarOpacity={radarOpacity}
              onRadarOpacityChange={onRadarOpacityChange}
            />
          )}
        </div>
      </div>
    </div>
  );
}
