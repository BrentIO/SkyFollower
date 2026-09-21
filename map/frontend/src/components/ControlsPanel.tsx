import { useState } from "react";
import { PAUSE_ICON, PLAY_ICON, RADAR_ICON, ROUTE_ICON, TAGS_ICON, TYPE_ICON, WEATHER_RADAR_ICON } from "../lib/actionIcons";
import { crosshairSvgMarkup } from "../lib/crosshairIcon";
import { fullscreenIcon } from "../lib/fullscreen";
import { MAX_LABEL_Z_INDEX } from "../lib/labelStackOrder";
import { toggleButtonClass } from "../lib/toggleButtonStyle";
import { IconButton } from "./IconButton";

export interface ControlsPanelProps {
  historyAll: boolean;
  onToggleHistoryAll: () => void;
  labelsAll: boolean;
  onToggleLabelsAll: () => void;
  /** Basemap's own text labels (place names, road names/shields, water
   * names, airport labels) -- distinct from `labelsAll`, which is about
   * aircraft info boxes. Defaults on (basemap unchanged out of the box);
   * turning it off is what hides the basemap's text. */
  mapLabelsOn: boolean;
  onToggleMapLabels: () => void;
  /** Daily reception range outline overlay (envelope band only, see
   * lib/mapLayerIds.ts's RANGE_OUTLINE_* ids). Disabled -- not just
   * unchecked -- when no center is configured, matching `recenterDisabled`:
   * the backend always returns an empty FeatureCollection in that case, so
   * there's nothing to show. */
  rangeOutlineVisible: boolean;
  onToggleRangeOutline: () => void;
  rangeOutlineDisabled: boolean;
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
   * for why 0.2 is the default. */
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
}

// Top-right floating controls: one unified vertically stacked column mixing
// the recenter button (its click, `onRecenter`, is still a momentary
// one-shot action, not an on/off toggle -- but its *appearance* now follows
// this panel's shared toggle-button convention, reflecting whether the
// camera happens to already be centered; see `recenterActive`, #1847) with
// the icon buttons for the "Fullscreen", "Labels", "Trails", "Range
// Outline", and "Map Labels" toggles, in that top-to-bottom order. Icon
// buttons share the exact rendering mechanism (IconButton, toggleButtonClass
// coloring) as AircraftDetailPanel's action row, sized via IconButton's "md"
// size prop to match the recenter button's own h-9 w-9 -- AircraftDetailPanel's
// row keeps IconButton's default size and its own horizontal layout,
// unrelated to this column. The recenter button can't use IconButton
// itself -- its crosshair icon is rendered via `dangerouslySetInnerHTML`
// (lib/crosshairIcon.ts's dashed-circle markup, not expressible as an
// IconButton `IconSpec`) -- so it imports `toggleButtonClass()` directly
// instead, applying the exact same active/inactive classes IconButton does.
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
}: ControlsPanelProps) {
  // Purely local, transient UI state -- whether the radar popover is open.
  // Not lifted to MapView/persisted: unlike radarOn/radarOpacity, this
  // isn't an operator preference worth restoring on reload (matching
  // AircraftListPanel's own open/closed drawer state, which also resets
  // fresh each load).
  const [radarExpanded, setRadarExpanded] = useState(false);

  return (
    <div className="pointer-events-none absolute top-4 right-4 flex flex-col items-end gap-2">
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
        <IconButton
          label="Range Outline"
          icon={RADAR_ICON}
          active={rangeOutlineVisible}
          onClick={onToggleRangeOutline}
          disabled={rangeOutlineDisabled}
          size="md"
        />
        <IconButton label="Map Labels" icon={TYPE_ICON} active={mapLabelsOn} onClick={onToggleMapLabels} size="md" />
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
              // explicit toggle).
              //
              // #1909: pinned above every InfoBoxLayer label box, same
              // MAX_LABEL_Z_INDEX + 1 convention AircraftDetailPanel/
              // AircraftListPanel already use -- without this, an
              // info box with a high altitude-derived z-index (up to
              // MAX_LABEL_Z_INDEX itself) paints over this popover.
              style={{ zIndex: MAX_LABEL_Z_INDEX + 1 }}
            >
              <div className="flex items-center justify-between">
                <span className="text-sm text-slate-700 dark:text-slate-200">Radar</span>
                {/* #1911: phone-style toggle switch, replacing the bordered
                    On/Off text button -- radar-specific, not a shared
                    component (per Brent's call). role="switch"/aria-checked
                    is the correct ARIA pattern for this control shape,
                    matching (and improving on) the aria-pressed convention
                    every other on/off button here still uses. */}
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
              <label className="flex flex-col gap-1 text-xs text-slate-700 dark:text-slate-200">
                <span>Opacity</span>
                <input
                  type="range"
                  min={0}
                  max={1}
                  step={0.05}
                  value={radarOpacity}
                  disabled={!radarOn}
                  onChange={(e) => onRadarOpacityChange(Number(e.target.value))}
                  aria-label="Radar opacity"
                  className="disabled:opacity-50"
                />
              </label>
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
      </div>
    </div>
  );
}
