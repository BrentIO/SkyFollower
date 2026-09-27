import { useState } from "react";
import { PAUSE_ICON, ROUTE_ICON, SETTINGS_ICON, TAGS_ICON, WEATHER_RADAR_ICON } from "../lib/actionIcons";
import { crosshairSvgMarkup } from "../lib/crosshairIcon";
import { fullscreenIcon } from "../lib/fullscreen";
import { MAX_LABEL_Z_INDEX } from "../lib/labelStackOrder";
import type { RadarState } from "../lib/radar";
import { toggleButtonClass } from "../lib/toggleButtonStyle";
import { IconButton } from "./IconButton";
import { SettingsPanel } from "./SettingsPanel";

export interface ControlsPanelProps {
  historyAll: boolean;
  onToggleHistoryAll: () => void;
  labelsAll: boolean;
  onToggleLabelsAll: () => void;
  /** Basemap's own text labels -- distinct from `labelsAll`, which is about
   * aircraft info boxes. Defaults on; turning it off hides the basemap's
   * text. */
  mapLabelsOn: boolean;
  onToggleMapLabels: () => void;
  /** Daily reception range outline overlay. Disabled -- not just
   * unchecked -- when no center is configured, matching `recenterDisabled`:
   * the backend always returns an empty FeatureCollection in that case. */
  rangeOutlineVisible: boolean;
  onToggleRangeOutline: () => void;
  rangeOutlineDisabled: boolean;
  /** The static 100/150/200nmi rings (lib/rangeRings.ts). Same
   * disabled-when-no-center convention as rangeOutlineDisabled. */
  rangeRingsVisible: boolean;
  onToggleRangeRings: () => void;
  rangeRingsDisabled: boolean;
  onRecenter: () => void;
  recenterDisabled: boolean;
  /** True whenever the camera is currently centered on the configured
   * center point -- see lib/mapCentered.ts's isWithinCenterTolerance.
   * Drives the button's toggle-active styling; irrelevant while
   * `recenterDisabled` is true. */
  recenterActive: boolean;
  /** Whole-page Fullscreen API toggle -- true once
   * `document.fullscreenElement` is set, kept in sync via a
   * `fullscreenchange` listener in MapView.tsx rather than only optimistic
   * state, since Esc/F11/OS gestures exit fullscreen without going through
   * `onToggleFullscreen`. */
  fullscreen: boolean;
  onToggleFullscreen: () => void;
  /** Mirrors `rangeOutlineDisabled`'s convention: the button always
   * renders, just disabled, when `document.fullscreenEnabled` is false. */
  fullscreenDisabled: boolean;
  /** Live weather radar overlay (see lib/radar.ts). One click cycles
   * off -> on -> animate -> off, driving both the button's active styling
   * (on and animate both read active) and which icon it shows (the JSX
   * below swaps in PAUSE_ICON once actually animating). No popover, no
   * local disclosure state -- the button *is* the whole control. */
  radarState: RadarState;
  onCycleRadar: () => void;
  /** 0-1, applied live via `raster-opacity`. Lives in the Settings panel's
   * "Radar" heading; this component only forwards it through. */
  radarOpacity: number;
  onRadarOpacityChange: (value: number) => void;
  /** True only while animate's frame-prefetch phase is in progress --
   * shows a spinner on the Radar button instead of its icon. Unlike every
   * other IconButton `loading` caller, this one must stay clickable
   * through the spinner (see `loadingDisabled={false}` below) -- the
   * operator can cycle straight out of animate without waiting. */
  radarPlaybackLoading: boolean;
  /** Multiplies both the aircraft icon-size expression (MapView.tsx) and
   * the info box's rendered size (InfoBoxLayer.tsx). A display's true
   * physical pixel density can't be read from the browser, so this is a
   * manual control rather than an automatic one. 1 is the no-op default. */
  displayScale: number;
  onDisplayScaleChange: (value: number) => void;
}

// Top-right floating controls: one unified vertically stacked column mixing
// the recenter button (a momentary one-shot action whose *appearance*
// follows this panel's shared toggle-button convention, reflecting whether
// the camera happens to already be centered) with icon buttons for the
// "Fullscreen", "Labels", "Trails", "Radar", and "Settings" toggles, in
// that top-to-bottom order. Icon buttons share the exact rendering
// mechanism (IconButton, toggleButtonClass coloring) as
// AircraftDetailPanel's action row, sized via IconButton's "md" size prop
// to match the recenter button's own h-9 w-9. The recenter button can't
// use IconButton itself -- its crosshair icon is rendered via
// `dangerouslySetInnerHTML` (lib/crosshairIcon.ts's dashed-circle markup,
// not expressible as an IconButton `IconSpec`) -- so it imports
// `toggleButtonClass()` directly instead.
//
// Range Outline, Map Labels, and Display Scale live in the Settings panel
// below (components/SettingsPanel.tsx), along with Range Rings and Radar's
// opacity slider. This column ends in Radar (a single tri-state button,
// no popover) followed by the Settings button.
//
// The connection-status dot has moved into AircraftListPanel's header,
// next to the aircraft count, after this corner needed repeated
// z-index/position fixes as the icon column below kept changing shape.
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
  radarState,
  onCycleRadar,
  radarOpacity,
  onRadarOpacityChange,
  radarPlaybackLoading,
  displayScale,
  onDisplayScaleChange,
}: ControlsPanelProps) {
  // Purely local, transient UI state -- whether the Settings panel is
  // open. Not lifted to MapView/persisted: every value the panel itself
  // shows/edits is already lifted and persisted there; only its own
  // open/closed disclosure state is local. The tri-state Radar button
  // needs no local state of its own -- no popover to gate.
  const [settingsExpanded, setSettingsExpanded] = useState(false);

  return (
    <div
      className="pointer-events-none absolute top-4 right-4 flex flex-col items-end gap-2"
      // Pinned above every InfoBoxLayer label box, same MAX_LABEL_Z_INDEX + 1
      // convention AircraftDetailPanel/AircraftListPanel use -- without
      // this, a high-z-index info box paints over whichever button in this
      // column it happens to overlap. The Settings popover below relies on
      // this same wrapper rather than setting its own zIndex.
      style={{ zIndex: MAX_LABEL_Z_INDEX + 1 }}
    >
      {/* `relative` here (not per-button) is load-bearing: it anchors
          SettingsPanel to this column's fixed top edge rather than to
          wherever its trigger button sits, so the space below it never
          depends on button order (#2031). */}
      <div className="pointer-events-auto relative flex flex-col gap-2">
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
          // text-color classes drive the icon color, unlike the center
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
        {/* One button, three states (off -> on -> animate -> off on each
            click), no popover. `loadingDisabled={false}` is the
            load-bearing part: every other IconButton `loading` caller lets
            it disable the button too, but this one must stay clickable
            through animate's prefetch spinner so the operator can cycle
            straight back out (e.g. to "off") without waiting. */}
        <IconButton
          label={radarButtonLabel(radarState, radarPlaybackLoading)}
          icon={radarState === "animate" && !radarPlaybackLoading ? PAUSE_ICON : WEATHER_RADAR_ICON}
          active={radarState !== "off"}
          onClick={onCycleRadar}
          loading={radarState === "animate" && radarPlaybackLoading}
          loadingDisabled={false}
          size="md"
        />
        {/* Consolidates Map Labels, Text & Icon Size, Range Outline, Range
            Rings, and Radar's opacity slider into one panel (see
            SettingsPanel.tsx). The icon reads "active" while the panel is
            open, matching a disclosure button rather than an on/off
            feature -- Settings itself has no on/off state of its own. */}
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
  );
}

// Title/aria-label text for the tri-state Radar button, covering all four
// visually distinct moments -- off, on, animate-loading (spinner), and
// animate-playing (PAUSE_ICON) -- so a screen reader or hover tooltip
// announces the same distinction the icon/spinner swap conveys visually.
function radarButtonLabel(state: RadarState, loading: boolean): string {
  if (state === "off") return "Radar off (click to turn on)";
  if (state === "on") return "Radar on (click to animate)";
  return loading ? "Radar animating: loading frames (click to stop)" : "Radar animating (click to stop)";
}
