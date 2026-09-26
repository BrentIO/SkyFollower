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
  /** Live weather radar overlay (#1896, see lib/radar.ts). #2015 collapsed
   * the old separate on/off switch + Play/Pause button (and their popover)
   * into this one tri-state value -- one click cycles
   * off -> on -> animate -> off, driving both the button's active styling
   * (on and animate both read active) and which icon it shows (the JSX
   * below swaps in PAUSE_ICON once actually animating). No popover, no
   * `radarExpanded`-style local state anymore -- the button *is* the
   * whole control. */
  radarState: RadarState;
  onCycleRadar: () => void;
  /** 0-1, applied live via `raster-opacity` -- see lib/controlsPersistence.ts
   * for why 0.2 is the default. Lives in the Settings panel's "Radar"
   * heading (#2012); this component only forwards it through. */
  radarOpacity: number;
  onRadarOpacityChange: (value: number) => void;
  /** #1910: true only while animate's frame-prefetch phase is in progress
   * -- shows a spinner on the Radar button instead of its icon. #2015:
   * unlike every other IconButton `loading` caller, this one must stay
   * clickable through the spinner (see `loadingDisabled={false}` below) --
   * the operator can cycle straight out of animate (e.g. back to "off")
   * without waiting for prefetch to finish or time out. */
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
// column now ends in Radar (#2015 collapsed its old on/off switch + Play/
// Pause popover into a single tri-state button, no popover at all) followed
// by the new Settings button.
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
  // open/closed disclosure state is local (matching AircraftListPanel's
  // own open/closed drawer state, which also resets fresh each load).
  // #2015 removed this component's other local disclosure state
  // (`radarExpanded`) along with the Radar popover it gated -- the new
  // tri-state Radar button below needs no local state of its own.
  const [settingsExpanded, setSettingsExpanded] = useState(false);

  return (
    <div
      className="pointer-events-none absolute top-4 right-4 flex flex-col items-end gap-2"
      // #1953: pinned above every InfoBoxLayer label box, same
      // MAX_LABEL_Z_INDEX + 1 convention AircraftDetailPanel/
      // AircraftListPanel use -- without this, an info box with a high
      // altitude-derived z-index (up to MAX_LABEL_Z_INDEX itself) paints
      // over whichever button in this column it happens to overlap.
      // #1909 originally fixed only the radar popover this way; this
      // covers the whole column (the Radar popover itself is gone as of
      // #2015 -- the Settings popover below is the only one left, and
      // relies on this same wrapper rather than setting its own zIndex).
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
        {/* #2015: one button, three states (off -> on -> animate -> off on
            each click), no popover -- opacity (its only remaining
            popover content pre-#2015) already moved to Settings in
            #2012. `loadingDisabled={false}` is the load-bearing part:
            every other IconButton `loading` caller lets it disable the
            button too, but this one must stay clickable through
            animate's prefetch spinner so the operator can cycle straight
            back out (e.g. to "off") without waiting. */}
        <IconButton
          label={radarButtonLabel(radarState, radarPlaybackLoading)}
          icon={radarState === "animate" && !radarPlaybackLoading ? PAUSE_ICON : WEATHER_RADAR_ICON}
          active={radarState !== "off"}
          onClick={onCycleRadar}
          loading={radarState === "animate" && radarPlaybackLoading}
          loadingDisabled={false}
          size="md"
        />
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

// #2015: title/aria-label text for the tri-state Radar button, covering
// all four visually distinct moments -- off, on, animate-loading (spinner),
// and animate-playing (PAUSE_ICON, "click to stop") -- so a screen reader
// or hover tooltip announces the same distinction the icon/spinner swap
// conveys visually.
function radarButtonLabel(state: RadarState, loading: boolean): string {
  if (state === "off") return "Radar off (click to turn on)";
  if (state === "on") return "Radar on (click to animate)";
  return loading ? "Radar animating: loading frames (click to stop)" : "Radar animating (click to stop)";
}
