import { RADAR_ICON, ROUTE_ICON, TAGS_ICON, TYPE_ICON } from "../lib/actionIcons";
import { crosshairSvgMarkup } from "../lib/crosshairIcon";
import { fullscreenIcon } from "../lib/fullscreen";
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
}: ControlsPanelProps) {
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
      </div>
    </div>
  );
}
