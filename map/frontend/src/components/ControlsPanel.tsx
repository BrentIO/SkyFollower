import { RADAR_ICON, ROUTE_ICON, TAGS_ICON, TYPE_ICON } from "../lib/actionIcons";
import { crosshairSvgMarkup } from "../lib/crosshairIcon";
import { fullscreenIcon } from "../lib/fullscreen";
import { connectionTooltip, overallConnectionStatus, PROCESSOR_STATUS_DOT_COLOR } from "../lib/processorStatus";
import type { ProcessorRoster } from "../api/types";
import { IconButton } from "./IconButton";

export interface ControlsPanelProps {
  /** The browser's own WebSocket connection to this map backend -- distinct
   * from `roster`, which is the message-processor liveness roster *that
   * backend* has derived from UDP traffic. If this is false there is no
   * live proof of anything, so the indicator renders red regardless of the
   * last-known roster snapshot. */
  wsConnected: boolean;
  roster: ProcessorRoster;
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

// Top-right floating controls: a bare connection-status dot (no card/
// background of its own -- see the issue that moved the aircraft count out
// of this component into AircraftListPanel's header, leaving the dot as
// an independent floating indicator), and -- below that -- one unified
// vertically stacked column mixing the recenter button (its own
// separately-styled square control -- a momentary action, not an on/off
// toggle) with the icon buttons for the "Fullscreen", "Labels", "Trails",
// "Range Outline", and "Map Labels" toggles, in that top-to-bottom order.
// Icon buttons share the exact rendering mechanism (IconButton,
// toggleButtonClass coloring) as AircraftDetailPanel's action row, sized
// via IconButton's "md" size prop to match the recenter button's own
// h-9 w-9 -- AircraftDetailPanel's row keeps IconButton's default size and
// its own horizontal layout, unrelated to this column.
export function ControlsPanel({
  wsConnected,
  roster,
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
  fullscreen,
  onToggleFullscreen,
  fullscreenDisabled,
}: ControlsPanelProps) {
  const overallStatus = overallConnectionStatus(wsConnected, roster);

  return (
    <>
      {/* Bare floating indicator -- no card/background, per the issue that
          detached this from the aircraft count (now in AircraftListPanel's
          header). Positioned independently of the icon column below (its
          own top-2/right-2 inset, half the column's top-4/right-4) so it
          sits tucked into the true corner -- equidistant from both edges --
          rather than inheriting the column's shared alignment. The h-8 w-8
          button gives it a comfortable hover/tap hit area around the
          visually small 2.5x2.5 dot. */}
      <span
        title={connectionTooltip(wsConnected, roster)}
        className="pointer-events-auto absolute top-2 right-2 flex h-8 w-8 items-center justify-center"
      >
        <span className={`h-2.5 w-2.5 rounded-full ${PROCESSOR_STATUS_DOT_COLOR[overallStatus]}`} />
      </span>

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
            className="flex h-9 w-9 items-center justify-center rounded-md bg-white/90 text-slate-700 shadow-md hover:bg-white disabled:cursor-not-allowed disabled:opacity-50 dark:bg-slate-900/90 dark:text-white dark:hover:bg-slate-900"
            // Same crosshair markup as the on-map center marker -- see
            // lib/crosshairIcon.ts's docstring for why they must stay
            // visually identical. "currentColor" lets the button's own
            // text-color classes (light/dark) drive the icon color, unlike
            // the center marker which passes a fixed color of its own.
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
    </>
  );
}
