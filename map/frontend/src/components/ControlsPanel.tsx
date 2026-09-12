import { RADAR_ICON, ROUTE_ICON, TAGS_ICON, TYPE_ICON } from "../lib/actionIcons";
import { crosshairSvgMarkup } from "../lib/crosshairIcon";
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
  aircraftCount: number;
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
}

// Top-right floating controls: connection-status dot + aircraft-tracked
// count (status box), the recenter button below it (its own separate
// square control), and -- below that -- an icon-button row for the
// "History: All", "Labels: All", "Map Labels", and "Range Outline"
// toggles. Icon buttons share the exact rendering mechanism (IconButton,
// toggleButtonClass coloring) as AircraftDetailPanel's action row. No
// persistent side panel in v1 -- see the issue's Page layout section.
export function ControlsPanel({
  wsConnected,
  roster,
  aircraftCount,
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
}: ControlsPanelProps) {
  const overallStatus = overallConnectionStatus(wsConnected, roster);

  return (
    <div className="pointer-events-none absolute top-4 right-4 flex flex-col items-end gap-2">
      <div className="pointer-events-auto flex flex-col gap-2 rounded-md bg-white/90 p-3 text-sm text-slate-900 shadow-md dark:bg-slate-900/90 dark:text-slate-100">
        <div className="flex items-center gap-2">
          <span
            title={connectionTooltip(wsConnected, roster)}
            className={`h-2.5 w-2.5 rounded-full ${PROCESSOR_STATUS_DOT_COLOR[overallStatus]}`}
          />
          <span className="tabular-nums">{aircraftCount} aircraft</span>
        </div>
      </div>

      <button
        type="button"
        onClick={onRecenter}
        disabled={recenterDisabled}
        title="Return to center"
        aria-label="Return to center"
        className="pointer-events-auto flex h-9 w-9 items-center justify-center rounded-md bg-white/90 text-slate-700 shadow-md hover:bg-white disabled:cursor-not-allowed disabled:opacity-50 dark:bg-slate-900/90 dark:text-white dark:hover:bg-slate-900"
        // Same crosshair markup as the on-map center marker -- see
        // lib/crosshairIcon.ts's docstring for why they must stay
        // visually identical. "currentColor" lets the button's own
        // text-color classes (light/dark) drive the icon color, unlike
        // the center marker which passes a fixed color of its own.
        dangerouslySetInnerHTML={{ __html: crosshairSvgMarkup(20, "currentColor") }}
      />

      <div className="pointer-events-auto flex gap-2">
        <IconButton label="History: All" icon={ROUTE_ICON} active={historyAll} onClick={onToggleHistoryAll} />
        <IconButton label="Labels: All" icon={TAGS_ICON} active={labelsAll} onClick={onToggleLabelsAll} />
        <IconButton label="Map Labels" icon={TYPE_ICON} active={mapLabelsOn} onClick={onToggleMapLabels} />
        <IconButton
          label="Range Outline"
          icon={RADAR_ICON}
          active={rangeOutlineVisible}
          onClick={onToggleRangeOutline}
          disabled={rangeOutlineDisabled}
        />
      </div>
    </div>
  );
}
