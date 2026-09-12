import { crosshairSvgMarkup } from "../lib/crosshairIcon";
import { connectionTooltip, overallConnectionStatus, PROCESSOR_STATUS_DOT_COLOR } from "../lib/processorStatus";
import { toggleButtonClass } from "../lib/toggleButtonStyle";
import type { ProcessorRoster } from "../api/types";

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
  onRecenter: () => void;
  recenterDisabled: boolean;
}

// Top-right floating controls: connection-status dot, aircraft-tracked
// count, the "History: All" and "Labels: All" toggles, and (as its own
// separate square control below the status panel) the recenter button. No
// persistent side panel in v1 -- see the issue's Page layout section.
export function ControlsPanel({
  wsConnected,
  roster,
  aircraftCount,
  historyAll,
  onToggleHistoryAll,
  labelsAll,
  onToggleLabelsAll,
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
        <button
          type="button"
          onClick={onToggleHistoryAll}
          aria-pressed={historyAll}
          className={`rounded border px-2 py-1 text-left text-xs font-medium transition-colors ${toggleButtonClass(historyAll)}`}
        >
          History: All
        </button>
        <button
          type="button"
          onClick={onToggleLabelsAll}
          aria-pressed={labelsAll}
          className={`rounded border px-2 py-1 text-left text-xs font-medium transition-colors ${toggleButtonClass(labelsAll)}`}
        >
          Labels: All
        </button>
      </div>

      <button
        type="button"
        onClick={onRecenter}
        disabled={recenterDisabled}
        title="Recenter on home"
        aria-label="Recenter on home"
        className="pointer-events-auto flex h-9 w-9 items-center justify-center rounded-md bg-white/90 text-slate-700 shadow-md hover:bg-white disabled:cursor-not-allowed disabled:opacity-50 dark:bg-slate-900/90 dark:text-white dark:hover:bg-slate-900"
        // Same crosshair markup as the on-map home marker -- see
        // lib/crosshairIcon.ts's docstring for why they must stay
        // visually identical. "currentColor" lets the button's own
        // text-color classes (light/dark) drive the icon color, unlike
        // the home marker which passes a fixed color of its own.
        dangerouslySetInnerHTML={{ __html: crosshairSvgMarkup(20, "currentColor") }}
      />
    </div>
  );
}
