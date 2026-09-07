import { crosshairSvgMarkup } from "../lib/crosshairIcon";

export interface ControlsPanelProps {
  connected: boolean;
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
  connected,
  aircraftCount,
  historyAll,
  onToggleHistoryAll,
  labelsAll,
  onToggleLabelsAll,
  onRecenter,
  recenterDisabled,
}: ControlsPanelProps) {
  return (
    <div className="pointer-events-none absolute top-4 right-4 flex flex-col items-end gap-2">
      <div className="pointer-events-auto flex flex-col gap-2 rounded-md bg-white/90 p-3 text-sm text-slate-900 shadow-md dark:bg-slate-900/90 dark:text-slate-100">
        <div className="flex items-center gap-2">
          <span
            title={connected ? "Connected" : "Disconnected"}
            className={`h-2.5 w-2.5 rounded-full ${connected ? "bg-green-500" : "bg-red-500"}`}
          />
          <span className="tabular-nums">{aircraftCount} aircraft</span>
        </div>
        <button
          type="button"
          onClick={onToggleHistoryAll}
          aria-pressed={historyAll}
          className={`rounded border px-2 py-1 text-left text-xs font-medium transition-colors ${
            historyAll
              ? "border-sky-500 bg-sky-500 text-white"
              : "border-slate-300 bg-white text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-700"
          }`}
        >
          History: All
        </button>
        <button
          type="button"
          onClick={onToggleLabelsAll}
          aria-pressed={labelsAll}
          className={`rounded border px-2 py-1 text-left text-xs font-medium transition-colors ${
            labelsAll
              ? "border-sky-500 bg-sky-500 text-white"
              : "border-slate-300 bg-white text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-700"
          }`}
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
        className="pointer-events-auto flex h-9 w-9 items-center justify-center rounded-md bg-white/90 shadow-md hover:bg-white disabled:cursor-not-allowed disabled:opacity-50 dark:bg-slate-900/90 dark:hover:bg-slate-900"
        // Same crosshair markup as the on-map home marker -- see
        // lib/crosshairIcon.ts's docstring for why they must stay
        // visually identical.
        dangerouslySetInnerHTML={{ __html: crosshairSvgMarkup(20, "#334155") }}
      />
    </div>
  );
}
