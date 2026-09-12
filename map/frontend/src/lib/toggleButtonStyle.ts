// Shared toggle-button coloring for every small on/off control this map
// view renders as a bordered button (components/ControlsPanel.tsx's
// "History: All"/"Labels: All", components/AircraftDetailPanel.tsx's
// Isolate/Follow/Trace Points action-row buttons). Centralized here rather
// than duplicated per component so the two families of toggle buttons
// provably share one definition instead of two copies that could drift.
export const TOGGLE_ACTIVE_CLASS = "border-sky-500 bg-sky-500 text-white";
export const TOGGLE_INACTIVE_CLASS =
  "border-slate-300 bg-white text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200 dark:hover:bg-slate-700";

export function toggleButtonClass(active: boolean): string {
  return active ? TOGGLE_ACTIVE_CLASS : TOGGLE_INACTIVE_CLASS;
}
