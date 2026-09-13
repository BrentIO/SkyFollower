import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import type { AircraftMap } from "../lib/aircraftState";
import { buildAircraftListRows, type AircraftListRow } from "../lib/aircraftListRow";
import { nextAircraftListSortState, sortAircraftListRows, type AircraftListSortState } from "../lib/aircraftListSort";
import type { CenterPoint } from "../lib/config";
import { MAX_LABEL_Z_INDEX } from "../lib/labelStackOrder";
import { createTrailingThrottle, MAP_SYNC_THROTTLE_MS } from "../lib/syncThrottle";
import { BADGE_BASE, BADGE_CLASSES } from "./AircraftDetailPanel";

// Column definitions -- a data-driven array rather than hardcoded per-column
// JSX, so per-column show/hide (explicitly deferred by the issue this
// implements) is additive later instead of a restructure. `sortKey` reads
// only the plain, comparable value a column sorts by -- for Ident, that is
// deliberately just the ident string, never the Military/Special-livery
// pills `render` prepends, per the issue's "pill is decoration only,
// excluded from the sort key" rule.
interface AircraftListColumn {
  key: string;
  header: string;
  sortKey: (row: AircraftListRow) => string | number | null;
  render: (row: AircraftListRow) => ReactNode;
}

// Reuses AircraftDetailPanel's own BADGE_BASE/BADGE_CLASSES convention
// exactly (green "M" for military, yellow "S" for special livery), in that
// order, immediately before the ident text -- see the issue's "Military /
// Special Livery pill" section.
const AIRCRAFT_LIST_COLUMNS: AircraftListColumn[] = [
  {
    key: "ident",
    header: "Ident",
    sortKey: (row) => row.ident,
    render: (row) => (
      <span className="flex items-center gap-1">
        {row.military && <span className={`${BADGE_BASE} ${BADGE_CLASSES.green}`}>M</span>}
        {row.specialLivery != null && <span className={`${BADGE_BASE} ${BADGE_CLASSES.yellow}`}>S</span>}
        <span>{row.ident ?? ""}</span>
      </span>
    ),
  },
  {
    key: "registration",
    header: "Registration",
    sortKey: (row) => row.registration,
    render: (row) => row.registration ?? "",
  },
  {
    key: "type",
    header: "Type",
    sortKey: (row) => row.typeDesignator,
    render: (row) => row.typeDesignator ?? "",
  },
  {
    key: "desc",
    header: "Desc",
    sortKey: (row) => row.descriptionCode,
    render: (row) => row.descriptionCode ?? "",
  },
  {
    key: "altitude",
    header: "Altitude",
    sortKey: (row) => row.altitudeFt,
    render: (row) => row.altitudeDisplay ?? "",
  },
  {
    key: "distance",
    header: "Distance",
    sortKey: (row) => row.distanceNm,
    render: (row) => row.distanceDisplay ?? "",
  },
];

// Default sort on first open: Distance, ascending (closest first) -- see
// the issue's "Sorting" section.
const DEFAULT_SORT_STATE: AircraftListSortState = { columnKey: "distance", dir: "asc" };

// Fixed (not resizable), wide enough for all six columns without wrapping
// at a normal viewport width -- see the issue's "Width" note. Applied via
// inline style (both here and in the wrapper's closed-state translateX
// below) rather than a Tailwind arbitrary-value class, since Tailwind's
// JIT scanner can't see a class name built from a template literal.
const PANEL_WIDTH_PX = 720;

const ROW_BAND_EVEN = "bg-white dark:bg-slate-900";
const ROW_BAND_ODD = "bg-slate-50 dark:bg-slate-800/60";
// Consistent with AircraftDetailPanel.tsx's SQUAWK_EMERGENCY_TEXT red
// family -- overrides normal banding entirely (not blended) for a row
// squawking 7500/7600/7700/7777.
const ROW_EMERGENCY = "bg-red-100 dark:bg-red-900/70";

// Same chevron-up/chevron-down glyph shapes as lucide-react's ChevronUp/
// ChevronDown (management-ui/frontend's HistoryView.tsx uses that package
// directly) -- this frontend has no lucide-react dependency and instead
// hand-rolls every icon as inline SVG (see lib/actionIcons.ts/IconButton.tsx's
// ActionIcon), so this is drawn the same way rather than adding a second
// icon mechanism for one component. Rendered only for the active sort
// column, matching HistoryView's own convention exactly.
function SortChevron({ direction }: { direction: "asc" | "desc" }) {
  return (
    <svg
      width="12"
      height="12"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
    >
      {direction === "asc" ? <polyline points="18 15 12 9 6 15" /> : <polyline points="6 9 12 15 18 9" />}
    </svg>
  );
}

export interface AircraftListPanelProps {
  aircraft: AircraftMap;
  /** Same value ControlsPanel used to show -- see the issue's "Status box
   * changes" section, which relocates this text rather than recomputing
   * it from a different source. */
  aircraftCount: number;
  center: CenterPoint | null;
  selected: Set<string>;
  onSelect: (icaoHex: string) => void;
}

// Right-side flyout: a sortable, columnar list of every currently-tracked,
// non-hidden aircraft (tar1090-style) -- see the issue this implements for
// the full design. Opens/closes via its own edge-tab handle, vertically
// centered on the viewport (not top-aligned like tar1090's own handle) so
// it can never collide with ControlsPanel's top-right icon column/status
// dot, both of which are anchored top-right.
export function AircraftListPanel({ aircraft, aircraftCount, center, selected, onSelect }: AircraftListPanelProps) {
  // Closed by default -- an on-demand addition to the view, not something
  // that should claim screen space (and partially occlude the map) on
  // every page load the way the always-selected AircraftDetailPanel does.
  const [open, setOpen] = useState(false);
  const [sort, setSort] = useState<AircraftListSortState>(DEFAULT_SORT_STATE);

  // Coalesces this panel's own row rebuild the same way MapView.tsx
  // coalesces its MapLibre source rebuild (see syncThrottle.ts's module
  // docstring) -- a WebSocket batch touching hundreds of aircraft would
  // otherwise re-sort/re-render the full row list on every single event.
  // One throttle instance for this component's whole lifetime, not
  // per-render, so `lastRunAt` actually accumulates across requests.
  const aircraftRef = useRef(aircraft);
  aircraftRef.current = aircraft;
  const [displayedAircraft, setDisplayedAircraft] = useState<AircraftMap>(aircraft);
  const throttleRef = useRef(createTrailingThrottle(MAP_SYNC_THROTTLE_MS));

  useEffect(() => {
    return () => throttleRef.current.cancel();
  }, []);

  // Only pays for a rebuild while the panel is actually open -- closed,
  // there is no row list to keep in sync, so every WS batch is skipped
  // outright rather than throttled-and-discarded.
  useEffect(() => {
    if (!open) return;
    throttleRef.current.request(() => setDisplayedAircraft(aircraftRef.current));
  }, [aircraft, open]);

  // Reopening should reflect the current live state immediately (leading
  // edge), not whatever was last displayed before it was closed.
  useEffect(() => {
    if (open) setDisplayedAircraft(aircraftRef.current);
  }, [open]);

  const rows = useMemo(() => buildAircraftListRows(displayedAircraft, center), [displayedAircraft, center]);

  const activeColumn = AIRCRAFT_LIST_COLUMNS.find((c) => c.key === sort.columnKey) ?? AIRCRAFT_LIST_COLUMNS[0];
  const sortedRows = useMemo(
    () => sortAircraftListRows(rows, activeColumn.sortKey, sort.dir),
    [rows, activeColumn, sort.dir],
  );

  function handleSortChange(columnKey: string) {
    setSort((current) => nextAircraftListSortState(current, columnKey));
  }

  return (
    // Translates by exactly the *panel's* own width (PANEL_WIDTH_PX, below
    // -- not this wrapper's combined tab+panel width, which `translate-x-full`
    // would use) when closed. That difference is what keeps the tab flush
    // against the viewport's right edge (still visible/clickable to reopen)
    // while only the panel itself slides past it, off-screen -- rather than
    // dragging the tab off-screen along with the panel.
    <div
      className="pointer-events-none absolute top-1/2 right-0 flex items-center transition-transform duration-200"
      style={{
        zIndex: MAX_LABEL_Z_INDEX,
        transform: `translateY(-50%) translateX(${open ? 0 : PANEL_WIDTH_PX}px)`,
      }}
    >
      <button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        title={open ? "Close aircraft list" : "Open aircraft list"}
        aria-label={open ? "Close aircraft list" : "Open aircraft list"}
        aria-expanded={open}
        className="pointer-events-auto flex h-12 w-6 items-center justify-center rounded-l-md bg-white text-slate-700 shadow-md hover:bg-slate-50 dark:bg-slate-900 dark:text-white dark:hover:bg-slate-800"
      >
        {open ? "▶" : "◀"}
      </button>

      <div
        className="pointer-events-auto flex max-h-[80vh] flex-col overflow-hidden rounded-l-md bg-white text-slate-900 shadow-md dark:bg-slate-900 dark:text-slate-100"
        style={{ width: PANEL_WIDTH_PX }}
      >
        <div className="flex items-baseline justify-between gap-3 border-b border-slate-200 px-4 py-2.5 dark:border-slate-700">
          <div className="text-base font-bold">Aircraft List</div>
          <div className="tabular-nums text-xs text-slate-500 dark:text-slate-400">{aircraftCount} aircraft</div>
        </div>

        <div className="overflow-y-auto">
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr>
                {AIRCRAFT_LIST_COLUMNS.map((column) => {
                  const active = sort.columnKey === column.key;
                  return (
                    <th
                      key={column.key}
                      className="sticky top-0 z-10 border-b border-slate-200 bg-slate-100 px-3 py-2 text-left dark:border-slate-700 dark:bg-slate-800"
                    >
                      <button
                        type="button"
                        onClick={() => handleSortChange(column.key)}
                        aria-sort={active ? (sort.dir === "asc" ? "ascending" : "descending") : "none"}
                        className="flex items-center gap-1 text-xs font-bold tracking-wide text-slate-500 uppercase hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200"
                      >
                        {column.header}
                        {active && <SortChevron direction={sort.dir} />}
                      </button>
                    </th>
                  );
                })}
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((row, index) => {
                const banding = row.emergency ? ROW_EMERGENCY : index % 2 === 0 ? ROW_BAND_EVEN : ROW_BAND_ODD;
                return (
                  <tr
                    key={row.icaoHex}
                    onClick={() => onSelect(row.icaoHex)}
                    className={`cursor-pointer ${banding} ${selected.has(row.icaoHex) ? "outline outline-1 -outline-offset-1 outline-slate-400 dark:outline-slate-500" : ""}`}
                  >
                    {AIRCRAFT_LIST_COLUMNS.map((column) => (
                      <td key={column.key} className="px-3 py-1.5 whitespace-nowrap">
                        {column.render(row)}
                      </td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
