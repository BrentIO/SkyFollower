import { useEffect, useMemo, useRef, useState, type PointerEvent, type ReactNode } from "react";
import type { AircraftMap } from "../lib/aircraftState";
import {
  clampPanelWidth,
  loadPersistedPanelWidth,
  savePersistedPanelWidth,
} from "../lib/aircraftListPanelPersistence";
import { buildAircraftListRows, type AircraftListRow } from "../lib/aircraftListRow";
import { nextAircraftListSortState, sortAircraftListRows, type AircraftListSortState } from "../lib/aircraftListSort";
import type { CenterPoint } from "../lib/config";
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

// The edge tab's own width (h-12 w-6 button -- see its className below).
// Kept as a named constant since the wrapper's open/closed width (below)
// needs it alongside the panel's own (now operator-resizable, see #1784)
// width, rather than hardcoding "24" a second time disconnected from the
// button's own w-6 class.
const TAB_WIDTH_PX = 24;

// A floor under the map area's own width during an active resize drag, so
// a narrow browser window can't have its map squeezed to nothing -- on
// top of aircraftListPanelPersistence.ts's own static MIN/MAX_PANEL_WIDTH_PX
// clamp, which is independent of the live viewport.
const MIN_MAP_AREA_WIDTH_PX = 320;

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
//
// A real flex sibling of the map area (see MapView.tsx's root layout), not
// an absolute overlay on top of it -- this component's own box width is
// what actually pushes the map narrower while open, so the drawer can
// never cover ControlsPanel (which tracks the map area's own, now-
// narrower, right edge) and the map keeps its configured center centered
// in whatever width remains, the same way resizing any MapLibre container
// does.
export function AircraftListPanel({ aircraft, aircraftCount, center, selected, onSelect }: AircraftListPanelProps) {
  // Closed by default -- an on-demand addition to the view, not something
  // that should claim screen space (and partially occlude the map) on
  // every page load the way the always-selected AircraftDetailPanel does.
  const [open, setOpen] = useState(false);
  const [sort, setSort] = useState<AircraftListSortState>(DEFAULT_SORT_STATE);

  // Operator-resizable width (#1784) -- seeded from whatever this browser
  // last persisted, defaulting to the original fixed 720px otherwise. Kept
  // in a ref alongside the state so handleResizeEnd's save always sees the
  // latest value regardless of closure timing (same pattern as aircraftRef
  // above).
  const [width, setWidth] = useState(() => loadPersistedPanelWidth());
  const widthRef = useRef(width);
  widthRef.current = width;
  const [resizing, setResizing] = useState(false);
  const dragStartRef = useRef<{ startX: number; startWidth: number } | null>(null);

  function handleResizeStart(e: PointerEvent<HTMLDivElement>) {
    e.preventDefault();
    dragStartRef.current = { startX: e.clientX, startWidth: widthRef.current };
    setResizing(true);
    e.currentTarget.setPointerCapture(e.pointerId);
  }

  function handleResizeMove(e: PointerEvent<HTMLDivElement>) {
    if (!dragStartRef.current) return;
    const raw = dragStartRef.current.startWidth + (dragStartRef.current.startX - e.clientX);
    // Extra viewport-aware ceiling on top of clampPanelWidth's static
    // MIN/MAX -- never leaves less than MIN_MAP_AREA_WIDTH_PX for the map
    // area itself, even on a narrow browser window.
    const viewportMax = Math.max(0, window.innerWidth - TAB_WIDTH_PX - MIN_MAP_AREA_WIDTH_PX);
    setWidth(clampPanelWidth(Math.min(raw, viewportMax)));
  }

  function handleResizeEnd(e: PointerEvent<HTMLDivElement>) {
    if (!dragStartRef.current) return;
    dragStartRef.current = null;
    setResizing(false);
    savePersistedPanelWidth(widthRef.current);
    if (e.currentTarget.hasPointerCapture(e.pointerId)) {
      e.currentTarget.releasePointerCapture(e.pointerId);
    }
  }

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
    // The wrapper's own inline-style `width` (not a `transform`) is what's
    // transitioned -- a transform doesn't change a flex sibling's layout
    // box the way it could get away with as an absolute overlay, which is
    // exactly what let the map area's box actually shrink/grow here.
    // `overflow-hidden` clips the panel content out of view once the
    // wrapper narrows to just the tab's own width, rather than reflowing
    // or wrapping it. The open/close width transition is suppressed while
    // actively resize-dragging (#1784) so the live width tracks the
    // pointer instead of lagging behind a 200ms transition.
    <div
      className={`flex h-full flex-none items-stretch overflow-hidden ${resizing ? "" : "transition-[width] duration-200"}`}
      style={{ width: open ? TAB_WIDTH_PX + width : TAB_WIDTH_PX }}
    >
      {/* Tab stays vertically centered within the drawer's full height --
          not top-aligned like tar1090's own handle -- so it can never
          collide with ControlsPanel's top-right icon column/status dot. */}
      <div className="flex h-full flex-none items-center" style={{ width: TAB_WIDTH_PX }}>
        <button
          type="button"
          onClick={() => setOpen((prev) => !prev)}
          title={open ? "Close aircraft list" : "Open aircraft list"}
          aria-label={open ? "Close aircraft list" : "Open aircraft list"}
          aria-expanded={open}
          className="flex h-12 w-6 items-center justify-center rounded-l-md bg-white text-slate-700 shadow-md hover:bg-slate-50 dark:bg-slate-900 dark:text-white dark:hover:bg-slate-800"
        >
          {open ? "▶" : "◀"}
        </button>
      </div>

      <div
        className="relative flex h-full flex-none flex-col overflow-hidden rounded-l-md bg-white text-slate-900 shadow-md dark:bg-slate-900 dark:text-slate-100"
        style={{ width }}
      >
        {/* Drag-to-resize handle (#1784) -- absolutely positioned so it
            doesn't add to the panel's own flex width, straddling the
            panel's left edge. Only rendered while open: resizing a
            collapsed, invisible drawer doesn't make sense, and this keeps
            it out of the tab-order/accessibility tree when it can't do
            anything. Pointer Events (not mouse-only) so this works for
            touch too; pointer capture keeps receiving move/up events even
            if the pointer leaves this thin strip mid-drag. */}
        {open && (
          <div
            role="separator"
            aria-orientation="vertical"
            aria-label="Resize aircraft list"
            onPointerDown={handleResizeStart}
            onPointerMove={handleResizeMove}
            onPointerUp={handleResizeEnd}
            onPointerCancel={handleResizeEnd}
            className={`absolute top-0 left-0 z-10 h-full w-1.5 -translate-x-1/2 cursor-col-resize touch-none ${
              resizing ? "bg-slate-400/70 dark:bg-slate-500/70" : "hover:bg-slate-300/70 dark:hover:bg-slate-600/70"
            }`}
          />
        )}

        <div className="flex items-baseline justify-between gap-3 border-b border-slate-200 px-4 py-2.5 dark:border-slate-700">
          <div className="text-base font-bold">Aircraft List</div>
          <div className="tabular-nums text-xs text-slate-500 dark:text-slate-400">{aircraftCount} aircraft</div>
        </div>

        {/* overflow-auto (not just -y): a user-dragged width narrower than
            the table's natural content width (#1784) scrolls horizontally
            instead of visually overflowing the rounded panel card. */}
        <div className="overflow-auto">
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
