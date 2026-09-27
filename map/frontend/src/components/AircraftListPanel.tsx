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
import { MAX_LABEL_Z_INDEX } from "../lib/labelStackOrder";
import { connectionTooltip, overallConnectionStatus, PROCESSOR_STATUS_DOT_COLOR } from "../lib/processorStatus";
import { createTrailingThrottle, MAP_SYNC_THROTTLE_MS } from "../lib/syncThrottle";
import { countryFlag } from "../lib/countryFlag";
import type { ProcessorRoster } from "../api/types";
import { BADGE_BASE, BADGE_CLASSES } from "./AircraftDetailPanel";

// Column definitions -- a data-driven array rather than hardcoded per-column
// JSX, so per-column show/hide is additive later instead of a restructure.
// `sortKey` reads only the plain, comparable value a column sorts by --
// for Ident, that's just the ident string, never the Tags column's own
// badges, which sort on their own rank instead.
interface AircraftListColumn {
  key: string;
  header: string;
  sortKey: (row: AircraftListRow) => string | number | null;
  render: (row: AircraftListRow) => ReactNode;
  className?: string;
}

// The country-of-registration flag is its own column, to the left of
// Ident, so it lines up in its own vertical strip instead of shifting
// per-row with ident text width. Silently omitted (not a placeholder
// glyph) when no country has resolved, or countryCode isn't a valid
// 2-letter code.
const AIRCRAFT_LIST_COLUMNS: AircraftListColumn[] = [
  {
    key: "flag",
    header: "",
    sortKey: (row) => row.country ?? row.countryCode ?? null,
    render: (row) => {
      const flag = row.countryCode != null ? countryFlag(row.countryCode) : null;
      if (flag == null) return null;
      return <span title={row.country ?? row.countryCode ?? undefined}>{flag}</span>;
    },
  },
  {
    key: "ident",
    header: "Ident",
    sortKey: (row) => row.ident,
    render: (row) => row.ident ?? "",
  },
  // Tags column: the Military/Special-livery/UAT/External-source badges,
  // its own column so Ident stays plain text. Reuses AircraftDetailPanel's
  // BADGE_BASE/BADGE_CLASSES convention, each with a native `title` tooltip.
  //
  // Sorts by rank: 0 specialLivery, 1 military, 2 isUat/isExternal, 3
  // none -- specialLivery/military mark a notable *aircraft* so they stay
  // the top two tiers; isUat/isExternal mark a data-source characteristic,
  // lower priority, and share one tier since neither is more notable than
  // the other. Fixed narrow width (`w-16`) so badges wrap onto a second
  // line at the widest combination rather than widening every row.
  {
    key: "tags",
    header: "Tags",
    sortKey: (row) =>
      row.specialLivery != null ? 0 : row.military ? 1 : row.isUat || row.isExternal ? 2 : 3,
    render: (row) => (
      <span className="flex flex-wrap items-center gap-1">
        {row.military && <span title="Military" className={`${BADGE_BASE} ${BADGE_CLASSES.green}`}>M</span>}
        {row.specialLivery != null && (
          <span title={row.specialLivery} className={`${BADGE_BASE} ${BADGE_CLASSES.yellow}`}>
            S
          </span>
        )}
        {row.isUat && (
          <span title="ADS-B (UAT/978)" className={`${BADGE_BASE} ${BADGE_CLASSES.blue}`}>
            U
          </span>
        )}
        {row.isExternal && (
          <span title="External source" className={`${BADGE_BASE} ${BADGE_CLASSES.blue}`}>
            E
          </span>
        )}
      </span>
    ),
    className: "w-16",
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

// Default sort on first open: Distance, ascending (closest first).
const DEFAULT_SORT_STATE: AircraftListSortState = { columnKey: "distance", dir: "asc" };

// The edge tab's own width (h-12 w-6 button, see className below). Named
// so the wrapper's open/closed width below can reference it rather than
// hardcoding "24" disconnected from the button's own w-6 class.
const TAB_WIDTH_PX = 24;

// A floor under the map area's own width during an active resize drag, on
// top of aircraftListPanelPersistence.ts's static MIN/MAX_PANEL_WIDTH_PX
// clamp, which is independent of the live viewport.
const MIN_MAP_AREA_WIDTH_PX = 320;

// Absolute render-width floor on a viewport too narrow to fit even
// MIN_PANEL_WIDTH_PX (#2049) -- just enough to stay usable, not a target.
const MIN_USABLE_PANEL_WIDTH_PX = 200;

const ROW_BAND_EVEN = "bg-white dark:bg-slate-900";
const ROW_BAND_ODD = "bg-slate-50 dark:bg-slate-800/60";
// Overrides normal banding entirely (not blended) for a row squawking
// 7500/7600/7700/7777.
const ROW_EMERGENCY = "bg-red-100 dark:bg-red-900/70";

// This frontend has no lucide-react dependency and hand-rolls every icon
// as inline SVG instead; rendered only for the active sort column.
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
  aircraftCount: number;
  /** The browser's own WebSocket connection to this map backend -- distinct
   * from `roster`, the message-processor liveness roster that backend has
   * derived from UDP traffic. If this is false there is no live proof of
   * anything, so the indicator renders red regardless of the last-known
   * roster snapshot. */
  wsConnected: boolean;
  roster: ProcessorRoster;
  center: CenterPoint | null;
  selected: Set<string>;
  onSelect: (icaoHex: string) => void;
}

// Right-side flyout: a sortable, columnar list of every currently-tracked,
// non-hidden aircraft. Opens/closes via its own edge-tab handle, vertically
// centered on the viewport (not top-aligned) so it never collides with
// ControlsPanel's top-right icon column/status dot.
//
// A real flex sibling of the map area, not an absolute overlay on top of
// it -- this component's own box width pushes the map narrower while open,
// so the map keeps its configured center centered in whatever width
// remains, the same way resizing any MapLibre container does.
export function AircraftListPanel({
  aircraft,
  aircraftCount,
  wsConnected,
  roster,
  center,
  selected,
  onSelect,
}: AircraftListPanelProps) {
  const overallStatus = overallConnectionStatus(wsConnected, roster);
  // Closed by default -- an on-demand addition to the view, not something
  // that should claim screen space on every page load.
  const [open, setOpen] = useState(false);
  const [sort, setSort] = useState<AircraftListSortState>(DEFAULT_SORT_STATE);

  // Operator-resizable width, seeded from whatever this browser last
  // persisted. Kept in a ref alongside the state so handleResizeEnd's save
  // always sees the latest value regardless of closure timing.
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

  // Tracks live viewport width so renderWidth (below) can react to it --
  // React doesn't re-render on a bare window resize otherwise.
  const [viewportWidth, setViewportWidth] = useState(() => window.innerWidth);
  useEffect(() => {
    const onResize = () => setViewportWidth(window.innerWidth);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  // Viewport-clamped render width (vs. persisted `width`) -- lets the
  // table scroll horizontally on a phone instead of the panel running
  // off-screen. `width`/persistence stay untouched (#2049).
  const renderWidth = Math.max(
    MIN_USABLE_PANEL_WIDTH_PX,
    Math.min(width, viewportWidth - TAB_WIDTH_PX),
  );

  // Coalesces this panel's own row rebuild the same way MapView.tsx
  // coalesces its MapLibre source rebuild, so a WebSocket batch touching
  // hundreds of aircraft doesn't re-sort/re-render on every single event.
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
    // The wrapper's inline-style `width` (not `transform`) is transitioned
    // -- a transform doesn't change a flex sibling's layout box, which is
    // what lets the map area actually shrink/grow here. The transition is
    // suppressed while actively resize-dragging so the live width tracks
    // the pointer instead of lagging behind.
    <div
      className={`flex h-full flex-none items-stretch overflow-hidden ${resizing ? "" : "transition-[width] duration-200"}`}
      // Reuses MAX_LABEL_Z_INDEX rather than a second magic constant,
      // matching AircraftDetailPanel's own MAX_LABEL_Z_INDEX + 1, in case a
      // future DOM overlay needs the same "always above the map" guarantee.
      style={{ width: open ? TAB_WIDTH_PX + renderWidth : TAB_WIDTH_PX, zIndex: MAX_LABEL_Z_INDEX + 1 }}
    >
      {/* Tab stays vertically centered within the drawer's full height, not
          top-aligned, so it never collides with ControlsPanel's top-right
          icon column/status dot. `bg-white`/`dark:bg-slate-900` here (not
          just on the button below) is load-bearing: zIndex only wins the
          paint order where it actually paints a pixel, and an unpainted
          wrapper above/below the button would let a map overlay spill
          through even though it's stacked underneath. */}
      <div
        className="flex h-full flex-none items-center bg-white dark:bg-slate-900"
        style={{ width: TAB_WIDTH_PX }}
      >
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
        style={{ width: renderWidth }}
      >
        {/* Drag-to-resize handle -- absolutely positioned, straddling the
            panel's left edge. Only rendered while open. Pointer Events
            (not mouse-only) so touch works too; pointer capture keeps
            receiving move/up events if the pointer leaves this thin strip
            mid-drag. */}
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
          {/* Connection-status dot, a sibling of the aircraft count so it
              only renders while the drawer is open, matching the count's
              own hide-when-collapsed behavior. */}
          <div className="flex items-center gap-1.5 tabular-nums text-xs text-slate-500 dark:text-slate-400">
            <span>{aircraftCount} aircraft</span>
            <span
              title={connectionTooltip(wsConnected, roster)}
              className={`h-2.5 w-2.5 rounded-full ${PROCESSOR_STATUS_DOT_COLOR[overallStatus]}`}
            />
          </div>
        </div>

        {/* overflow-auto (not just -y): a user-dragged width narrower than
            the table's natural content width scrolls horizontally instead
            of overflowing the panel card. flex-1 so this wrapper, not the
            version footer, absorbs the panel's remaining height. */}
        <div className="flex-1 overflow-auto">
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr>
                {AIRCRAFT_LIST_COLUMNS.map((column) => {
                  const active = sort.columnKey === column.key;
                  return (
                    <th
                      key={column.key}
                      className={`sticky top-0 z-10 border-b border-slate-200 bg-slate-100 px-3 py-2 text-left dark:border-slate-700 dark:bg-slate-800 ${column.className ?? ""}`}
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
                      <td
                        key={column.key}
                        className={`px-3 py-1.5 whitespace-nowrap ${column.className ?? ""}`}
                      >
                        {column.render(row)}
                      </td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Sticky version/commit footer -- mt-auto pins it to the bottom of
            this flex column regardless of table content length. */}
        <div className="mt-auto shrink-0 border-t border-slate-200 px-4 py-1.5 text-right text-[10px] text-slate-400 dark:border-slate-700 dark:text-slate-600">
          {import.meta.env.VITE_VERSION || "dev"}
          {import.meta.env.VITE_COMMIT && import.meta.env.VITE_COMMIT !== "unknown"
            ? ` (${import.meta.env.VITE_COMMIT})`
            : ""}
        </div>
      </div>
    </div>
  );
}
