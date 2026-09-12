import { useMemo } from "react";
import type { MapFlight } from "../api/types";
import { FOLLOW_ICON, ISOLATE_ICON, TRACE_POINTS_ICON, ZOOM_TO_ICON, type IconSpec } from "../lib/actionIcons";
import { buildAircraftDetail, type AircraftDetailData, type AirportBlockData } from "../lib/aircraftDetail";
import type { HomePoint } from "../lib/config";
import { toggleButtonClass } from "../lib/toggleButtonStyle";

// Copied verbatim (Tailwind class strings, not just visually similar hex
// values) from management-ui/frontend/src/views/LookupView.tsx's
// BADGE_CLASSES/Badge -- only the "green" (Military) and "yellow" (Special
// Livery) variants are used here. Exported so a plain unit test (no
// jsdom/render harness in this project -- see lib/config.test.ts) can
// assert byte-for-byte equality against that source, rather than only
// exercising these strings through a rendered DOM.
export const BADGE_CLASSES = {
  yellow: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
  green: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
} as const;
export const BADGE_BASE = "rounded px-2 py-0.5 text-xs font-semibold";

// Copied verbatim from management-ui/frontend/src/components/FlightViewModal.tsx --
// PILL is sized up to px-3 py-1 text-base for the route pills, exactly as
// AirportBlock itself does there. PILL_RED doubles as the Squawk row's
// emergency-code text color (`text-red-800`/dark `text-red-200`).
export const PILL = "rounded px-2 py-0.5 text-xs font-semibold";
export const PILL_GREEN = "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200";
export const PILL_RED = "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200";
export const SQUAWK_EMERGENCY_TEXT = "text-red-800 dark:text-red-200";

// FlightViewModal.tsx's Matched Rules/Receiver Sources pill treatment,
// verbatim -- right-justified here (`justify-end`) instead of that
// component's left-aligned `flex-wrap`, per this panel's own convention of
// hanging values against the right edge.
export const TAG_PILL =
  "rounded bg-slate-100 px-2 py-0.5 font-mono text-xs text-slate-700 dark:bg-slate-900 dark:text-slate-300";

const SECTION_BAR =
  "bg-slate-100 px-4 py-1 text-[10px] font-bold tracking-wider text-slate-600 uppercase dark:bg-slate-800 dark:text-slate-300";
const DIVIDER = "border-t border-slate-200 dark:border-slate-700";
const ROW = "flex items-baseline justify-between gap-3 px-4 py-1.5";
const ROW_LABEL = "text-xs text-slate-500 dark:text-slate-400";
const ROW_VALUE = "text-sm text-slate-900 dark:text-slate-100";

export interface AircraftDetailPanelProps {
  aircraft: MapFlight;
  home: HomePoint | null;
  onClose: () => void;
  /** Isolate/Follow/Trace Points are real toggles; Zoom To is one-shot and
   * never shows an "active" state, so it has no *Active prop. All of the
   * on/off decisions (re-targeting Isolate on reselect, resetting Follow/
   * Trace Points on reselect vs. close, deferred eviction) live in
   * MapView.tsx -- this component is just the controlled button row. */
  isolateActive: boolean;
  onToggleIsolate: () => void;
  onZoomTo: () => void;
  followActive: boolean;
  onToggleFollow: () => void;
  tracePointsActive: boolean;
  onToggleTracePoints: () => void;
}

// Persistent left-docked side panel for the single currently-selected
// aircraft (see lib/selection.ts's single-select nextSelection) -- a
// different, additional surface from the floating per-aircraft
// InfoBoxLayer boxes, not a replacement for them. Same visual language as
// ControlsPanel (rounded-md, bg-white/90 dark:bg-slate-900/90, shadow-md),
// docked left with the matching top-4/left-4 margin so it never collides
// with that top-right panel.
export function AircraftDetailPanel({
  aircraft,
  home,
  onClose,
  isolateActive,
  onToggleIsolate,
  onZoomTo,
  followActive,
  onToggleFollow,
  tracePointsActive,
  onToggleTracePoints,
}: AircraftDetailPanelProps) {
  const data = useMemo(() => buildAircraftDetail(aircraft, home), [aircraft, home]);

  const hasHeaderSubline = data.registration != null || data.icaoHex != null;
  const hasBadges = data.military || data.specialLivery != null;

  return (
    <div className="absolute top-4 left-4 w-80 overflow-hidden rounded-md bg-white/90 text-slate-900 shadow-md dark:bg-slate-900/90 dark:text-slate-100">
      <div className="flex items-start justify-between gap-3 p-3">
        <div className="min-w-0">
          <div className="text-xl leading-tight font-bold">{data.title}</div>
          {hasHeaderSubline && (
            <div className="mt-1 flex flex-wrap items-baseline gap-2">
              {data.registration != null && <span className="text-sm">{data.registration}</span>}
              {data.icaoHex != null && (
                <span className="font-mono text-xs text-slate-500 dark:text-slate-400">{data.icaoHex}</span>
              )}
            </div>
          )}
        </div>
        <button
          type="button"
          onClick={onClose}
          aria-label="Close"
          className="flex h-6 w-6 shrink-0 items-center justify-center rounded text-slate-500 hover:bg-slate-100 dark:text-slate-400 dark:hover:bg-slate-800"
        >
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path
              d="M4 4L12 12M12 4L4 12"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
            />
          </svg>
        </button>
      </div>

      {hasBadges && (
        <div className="flex flex-wrap gap-1.5 px-3 pb-3">
          {data.military && <span className={`${BADGE_BASE} ${BADGE_CLASSES.green}`}>Military</span>}
          {data.specialLivery != null && (
            <span className={`${BADGE_BASE} ${BADGE_CLASSES.yellow}`}>{data.specialLivery}</span>
          )}
        </div>
      )}

      {data.route && (
        <div className={DIVIDER}>
          <div className={SECTION_BAR}>Route</div>
          <div className="flex flex-col gap-3.5 px-4 py-3">
            <AirportBlock airport={data.route.origin} pillClass={PILL_GREEN} />
            <AirportBlock airport={data.route.destination} pillClass={PILL_RED} />
          </div>
        </div>
      )}

      {data.operator && (
        <div className={DIVIDER}>
          <div className={SECTION_BAR}>Operator</div>
          <div className="px-4 py-2.5 text-right">
            {data.operator.name != null && <div className="text-sm font-medium">{data.operator.name}</div>}
            {data.operator.callsign != null && (
              <div className="mt-0.5 text-xs italic">&quot;{data.operator.callsign}&quot;</div>
            )}
            {data.operator.country != null && (
              <div className="mt-0.5 text-xs text-slate-500 dark:text-slate-400">{data.operator.country}</div>
            )}
          </div>
        </div>
      )}

      <div className={DIVIDER}>
        <div className={SECTION_BAR}>Aircraft</div>
        <div>
          {data.manufacturerModel && (
            <div className={ROW}>
              <span className={ROW_LABEL}>{data.manufacturerModel.label}</span>
              <span className={`${ROW_VALUE} text-right`}>{data.manufacturerModel.value}</span>
            </div>
          )}
          {data.registrant != null && (
            <div className={`${ROW} ${data.manufacturerModel ? DIVIDER : ""}`}>
              <span className={ROW_LABEL}>Registrant</span>
              <span className={`${ROW_VALUE} text-right`}>{data.registrant}</span>
            </div>
          )}
        </div>
      </div>

      <div className={DIVIDER}>
        <div className={SECTION_BAR}>Flight</div>
        <FlightRows data={data} />
      </div>

      <div className={`${DIVIDER} flex items-center justify-center gap-2 px-4 py-2.5`}>
        <ActionButton label="Isolate" icon={ISOLATE_ICON} active={isolateActive} onClick={onToggleIsolate} />
        <ActionButton label="Zoom To" icon={ZOOM_TO_ICON} active={false} onClick={onZoomTo} />
        <ActionButton label="Follow" icon={FOLLOW_ICON} active={followActive} onClick={onToggleFollow} />
        <ActionButton
          label="Trace Points"
          icon={TRACE_POINTS_ICON}
          active={tracePointsActive}
          onClick={onToggleTracePoints}
        />
      </div>
    </div>
  );
}

// One icon-only action-row button. `active` drives the same toggle
// coloring as ControlsPanel's "History: All"/"Labels: All" buttons (see
// lib/toggleButtonStyle.ts) -- Zoom To is one-shot and always passes
// `active={false}` (see its call site above), so it never shows the
// pressed/"on" look. `title` doubles as the accessible name (no visible
// text label at this icon-only size).
function ActionButton({
  label,
  icon,
  active,
  onClick,
}: {
  label: string;
  icon: IconSpec;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      title={label}
      aria-label={label}
      aria-pressed={active}
      className={`flex h-8 w-8 items-center justify-center rounded border transition-colors ${toggleButtonClass(active)}`}
    >
      <ActionIcon spec={icon} />
    </button>
  );
}

// Renders one IconSpec (lib/actionIcons.ts) as an 18x18 stroke icon.
// `stroke="currentColor"` means the button's own active/inactive text
// color drives the icon color for free -- no separate icon-color logic.
function ActionIcon({ spec }: { spec: IconSpec }) {
  return (
    <svg
      width="18"
      height="18"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      xmlns="http://www.w3.org/2000/svg"
    >
      {spec.circles?.map((c, i) => <circle key={`c${i}`} cx={c.cx} cy={c.cy} r={c.r} />)}
      {spec.lines?.map((l, i) => <line key={`l${i}`} x1={l.x1} y1={l.y1} x2={l.x2} y2={l.y2} />)}
      {spec.paths?.map((p, i) => <path key={`p${i}`} d={p.d} />)}
      {spec.polygons?.map((pg, i) => <polygon key={`pg${i}`} points={pg.points} />)}
    </svg>
  );
}

function AirportBlock({ airport, pillClass }: { airport: AirportBlockData; pillClass: string }) {
  return (
    <div>
      <div className="flex flex-wrap items-baseline gap-2">
        <span className={`${PILL} px-3 py-1 text-base ${pillClass}`}>{airport.icaoCode}</span>
        {airport.iataCode != null && (
          <span className="font-mono text-sm text-slate-700 dark:text-slate-300">{airport.iataCode}</span>
        )}
      </div>
      {airport.name != null && <div className="mt-1 text-sm">{airport.name}</div>}
      {airport.location != null && (
        <div className="text-sm text-slate-500 dark:text-slate-400">{airport.location}</div>
      )}
    </div>
  );
}

// Flight section rows -- each independently omit-if-unknown. A divider only
// separates a row from a *previously rendered* row, never precedes the
// first visible one, so the section reads correctly no matter which rows
// are actually present.
function FlightRows({ data }: { data: AircraftDetailData }) {
  let rendered = false;
  function divider(): string {
    const cls = rendered ? DIVIDER : "";
    rendered = true;
    return cls;
  }

  return (
    <div>
      {data.squawk && (
        <div className={`${ROW} ${divider()}`}>
          <span className={ROW_LABEL}>Squawk</span>
          <span className={data.squawk.emergency ? `text-sm font-bold ${SQUAWK_EMERGENCY_TEXT}` : ROW_VALUE}>
            {data.squawk.value}
          </span>
        </div>
      )}
      {data.altitude != null && (
        <div className={`${ROW} ${divider()}`}>
          <span className={ROW_LABEL}>Altitude</span>
          <span className={ROW_VALUE}>{data.altitude} ft</span>
        </div>
      )}
      {data.speed != null && (
        <div className={`${ROW} ${divider()}`}>
          <span className={ROW_LABEL}>Speed</span>
          <span className={ROW_VALUE}>{data.speed} kt</span>
        </div>
      )}
      {data.verticalSpeed != null && (
        <div className={`${ROW} ${divider()}`}>
          <span className={ROW_LABEL}>Vertical Speed</span>
          <span className={ROW_VALUE}>{data.verticalSpeed}</span>
        </div>
      )}
      {data.track != null && (
        <div className={`${ROW} ${divider()}`}>
          <span className={ROW_LABEL}>Track</span>
          <span className={ROW_VALUE}>{data.track}°</span>
        </div>
      )}
      {data.distance != null && (
        <div className={`${ROW} ${divider()}`}>
          <span className={ROW_LABEL}>Distance</span>
          <span className={ROW_VALUE}>{data.distance} nmi</span>
        </div>
      )}
      {data.sources.length > 0 && (
        <div className={`px-4 py-2 ${divider()}`}>
          <div className={`${ROW_LABEL} mb-1`}>Sources</div>
          <div className="flex flex-wrap justify-end gap-1.5">
            {data.sources.map((source) => (
              <span key={source} className={TAG_PILL}>
                {source}
              </span>
            ))}
          </div>
        </div>
      )}
      {data.matchedRules.length > 0 && (
        <div className={`px-4 py-2 ${divider()}`}>
          <div className={`${ROW_LABEL} mb-1`}>Matched Rules</div>
          <div className="flex flex-wrap justify-end gap-1.5">
            {data.matchedRules.map((rule) => (
              <span key={rule} className={TAG_PILL}>
                {rule}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
