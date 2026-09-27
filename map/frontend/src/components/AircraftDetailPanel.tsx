import { useEffect, useMemo, useState } from "react";
import type { MapFlight } from "../api/types";
import { FOLLOW_ICON, ISOLATE_ICON, TRACE_POINTS_ICON, ZOOM_TO_ICON } from "../lib/actionIcons";
import { buildAircraftDetail, type AircraftDetailData, type AirportBlockData } from "../lib/aircraftDetail";
import type { CenterPoint } from "../lib/config";
import { countryFlag } from "../lib/countryFlag";
import { MAX_LABEL_Z_INDEX } from "../lib/labelStackOrder";
import { formatRelativeTime, relativeTimeTickIntervalMs } from "../lib/relativeTime";
import { IconButton } from "./IconButton";

// Copied verbatim (Tailwind class strings) from
// management-ui/frontend/src/views/LookupView.tsx's BADGE_CLASSES/Badge --
// "green" (Military), "yellow" (Special Livery), "blue" (UAT/External
// source tags). Exported so a plain unit test (no jsdom/render harness in
// this project) can assert byte-for-byte equality against that source.
export const BADGE_CLASSES = {
  yellow: "bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200",
  green: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
  blue: "bg-sky-100 text-sky-800 dark:bg-sky-900 dark:text-sky-200",
} as const;
export const BADGE_BASE = "rounded px-2 py-0.5 text-xs font-semibold";

// Copied verbatim from management-ui/frontend/src/components/FlightViewModal.tsx.
// PILL_RED doubles as the Squawk row's emergency-code text color.
export const PILL = "rounded px-2 py-0.5 text-xs font-semibold";
export const PILL_GREEN = "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200";
export const PILL_RED = "bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200";
export const SQUAWK_EMERGENCY_TEXT = "text-red-800 dark:text-red-200";

// FlightViewModal.tsx's Matched Rules/Receiver Sources pill treatment,
// right-justified here instead of that component's left-aligned flex-wrap,
// per this panel's convention of hanging values against the right edge.
export const TAG_PILL =
  "rounded bg-slate-100 px-2 py-0.5 font-mono text-xs text-slate-700 dark:bg-slate-900 dark:text-slate-300";

// Exported so components/SettingsPanel.tsx can reuse the exact same
// headed-section/divider look rather than a second, driftable copy.
export const SECTION_BAR =
  "bg-slate-100 px-4 py-1 text-[10px] font-bold tracking-wider text-slate-600 uppercase dark:bg-slate-800 dark:text-slate-300";
export const DIVIDER = "border-t border-slate-200 dark:border-slate-700";
const ROW = "flex items-baseline justify-between gap-3 px-4 py-1.5";
const ROW_LABEL = "text-xs text-slate-500 dark:text-slate-400";
const ROW_VALUE = "text-sm text-slate-900 dark:text-slate-100";

export interface AircraftDetailPanelProps {
  /** `lastReceivedAt` (epoch ms) is AircraftRecord's client-stamped field --
   * optional here so this prop stays structurally compatible with a bare
   * MapFlight (e.g. in tests) as well as the real AircraftMap record. */
  aircraft: MapFlight & { lastReceivedAt?: number };
  center: CenterPoint | null;
  onClose: () => void;
  /** Isolate/Follow/Trace Points are real toggles; Zoom To is one-shot and
   * never shows an "active" state. All on/off decisions live in
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
// aircraft -- an additional surface alongside the floating per-aircraft
// info-box labels, not a replacement for them. Docked left with a
// top-4/left-4 margin so it never collides with ControlsPanel's top-right
// panel. Pinned at MAX_LABEL_Z_INDEX + 1, above every info-box label
// regardless of altitude-based stack order, reusing the same constant
// AircraftListPanel.tsx uses for its own "always above the map" guarantee.
export function AircraftDetailPanel({
  aircraft,
  center,
  onClose,
  isolateActive,
  onToggleIsolate,
  onZoomTo,
  followActive,
  onToggleFollow,
  tracePointsActive,
  onToggleTracePoints,
}: AircraftDetailPanelProps) {
  const data = useMemo(() => buildAircraftDetail(aircraft, center), [aircraft, center]);

  // Live-ticking clock for the Last Message Received row -- re-renders the
  // formatted relative age while the panel stays open (e.g. "3s ago" ->
  // "4s ago"), backing off from every second to every 15s to every minute
  // as the age grows. Re-armed whenever the selected aircraft changes or
  // lastReceivedAt moves forward.
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (data.lastReceivedAt == null) return;
    const lastReceivedAt = data.lastReceivedAt;
    let timeoutId: ReturnType<typeof setTimeout>;
    const tick = () => {
      const current = Date.now();
      setNow(current);
      timeoutId = setTimeout(tick, relativeTimeTickIntervalMs(current - lastReceivedAt));
    };
    setNow(Date.now());
    timeoutId = setTimeout(tick, relativeTimeTickIntervalMs(Date.now() - lastReceivedAt));
    return () => clearTimeout(timeoutId);
  }, [aircraft.icao_hex, data.lastReceivedAt]);
  const lastMessageReceived = data.lastReceivedAt != null ? formatRelativeTime(data.lastReceivedAt, now) : null;

  const flag = data.countryCode != null ? countryFlag(data.countryCode) : null;
  const hasHeaderSubline = data.registration != null || data.icaoHex != null;
  const hasBadges = data.military || data.specialLivery != null;

  return (
    // max-w-[calc(100vw-76px)]: 76px = left-4 (16) + ControlsPanel's icon
    // column footprint (right-4 + one button, 52) + an 8px gap -- keeps
    // the panel clear of the icon column on narrow viewports (#2047).
    <div
      className="absolute top-4 left-4 max-h-[80vh] w-80 max-w-[calc(100vw-76px)] overflow-y-auto rounded-md bg-white text-slate-900 shadow-md dark:bg-slate-900 dark:text-slate-100"
      style={{ zIndex: MAX_LABEL_Z_INDEX + 1 }}
    >
      <div className="flex items-start justify-between gap-3 p-3">
        <div className="min-w-0">
          {data.title != null && <div className="text-xl leading-tight font-bold">{data.title}</div>}
          {hasHeaderSubline && (
            <div className="mt-1 flex flex-wrap items-baseline gap-2">
              {data.registration != null && <span className="text-sm">{data.registration}</span>}
              {data.icaoHex != null && (
                <span className="font-mono text-xs text-slate-500 dark:text-slate-400">{data.icaoHex}</span>
              )}
              {flag != null && <span title={data.country ?? data.countryCode ?? undefined}>{flag}</span>}
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
        <FlightRows data={data} lastMessageReceived={lastMessageReceived} />
      </div>

      <div className={`${DIVIDER} flex items-center justify-center gap-2 px-4 py-2.5`}>
        <IconButton label="Isolate" icon={ISOLATE_ICON} active={isolateActive} onClick={onToggleIsolate} />
        <IconButton label="Zoom To" icon={ZOOM_TO_ICON} active={false} onClick={onZoomTo} />
        <IconButton label="Follow" icon={FOLLOW_ICON} active={followActive} onClick={onToggleFollow} />
        <IconButton
          label="Trace Points"
          icon={TRACE_POINTS_ICON}
          active={tracePointsActive}
          onClick={onToggleTracePoints}
        />
      </div>
    </div>
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
function FlightRows({
  data,
  lastMessageReceived,
}: {
  data: AircraftDetailData;
  /** Pre-formatted by the parent (see its `now` ticking state) rather than
   * derived from data.lastReceivedAt here, so this component stays a plain
   * render of already-computed strings. */
  lastMessageReceived: string | null;
}) {
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
      {lastMessageReceived != null && (
        <div className={`${ROW} ${divider()}`}>
          <span className={ROW_LABEL}>Last Message Received</span>
          <span className={ROW_VALUE}>{lastMessageReceived}</span>
        </div>
      )}
      {data.sources.length > 0 && (
        // flex-wrap so the label shares the first line with the pills
        // whenever they fit, only wrapping to a second line when they don't.
        <div className={`flex flex-wrap items-baseline justify-between gap-3 px-4 py-2 ${divider()}`}>
          <span className={ROW_LABEL}>Sources</span>
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
        <div className={`flex flex-wrap items-baseline justify-between gap-3 px-4 py-2 ${divider()}`}>
          <span className={ROW_LABEL}>Matched Rules</span>
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
