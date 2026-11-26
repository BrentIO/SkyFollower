import { MAX_LABEL_Z_INDEX } from "../lib/labelStackOrder";
import { flightCategoryStyle, metarRows, type MetarStation } from "../lib/metar";
import { DIVIDER, PILL, SECTION_BAR } from "./AircraftDetailPanel";

export interface MetarDetailPanelProps {
  station: MetarStation;
  /** Epoch ms, for the report's age. */
  now: number;
  onClose: () => void;
}

// The selected weather station's card. Same position and look as
// AircraftDetailPanel; only one of the two is open at a time.
export function MetarDetailPanel({ station, now, onClose }: MetarDetailPanelProps) {
  const category = flightCategoryStyle(station.flight_category);
  return (
    <div
      className="absolute top-4 left-4 flex max-h-[80vh] w-80 max-w-[calc(100vw-100px)] flex-col overflow-hidden rounded-md bg-white text-slate-900 shadow-md dark:bg-slate-900 dark:text-slate-100"
      style={{ zIndex: MAX_LABEL_Z_INDEX + 1 }}
    >
      <div className="flex items-start justify-between gap-3 p-3">
        <div className="min-w-0">
          <span
            className={`${PILL} inline-block px-3 py-1 text-xl leading-tight font-bold`}
            style={{ backgroundColor: category.color, color: category.textColor }}
            title={category.label}
          >
            {station.station_id}
          </span>
          {station.name != null && <div className="mt-1 text-sm">{station.name}</div>}
        </div>
        <button
          type="button"
          onClick={onClose}
          aria-label="Close"
          className="flex h-6 w-6 shrink-0 items-center justify-center rounded text-slate-500 hover:bg-slate-100 dark:text-slate-400 dark:hover:bg-slate-800"
        >
          <svg width="14" height="14" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M4 4L12 12M12 4L4 12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" />
          </svg>
        </button>
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        <dl className={DIVIDER}>
          {metarRows(station, now).map(([label, value], index) => (
            <div key={label} className={`flex items-baseline justify-between gap-3 px-4 py-1.5 ${index ? DIVIDER : ""}`}>
              <dt className="shrink-0 text-xs text-slate-500 dark:text-slate-400">{label}</dt>
              <dd className="text-right text-sm">{value}</dd>
            </div>
          ))}
        </dl>
        <div className={DIVIDER}>
          <div className={SECTION_BAR}>Raw METAR</div>
          <div className="px-4 py-2.5 font-mono text-xs break-words">{station.raw}</div>
        </div>
      </div>
    </div>
  );
}
