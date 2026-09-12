import type { IconSpec } from "../lib/actionIcons";
import { toggleButtonClass } from "../lib/toggleButtonStyle";

// Shared icon-only button + SVG renderer for this map view's two icon-row
// surfaces: AircraftDetailPanel's action row (Isolate/Zoom To/Follow/Trace
// Points) and ControlsPanel's toggle row (History/Labels/Map Labels/Range
// Outline). Extracted to one file so both provably share a single
// rendering mechanism instead of two near-identical copies that could
// drift -- the same reasoning lib/toggleButtonStyle.ts already applies to
// the button coloring alone. `title`/`aria-label` both carry `label` since
// these buttons show no visible text at this icon-only size.
export interface IconButtonProps {
  label: string;
  icon: IconSpec;
  active: boolean;
  onClick: () => void;
  /** Zoom To (one-shot) and Range Outline (no center configured) are
   * the two existing callers that need this; every other button omits it
   * and behaves exactly as before. */
  disabled?: boolean;
}

export function IconButton({ label, icon, active, onClick, disabled = false }: IconButtonProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      title={label}
      aria-label={label}
      aria-pressed={active}
      className={`flex h-8 w-8 items-center justify-center rounded border transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${toggleButtonClass(active)}`}
    >
      <ActionIcon spec={icon} />
    </button>
  );
}

// Renders one IconSpec (lib/actionIcons.ts) as an 18x18 stroke icon.
// `stroke="currentColor"` means the button's own active/inactive text
// color drives the icon color for free -- no separate icon-color logic.
// A circle marked `filled` (IconCircle.filled -- e.g. TAGS_ICON's small
// punch-hole dot) additionally gets `fill="currentColor"`, matching how
// Lucide's own source renders it; every other shape keeps inheriting the
// svg's `fill="none"`.
export function ActionIcon({ spec }: { spec: IconSpec }) {
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
      {spec.circles?.map((c, i) => (
        <circle key={`c${i}`} cx={c.cx} cy={c.cy} r={c.r} fill={c.filled ? "currentColor" : undefined} />
      ))}
      {spec.lines?.map((l, i) => <line key={`l${i}`} x1={l.x1} y1={l.y1} x2={l.x2} y2={l.y2} />)}
      {spec.paths?.map((p, i) => <path key={`p${i}`} d={p.d} />)}
      {spec.polygons?.map((pg, i) => <polygon key={`pg${i}`} points={pg.points} />)}
    </svg>
  );
}
