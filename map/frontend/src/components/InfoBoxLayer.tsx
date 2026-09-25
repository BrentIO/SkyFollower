import { useMemo } from "react";
import { buildInfoBoxLines, type InfoBoxAircraft, type TrendDirection } from "../lib/infoBox";
import { altitudeZIndex, sortByLabelStackOrder } from "../lib/labelStackOrder";

// Restored (#1851) as the aircraft info-box's only implementation, after
// #1808 replaced it with a GPU/MapLibre symbol layer over CPU concerns that
// a live trace comparison (see #1851's own issue body) showed no longer
// hold now that #1838/#1840 fixed the actual dominant cost (trail
// rendering). This is an unmodified drop-in of the pre-#1808 component --
// lib/infoBox.ts's buildInfoBoxLines() and lib/labelStackOrder.ts's
// altitudeZIndex()/sortByLabelStackOrder() were confirmed unchanged in
// shape since #1808's removal, so no adaptation was needed here.

export interface InfoBoxLayerItem {
  id: string;
  /** Aircraft icon's current screen-space position (px), from maplibregl.Map.project(). */
  x: number;
  y: number;
  /**
   * Gap (px) from `x`/`y` to the box's near (top-left) corner, from
   * lib/infoBoxOffset.ts's zoom-scaled offset -- smaller at a zoomed-out
   * view so the box still reads as anchored to its icon. Fixed per render,
   * not nudged -- see lib/labelStackOrder.ts for how overlapping boxes are
   * stacked instead.
   */
  offset: number;
  aircraft: InfoBoxAircraft;
}

export interface InfoBoxLayerProps {
  items: InfoBoxLayerItem[];
  /** icao_hex of every currently-selected aircraft -- always labeled. */
  selected: Set<string>;
  /** "Labels: All" toggle -- when on, every aircraft is labeled regardless of selection. */
  showAll: boolean;
  /** icao_hex of the aircraft currently hovered, if any -- shown as a transient label. */
  hoveredId?: string | null;
  /** #2000: ControlsPanel's operator-facing display-scale multiplier
   * (MapView.tsx state, persisted via controlsPersistence.ts), shared with
   * the aircraft icon's own icon-size expression so both scale together
   * from one control. Applied as a CSS transform (see the box's style
   * below) rather than by recomputing each font-size/padding value --
   * scales the whole box uniformly, including its background/padding, with
   * no risk of drifting out of proportion with the text inside it. 1 is
   * the no-op default. */
  displayScale: number;
}

// Renders one floating ATC-style info box per labeled aircraft, directly
// on the map (no side panel). Hidden by default: a box only renders for a
// selected or hovered aircraft, unless the "Labels: All" toggle
// (ControlsPanel) is on. Each box sits at a zoom-scaled offset from its
// aircraft's icon (lib/infoBoxOffset.ts) -- no leader line, no
// collision-avoidance nudging -- so boxes are free to overlap when
// aircraft are close together. Where boxes overlap, the higher-altitude
// aircraft's box draws on top (see lib/labelStackOrder.ts); unknown-altitude
// aircraft sit at the bottom of the stack.
export function InfoBoxLayer({ items, selected, showAll, hoveredId, displayScale }: InfoBoxLayerProps) {
  const boxes = useMemo(() => {
    const labeled = items.filter((item) => showAll || selected.has(item.id) || item.id === hoveredId);

    const withLines = labeled
      .map((item) => ({ item, lines: buildInfoBoxLines(item.aircraft) }))
      // Nothing to show for an aircraft whose box is empty (no ident,
      // altitude, speed, registration, or type resolved yet).
      .filter(({ lines }) => lines.ident !== null || lines.altitudeSpeed !== null || lines.registrationType !== null);

    return sortByLabelStackOrder(withLines.map(({ item, lines }) => ({ id: item.id, altitude: item.aircraft.alt, item, lines })));
  }, [items, selected, showAll, hoveredId]);

  return (
    <div className="pointer-events-none absolute inset-0">
      {boxes.map((b) => (
        <div
          key={b.id}
          className="absolute rounded bg-black/40 px-1.5 py-1 font-mono leading-tight text-white"
          style={{
            left: b.item.x + b.item.offset,
            top: b.item.y + b.item.offset,
            zIndex: altitudeZIndex(b.item.aircraft.alt),
            // #2000: scaling from the top-left corner (the box's own
            // anchor point, per infoBoxOffset.ts) keeps that corner fixed
            // on screen as the box grows/shrinks, so this needs no changes
            // to the left/top math above. Only added when displayScale
            // actually differs from the 1.0 no-op default -- an unused
            // `transform` would still promote every box to its own
            // compositor layer, a real cost this component has been tuned
            // to avoid (see this file's own #1851 history) for zero visual
            // benefit at the default setting.
            ...(displayScale !== 1
              ? { transform: `scale(${displayScale})`, transformOrigin: "top left" }
              : {}),
          }}
        >
          {b.lines.ident !== null && <div className="text-[12px] font-bold whitespace-nowrap">{b.lines.ident}</div>}
          {b.lines.altitudeSpeed !== null && (
            <div className="text-[10.5px] whitespace-nowrap">
              {b.lines.altitudeSpeed.altitude}
              {b.lines.altitudeSpeed.trend !== null && <TrendGlyph direction={b.lines.altitudeSpeed.trend} />}
              {b.lines.altitudeSpeed.altitude !== null && b.lines.altitudeSpeed.groundspeed !== null && " "}
              {b.lines.altitudeSpeed.groundspeed}
            </div>
          )}
          {b.lines.registrationType !== null && (
            <div className="text-[10.5px] whitespace-nowrap">{b.lines.registrationType}</div>
          )}
        </div>
      ))}
    </div>
  );
}

// #2001: the vertical-speed trend used to be a plain Unicode up/down arrow
// character rendered inline as text. A specific character isn't guaranteed
// to be in every font's glyph table -- on macOS the font-mono stack's SF
// Mono glyph for it fell back to a different, undersized substitute font,
// while the surrounding digits (which every font covers) rendered fine. An SVG
// shape sized in `em`s (tied to the line's own font-size) sidesteps
// per-glyph font-fallback entirely and renders identically on every
// platform. `fill="currentColor"` picks up the box's white text color for
// free, matching how the arrow character inherited it before. `align-*`
// nudges the shape up from its own bottom edge onto the text baseline --
// a flat SVG's baseline is its bottom edge by default, which reads low
// next to the digits' x-height otherwise.
function TrendGlyph({ direction }: { direction: NonNullable<TrendDirection> }) {
  return (
    <svg
      viewBox="0 0 10 10"
      className="inline-block h-[0.7em] w-[0.7em] align-[0.05em]"
      fill="currentColor"
      aria-hidden="true"
    >
      <polygon points={direction === "up" ? "5,1 9,8 1,8" : "5,9 1,2 9,2"} />
    </svg>
  );
}
