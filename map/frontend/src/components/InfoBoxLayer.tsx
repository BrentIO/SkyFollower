import { useMemo } from "react";
import { buildInfoBoxLines, type InfoBoxAircraft } from "../lib/infoBox";
import { altitudeZIndex, sortByLabelStackOrder } from "../lib/labelStackOrder";

// Gap between the aircraft icon's screen position and the info box's
// near (top-left) corner. Fixed -- boxes are never nudged to avoid a
// collision, so this is the box's only position, not just a default.
export const DEFAULT_INFO_BOX_OFFSET = 34;

export interface InfoBoxLayerItem {
  id: string;
  /** Aircraft icon's current screen-space position (px), from maplibregl.Map.project(). */
  x: number;
  y: number;
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
}

// Renders one floating ATC-style info box per labeled aircraft, directly
// on the map (no side panel). Hidden by default: a box only renders for a
// selected or hovered aircraft, unless the "Labels: All" toggle
// (ControlsPanel) is on. Each box sits at a fixed offset from its
// aircraft's icon -- no leader line, no collision-avoidance nudging -- so
// boxes are free to overlap when aircraft are close together. Where boxes
// overlap, the higher-altitude aircraft's box draws on top (see
// lib/labelStackOrder.ts); unknown-altitude aircraft sit at the bottom of
// the stack.
export function InfoBoxLayer({ items, selected, showAll, hoveredId }: InfoBoxLayerProps) {
  const boxes = useMemo(() => {
    const labeled = items.filter((item) => showAll || selected.has(item.id) || item.id === hoveredId);

    const withLines = labeled
      .map((item) => ({ item, lines: buildInfoBoxLines(item.aircraft) }))
      // Nothing to show for an aircraft whose box is empty (no ident,
      // altitude, speed, registration, or type resolved yet).
      .filter(({ lines }) => lines.ident !== null || lines.altitudeSpeed !== null || lines.registrationType !== null);

    return sortByLabelStackOrder(withLines.map(({ item, lines }) => ({ id: item.id, altitude: item.aircraft.altitude, item, lines })));
  }, [items, selected, showAll, hoveredId]);

  return (
    <div className="pointer-events-none absolute inset-0">
      {boxes.map((b) => (
        <div
          key={b.id}
          className="absolute rounded bg-black/40 px-1.5 py-1 font-mono leading-tight text-white"
          style={{
            left: b.item.x + DEFAULT_INFO_BOX_OFFSET,
            top: b.item.y + DEFAULT_INFO_BOX_OFFSET,
            zIndex: altitudeZIndex(b.item.aircraft.altitude),
          }}
        >
          {b.lines.ident !== null && <div className="text-[12px] font-bold whitespace-nowrap">{b.lines.ident}</div>}
          {b.lines.altitudeSpeed !== null && (
            <div className="text-[10.5px] whitespace-nowrap">{b.lines.altitudeSpeed}</div>
          )}
          {b.lines.registrationType !== null && (
            <div className="text-[10.5px] whitespace-nowrap">{b.lines.registrationType}</div>
          )}
        </div>
      ))}
    </div>
  );
}
