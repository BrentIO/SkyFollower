import { useMemo } from "react";
import { buildInfoBoxLines, type InfoBoxAircraft } from "../lib/infoBox";
import { estimateInfoBoxSize } from "../lib/infoBoxSize";
import { placeInfoBoxes } from "../lib/placement";

export interface InfoBoxLayerItem {
  id: string;
  /** Aircraft icon's current screen-space position (px), from maplibregl.Map.project(). */
  x: number;
  y: number;
  aircraft: InfoBoxAircraft;
}

export interface InfoBoxLayerProps {
  items: InfoBoxLayerItem[];
}

// Renders every visible aircraft's ATC-style info box, floating directly
// on the map (no side panel), plus a leader line for any box that had to
// be nudged clear of a collision (see lib/placement.ts) -- never for a
// box at its default offset. Shown for every aircraft simultaneously, not
// hover/click-only.
export function InfoBoxLayer({ items }: InfoBoxLayerProps) {
  const placements = useMemo(() => {
    const inputs = items.map((item) => {
      const lines = buildInfoBoxLines(item.aircraft);
      const { width, height } = estimateInfoBoxSize(lines);
      return { id: item.id, x: item.x, y: item.y, width, height, lines };
    });
    // Nothing to place for an aircraft whose box is empty (no ident,
    // altitude, speed, registration, or type resolved yet).
    const placeable = inputs.filter((i) => i.width > 0 && i.height > 0);
    const placed = placeInfoBoxes(placeable);
    const linesById = new Map(inputs.map((i) => [i.id, i.lines]));
    return placed.map((p) => ({ ...p, lines: linesById.get(p.id)! }));
  }, [items]);

  return (
    <div className="pointer-events-none absolute inset-0">
      <svg className="absolute inset-0 h-full w-full">
        {placements
          .filter((p) => p.nudged)
          .map((p) => (
            <line
              key={p.id}
              x1={p.anchorX}
              y1={p.anchorY}
              x2={p.x}
              y2={p.y}
              // Dark, not white -- the basemap (MAP_STYLE, "positron") is
              // light, so a dark line is what actually stays visible
              // against it.
              stroke="rgba(30,41,59,0.7)"
              strokeWidth={1.5}
            />
          ))}
      </svg>
      {placements.map((p) => (
        <div
          key={p.id}
          className="absolute rounded bg-black/40 px-1.5 py-1 font-mono leading-tight text-white"
          style={{ left: p.x, top: p.y }}
        >
          {p.lines.ident !== null && <div className="text-[12px] font-bold whitespace-nowrap">{p.lines.ident}</div>}
          {p.lines.altitudeSpeed !== null && (
            <div className="text-[10.5px] whitespace-nowrap">{p.lines.altitudeSpeed}</div>
          )}
          {p.lines.registrationType !== null && (
            <div className="text-[10.5px] whitespace-nowrap">{p.lines.registrationType}</div>
          )}
        </div>
      ))}
    </div>
  );
}
