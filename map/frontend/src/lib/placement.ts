// Info-box overlap/nudge placement -- see components/InfoBoxLayer.tsx.
//
// Every aircraft's info box defaults to a fixed diagonal (south-east)
// offset from its icon's screen position -- ~34px, tuned against the
// icon's actual rotated footprint so the box never touches the icon at
// any heading. When two boxes at their default offsets would visually
// collide, this pushes the colliding one further out along that same
// diagonal until it clears every already-placed box -- every box stays
// visible, nothing is ever hidden (MapLibre's native
// text-allow-overlap:false collision-hiding is deliberately not used
// here, since hiding a box is not acceptable for this view). A leader
// line is drawn only for a box that ends up nudged past the default
// offset; a box at its default offset never gets one.
//
// Recomputed on each batch of incoming updates (not on every animation
// frame) -- cheap at the realistic aircraft counts this view targets (at
// most a few dozen).

// Gap between the aircraft icon's screen position and the info box's
// near (top-left) corner at its default, non-colliding placement.
export const DEFAULT_INFO_BOX_OFFSET = 34;

// How far each collision-resolution step pushes a box further out along
// the diagonal before re-checking for overlaps.
const NUDGE_STEP = 12;

// Upper bound on nudge steps for one box -- purely a safety valve against
// runaway loops; in practice a handful of steps resolves any realistic
// cluster of a few dozen aircraft.
const MAX_NUDGE_STEPS = 60;

export interface AircraftBoxInput {
  /** Stable identifier (icao_hex) -- also used to break placement ties deterministically. */
  id: string;
  /** Aircraft icon's screen-space x/y (px), e.g. from maplibregl.Map.project(). */
  x: number;
  y: number;
  width: number;
  height: number;
}

export interface BoxPlacement {
  id: string;
  /** Info box's top-left corner, screen-space px. */
  x: number;
  y: number;
  width: number;
  height: number;
  /** True once nudged past the default offset -- callers should draw a leader line only when this is true. */
  nudged: boolean;
  /** Anchor point the leader line should originate from (the aircraft icon's screen position). */
  anchorX: number;
  anchorY: number;
}

function rectsOverlap(
  a: { x: number; y: number; width: number; height: number },
  b: { x: number; y: number; width: number; height: number },
): boolean {
  return !(a.x + a.width <= b.x || b.x + b.width <= a.x || a.y + a.height <= b.y || b.y + b.height <= a.y);
}

// Places every aircraft's info box, nudging any that collide with an
// already-placed box further out along the default south-east diagonal.
// Deterministic: inputs are placed in ascending `id` order regardless of
// array order, so the same set of aircraft always resolves to the same
// layout (independent of e.g. WebSocket event arrival order).
export function placeInfoBoxes(inputs: AircraftBoxInput[]): BoxPlacement[] {
  const sorted = [...inputs].sort((a, b) => a.id.localeCompare(b.id));
  const placed: BoxPlacement[] = [];

  for (const input of sorted) {
    let offset = DEFAULT_INFO_BOX_OFFSET;
    let nudged = false;
    let rect = { x: input.x + offset, y: input.y + offset, width: input.width, height: input.height };

    for (let step = 0; step < MAX_NUDGE_STEPS; step++) {
      const collides = placed.some((p) => rectsOverlap(rect, p));
      if (!collides) break;
      offset += NUDGE_STEP;
      nudged = true;
      rect = { x: input.x + offset, y: input.y + offset, width: input.width, height: input.height };
    }

    placed.push({
      id: input.id,
      x: rect.x,
      y: rect.y,
      width: input.width,
      height: input.height,
      nudged,
      anchorX: input.x,
      anchorY: input.y,
    });
  }

  return placed;
}
