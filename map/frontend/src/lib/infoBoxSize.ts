import type { InfoBoxLines } from "./infoBox";

// Rough monospace-font size estimate used to feed the overlap-placement
// algorithm (lib/placement.ts) *before* the box's actual DOM element
// exists (placement has to run before render, since render needs the
// placement result). Deliberately approximate -- a slightly-off estimate
// just makes a nudge kick in a few pixels early/late, never a visible
// collision, since the algorithm re-checks against the estimate
// consistently on both sides of every comparison.
const IDENT_CHAR_WIDTH = 7.4; // ~12px bold monospace
const DETAIL_CHAR_WIDTH = 6.3; // ~10.5px monospace
const LINE_HEIGHT_IDENT = 16;
const LINE_HEIGHT_DETAIL = 14;
const PADDING_X = 12; // 6px each side
const PADDING_Y = 8; // 4px top/bottom

export function estimateInfoBoxSize(lines: InfoBoxLines): { width: number; height: number } {
  const widths: number[] = [];
  let height = PADDING_Y;

  if (lines.ident !== null) {
    widths.push(lines.ident.length * IDENT_CHAR_WIDTH);
    height += LINE_HEIGHT_IDENT;
  }
  if (lines.altitudeSpeed !== null) {
    widths.push(lines.altitudeSpeed.length * DETAIL_CHAR_WIDTH);
    height += LINE_HEIGHT_DETAIL;
  }
  if (lines.registrationType !== null) {
    widths.push(lines.registrationType.length * DETAIL_CHAR_WIDTH);
    height += LINE_HEIGHT_DETAIL;
  }

  const width = widths.length > 0 ? Math.max(...widths) + PADDING_X : 0;
  return { width, height: widths.length > 0 ? height : 0 };
}
