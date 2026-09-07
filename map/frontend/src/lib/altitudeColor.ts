// Ported verbatim (logic and breakpoint tables unchanged) from
// management-ui/frontend/src/lib/flightView.ts's altitudeColor() -- this
// is a separate, standalone frontend project, so it carries its own copy
// rather than importing across the two. Used for both the aircraft icon
// fill (MapView.tsx's symbol layer `icon-color`) and the live trail color
// (MapView.tsx's trail `line-color`), both driven off each aircraft's
// current altitude -- same mechanism for both, per design.

// Altitude-to-color lookup table (hue and lightness each interpolated from
// their own set of breakpoints below), giving a smooth climb/cruise/descent
// color ramp. Only an "air" table is needed here: null altitude is handled
// separately as pure black (see altitudeColor below) rather than through a
// ground/unknown table entry.
const COLOR_BY_ALT_AIR = {
  s: 88,
  h: [
    { alt: 0, val: 20 },
    { alt: 2000, val: 32.5 },
    { alt: 4000, val: 43 },
    { alt: 6000, val: 54 },
    { alt: 8000, val: 72 },
    { alt: 9000, val: 85 },
    { alt: 11000, val: 140 },
    { alt: 40000, val: 300 },
    { alt: 51000, val: 360 },
  ],
  l: [
    { h: 0, val: 53 },
    { h: 20, val: 50 },
    { h: 32, val: 54 },
    { h: 40, val: 52 },
    { h: 46, val: 51 },
    { h: 50, val: 46 },
    { h: 60, val: 43 },
    { h: 80, val: 41 },
    { h: 100, val: 41 },
    { h: 120, val: 41 },
    { h: 140, val: 41 },
    { h: 160, val: 40 },
    { h: 180, val: 40 },
    { h: 190, val: 44 },
    { h: 198, val: 50 },
    { h: 200, val: 58 },
    { h: 220, val: 58 },
    { h: 240, val: 58 },
    { h: 255, val: 55 },
    { h: 266, val: 55 },
    { h: 270, val: 58 },
    { h: 280, val: 58 },
    { h: 290, val: 47 },
    { h: 300, val: 43 },
    { h: 310, val: 48 },
    { h: 320, val: 48 },
    { h: 340, val: 52 },
    { h: 360, val: 53 },
  ],
};

// Interpolates hue then lightness from COLOR_BY_ALT_AIR's breakpoints.
//
// Two deliberate design choices here (both user-confirmed, carried over
// from the ported source):
//   - Null/unknown altitude renders pure black rather than a light gray --
//     the icon/line are thick enough that solid black reads clearly on the
//     light basemap.
//   - Altitude is interpolated at its raw value rather than quantized to
//     fixed bands first.
export function altitudeColor(altitudeFt: number | null): string {
  if (altitudeFt === null) return "hsl(0, 0%, 0%)";

  const s = COLOR_BY_ALT_AIR.s;

  const hpoints = COLOR_BY_ALT_AIR.h;
  let h = hpoints[0].val;
  for (let i = hpoints.length - 1; i >= 0; --i) {
    if (altitudeFt > hpoints[i].alt) {
      h =
        i === hpoints.length - 1
          ? hpoints[i].val
          : hpoints[i].val +
            ((hpoints[i + 1].val - hpoints[i].val) * (altitudeFt - hpoints[i].alt)) /
              (hpoints[i + 1].alt - hpoints[i].alt);
      break;
    }
  }

  const lpoints = COLOR_BY_ALT_AIR.l;
  let l = lpoints[0].val;
  for (let i = lpoints.length - 1; i >= 0; --i) {
    if (h > lpoints[i].h) {
      l =
        i === lpoints.length - 1
          ? lpoints[i].val
          : lpoints[i].val + ((lpoints[i + 1].val - lpoints[i].val) * (h - lpoints[i].h)) / (lpoints[i + 1].h - lpoints[i].h);
      break;
    }
  }

  if (h < 0) h = (h % 360) + 360;
  else if (h >= 360) h = h % 360;
  const clampedS = Math.max(0, Math.min(95, s));
  const clampedL = Math.max(0, Math.min(95, l));
  return `hsl(${h.toFixed(1)}, ${clampedS.toFixed(1)}%, ${clampedL.toFixed(1)}%)`;
}
