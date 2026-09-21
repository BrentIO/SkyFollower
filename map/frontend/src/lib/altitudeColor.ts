// Originally ported verbatim from management-ui/frontend/src/lib/
// flightView.ts's altitudeColor()/darkenColor() -- this is a separate,
// standalone frontend project, so it carries its own copy rather than
// importing across the two. The l breakpoints for h 60-140 were darkened
// (#1912, see the table below) and the same darkening was ported into
// management-ui's copy in lockstep, so the two tables stay identical.
// altitudeColor is used for the aircraft icon fill (MapView.tsx's symbol
// layer `icon-color`), the live trail color (MapView.tsx's trail
// `line-color`), and the Trace Points dot color (lib/tracePoints.ts), all
// driven off an altitude value -- same mechanism throughout, per design.
// darkenColor is used only by Trace Points, for the dot's stroke.

// Altitude-to-color lookup table (hue and lightness each interpolated from
// their own set of breakpoints below), giving a smooth climb/cruise/descent
// color ramp. Only an "air" table is needed here: null altitude is handled
// separately as pure black (see altitudeColor below) rather than through a
// ground/unknown table entry.
//
// The l breakpoints for h 60-140 (roughly the 6,000-11,000ft altitude band
// -- see the h table below) are darkened ~13-16 points relative to their
// neighbors (#1912): that hue range is a yellow-green-through-green that
// reads as low-contrast against NEXRAD weather-radar reflectivity, which
// conventionally also shades light/moderate precipitation in the same
// green family (confirmed against a real reported case -- an aircraft at
// 8,250ft nearly invisible over a green radar return). Hue is left
// untouched: shifting it risked colliding with the already-blue cruise-
// altitude band (h 200-266, roughly 18,500-27,000ft) further up this same
// table. Darkening instead leans on this file's own already-established
// principle that a darker color reads clearly against this app's light
// basemap (see the null-altitude case below). The darkening is isolated to
// h 60-140 -- h 50 and h 160, its boundary breakpoints, are unchanged, so
// the dip tapers smoothly in from the lighter neighboring bands on both
// sides rather than creating a hard edge. Every altitude outside roughly
// 6,000-15,000ft renders byte-for-byte identically to before this change.
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
    { h: 60, val: 30 },
    { h: 80, val: 25 },
    { h: 100, val: 24 },
    { h: 120, val: 25 },
    { h: 140, val: 28 },
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

// Darkens an `altitudeColor()` output by ~10 lightness percentage points
// (clamped at 0), same hue/saturation -- used for Trace Points circle
// strokes so overlapping points at low zoom read as a darker shade of the
// same altitude color instead of merging into a flat near-black outline.
export function darkenColor(color: string): string {
  const match = color.match(/^hsl\(([\d.]+), ([\d.]+)%, ([\d.]+)%\)$/);
  if (!match) return color;
  const [, h, s, l] = match;
  const darkenedL = Math.max(0, Number(l) - 10);
  return `hsl(${h}, ${s}%, ${darkenedL.toFixed(1)}%)`;
}
