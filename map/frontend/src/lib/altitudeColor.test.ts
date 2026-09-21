import { describe, expect, it } from "vitest";
import { altitudeColor, darkenColor } from "./altitudeColor";

// Reference values mirrored from
// management-ui/frontend/src/lib/flightView.test.ts's altitudeColor
// suite -- this was originally a verbatim port of the same function, so
// most input/output pairs still hold here too. The 6,000-15,000ft range
// has since diverged (#1912, see altitudeColor.ts's own comment) -- every
// value below is deliberately chosen outside that range so this suite's
// existing "matches management-ui" guarantee stays meaningful; the
// diverged range gets its own dedicated tests further down.
describe("altitudeColor", () => {
  it("returns pure black for unknown/null altitude", () => {
    expect(altitudeColor(null)).toBe("hsl(0, 0%, 0%)");
  });

  it("produces a teal hue at 16000ft (verified reference value)", () => {
    expect(altitudeColor(16000)).toBe("hsl(167.6, 88.0%, 40.0%)");
  });

  it("is at or below ground level (0ft) with a low, orange-brown hue", () => {
    expect(altitudeColor(0)).toBe("hsl(20.0, 88.0%, 50.0%)");
    // Altitudes at or below the table's floor don't extrapolate past it.
    expect(altitudeColor(-2000)).toBe("hsl(20.0, 88.0%, 50.0%)");
  });

  it("interpolates smoothly between table breakpoints rather than banding", () => {
    // 1125ft falls between the 0ft/2000ft h-breakpoints and the 20/32
    // l-breakpoints -- neither an exact table value.
    expect(altitudeColor(1125)).toBe("hsl(27.0, 88.0%, 52.3%)");
  });

  it("reaches a teal/cyan hue at cruise-adjacent mid altitudes", () => {
    expect(altitudeColor(21500)).toBe("hsl(197.9, 88.0%, 49.9%)");
  });

  it("extrapolates flat beyond the table's highest breakpoint (51000ft)", () => {
    // Both altitudes fall on/after the last h-breakpoint (51000ft, val
    // 360 -> wraps to hue 0), so the color no longer changes past there.
    expect(altitudeColor(51000)).toBe(altitudeColor(100000));
  });
});

// #1912: the darkened 6,000-15,000ft band (roughly h 54-160), where the
// ramp reads as low-contrast against green NEXRAD radar returns. Hue and
// saturation are unchanged from before #1912 throughout this whole
// describe block -- only lightness moved -- so these assertions pin down
// exactly that: same h/s, meaningfully lower l than the pre-#1912 values
// (asserted directly, not just "differs").
describe("altitudeColor -- #1912 darkened band", () => {
  it("darkens the real reported case (SWA1760, 8250ft) by double digits", () => {
    // Pre-#1912 value was hsl(75.3, 88.0%, 41.5%).
    expect(altitudeColor(8250)).toBe("hsl(75.3, 88.0%, 26.2%)");
  });

  it("is darkest near the middle of the band (9000-10000ft), not just at the edges", () => {
    expect(altitudeColor(9000)).toBe("hsl(85.0, 88.0%, 24.8%)");
    expect(altitudeColor(10000)).toBe("hsl(112.5, 88.0%, 24.6%)");
  });

  it("tapers back to the unchanged neighboring value exactly at the 6000ft lower boundary", () => {
    // h=54 at 6000ft is a shared breakpoint between the untouched h<=50
    // region and the darkened region -- the l value here already reflects
    // interpolation into the darkened band (44.8 pre-#1912 -> 39.6 now),
    // not a hard edge.
    expect(altitudeColor(6000)).toBe("hsl(54.0, 88.0%, 39.6%)");
  });

  it("has fully rejoined the unchanged ramp by 15000ft", () => {
    // h=162.1 at 15000ft falls in the h>=160 region, whose l breakpoints
    // were not touched -- identical to the pre-#1912 value.
    expect(altitudeColor(15000)).toBe("hsl(162.1, 88.0%, 40.0%)");
  });

  it("keeps every altitude outside the band byte-identical to before #1912", () => {
    // Representative samples spanning ground level through the table's
    // ceiling, all outside the touched h 60-140 range.
    expect(altitudeColor(0)).toBe("hsl(20.0, 88.0%, 50.0%)");
    expect(altitudeColor(5000)).toBe("hsl(48.5, 88.0%, 47.9%)");
    expect(altitudeColor(22000)).toBe("hsl(200.7, 88.0%, 58.0%)");
    expect(altitudeColor(40000)).toBe("hsl(300.0, 88.0%, 43.0%)");
    expect(altitudeColor(51000)).toBe("hsl(0.0, 88.0%, 53.0%)");
  });
});

// Reference values mirrored from management-ui/frontend/src/lib/
// flightView.test.ts's darkenColor suite -- verbatim port of the same
// function.
describe("darkenColor", () => {
  it("subtracts 10 lightness points, same hue/saturation", () => {
    expect(darkenColor("hsl(167.6, 88.0%, 40.0%)")).toBe("hsl(167.6, 88.0%, 30.0%)");
  });

  it("clamps lightness at 0 rather than going negative", () => {
    expect(darkenColor("hsl(0, 0%, 5.0%)")).toBe("hsl(0, 0%, 0.0%)");
  });

  it("passes through a string that isn't an hsl(...) color unchanged", () => {
    expect(darkenColor("not-a-color")).toBe("not-a-color");
  });
});
