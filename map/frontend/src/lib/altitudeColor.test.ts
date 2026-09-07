import { describe, expect, it } from "vitest";
import { altitudeColor } from "./altitudeColor";

// Reference values mirrored from
// management-ui/frontend/src/lib/flightView.test.ts's altitudeColor
// suite -- this is a verbatim port of the same function, so the same
// input/output pairs must hold here too.
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
