// Crosshair/target glyph markup for the recenter button
// (components/ControlsPanel.tsx, rendered via dangerouslySetInnerHTML).
// The on-map "center" marker (components/MapView.tsx) renders as a plain
// solid dot instead and does not use this module.

const MUTED_GRAY = "#6b7280"; // Tailwind slate-500 -- small, muted, doesn't compete with aircraft icons.

export function crosshairSvgMarkup(size: number, color: string = MUTED_GRAY): string {
  const c = size / 2;
  const r = size * 0.36;
  const tickInner = r * 0.55;
  const tickOuter = r * 1.15;
  return `
    <svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="${c}" cy="${c}" r="${r}" stroke="${color}" stroke-width="1.5" stroke-dasharray="3 3" />
      <line x1="${c}" y1="${c - tickOuter}" x2="${c}" y2="${c - tickInner}" stroke="${color}" stroke-width="1.5" />
      <line x1="${c}" y1="${c + tickInner}" x2="${c}" y2="${c + tickOuter}" stroke="${color}" stroke-width="1.5" />
      <line x1="${c - tickOuter}" y1="${c}" x2="${c - tickInner}" y2="${c}" stroke="${color}" stroke-width="1.5" />
      <line x1="${c + tickInner}" y1="${c}" x2="${c + tickOuter}" y2="${c}" stroke="${color}" stroke-width="1.5" />
    </svg>
  `.trim();
}

export { MUTED_GRAY };
