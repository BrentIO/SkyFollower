// Presentation logic for ControlsPanel's connection-status dot: turns a
// GET /api/processors roster (see api/types.ts's ProcessorRoster) plus the
// browser's own WebSocket connection state into the dot's color and hover
// tooltip. Pure functions, kept out of the component so they're testable
// without a DOM/React-rendering dependency, same convention as
// labelStackOrder.ts/infoBox.ts.

import type { ProcessorRoster, ProcessorStatusValue } from "../api/types";

export const PROCESSOR_STATUS_DOT_COLOR: Record<ProcessorStatusValue, string> = {
  green: "bg-green-500",
  amber: "bg-amber-500",
  red: "bg-red-500",
};

// "Connected"/"Reconnecting"/"Disconnected" -- the issue's own wording for
// each per-processor row in the hover tooltip.
export const PROCESSOR_STATUS_LABEL: Record<ProcessorStatusValue, string> = {
  green: "Connected",
  amber: "Reconnecting",
  red: "Disconnected",
};

// The overall indicator's color: if the browser's own WebSocket to this
// map backend is down, there is no live proof of anything, so this is red
// regardless of the last-known roster snapshot (which could otherwise be
// stale rather than genuinely green).
export function overallConnectionStatus(wsConnected: boolean, roster: ProcessorRoster): ProcessorStatusValue {
  return wsConnected ? roster.overall : "red";
}

// The dot's hover tooltip: one line per rostered processor
// ("mp-1: Connected"), or an explanatory line for the two edge cases a
// per-processor label can't cover on its own (no WS connection at all, or
// a WS connection with nothing rostered yet).
export function connectionTooltip(wsConnected: boolean, roster: ProcessorRoster): string {
  if (!wsConnected) return "Disconnected from map service";
  if (roster.processors.length === 0) return "No message processors seen yet";
  return roster.processors
    .map((p) => `${p.processor_id}: ${PROCESSOR_STATUS_LABEL[p.status]}`)
    .join("\n");
}
