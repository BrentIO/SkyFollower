// Formats a past epoch-ms timestamp as a coarse, human-relative age string
// ("1 second ago", "4 minutes ago", "2 hours ago", "3 days ago") for the
// aircraft detail panel's Last Message Received row (see
// components/AircraftDetailPanel.tsx and lib/aircraftDetail.ts). Always the
// single largest applicable unit, no fractional units, and floors (never
// rounds) so the displayed age only ever counts up as time passes rather
// than jumping ahead of the real elapsed time.

const SECOND_MS = 1000;
const MINUTE_MS = 60 * SECOND_MS;
const HOUR_MS = 60 * MINUTE_MS;
const DAY_MS = 24 * HOUR_MS;

function pluralize(value: number, unit: string): string {
  return `${value} ${unit}${value === 1 ? "" : "s"} ago`;
}

// `now` is an injectable epoch-ms reading (rather than an implicit
// Date.now()) purely so callers/tests can pin it -- same convention as
// aircraftState.ts's pushTracePoint. A `pastEpochMs` at or after `now`
// (clock skew, or the very instant a message arrives) clamps to zero
// rather than going negative.
export function formatRelativeTime(pastEpochMs: number, now: number): string {
  const deltaMs = Math.max(0, now - pastEpochMs);
  if (deltaMs < MINUTE_MS) return pluralize(Math.floor(deltaMs / SECOND_MS), "second");
  if (deltaMs < HOUR_MS) return pluralize(Math.floor(deltaMs / MINUTE_MS), "minute");
  if (deltaMs < DAY_MS) return pluralize(Math.floor(deltaMs / HOUR_MS), "hour");
  return pluralize(Math.floor(deltaMs / DAY_MS), "day");
}

// Live-ticking cadence for the detail panel's re-render schedule: every
// second while the age is under a minute (so "1 second ago" visibly counts
// up), backing off to every 15 seconds once in minutes, and every minute
// once in hours/days, where finer-grained ticking would never produce a
// visibly different string anyway. Takes the *current* age in ms so the
// caller can re-derive its own next delay after every tick rather than
// relying on one fixed interval for the whole panel session.
export function relativeTimeTickIntervalMs(ageMs: number): number {
  if (ageMs < MINUTE_MS) return SECOND_MS;
  if (ageMs < HOUR_MS) return 15 * SECOND_MS;
  return MINUTE_MS;
}
