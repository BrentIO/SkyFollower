import { describe, expect, it } from "vitest";
import { formatRelativeTime, relativeTimeTickIntervalMs } from "./relativeTime";

describe("formatRelativeTime -- seconds", () => {
  it("formats zero elapsed time as 0 seconds ago", () => {
    expect(formatRelativeTime(1000, 1000)).toBe("0 seconds ago");
  });

  it("singularizes exactly 1 second", () => {
    expect(formatRelativeTime(0, 1000)).toBe("1 second ago");
  });

  it("pluralizes 2-59 seconds", () => {
    expect(formatRelativeTime(0, 2000)).toBe("2 seconds ago");
    expect(formatRelativeTime(0, 59000)).toBe("59 seconds ago");
  });

  it("floors rather than rounds -- 1999ms is still 1 second, not 2", () => {
    expect(formatRelativeTime(0, 1999)).toBe("1 second ago");
  });
});

describe("formatRelativeTime -- minutes boundary", () => {
  it("rolls over to 1 minute ago at exactly 60000ms", () => {
    expect(formatRelativeTime(0, 60000)).toBe("1 minute ago");
  });

  it("stays at 59 seconds ago just below the boundary", () => {
    expect(formatRelativeTime(0, 59999)).toBe("59 seconds ago");
  });

  it("pluralizes minutes and floors within the minute", () => {
    expect(formatRelativeTime(0, 4 * 60000)).toBe("4 minutes ago");
    expect(formatRelativeTime(0, 4 * 60000 + 59000)).toBe("4 minutes ago");
    expect(formatRelativeTime(0, 59 * 60000)).toBe("59 minutes ago");
  });
});

describe("formatRelativeTime -- hours boundary", () => {
  it("rolls over to 1 hour ago at exactly 3600000ms", () => {
    expect(formatRelativeTime(0, 3600000)).toBe("1 hour ago");
  });

  it("stays at 59 minutes ago just below the boundary", () => {
    expect(formatRelativeTime(0, 3600000 - 1)).toBe("59 minutes ago");
  });

  it("pluralizes hours and floors within the hour", () => {
    expect(formatRelativeTime(0, 2 * 3600000)).toBe("2 hours ago");
    expect(formatRelativeTime(0, 23 * 3600000)).toBe("23 hours ago");
  });
});

describe("formatRelativeTime -- days boundary", () => {
  it("rolls over to 1 day ago at exactly 86400000ms", () => {
    expect(formatRelativeTime(0, 86400000)).toBe("1 day ago");
  });

  it("stays at 23 hours ago just below the boundary", () => {
    expect(formatRelativeTime(0, 86400000 - 1)).toBe("23 hours ago");
  });

  it("pluralizes days and floors within the day", () => {
    expect(formatRelativeTime(0, 3 * 86400000)).toBe("3 days ago");
  });
});

describe("formatRelativeTime -- clock skew", () => {
  it("clamps a pastEpochMs after `now` to zero rather than going negative", () => {
    expect(formatRelativeTime(5000, 1000)).toBe("0 seconds ago");
  });
});

describe("relativeTimeTickIntervalMs", () => {
  it("ticks every second under a minute", () => {
    expect(relativeTimeTickIntervalMs(0)).toBe(1000);
    expect(relativeTimeTickIntervalMs(59999)).toBe(1000);
  });

  it("backs off to every 15 seconds once in minutes", () => {
    expect(relativeTimeTickIntervalMs(60000)).toBe(15000);
    expect(relativeTimeTickIntervalMs(3599999)).toBe(15000);
  });

  it("backs off to every minute once in hours", () => {
    expect(relativeTimeTickIntervalMs(3600000)).toBe(60000);
    expect(relativeTimeTickIntervalMs(86400000 * 5)).toBe(60000);
  });
});
