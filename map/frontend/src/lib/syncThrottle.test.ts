import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createTrailingThrottle } from "./syncThrottle";

describe("createTrailingThrottle", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("runs the first request immediately", () => {
    const throttle = createTrailingThrottle(200);
    const fn = vi.fn();
    throttle.request(fn);
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it("coalesces requests arriving within the window into one trailing run", () => {
    const throttle = createTrailingThrottle(200);
    const first = vi.fn();
    const second = vi.fn();
    const third = vi.fn();

    throttle.request(first); // Leading edge -- runs now.
    vi.advanceTimersByTime(50);
    throttle.request(second); // Within window -- deferred.
    vi.advanceTimersByTime(50);
    throttle.request(third); // Still within window -- replaces `second`.

    expect(first).toHaveBeenCalledTimes(1);
    expect(second).not.toHaveBeenCalled();
    expect(third).not.toHaveBeenCalled();

    // Trailing edge fires 200ms after the leading run (at t=200; 100ms
    // already elapsed above).
    vi.advanceTimersByTime(100);
    expect(second).not.toHaveBeenCalled(); // Superseded -- never runs.
    expect(third).toHaveBeenCalledTimes(1);
  });

  it("runs immediately again once the window has fully elapsed", () => {
    const throttle = createTrailingThrottle(200);
    const first = vi.fn();
    const second = vi.fn();

    throttle.request(first);
    vi.advanceTimersByTime(200);
    throttle.request(second);

    expect(first).toHaveBeenCalledTimes(1);
    expect(second).toHaveBeenCalledTimes(1);
  });

  it("only schedules one trailing timer no matter how many requests land in the window", () => {
    const throttle = createTrailingThrottle(200);
    throttle.request(vi.fn());
    for (let i = 0; i < 10; i++) {
      throttle.request(vi.fn());
    }
    expect(vi.getTimerCount()).toBe(1);
  });

  it("cancel() drops a pending trailing run", () => {
    const throttle = createTrailingThrottle(200);
    const first = vi.fn();
    const second = vi.fn();

    throttle.request(first);
    throttle.request(second);
    throttle.cancel();
    vi.advanceTimersByTime(500);

    expect(first).toHaveBeenCalledTimes(1);
    expect(second).not.toHaveBeenCalled();
  });

  it("cancel() is a no-op when nothing is pending", () => {
    const throttle = createTrailingThrottle(200);
    expect(() => throttle.cancel()).not.toThrow();
  });

  it("supports an injected clock", () => {
    let now = 1_000;
    const throttle = createTrailingThrottle(200, () => now);
    const first = vi.fn();
    const second = vi.fn();

    throttle.request(first);
    now += 200;
    throttle.request(second);

    expect(first).toHaveBeenCalledTimes(1);
    expect(second).toHaveBeenCalledTimes(1);
  });
});
