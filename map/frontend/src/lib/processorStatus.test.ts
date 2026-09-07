import { describe, expect, it } from "vitest";
import { connectionTooltip, overallConnectionStatus } from "./processorStatus";
import type { ProcessorRoster } from "../api/types";

function roster(overall: ProcessorRoster["overall"], processors: ProcessorRoster["processors"] = []): ProcessorRoster {
  return { overall, processors };
}

describe("overallConnectionStatus", () => {
  it("reflects the roster's overall status when the WebSocket is connected", () => {
    expect(overallConnectionStatus(true, roster("green"))).toBe("green");
    expect(overallConnectionStatus(true, roster("amber"))).toBe("amber");
    expect(overallConnectionStatus(true, roster("red"))).toBe("red");
  });

  it("forces red when the WebSocket itself is disconnected, regardless of the roster", () => {
    // A stale "green" snapshot from before the WS dropped must not be
    // shown as live/current -- there's no live proof of anything once the
    // browser can't even reach the map backend.
    expect(overallConnectionStatus(false, roster("green"))).toBe("red");
  });
});

describe("connectionTooltip", () => {
  it("explains a disconnected WebSocket distinctly from an empty roster", () => {
    expect(connectionTooltip(false, roster("red"))).toBe("Disconnected from map service");
  });

  it("explains an empty roster distinctly from a disconnected WebSocket", () => {
    expect(connectionTooltip(true, roster("red", []))).toBe("No message processors seen yet");
  });

  it("lists every rostered processor by id with its status label", () => {
    const tooltip = connectionTooltip(
      true,
      roster("amber", [
        { processor_id: "mp-1", last_seen: 1000, status: "green" },
        { processor_id: "mp-2", last_seen: 900, status: "amber" },
        { processor_id: "mp-3", last_seen: 0, status: "red" },
      ]),
    );

    expect(tooltip).toBe("mp-1: Connected\nmp-2: Reconnecting\nmp-3: Disconnected");
  });
});
