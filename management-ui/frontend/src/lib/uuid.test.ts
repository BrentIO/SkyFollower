import { describe, expect, it } from "vitest";
import { uuidv4 } from "./uuid";

const UUID_V4 =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

describe("uuidv4", () => {
  it("produces a well-formed UUID v4 string", () => {
    for (let i = 0; i < 1000; i++) {
      expect(uuidv4()).toMatch(UUID_V4);
    }
  });

  it("sets the version nibble to 4 and the variant nibble to 8/9/a/b", () => {
    for (let i = 0; i < 1000; i++) {
      const id = uuidv4();
      expect(id[14]).toBe("4");
      expect(["8", "9", "a", "b"]).toContain(id[19]);
    }
  });

  it("does not collide across many calls", () => {
    const seen = new Set<string>();
    for (let i = 0; i < 10000; i++) {
      seen.add(uuidv4());
    }
    expect(seen.size).toBe(10000);
  });

  it("does not depend on crypto.randomUUID", () => {
    const original = crypto.randomUUID;
    try {
      // Simulate an insecure context, where randomUUID is unavailable.
      (crypto as { randomUUID?: unknown }).randomUUID = undefined;
      expect(uuidv4()).toMatch(UUID_V4);
    } finally {
      crypto.randomUUID = original;
    }
  });
});
