// Generates a RFC 4122 version 4 UUID string.
//
// Deliberately built on `crypto.getRandomValues()` rather than
// `crypto.randomUUID()`: the latter is only exposed in a secure context
// (HTTPS or `http://localhost`), so it is `undefined` on a plain-HTTP LAN
// deployment served from an IP address and every call throws. The former
// has no secure-context requirement and is available on every origin.
//
// These IDs are purely ephemeral client-side bookkeeping (e.g. Terra Draw's
// per-feature `id`); they are never persisted or sent to the backend as an
// area's real `identifier`.

const HEX: string[] = Array.from({ length: 256 }, (_, i) =>
  i.toString(16).padStart(2, "0"),
);

export function uuidv4(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);

  // Force the version (4) and variant (10xx) bits.
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;

  return (
    HEX[bytes[0]] +
    HEX[bytes[1]] +
    HEX[bytes[2]] +
    HEX[bytes[3]] +
    "-" +
    HEX[bytes[4]] +
    HEX[bytes[5]] +
    "-" +
    HEX[bytes[6]] +
    HEX[bytes[7]] +
    "-" +
    HEX[bytes[8]] +
    HEX[bytes[9]] +
    "-" +
    HEX[bytes[10]] +
    HEX[bytes[11]] +
    HEX[bytes[12]] +
    HEX[bytes[13]] +
    HEX[bytes[14]] +
    HEX[bytes[15]]
  );
}
