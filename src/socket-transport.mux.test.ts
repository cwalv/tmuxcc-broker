/**
 * socket-transport.mux.test.ts — tc-3y8.9 regression: exact-boundary demux of
 * the multiplexed control/data stream.
 *
 * # The bug (tc-3y8.9, root cause of the dead live-delta path)
 *
 * A single unix socket carries both planes:
 *   - data frames:    [0xCC][u32be SEQ][u32be PAYLEN][u16be IDLEN][PANEID][PAYLOAD]
 *   - control plane:  [u32be LEN][JSON + "\n"]   (LEN first byte is 0x00, never 0xCC)
 *
 * TCP/unix-socket reads do not respect write boundaries: one 'data' event can
 * contain [data frame][control message] (coalescing under load), and a single
 * data frame can SPAN two 'data' events (large frame or kernel chunking).
 *
 * The old `_processBuffer` had no exact frame-boundary tracking on the data
 * plane: on seeing a leading 0xCC it fed the ENTIRE buffer to FrameDecoder and
 * cleared `_buf`.  Two failure modes followed:
 *
 *   (A) [frame][control] coalesced in one chunk → FrameDecoder consumed the
 *       frames, then saw the control LEN prefix (0x00 ≠ 0xCC magic) and threw
 *       → transport closed with "data-plane framing error".  The control
 *       message (e.g. a pane.opened delta) was lost with the connection.
 *
 *   (B) A data frame split across two 'data' events → the continuation chunk's
 *       first byte is arbitrary terminal-output payload, not 0xCC → routed to
 *       the CONTROL branch → its first 4 bytes misread as a u32be JSON length
 *       (printable ASCII ⇒ ~540 MB) → `_buf.length < totalLen` forever →
 *       inbound processing SILENTLY stalled while the socket stayed open.
 *       Outbound sends kept working, so commands still reached tmux while
 *       every session-proxy→client delta vanished: exactly the tc-3y8.9 symptom
 *       (bootstrap snapshot delivered — it precedes data-plane traffic — but
 *       live pane.opened/closed deltas never arrive).
 *
 * These tests pin the fixed contract: the demux must track exact byte
 * boundaries for BOTH planes, deliver every message regardless of how the
 * stream is chunked or interleaved, and keep the transport open.
 *
 * @module socket-transport.mux.test
 */

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import * as fs from "node:fs";

import { encodeFrame, paneId } from "@tmuxcc/session-proxy";
import type { Transport, PaneId } from "@tmuxcc/session-proxy";
import { createSocketServer } from "./socket-transport.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function tmpSocketPath(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "tmuxcc-test-mux-"));
  return path.join(dir, "test.sock");
}

/** Frame a control message exactly as SocketTransport.sendControl does. */
function encodeControl(msg: object): Buffer {
  const payload = Buffer.from(JSON.stringify(msg) + "\n", "utf8");
  const lenBuf = Buffer.allocUnsafe(4);
  lenBuf.writeUInt32BE(payload.length, 0);
  return Buffer.concat([lenBuf, payload]);
}

interface Received {
  controls: Array<Record<string, unknown>>;
  data: Array<{ paneId: PaneId; payload: Uint8Array }>;
  closed: boolean;
  closeErr: Error | undefined;
}

/**
 * Start a server whose accepted connections are wrapped in SocketTransport
 * (the receiver under test), plus a RAW client socket we write crafted byte
 * sequences to.  Returns the recording sinks and a cleanup function.
 */
async function muxFixture(): Promise<{
  raw: net.Socket;
  received: Received;
  cleanup: () => Promise<void>;
}> {
  const sockPath = tmpSocketPath();
  const received: Received = { controls: [], data: [], closed: false, closeErr: undefined };

  const transports: Transport[] = [];
  const server = await createSocketServer(sockPath, (transport) => {
    transports.push(transport);
    transport.onControl((msg) => {
      received.controls.push(msg as unknown as Record<string, unknown>);
    });
    transport.onData((pid, bytes) => {
      received.data.push({ paneId: pid, payload: bytes });
    });
    transport.onClose((err) => {
      received.closed = true;
      received.closeErr = err;
    });
  });

  const raw = await new Promise<net.Socket>((resolve, reject) => {
    const s = net.createConnection(sockPath);
    s.once("connect", () => resolve(s));
    s.once("error", reject);
  });

  return {
    raw,
    received,
    cleanup: async () => {
      raw.destroy();
      for (const t of transports) {
        try { t.close(); } catch { /* already closed */ }
      }
      await server.close();
      try { fs.unlinkSync(sockPath); } catch { /* ignore */ }
    },
  };
}

/** Wait until cond() or timeout; resolves true if cond met. */
async function waitFor(cond: () => boolean, timeoutMs = 3_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) return false;
    await new Promise((r) => setTimeout(r, 20));
  }
  return true;
}

const PANE = paneId("p1");

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("tc-3y8.9 · multiplexed-stream exact-boundary demux", () => {
  it("(A) delivers a control message coalesced AFTER a data frame in one chunk, transport stays open", async () => {
    const { raw, received, cleanup } = await muxFixture();
    try {
      const frame = encodeFrame(PANE, 0, Buffer.from("hello from tmux\r\n"));
      const delta = encodeControl({ type: "pane.opened", seq: 2, paneId: "p9" });

      // ONE write → one coalesced chunk at the receiver.
      raw.write(Buffer.concat([Buffer.from(frame), delta]));

      assert.ok(
        await waitFor(() => received.controls.length >= 1),
        `control message after a data frame must be delivered (got controls=${received.controls.length}, closed=${received.closed}, closeErr=${received.closeErr?.message ?? "none"})`,
      );
      assert.equal(received.controls[0]!.type, "pane.opened");
      assert.equal(received.data.length, 1);
      assert.equal(received.closed, false, "transport must NOT close on a coalesced [frame][control] chunk");
    } finally {
      await cleanup();
    }
  });

  it("(B) survives a data frame split across two chunks; following control message is delivered", async () => {
    const { raw, received, cleanup } = await muxFixture();
    try {
      // Payload of printable ASCII — the continuation chunk's first bytes are
      // text, which the buggy demux misread as a ~540 MB control length.
      const payload = Buffer.from("ls -la\r\ntotal 48\r\ndrwxr-xr-x ...\r\n".repeat(8));
      const frame = Buffer.from(encodeFrame(PANE, 0, payload));

      // Split INSIDE the payload, well past the 11-byte header + paneId.
      const splitAt = 20;
      raw.write(frame.subarray(0, splitAt));
      // Let the first chunk be processed alone (separate 'data' events).
      await new Promise((r) => setTimeout(r, 50));
      const delta = encodeControl({ type: "pane.opened", seq: 3, paneId: "p9" });
      raw.write(Buffer.concat([frame.subarray(splitAt), delta]));

      assert.ok(
        await waitFor(() => received.data.length >= 1),
        "the split data frame must be reassembled and delivered",
      );
      assert.equal(Buffer.compare(Buffer.from(received.data[0]!.payload), payload), 0);
      assert.ok(
        await waitFor(() => received.controls.length >= 1),
        `the control message after the split frame must be delivered (closed=${received.closed})`,
      );
      assert.equal(received.controls[0]!.type, "pane.opened");
      assert.equal(received.closed, false);
    } finally {
      await cleanup();
    }
  });

  it("(C) handles a fully interleaved coalesced stream: control/data/control/data in one chunk", async () => {
    const { raw, received, cleanup } = await muxFixture();
    try {
      const c1 = encodeControl({ type: "window.opened", seq: 2, windowId: "w1" });
      const f1 = Buffer.from(encodeFrame(PANE, 0, Buffer.from("output-1")));
      const c2 = encodeControl({ type: "pane.opened", seq: 3, paneId: "p2" });
      const f2 = Buffer.from(encodeFrame(PANE, 1, Buffer.from("output-2")));

      raw.write(Buffer.concat([c1, f1, c2, f2]));

      assert.ok(
        await waitFor(() => received.controls.length >= 2 && received.data.length >= 2),
        `all four messages must arrive (controls=${received.controls.length}, data=${received.data.length}, closed=${received.closed})`,
      );
      assert.deepEqual(
        received.controls.map((m) => m.type),
        ["window.opened", "pane.opened"],
      );
      assert.deepEqual(
        received.data.map((d) => Buffer.from(d.payload).toString("utf8")),
        ["output-1", "output-2"],
      );
      assert.equal(received.closed, false);
    } finally {
      await cleanup();
    }
  });

  it("(D) control message itself split across chunks (length prefix split) still delivered", async () => {
    const { raw, received, cleanup } = await muxFixture();
    try {
      const ctrl = encodeControl({ type: "pane.closed", seq: 4, paneId: "p3" });
      // Split inside the 4-byte length prefix, then inside the JSON body.
      raw.write(ctrl.subarray(0, 2));
      await new Promise((r) => setTimeout(r, 30));
      raw.write(ctrl.subarray(2, 10));
      await new Promise((r) => setTimeout(r, 30));
      raw.write(ctrl.subarray(10));

      assert.ok(await waitFor(() => received.controls.length >= 1));
      assert.equal(received.controls[0]!.type, "pane.closed");
      assert.equal(received.closed, false);
    } finally {
      await cleanup();
    }
  });
});
