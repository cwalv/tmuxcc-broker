/**
 * wedge-regression.test.ts — tc-7xv.24 regression: real-socket backpressure.
 *
 * # What this verifies
 *
 * The wedge bug (tc-7xv.24): under sustained high-throughput pane output
 * (e.g. `find /` in a tmuxcc terminal) the pty wedged because the server-proxy's
 * `SocketTransport.sendData` ignored Node's backpressure return value.  When
 * the consumer was slow:
 *
 *   1. tmux fired %output bytes into the session-proxy pipeline.
 *   2. demux.store.append called transport.sendData.
 *   3. socket.write returned false (kernel send buffer full).
 *   4. SocketTransport ignored the return value (fire-and-forget).
 *   5. sessionProxy.addClient wrapper called fc.noteDrained immediately, crediting
 *      the bytes as drained the instant they entered the kernel send buffer.
 *   6. fc.bufferedBytes never grew → high-water never crossed → tmux never
 *      paused → session-proxy's outbound buffer grew without bound until V8 stalled.
 *
 * The fix (tc-7xv.6):
 *
 *   - SocketTransport.sendData returns Promise<void> when socket.write returns
 *     false.  The promise resolves on the socket's 'drain' event.
 *   - sessionProxy.addClient's wrapper chains fc.noteDrained off that promise so the
 *     drain credit fires only after actual consumer consumption.
 *   - Now fc.bufferedBytes accurately reflects in-flight bytes; under a slow
 *     consumer it crosses high-water and tmux is correctly told to pause.
 *
 * # How this test works (no real tmux required)
 *
 * We build a fake TmuxHost that records writes (so we can observe pause /
 * continue commands) plus a real SocketTransport pair (Unix domain sockets).
 * The consumer side pauses its socket (`sock.pause()`), simulating a stalled
 * VS Code extension.  We then call demux.store.append directly with byte
 * batches and assert:
 *
 *   1. fc.bufferedBytes grows past high-water (because drain is gated by the
 *      blocked socket).
 *   2. The fake host receives a pause command (refresh-client -A '%pane:pause').
 *   3. After we resume the consumer side, the socket drains; fc.noteDrained
 *      fires and tmux is told to continue.
 *
 * @module runtime/wedge-regression.test
 */

import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import * as fs from "node:fs";

// Pull the session-proxy-side flow-control and demux primitives from @tmuxcc/session-proxy
// (server-proxy depends on sessionProxy).  The SocketTransport lives in this same package,
// so we import it directly via the relative path.
import {
  createOutputDemux,
  createFlowController,
  DEFAULT_HIGH_WATER_BYTES,
  paneId as mintPaneId,
} from "@tmuxcc/session-proxy";
import type {
  PaneId,
  TmuxHost,
  Transport,
} from "@tmuxcc/session-proxy";

import { connectSocketTransport } from "./socket-transport.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function tmpSocketPath(label: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `tmuxcc-wedge-${label}-`));
  return path.join(dir, "wedge.sock");
}

/**
 * Build a fake TmuxHost that records every write() call.  The FlowController
 * only invokes host.write() (to send refresh-client -A '%pane:pause' /
 * 'continue' commands), so we cast a minimal shape to TmuxHost via unknown —
 * no need to implement the unused process-lifecycle methods.
 */
function makeFakeHost(): { write(data: string): void; writes: string[] } & TmuxHost {
  const writes: string[] = [];
  const stub = {
    writes,
    write(data: string | Uint8Array | Buffer): void {
      writes.push(typeof data === "string" ? data : Buffer.from(data).toString("utf8"));
    },
    onError() { /* unused */ },
    onExit() { /* unused */ },
    onData(): () => void { return () => { /* unused */ }; },
    onStderr(): () => void { return () => { /* unused */ }; },
    pid: undefined,
    exited: false,
    async start(): Promise<void> { /* unused */ },
    async stop(): Promise<void> { /* unused */ },
    kill(): void { /* unused */ },
  };
  return stub as unknown as { write(data: string): void; writes: string[] } & TmuxHost;
}

/**
 * Build a "draining transport" identical in shape to the one session-proxy.ts wraps
 * around every attached transport — calls fc.noteDrained after sendData (sync
 * or after promise resolution per the tc-7xv.6 fix).
 */
function buildDrainingTransport(
  inner: Transport,
  fc: ReturnType<typeof createFlowController>,
): Transport {
  return {
    ...inner,
    sendData(pid: PaneId, bytes: Uint8Array): void | Promise<void> {
      const result = inner.sendData(pid, bytes);
      if (bytes.length === 0) return result;
      if (result !== undefined && typeof (result as Promise<void>).then === "function") {
        return (result as Promise<void>).then(() => {
          fc.noteDrained(pid, bytes.length);
        });
      }
      fc.noteDrained(pid, bytes.length);
      return undefined;
    },
  };
}

// ---------------------------------------------------------------------------
// Suite: tc-7xv.24 regression
// ---------------------------------------------------------------------------

describe("tc-7xv.24 wedge regression — real-socket backpressure engages tmux pause", () => {
  it("slow consumer triggers fc pause within bounded time", async () => {
    const sockPath = tmpSocketPath("pause");
    const heldSockets: net.Socket[] = [];

    // Server side: pause reads on accept.  This stalls the client kernel send
    // buffer and forces SocketTransport.sendData to return a Promise.
    const server = net.createServer((sock) => {
      sock.pause();
      heldSockets.push(sock);
    });
    await new Promise<void>((resolve) => server.listen(sockPath, resolve));
    after(async () => {
      for (const s of heldSockets) { try { s.destroy(); } catch { /* ignore */ } }
      await new Promise<void>((resolve) => server.close(() => resolve()));
      try { fs.unlinkSync(sockPath); } catch { /* ignore */ }
    });

    const clientTransport = await connectSocketTransport(sockPath);
    after(() => clientTransport.close());

    // SessionProxy-side wiring (mirrors session-proxy.ts createSessionProxy).
    const host = makeFakeHost();
    const demux = createOutputDemux();
    const fc = createFlowController(host, demux);

    // Wrap demux.store with the same accounting tap that production uses.
    const baseStore = demux.store;
    const accountingStore = {
      append(pid: PaneId, bytes: Uint8Array): void {
        baseStore.append(pid, bytes);
        if (bytes.length > 0) fc.onPaneBytes(pid, bytes.length);
      },
      getContents: baseStore.getContents.bind(baseStore),
      size: baseStore.size.bind(baseStore),
      drop: baseStore.drop.bind(baseStore),
      clear: baseStore.clear.bind(baseStore),
    };

    // Attach the draining transport (same wrapper session-proxy.ts addClient builds).
    const drainingTransport = buildDrainingTransport(clientTransport, fc);
    demux.attachTransport(drainingTransport);

    // Pump bytes through accountingStore for a single pane.  Each append
    // simulates one %output notification from tmux.  Chunks are 64 KiB.
    const paneId = mintPaneId("p1");
    const chunk = new Uint8Array(64 * 1024); // 64 KiB of zeros

    // Production high-water is 256 KiB; we should observe pause within
    // a small number of chunks (typically 4–8 chunks, depending on the OS
    // send-buffer size).  Bound the loop generously.
    let pauseSeen = false;
    for (let i = 0; i < 200; i++) {
      accountingStore.append(paneId, chunk);
      // Yield to the event loop so socket writes propagate and 'drain' has a
      // chance to fire if the consumer is alive (it isn't — paused).
      await new Promise<void>((resolve) => setImmediate(resolve));
      if (fc.isPanePaused(paneId)) {
        pauseSeen = true;
        break;
      }
    }

    assert.equal(
      pauseSeen,
      true,
      `flow controller must pause within 200 chunks of slow consumer; ` +
      `bufferedBytes=${fc.bufferedBytes(paneId)} highWater=${DEFAULT_HIGH_WATER_BYTES}`,
    );

    // The host must have received a refresh-client -A '%1:pause' command.
    const pauseCmd = host.writes.find((w) => w.includes(":pause"));
    assert.ok(
      pauseCmd !== undefined,
      `fake host must have received a pause command (got ${host.writes.length} writes)`,
    );
  });

  it("resuming consumer drains buffered bytes and unpauses tmux", async () => {
    const sockPath = tmpSocketPath("resume");
    const heldSockets: net.Socket[] = [];

    const server = net.createServer((sock) => {
      sock.pause();
      heldSockets.push(sock);
    });
    await new Promise<void>((resolve) => server.listen(sockPath, resolve));
    after(async () => {
      for (const s of heldSockets) { try { s.destroy(); } catch { /* ignore */ } }
      await new Promise<void>((resolve) => server.close(() => resolve()));
      try { fs.unlinkSync(sockPath); } catch { /* ignore */ }
    });

    const clientTransport = await connectSocketTransport(sockPath);
    after(() => clientTransport.close());

    const host = makeFakeHost();
    const demux = createOutputDemux();
    const fc = createFlowController(host, demux);

    const baseStore = demux.store;
    const accountingStore = {
      append(pid: PaneId, bytes: Uint8Array): void {
        baseStore.append(pid, bytes);
        if (bytes.length > 0) fc.onPaneBytes(pid, bytes.length);
      },
      getContents: baseStore.getContents.bind(baseStore),
      size: baseStore.size.bind(baseStore),
      drop: baseStore.drop.bind(baseStore),
      clear: baseStore.clear.bind(baseStore),
    };

    const drainingTransport = buildDrainingTransport(clientTransport, fc);
    demux.attachTransport(drainingTransport);

    const paneId = mintPaneId("p1");
    const chunk = new Uint8Array(64 * 1024);

    // 1. Fill until paused.
    for (let i = 0; i < 200 && !fc.isPanePaused(paneId); i++) {
      accountingStore.append(paneId, chunk);
      await new Promise<void>((resolve) => setImmediate(resolve));
    }
    assert.equal(fc.isPanePaused(paneId), true, "pre-condition: pane must be paused");

    const pausedBytes = fc.bufferedBytes(paneId);

    // 2. Resume the consumer side — bytes will drain, 'drain' will fire on
    //    the client socket, the noteDrained promise will resolve, fc counter
    //    falls below low-water, tmux is told to continue.
    for (const s of heldSockets) {
      s.on("data", () => { /* discard */ });
      s.resume();
    }

    // Wait for resume — generous timeout to allow the kernel to ack.
    const start = Date.now();
    while (fc.isPanePaused(paneId) && (Date.now() - start) < 5_000) {
      await new Promise<void>((resolve) => setTimeout(resolve, 20));
    }
    assert.equal(
      fc.isPanePaused(paneId),
      false,
      `pane must resume within 5s of consumer recovery; ` +
      `bufferedBytes=${fc.bufferedBytes(paneId)} pausedAt=${pausedBytes}`,
    );

    // The host must have received a continue command.
    const continueCmd = host.writes.find((w) => w.includes(":continue"));
    assert.ok(
      continueCmd !== undefined,
      `fake host must have received a continue command after drain (got ${host.writes.length} writes)`,
    );
  });
});
