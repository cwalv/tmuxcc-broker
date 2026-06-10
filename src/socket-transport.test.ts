/**
 * socket-transport.test.ts — unit tests for SocketTransport.
 *
 * Tests the control-plane framing (0xCB + u32be len + JSON) and verifies that
 * the transport correctly routes messages between two ends of an in-process
 * socket pair.
 *
 * @module socket-transport.test
 */

import { describe, it, after } from "node:test";
import assert from "node:assert/strict";
import * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import * as fs from "node:fs";

import { createSocketTransport, connectSocketTransport, createSocketServer } from "./socket-transport.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function tmpSocketPath(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "tmuxcc-test-transport-"));
  return path.join(dir, "test.sock");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("socket-transport", () => {
  it("round-trips a control-plane message between server and client", async () => {
    const sockPath = tmpSocketPath();
    const received: unknown[] = [];

    const server = await createSocketServer(sockPath, (transport) => {
      transport.onControl((msg) => {
        received.push(msg);
        // Echo back
        transport.sendControl({ ...msg, type: "echo.response" } as unknown as Parameters<typeof transport.sendControl>[0]);
      });
    });

    after(async () => {
      await server.close();
      try { fs.unlinkSync(sockPath); } catch { /* ignore */ }
    });

    const clientTransport = await connectSocketTransport(sockPath);

    const echoPromise = new Promise<unknown>((resolve) => {
      clientTransport.onControl((msg) => {
        if ((msg as unknown as { type: string }).type === "echo.response") resolve(msg);
      });
    });

    clientTransport.sendControl({ type: "test.ping", seq: 1 } as unknown as Parameters<typeof clientTransport.sendControl>[0]);

    const echoed = await echoPromise;
    assert.equal((echoed as unknown as { type: string }).type, "echo.response");
    assert.equal((echoed as unknown as { seq: number }).seq, 1);

    assert.equal(received.length, 1);
    assert.equal((received[0] as any).type, "test.ping");

    clientTransport.close();
  });

  it("handles close gracefully", async () => {
    const sockPath = tmpSocketPath();

    const server = await createSocketServer(sockPath, (transport) => {
      transport.onClose(() => {
        // server side sees close
      });
    });

    after(async () => {
      await server.close();
      try { fs.unlinkSync(sockPath); } catch { /* ignore */ }
    });

    const clientTransport = await connectSocketTransport(sockPath);
    const closePromise = new Promise<void>((resolve) => {
      clientTransport.onClose(() => resolve());
    });

    clientTransport.close();
    await closePromise;
  });

  it("handles large messages correctly", async () => {
    const sockPath = tmpSocketPath();
    const received: unknown[] = [];

    const server = await createSocketServer(sockPath, (transport) => {
      transport.onControl((msg) => received.push(msg));
    });

    after(async () => {
      await server.close();
      try { fs.unlinkSync(sockPath); } catch { /* ignore */ }
    });

    const clientTransport = await connectSocketTransport(sockPath);

    // Send a message with a large payload
    const bigPayload = "x".repeat(64_000);
    const donePromise = new Promise<void>((resolve) => {
      const interval = setInterval(() => {
        if (received.length >= 1) {
          clearInterval(interval);
          resolve();
        }
      }, 10);
    });

    clientTransport.sendControl({ type: "big.msg", seq: 1, data: bigPayload } as unknown as Parameters<typeof clientTransport.sendControl>[0]);

    await donePromise;
    assert.equal(received.length, 1);
    assert.equal((received[0] as any).data, bigPayload);

    clientTransport.close();
  });

  // ---------------------------------------------------------------------------
  // tc-7xv.6 / tc-7xv.24: backpressure regression
  // ---------------------------------------------------------------------------
  //
  // The wedge bug: SocketTransport.sendData and sendControl ignored the return
  // value from socket.write().  When the kernel send buffer filled (slow
  // consumer), Node.js queues bytes in user-space without bound and the session-proxy
  // never saw backpressure — so its flow controller credited bytes as drained
  // the instant they entered the send queue and never triggered tmux pause.
  //
  // The fix: when socket.write returns false, sendData/sendControl return a
  // Promise<void> that resolves on the next 'drain' event.  The session-proxy's
  // addClient wrapper now chains noteDrained off that Promise so the flow
  // controller's bufferedBytes counter reflects actual consumer consumption.
  //
  // Regression tests below assert the new contract:
  //   1. A fast producer with a slow / paused reader gets back a Promise from
  //      sendData once the socket buffer fills.
  //   2. Resuming the reader resolves the Promise.
  //   3. After backpressure has been released, subsequent sendData returns
  //      void again (until the next backpressure window).
  // ---------------------------------------------------------------------------
  describe("tc-7xv.6 backpressure", () => {
    it("sendData returns a Promise when the kernel send buffer fills", async () => {
      const sockPath = tmpSocketPath();
      const heldSockets: net.Socket[] = [];
      const server = net.createServer((sock) => {
        // Pause reads on the server side so the kernel stops ACKing.  Eventually
        // the client's send buffer fills and socket.write returns false.
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

      const paneIdStr = "p1" as unknown as Parameters<typeof clientTransport.sendData>[0];
      // Pump data until we observe a Promise return.  Bound the loop to avoid
      // infinite spin: 256 chunks × 64 KiB = 16 MiB is well past any sane
      // kernel send buffer.
      let backpressured = false;
      const chunk = new Uint8Array(64 * 1024);
      for (let i = 0; i < 256; i++) {
        const result = clientTransport.sendData(paneIdStr, chunk);
        if (result !== undefined && typeof (result as Promise<void>).then === "function") {
          backpressured = true;
          break;
        }
      }
      assert.equal(
        backpressured,
        true,
        "sendData must return a Promise once the kernel send buffer fills",
      );
    });

    it("the backpressure Promise resolves when the reader resumes", async () => {
      const sockPath = tmpSocketPath();
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

      // Fill the send buffer until we get a Promise.
      const paneIdStr = "p1" as unknown as Parameters<typeof clientTransport.sendData>[0];
      const chunk = new Uint8Array(64 * 1024);
      let pending: Promise<void> | null = null;
      for (let i = 0; i < 256 && pending === null; i++) {
        const result = clientTransport.sendData(paneIdStr, chunk);
        if (result !== undefined && typeof (result as Promise<void>).then === "function") {
          pending = result as Promise<void>;
        }
      }
      assert.ok(pending !== null, "backpressure must engage");

      // Resume the held server-side socket so data drains.  After enough data
      // moves, the client's 'drain' event fires and resolves the Promise.
      for (const s of heldSockets) {
        s.on("data", () => { /* discard */ });
        s.resume();
      }

      // Promise must resolve within a reasonable time.
      let resolved = false;
      await Promise.race([
        (pending as Promise<void>).then(() => { resolved = true; }),
        new Promise<void>((_, reject) => setTimeout(() => reject(new Error("drain timeout")), 5_000)),
      ]);
      assert.equal(resolved, true, "drain Promise must resolve after reader resumes");
    });

    it("close() releases pending backpressure awaiters", async () => {
      const sockPath = tmpSocketPath();
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

      // Fill the buffer until backpressured.
      const paneIdStr = "p1" as unknown as Parameters<typeof clientTransport.sendData>[0];
      const chunk = new Uint8Array(64 * 1024);
      let pending: Promise<void> | null = null;
      for (let i = 0; i < 256 && pending === null; i++) {
        const result = clientTransport.sendData(paneIdStr, chunk);
        if (result !== undefined && typeof (result as Promise<void>).then === "function") {
          pending = result as Promise<void>;
        }
      }
      assert.ok(pending !== null, "backpressure must engage");

      // Close the transport before the reader drains.  The pending Promise
      // must still resolve so callers don't deadlock on shutdown.
      clientTransport.close();

      let resolved = false;
      await Promise.race([
        (pending as Promise<void>).then(() => { resolved = true; }),
        new Promise<void>((_, reject) => setTimeout(() => reject(new Error("close-resolve timeout")), 2_000)),
      ]);
      assert.equal(resolved, true, "close() must resolve pending drain Promises");
    });
  });
});
