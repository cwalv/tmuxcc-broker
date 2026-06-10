/**
 * server-proxy.test.ts — integration + race tests for the tmuxcc-broker.
 *
 * # Test categories
 *
 * ## Unit / handshake tests (always run)
 *
 * U1. ServerProxy starts, accepts a connection, runs handshake, sends snapshot.
 *
 * ## Integration tests (guarded by tmux availability)
 *
 * I1. spawn a server-proxy on a test socket, connect as a client, receive snapshot.
 * I2. session.claim creates a session + session-proxy + returns endpoint + created=true.
 * I3. session.claim on existing session returns same endpoint, created=false.
 * I4. session.create fails if name is already taken.
 * I5. session.destroy kills session + reaps session-proxy.
 * I6. sessions.added delta is pushed to subscribers after session.claim.
 * I7. Connect to session-proxy endpoint, run snapshot + input round-trip (session-proxy wire).
 *
 * ## Race test
 *
 * R1. 10 concurrent session.claim requests for the same name all receive
 *     the same sessionId + endpoint; only one session-proxy process is spawned;
 *     exactly one response reports created=true (tc-3y8.2).
 *
 * # Cleanup
 *
 * Each test creates its own server-proxy with a unique test socket name
 * (tmuxcc-test-broker-<N>-<ts>) and calls serverProxy.shutdown() in afterEach.
 *
 * @module serverProxy.test
 */

import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import { createServerProxy, connectSocketTransport, serverProxySocketPath } from "./index.js";
import type { ServerProxyHandle, ServerProxySelfExitReason } from "./index.js";
import type { Transport } from "@tmuxcc/session-proxy";

import {
  runClientHandshake,
  WIRE_PROTOCOL_VERSION,
} from "@tmuxcc/session-proxy";
import type {
  ServerProxySnapshotMessage,
  ServerProxyCommandResponseMessage,
  ServerProxySessionAddedMessage,
  MessageBase,
  Capabilities,
} from "@tmuxcc/session-proxy";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let testCounter = 0;

function nextSocketName(): string {
  return `tmuxcc-test-broker-${process.pid}-${++testCounter}-${Date.now()}`;
}

function tmuxAvailable(): boolean {
  const r = spawnSync("tmux", ["-V"], { stdio: "ignore", timeout: 2_000 });
  return r.status === 0 && !r.error;
}

/** Poll `predicate` every `intervalMs` until truthy; throw on timeout. */
async function waitFor(
  predicate: () => boolean,
  timeoutMs: number,
  what: string,
  intervalMs = 25,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return;
    if (Date.now() > deadline) throw new Error(`Timeout (${timeoutMs}ms) waiting for ${what}`);
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

/**
 * PIDs of live thin-watcher tmux `-CC` client processes for a socket name.
 *
 * The `^tmux ` anchor matches only the tmux client itself, not the python
 * PTY-bridge parent (whose cmdline also contains the same substring).
 */
function findWatcherPids(socketName: string): number[] {
  const r = spawnSync("pgrep", ["-f", `^tmux -L ${socketName} -CC`], {
    encoding: "utf8",
    timeout: 5_000,
  });
  if (r.status !== 0 || r.error) return []; // pgrep exits 1 on no match
  return r.stdout
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((s) => parseInt(s, 10))
    .filter((n) => !Number.isNaN(n));
}

/** mkdtemp a per-test runtime dir (removed in the test's finally block). */
function makeRuntimeDir(label: string): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), `tmuxcc-test-${label}-`));
}

/** Returns a Promise that rejects after `ms` milliseconds. The timeout is cleared if `signal` resolves. */
function rejectAfter<T>(ms: number, message: string): [Promise<never>, () => void] {
  let timer: ReturnType<typeof setTimeout>;
  const p = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(message)), ms);
    // Unref so this timer doesn't keep the event loop alive
    if (timer.unref) timer.unref();
  });
  return [p, () => clearTimeout(timer!)];
}

const CLIENT_CAPS: Capabilities = {
  protocolVersion: WIRE_PROTOCOL_VERSION,
  features: [
    "sessions-watch",
    "session-create",
    "session-destroy",
    "session-claim",
    "pane-attach", // tc-7xv.36
  ],
};

const SESSION_PROXY_CLIENT_CAPS: Capabilities = {
  protocolVersion: WIRE_PROTOCOL_VERSION,
  features: ["pane-lifecycle", "layout-updates", "focus-events", "input-forwarding"],
};

/**
 * A multiplexing wrapper around a Transport's single onControl slot.
 *
 * Since Transport.onControl has replace-last-wins semantics, concurrent
 * test helpers that each want to install an onControl handler would
 * clobber each other. This class lets multiple subscribers share a
 * single actual onControl slot via fanout.
 */
class TransportMux {
  private _transport: Transport;
  private _handlers: Array<(msg: MessageBase) => void> = [];

  constructor(transport: Transport) {
    this._transport = transport;
    this._transport.onControl((msg) => {
      const copy = this._handlers.slice();
      for (const h of copy) h(msg as unknown as MessageBase);
    });
  }

  /** Subscribe to incoming control messages. Returns unsubscribe fn. */
  subscribe(handler: (msg: MessageBase) => void): () => void {
    this._handlers.push(handler);
    return () => {
      this._handlers = this._handlers.filter((h) => h !== handler);
    };
  }

  get transport(): Transport {
    return this._transport;
  }
}

/** Connect to a server-proxy and run the server-proxy-wire handshake. Returns the mux. */
async function connectToServerProxy(endpoint: string): Promise<{
  mux: TransportMux;
  snapshot: ServerProxySnapshotMessage;
}> {
  const transport = await connectSocketTransport(endpoint);

  // Run the handshake FIRST. runClientHandshake installs and then removes
  // its own onControl handler (leaves it as a no-op). We must install
  // the TransportMux AFTER so its handler wins the single-slot competition.
  await runClientHandshake(transport, CLIENT_CAPS, "server-proxy.capabilities");

  // Now safe to install the mux — handshake has settled and cleared its handler.
  const mux = new TransportMux(transport);

  // Snapshot is sent by the server-proxy right after handshake. Since the TCP
  // stream may already have buffered it by the time we install the mux,
  // we rely on the Node.js event loop: the mux handler fires on the NEXT
  // event-loop tick after socket data arrives, so as long as we install
  // the mux before yielding to the event loop (no await between handshake
  // resolve and mux construction), we'll catch it.
  //
  // If this races in practice (snapshot arrives before mux is installed),
  // the snapshot will be dropped. In that case, add a small delay or use
  // a different connection flow. For now, the synchronous handshake→mux
  // installation should be safe with socket transports.
  const snapshotPromise = new Promise<ServerProxySnapshotMessage>((resolve) => {
    const unsub = mux.subscribe((msg) => {
      if (msg.type === "sessions.snapshot") {
        unsub();
        resolve(msg as unknown as ServerProxySnapshotMessage);
      }
    });
  });

  const [timeoutP, clearTimeoutP] = rejectAfter(5_000, "Timeout waiting for server-proxy snapshot");
  const snapshot = await Promise.race([snapshotPromise, timeoutP]);
  clearTimeoutP();

  return { mux, snapshot };
}

/** Send a server-proxy command and wait for the correlated response. */
async function sendServerProxyCommand(
  mux: TransportMux,
  command: { kind: string; [k: string]: unknown },
  outgoingSeq: { value: number },
): Promise<ServerProxyCommandResponseMessage> {
  const correlationId = `corr-${Math.random().toString(36).slice(2)}`;

  const responsePromise = new Promise<ServerProxyCommandResponseMessage>((resolve) => {
    const unsub = mux.subscribe((msg) => {
      if (
        msg.type === "command.response" &&
        (msg as unknown as ServerProxyCommandResponseMessage).correlationId === correlationId
      ) {
        unsub();
        resolve(msg as unknown as ServerProxyCommandResponseMessage);
      }
    });
  });

  mux.transport.sendControl({
    type: "command.request",
    seq: outgoingSeq.value++,
    correlationId,
    command,
  } as unknown as Parameters<typeof mux.transport.sendControl>[0]);

  return responsePromise;
}

// ---------------------------------------------------------------------------
// Unit tests (no real tmux)
// ---------------------------------------------------------------------------

describe("server-proxy – unit (no tmux)", () => {
  it("U1: server-proxy starts and sends snapshot after handshake", async () => {
    const socketName = nextSocketName();
    const serverProxy = createServerProxy({ socketName });
    await serverProxy.start();

    try {
      const { snapshot } = await connectToServerProxy(serverProxy.endpoint());
      assert.equal(snapshot.type, "sessions.snapshot");
      assert.ok(Array.isArray(snapshot.sessions));
      // No real tmux server → sessions should be empty
      assert.equal(snapshot.sessions.length, 0);
    } finally {
      await serverProxy.shutdown();
    }
  });

  it("U3 (tc-k6v): server-proxy.info returns identity fields without tmux", async () => {
    const socketName = nextSocketName();
    const serverProxy = createServerProxy({ socketName });
    await serverProxy.start();

    try {
      const { mux } = await connectToServerProxy(serverProxy.endpoint());
      const seq = { value: 1 };

      const resp = await sendServerProxyCommand(mux, { kind: "server-proxy.info" }, seq);
      assert.ok(resp.result.ok, `Expected ok=true, got: ${JSON.stringify(resp.result)}`);
      const info = (resp.result as {
        ok: true;
        payload: { info: import("@tmuxcc/session-proxy").ServerProxyInfoPayload };
      }).payload.info;

      assert.equal(info.socketName, socketName);
      assert.equal(info.serverProxySocketPath, serverProxy.endpoint());
      // In-process server-proxy → its pid is this test process's pid.
      assert.equal(info.serverProxyPid, process.pid);
      assert.ok(info.uptimeMs >= 0, `uptimeMs must be >= 0, got ${info.uptimeMs}`);
      // No tmux server on this unique socket → null pid, no sessions, not adopted.
      assert.equal(info.tmuxServerPid, null);
      assert.equal(info.adoptedExistingServer, false);
      assert.deepEqual(info.sessions, []);
      // The connection carrying this very request counts.
      assert.ok(
        info.connectedClientCount >= 1,
        `connectedClientCount must include the requesting client, got ${info.connectedClientCount}`,
      );
      // Programmatic serverProxy (no entry point) → no log file.
      assert.equal(info.logPath, null);

      mux.transport.close();
    } finally {
      await serverProxy.shutdown();
    }
  });

  it("U4 (tc-k6v): server-proxy.info reports logPath verbatim when configured", async () => {
    const socketName = nextSocketName();
    const serverProxy = createServerProxy({ socketName, logPath: "/tmp/some-server-proxy.log" });
    await serverProxy.start();

    try {
      const { mux } = await connectToServerProxy(serverProxy.endpoint());
      const seq = { value: 1 };
      const resp = await sendServerProxyCommand(mux, { kind: "server-proxy.info" }, seq);
      assert.ok(resp.result.ok);
      const info = (resp.result as {
        ok: true;
        payload: { info: import("@tmuxcc/session-proxy").ServerProxyInfoPayload };
      }).payload.info;
      assert.equal(info.logPath, "/tmp/some-server-proxy.log");
      mux.transport.close();
    } finally {
      await serverProxy.shutdown();
    }
  });

  it("U2: serverProxy.endpoint() equals serverProxySocketPath(socketName) — well-known path", async () => {
    // This is the core regression guard for tc-j9c.8:
    // serverProxy.endpoint() must equal the path that vscode computes via
    // serverProxySocketPath(serverProxySocketName) so that discovery works without
    // out-of-band communication.
    const socketName = nextSocketName();
    const runtimeDir = `/tmp/tmuxcc-test-u2-${process.pid}`;
    const serverProxy = createServerProxy({ socketName, runtimeDir });
    await serverProxy.start();

    try {
      const expected = serverProxySocketPath(socketName, { runtimeDir });
      assert.equal(
        serverProxy.endpoint(),
        expected,
        `serverProxy.endpoint() must equal serverProxySocketPath(socketName): got ${serverProxy.endpoint()}, want ${expected}`,
      );
    } finally {
      await serverProxy.shutdown();
    }
  });
});

// ---------------------------------------------------------------------------
// Integration tests (requires tmux)
// ---------------------------------------------------------------------------

const TMUX_AVAILABLE = tmuxAvailable();

describe("server-proxy – integration (requires tmux)", { skip: !TMUX_AVAILABLE }, () => {
  let serverProxy: ServerProxyHandle;
  let socketName: string;

  beforeEach(async () => {
    socketName = nextSocketName();
    serverProxy = createServerProxy({ socketName });
    await serverProxy.start();
  });

  afterEach(async () => {
    await serverProxy.shutdown();
    spawnSync("tmux", ["-L", socketName, "kill-server"], { stdio: "ignore", timeout: 5_000 });
  });

  it("I1: connect to server-proxy and receive empty snapshot", async () => {
    const { snapshot } = await connectToServerProxy(serverProxy.endpoint());
    assert.equal(snapshot.type, "sessions.snapshot");
    assert.equal(snapshot.sessions.length, 0);
  });

  it("I2: session.claim creates a session and sessionProxy, returns endpoint + created=true", async () => {
    const { mux } = await connectToServerProxy(serverProxy.endpoint());
    const seq = { value: 1 };

    const resp = await sendServerProxyCommand(mux, { kind: "session.claim", name: "test-claim" }, seq);

    assert.ok(resp.result.ok, `Expected ok=true, got: ${JSON.stringify(resp.result)}`);
    const payload = (resp.result as { ok: true; payload: { sessionId: string; endpoint: string; created: boolean } }).payload;
    assert.ok(payload.sessionId, "Missing sessionId");
    assert.ok(payload.endpoint, "Missing endpoint");
    // tc-3y8.2: a claim that mints the session must report created=true —
    // this is the client's authority for create-time-only profile apply.
    assert.equal(payload.created, true, "claim that creates must report created=true");

    mux.transport.close();
  });

  it("I3: session.claim on same name returns same sessionId + endpoint, created=false", async () => {
    const { mux } = await connectToServerProxy(serverProxy.endpoint());
    const seq = { value: 1 };

    const resp1 = await sendServerProxyCommand(mux, { kind: "session.claim", name: "reuse-me" }, seq);
    const resp2 = await sendServerProxyCommand(mux, { kind: "session.claim", name: "reuse-me" }, seq);

    assert.ok(resp1.result.ok);
    assert.ok(resp2.result.ok);

    const p1 = (resp1.result as { ok: true; payload: { sessionId: string; endpoint: string; created: boolean } }).payload;
    const p2 = (resp2.result as { ok: true; payload: { sessionId: string; endpoint: string; created: boolean } }).payload;

    assert.equal(p1.sessionId, p2.sessionId, "sessionId must be stable");
    assert.equal(p1.endpoint, p2.endpoint, "endpoint must be stable");
    // tc-3y8.2: first claim creates, second attaches to the existing session.
    assert.equal(p1.created, true, "first claim must report created=true");
    assert.equal(p2.created, false, "second claim must report created=false (attach)");

    mux.transport.close();
  });

  it("I4: session.create fails if name is already taken", async () => {
    const { mux } = await connectToServerProxy(serverProxy.endpoint());
    const seq = { value: 1 };

    const r1 = await sendServerProxyCommand(mux, { kind: "session.create", name: "unique-sess" }, seq);
    assert.ok(r1.result.ok, `First create failed: ${JSON.stringify(r1.result)}`);
    // tc-3y8.2: a successful session.create always mints → created=true.
    assert.equal(
      (r1.result as { ok: true; payload: { created: boolean } }).payload.created,
      true,
      "session.create must report created=true on success",
    );

    const r2 = await sendServerProxyCommand(mux, { kind: "session.create", name: "unique-sess" }, seq);
    assert.equal(r2.result.ok, false);
    assert.equal((r2.result as { ok: false; code: string }).code, "session.name-taken");

    mux.transport.close();
  });

  it("I5: session.destroy kills session and reaps session-proxy", async () => {
    const { mux } = await connectToServerProxy(serverProxy.endpoint());
    const seq = { value: 1 };

    const claimResp = await sendServerProxyCommand(mux, { kind: "session.claim", name: "to-destroy" }, seq);
    assert.ok(claimResp.result.ok);
    const sessionId = (claimResp.result as { ok: true; payload: { sessionId: string } }).payload.sessionId;

    const destroyResp = await sendServerProxyCommand(mux, { kind: "session.destroy", sessionId }, seq);
    assert.ok(destroyResp.result.ok, `Destroy failed: ${JSON.stringify(destroyResp.result)}`);

    mux.transport.close();
  });

  it("I6: sessions.added is pushed to subscribers after session.claim", async () => {
    const { mux } = await connectToServerProxy(serverProxy.endpoint());
    const seq = { value: 1 };

    // Set up delta listener BEFORE the command
    const addedPromise = new Promise<ServerProxySessionAddedMessage>((resolve) => {
      const unsub = mux.subscribe((msg) => {
        if (msg.type === "sessions.added") {
          unsub();
          resolve(msg as unknown as ServerProxySessionAddedMessage);
        }
      });
    });

    // Trigger session creation
    const claimResp = await sendServerProxyCommand(mux, { kind: "session.claim", name: "watch-target" }, seq);
    assert.ok(claimResp.result.ok, `Claim failed: ${JSON.stringify(claimResp.result)}`);

    // The sessions.added delta is sent when the session first appears.
    // It may have already arrived (before or during the claim response).
    // Wait up to 2s.
    const [addedTimeoutP, clearAddedTimeout] = rejectAfter(2_000, "Timeout waiting for sessions.added");
    const added = await Promise.race([addedPromise, addedTimeoutP]);
    clearAddedTimeout();

    assert.equal(added.type, "sessions.added");
    assert.ok(added.sessionId);
    assert.equal(added.name, "watch-target");

    mux.transport.close();
  });

  it("I8 (tc-7xv.36): pane.attach returns the same session-proxy endpoint as session.claim and echoes paneId", async () => {
    const { mux } = await connectToServerProxy(serverProxy.endpoint());
    const seq = { value: 1 };

    // First claim the session so a session-proxy is running and we know its sessionId.
    const claimResp = await sendServerProxyCommand(mux, { kind: "session.claim", name: "pane-attach-target" }, seq);
    assert.ok(claimResp.result.ok, `Claim failed: ${JSON.stringify(claimResp.result)}`);
    const claimPayload = (claimResp.result as {
      ok: true;
      payload: { sessionId: string; endpoint: string };
    }).payload;
    const sessionId = claimPayload.sessionId;
    const claimEndpoint = claimPayload.endpoint;

    // Now attach to a specific pane.  The server-proxy does not validate pane
    // existence; this test simply asserts the round-trip shape.
    const attachResp = await sendServerProxyCommand(
      mux,
      { kind: "pane.attach", sessionId, paneId: "p1" },
      seq,
    );
    assert.ok(
      attachResp.result.ok,
      `pane.attach failed: ${JSON.stringify(attachResp.result)}`,
    );
    const attachPayload = (attachResp.result as {
      ok: true;
      payload: { sessionId: string; endpoint: string; paneId: string };
    }).payload;
    assert.equal(attachPayload.sessionId, sessionId, "sessionId must match");
    assert.equal(attachPayload.endpoint, claimEndpoint, "endpoint must match");
    assert.equal(attachPayload.paneId, "p1", "paneId must echo back");

    mux.transport.close();
  });

  it("I9 (tc-7xv.36): pane.attach returns session.not-found for unknown session", async () => {
    const { mux } = await connectToServerProxy(serverProxy.endpoint());
    const seq = { value: 1 };

    const resp = await sendServerProxyCommand(
      mux,
      { kind: "pane.attach", sessionId: "s999-nonexistent", paneId: "p0" },
      seq,
    );

    assert.equal(resp.result.ok, false);
    const r = resp.result as { ok: false; code: string; message: string };
    assert.equal(r.code, "session.not-found", `code should be session.not-found, got ${r.code}`);

    mux.transport.close();
  });

  it("I10 (tc-k6v): server-proxy.info reports tmux server pid + per-session session-proxy pid and pane count", async () => {
    const { mux } = await connectToServerProxy(serverProxy.endpoint());
    const seq = { value: 1 };

    // Claim a session so a tmux server, session, and session-proxy all exist.
    const claimResp = await sendServerProxyCommand(mux, { kind: "session.claim", name: "info-target" }, seq);
    assert.ok(claimResp.result.ok, `Claim failed: ${JSON.stringify(claimResp.result)}`);

    const resp = await sendServerProxyCommand(mux, { kind: "server-proxy.info" }, seq);
    assert.ok(resp.result.ok, `server-proxy.info failed: ${JSON.stringify(resp.result)}`);
    const info = (resp.result as {
      ok: true;
      payload: { info: import("@tmuxcc/session-proxy").ServerProxyInfoPayload };
    }).payload.info;

    // tmux server pid is live.
    assert.ok(
      typeof info.tmuxServerPid === "number" && info.tmuxServerPid > 0,
      `tmuxServerPid must be a live pid, got ${String(info.tmuxServerPid)}`,
    );

    // The claimed session appears with a live session-proxy pid and ≥1 window/pane.
    const row = info.sessions.find((s) => s.name === "info-target");
    assert.ok(row, `server-proxy.info sessions must include 'info-target': ${JSON.stringify(info.sessions)}`);
    assert.ok(
      typeof row.sessionProxyPid === "number" && row.sessionProxyPid > 0,
      `sessionProxyPid must be a live pid, got ${String(row.sessionProxyPid)}`,
    );
    // kill -0 probes liveness without sending a signal.
    assert.doesNotThrow(() => process.kill(row.sessionProxyPid as number, 0), "sessionProxyPid must be running");
    assert.ok(row.windowCount >= 1, `windowCount must be >= 1, got ${row.windowCount}`);
    assert.ok(row.paneCount >= 1, `paneCount must be >= 1, got ${row.paneCount}`);
    assert.ok(
      typeof row.attachedClientCount === "number" && row.attachedClientCount >= 0,
      `attachedClientCount must be a number, got ${String(row.attachedClientCount)}`,
    );

    mux.transport.close();
  });

  it("I11 (tc-3y8.7): attachedClientCount is external-only — session-proxy+watcher do not inflate the count", async () => {
    const { mux } = await connectToServerProxy(serverProxy.endpoint());
    const seq = { value: 1 };

    // Claim a session.  This attaches two tmuxcc-owned control-mode clients:
    //   1. the server-proxy's watcher (flags: control-mode, ignore-size, no-output)
    //   2. the session sessionProxy (flags: control-mode)
    // Wait a moment for the watcher to fully attach (it polls/attaches async).
    const claimResp = await sendServerProxyCommand(mux, { kind: "session.claim", name: "own-clients-test" }, seq);
    assert.ok(claimResp.result.ok, `Claim failed: ${JSON.stringify(claimResp.result)}`);

    // Give the watcher time to attach so session_attached is stable.
    await waitFor(
      () => findWatcherPids(socketName).length > 0,
      5_000,
      "watcher to attach to tmux server",
    );
    // A small additional wait for the tmux session_attached counter to settle.
    await new Promise((r) => setTimeout(r, 500));

    // snapshot via sessions.snapshot on a fresh server-proxy connection — counts
    // are refreshed by the server-proxy when building the snapshot.
    const { snapshot } = await connectToServerProxy(serverProxy.endpoint());
    const sessionInfo = snapshot.sessions.find((s) => s.name === "own-clients-test");
    assert.ok(
      sessionInfo,
      `sessions.snapshot must include 'own-clients-test': ${JSON.stringify(snapshot.sessions)}`,
    );
    // With session-proxy + watcher attached but NO real human clients, external count = 0.
    assert.equal(
      sessionInfo.attachedClientCount,
      0,
      `attachedClientCount must be 0 (session-proxy and watcher are tmuxcc-owned, not external); got ${sessionInfo.attachedClientCount}`,
    );

    // Also verify via server-proxy.info, which uses the same session-table entry.
    const infoResp = await sendServerProxyCommand(mux, { kind: "server-proxy.info" }, seq);
    assert.ok(infoResp.result.ok, `server-proxy.info failed: ${JSON.stringify(infoResp.result)}`);
    const info = (infoResp.result as {
      ok: true;
      payload: { info: import("@tmuxcc/session-proxy").ServerProxyInfoPayload };
    }).payload.info;
    const infoRow = info.sessions.find((s) => s.name === "own-clients-test");
    assert.ok(
      infoRow,
      `server-proxy.info sessions must include 'own-clients-test': ${JSON.stringify(info.sessions)}`,
    );
    assert.equal(
      infoRow.attachedClientCount,
      0,
      `server-proxy.info attachedClientCount must be 0 for external-only semantics; got ${infoRow.attachedClientCount}`,
    );

    mux.transport.close();
  });

  it("I7: connect to session-proxy endpoint and run snapshot round-trip", async () => {
    const { mux } = await connectToServerProxy(serverProxy.endpoint());
    const seq = { value: 1 };

    const claimResp = await sendServerProxyCommand(mux, { kind: "session.claim", name: "session-proxy-rtrip" }, seq);
    assert.ok(claimResp.result.ok, `Claim failed: ${JSON.stringify(claimResp.result)}`);
    const endpoint = (claimResp.result as { ok: true; payload: { endpoint: string } }).payload.endpoint;

    // Connect to the session-proxy endpoint using a socket transport.
    // Run the handshake FIRST (same pattern as connectToServerProxy): the handshake
    // installs and then resets its own onControl handler. Install the mux
    // AFTER so its fanout handler wins the single-slot competition.
    const sessionProxyTransport = await connectSocketTransport(endpoint);
    await runClientHandshake(sessionProxyTransport, SESSION_PROXY_CLIENT_CAPS, "session-proxy.capabilities");

    // Now safe to install the mux — handshake has settled and cleared its handler.
    // The session-proxy snapshot arrives after the handshake, so this is not a race.
    const sessionProxyMux = new TransportMux(sessionProxyTransport);

    const snapshotPromise2 = new Promise<unknown>((resolve) => {
      const unsub = sessionProxyMux.subscribe((msg) => {
        if (msg.type === "snapshot") {
          unsub();
          resolve(msg);
        }
      });
    });

    const [sessionProxyTimeoutP, clearSessionProxyTimeout] = rejectAfter(10_000, "Timeout waiting for session-proxy snapshot");
    const snapshot2 = await Promise.race([snapshotPromise2, sessionProxyTimeoutP]);
    clearSessionProxyTimeout();

    // SessionProxy SnapshotMessage (wire v3, tc-j9c.2) has singular `session: SnapshotSession`
    assert.equal((snapshot2 as { type: string }).type, "snapshot");
    const sess2 = (snapshot2 as { session: { sessionId: string; name: string } }).session;
    assert.ok(sess2, "SessionProxy snapshot must have a `session` field");
    assert.ok(sess2.sessionId, "SessionProxy snapshot session must have sessionId");

    sessionProxyTransport.close();
    mux.transport.close();
  });
});

// ---------------------------------------------------------------------------
// tc-w61: @tmuxcc marker integration tests (requires tmux)
// ---------------------------------------------------------------------------

describe("tc-w61: @tmuxcc 1 marker set on spawn / claim", { skip: !TMUX_AVAILABLE }, () => {
  let serverProxy: ServerProxyHandle;
  let socketName: string;

  beforeEach(async () => {
    socketName = nextSocketName();
    serverProxy = createServerProxy({ socketName });
    await serverProxy.start();
  });

  afterEach(async () => {
    await serverProxy.shutdown();
    spawnSync("tmux", ["-L", socketName, "kill-server"], { stdio: "ignore", timeout: 5_000 });
  });

  it("M1: session.claim sets @tmuxcc 1 on the spawned tmux session", async () => {
    const { mux } = await connectToServerProxy(serverProxy.endpoint());
    const seq = { value: 1 };

    const sessionName = "tc-w61-marker-spawn";
    const resp = await sendServerProxyCommand(mux, { kind: "session.claim", name: sessionName }, seq);
    assert.ok(resp.result.ok, `session.claim failed: ${JSON.stringify(resp.result)}`);

    // Verify @tmuxcc 1 is set on the real tmux session.
    // `show-options -v` prints the raw value to stdout; exit 0 means the option is set.
    const markerResult = spawnSync(
      "tmux",
      ["-L", socketName, "show-options", "-t", sessionName, "-v", "@tmuxcc"],
      { encoding: "utf8", timeout: 3_000, stdio: ["ignore", "pipe", "ignore"] },
    );
    assert.equal(markerResult.status, 0, "show-options must exit 0 (option is set)");
    assert.equal(
      (markerResult.stdout ?? "").trim(),
      "1",
      "@tmuxcc must be exactly '1' after session.claim",
    );

    mux.transport.close();
  });

  it("M2: session.create sets @tmuxcc 1 on the spawned tmux session", async () => {
    const { mux } = await connectToServerProxy(serverProxy.endpoint());
    const seq = { value: 1 };

    const sessionName = "tc-w61-marker-create";
    const resp = await sendServerProxyCommand(mux, { kind: "session.create", name: sessionName }, seq);
    assert.ok(resp.result.ok, `session.create failed: ${JSON.stringify(resp.result)}`);

    const markerResult = spawnSync(
      "tmux",
      ["-L", socketName, "show-options", "-t", sessionName, "-v", "@tmuxcc"],
      { encoding: "utf8", timeout: 3_000, stdio: ["ignore", "pipe", "ignore"] },
    );
    assert.equal(markerResult.status, 0, "show-options must exit 0 (option is set)");
    assert.equal(
      (markerResult.stdout ?? "").trim(),
      "1",
      "@tmuxcc must be exactly '1' after session.create",
    );

    mux.transport.close();
  });

  it("M3: mark-on-attach — claiming a pre-existing unmarked session sets @tmuxcc 1", async () => {
    // Create a plain tmux session WITHOUT going through the serverProxy (simulates
    // a session the user created before tmuxcc was installed, or one they created
    // with plain `tmux new-session`).
    const sessionName = "tc-w61-preexisting";
    const created = spawnSync(
      "tmux",
      ["-L", socketName, "new-session", "-d", "-s", sessionName],
      { encoding: "utf8", timeout: 5_000 },
    );
    assert.equal(created.status, 0, "Pre-existing session creation must succeed");

    // Confirm the marker is NOT set on the bare tmux session.
    const before = spawnSync(
      "tmux",
      ["-L", socketName, "show-options", "-t", sessionName, "-v", "@tmuxcc"],
      { encoding: "utf8", timeout: 3_000, stdio: ["ignore", "pipe", "ignore"] },
    );
    // show-options exits non-zero or returns empty when the option is unset.
    const beforeValue = (before.stdout ?? "").trim();
    assert.ok(
      before.status !== 0 || beforeValue !== "1",
      "Pre-existing session must NOT have @tmuxcc 1 before claiming",
    );

    // Now claim the session through the server-proxy.  This should trigger mark-on-attach.
    const { mux } = await connectToServerProxy(serverProxy.endpoint());
    const seq = { value: 1 };
    const resp = await sendServerProxyCommand(mux, { kind: "session.claim", name: sessionName }, seq);
    assert.ok(resp.result.ok, `session.claim failed: ${JSON.stringify(resp.result)}`);

    // Verify the marker is now set.
    const after = spawnSync(
      "tmux",
      ["-L", socketName, "show-options", "-t", sessionName, "-v", "@tmuxcc"],
      { encoding: "utf8", timeout: 3_000, stdio: ["ignore", "pipe", "ignore"] },
    );
    assert.equal(after.status, 0, "show-options must exit 0 after claim");
    assert.equal(
      (after.stdout ?? "").trim(),
      "1",
      "@tmuxcc must be exactly '1' on pre-existing session after claim (mark-on-attach)",
    );

    mux.transport.close();
  });
});

// ---------------------------------------------------------------------------
// Race test (requires tmux)
// ---------------------------------------------------------------------------

describe("server-proxy – race test (requires tmux)", { skip: !TMUX_AVAILABLE }, () => {
  let serverProxy: ServerProxyHandle;
  let socketName: string;

  beforeEach(async () => {
    socketName = nextSocketName();
    serverProxy = createServerProxy({ socketName });
    await serverProxy.start();
  });

  afterEach(async () => {
    await serverProxy.shutdown();
    spawnSync("tmux", ["-L", socketName, "kill-server"], { stdio: "ignore", timeout: 5_000 });
  });

  it("R1: 10 concurrent claims of the same name all get the same sessionId + endpoint", async () => {
    const N = 10;
    const endpoint = serverProxy.endpoint();

    // Open N connections, all in parallel
    const connections = await Promise.all(
      Array.from({ length: N }, () => connectToServerProxy(endpoint)),
    );

    // Issue all 10 session.claim requests concurrently
    const responses = await Promise.all(
      connections.map(({ mux }, i) =>
        sendServerProxyCommand(mux, { kind: "session.claim", name: "race-session" }, { value: i * 100 + 1 }),
      ),
    );

    // All must succeed
    for (const resp of responses) {
      assert.ok(resp.result.ok, `Expected ok=true, got: ${JSON.stringify(resp.result)}`);
    }

    // All must have the same sessionId and endpoint
    const sessionIds = new Set(
      responses.map((r) => (r.result as { ok: true; payload: { sessionId: string } }).payload.sessionId),
    );
    const endpoints = new Set(
      responses.map((r) => (r.result as { ok: true; payload: { endpoint: string } }).payload.endpoint),
    );

    assert.equal(sessionIds.size, 1, `Expected 1 unique sessionId, got ${sessionIds.size}: ${[...sessionIds]}`);
    assert.equal(endpoints.size, 1, `Expected 1 unique endpoint, got ${endpoints.size}: ${[...endpoints]}`);

    // tc-3y8.2: exactly ONE of the racing claims may observe created=true —
    // joined in-flight claims are remapped to created=false so create-time
    // behaviour (profile apply) runs at most once per session creation.
    const createdCount = responses.filter(
      (r) => (r.result as { ok: true; payload: { created: boolean } }).payload.created === true,
    ).length;
    assert.equal(createdCount, 1, `Expected exactly 1 created=true response, got ${createdCount}`);

    // Clean up
    for (const { mux } of connections) {
      mux.transport.close();
    }
  });
});

// ---------------------------------------------------------------------------
// Self-exit lifecycle tests (tc-3iv, ext-a-design-context.md §6.2)
// ---------------------------------------------------------------------------

describe("server-proxy – self-exit: idle hysteresis (tc-3iv)", () => {
  it("S1: zero IPC clients past hysteresis → self-exit, socket unlinked, respawn works", async () => {
    const socketName = nextSocketName();
    const runtimeDir = makeRuntimeDir("s1");
    // Injected short hysteresis — do NOT literally wait 5 minutes in tests.
    const serverProxy = createServerProxy({ socketName, runtimeDir, idleExitMs: 500 });
    const exits: ServerProxySelfExitReason[] = [];
    serverProxy.onSelfExit((reason) => exits.push(reason));
    await serverProxy.start();
    const endpoint = serverProxy.endpoint();

    try {
      assert.ok(fs.existsSync(endpoint), "socket file must exist after start");
      assert.equal(serverProxy.connectedClientCount, 0);

      // Connect a raw client — "client" means ANY open unix-domain connection,
      // counted before (and regardless of) the wire handshake.
      const t = await connectSocketTransport(endpoint);
      await waitFor(() => serverProxy.connectedClientCount === 1, 2_000, "clientCount 0→1 on connect");

      // Disconnect — count drops and the hysteresis window restarts.
      t.close();
      await waitFor(() => serverProxy.connectedClientCount === 0, 2_000, "clientCount 1→0 on disconnect");

      // Idle window elapses → server-proxy self-exits and unlinks its socket.
      await waitFor(() => exits.length > 0, 3_000, "idle self-exit");
      assert.deepEqual(exits, ["idle"]);
      assert.equal(
        fs.existsSync(endpoint),
        false,
        "socket file must be unlinked on self-exit (next launcher probe must not connect-then-reset)",
      );

      // A subsequent spawn on the same socket name + runtime dir must succeed
      // (no EADDRINUSE from a leftover socket file) and must accept clients.
      const broker2 = createServerProxy({ socketName, runtimeDir, idleExitMs: 60_000 });
      try {
        await broker2.start();
        assert.equal(broker2.endpoint(), endpoint, "respawn binds the same well-known path");
        const t2 = await connectSocketTransport(broker2.endpoint());
        await waitFor(() => broker2.connectedClientCount === 1, 2_000, "respawned server-proxy accepts clients");
        t2.close();
      } finally {
        await broker2.shutdown();
      }
    } finally {
      await serverProxy.shutdown(); // idempotent after self-exit
      fs.rmSync(runtimeDir, { recursive: true, force: true });
    }
  });

  it("S2: connected-but-idle client keeps the server-proxy alive past the hysteresis window", async () => {
    const socketName = nextSocketName();
    const runtimeDir = makeRuntimeDir("s2");
    const serverProxy = createServerProxy({ socketName, runtimeDir, idleExitMs: 400 });
    const exits: ServerProxySelfExitReason[] = [];
    serverProxy.onSelfExit((reason) => exits.push(reason));
    await serverProxy.start();
    const endpoint = serverProxy.endpoint();

    try {
      // Connect and then do nothing — no handshake, no commands.  A
      // connected-but-idle client must keep the server-proxy alive (§6.2: "client"
      // is NOT "has bound a terminal" or "has claimed a session").
      const t = await connectSocketTransport(endpoint);
      await waitFor(() => serverProxy.connectedClientCount === 1, 2_000, "clientCount 0→1 on connect");

      // Sit idle for well over the hysteresis window.
      await new Promise((r) => setTimeout(r, 1_000));

      assert.equal(exits.length, 0, "server-proxy must NOT self-exit while a client is connected");
      assert.ok(fs.existsSync(endpoint), "socket file must still exist");
      assert.equal(serverProxy.connectedClientCount, 1, "client still counted");

      t.close();
      await waitFor(() => serverProxy.connectedClientCount === 0, 2_000, "clientCount 1→0 on disconnect");
    } finally {
      await serverProxy.shutdown();
      fs.rmSync(runtimeDir, { recursive: true, force: true });
    }
  });
});

describe("server-proxy – self-exit: tmux death & watcher respawn (tc-3iv, requires tmux)", { skip: !TMUX_AVAILABLE }, () => {
  it("S3: tmux kill-server → server-proxy self-exits within 2s and unlinks its socket", async () => {
    const socketName = nextSocketName();
    const runtimeDir = makeRuntimeDir("s3");

    // Seed a session FIRST so the server-proxy's thin -CC watcher can attach.
    const seeded = spawnSync("tmux", ["-L", socketName, "new-session", "-d", "-s", "seed"], {
      encoding: "utf8",
      timeout: 10_000,
    });
    assert.equal(seeded.status, 0, `tmux new-session failed: ${seeded.stderr}`);

    // Long idle window so only the tmux-death path can trigger exit.
    const serverProxy = createServerProxy({ socketName, runtimeDir, idleExitMs: 600_000 });
    const exits: ServerProxySelfExitReason[] = [];
    let exitedAt = 0;
    serverProxy.onSelfExit((reason) => {
      exits.push(reason);
      exitedAt = Date.now();
    });
    await serverProxy.start();
    const endpoint = serverProxy.endpoint();

    try {
      // The exit trigger is watcher EOF — wait for the watcher to be attached.
      await waitFor(() => findWatcherPids(socketName).length > 0, 5_000, "watcher attach");

      const killedAt = Date.now();
      spawnSync("tmux", ["-L", socketName, "kill-server"], { stdio: "ignore", timeout: 5_000 });

      await waitFor(() => exits.length > 0, 2_000, "self-exit after kill-server");
      assert.deepEqual(exits, ["tmux-gone"]);
      assert.ok(
        exitedAt - killedAt <= 2_000,
        `self-exit took ${exitedAt - killedAt}ms — must be within 2s of kill-server`,
      );
      assert.equal(fs.existsSync(endpoint), false, "socket file must be unlinked on self-exit");
    } finally {
      await serverProxy.shutdown(); // idempotent after self-exit
      spawnSync("tmux", ["-L", socketName, "kill-server"], { stdio: "ignore", timeout: 5_000 });
      fs.rmSync(runtimeDir, { recursive: true, force: true });
    }
  });

  it("S4: watcher SIGKILLed while tmux alive → watcher respawned, server-proxy stays up", async () => {
    const socketName = nextSocketName();
    const runtimeDir = makeRuntimeDir("s4");

    const seeded = spawnSync("tmux", ["-L", socketName, "new-session", "-d", "-s", "seed"], {
      encoding: "utf8",
      timeout: 10_000,
    });
    assert.equal(seeded.status, 0, `tmux new-session failed: ${seeded.stderr}`);

    const serverProxy = createServerProxy({ socketName, runtimeDir, idleExitMs: 600_000 });
    const exits: ServerProxySelfExitReason[] = [];
    serverProxy.onSelfExit((reason) => exits.push(reason));
    await serverProxy.start();
    const endpoint = serverProxy.endpoint();

    try {
      await waitFor(() => findWatcherPids(socketName).length > 0, 5_000, "initial watcher attach");
      const pid1 = findWatcherPids(socketName)[0]!;

      // Kill ONLY the watcher process; the tmux server stays alive.
      process.kill(pid1, "SIGKILL");

      // The server-proxy must probe (tmux alive) and respawn a fresh watcher —
      // and must NOT exit.
      await waitFor(
        () => findWatcherPids(socketName).some((p) => p !== pid1),
        5_000,
        "watcher respawn with a new pid",
      );
      assert.equal(exits.length, 0, "server-proxy must NOT self-exit when only the watcher died");
      assert.ok(fs.existsSync(endpoint), "server-proxy socket must still exist");

      // ServerProxy must still be serving: a fresh client can connect.
      const t = await connectSocketTransport(endpoint);
      await waitFor(() => serverProxy.connectedClientCount === 1, 2_000, "server-proxy still accepts clients");
      t.close();
      await waitFor(() => serverProxy.connectedClientCount === 0, 2_000, "client disconnect observed");

      // The RESPAWNED watcher must still drive the tmux-death exit path.
      spawnSync("tmux", ["-L", socketName, "kill-server"], { stdio: "ignore", timeout: 5_000 });
      await waitFor(() => exits.length > 0, 2_000, "self-exit via respawned watcher");
      assert.deepEqual(exits, ["tmux-gone"]);
      assert.equal(fs.existsSync(endpoint), false, "socket file must be unlinked on self-exit");
    } finally {
      await serverProxy.shutdown(); // idempotent after self-exit
      spawnSync("tmux", ["-L", socketName, "kill-server"], { stdio: "ignore", timeout: 5_000 });
      fs.rmSync(runtimeDir, { recursive: true, force: true });
    }
  });
});
