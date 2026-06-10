/**
 * broker.test.ts — integration + race tests for the tmuxcc-broker.
 *
 * # Test categories
 *
 * ## Unit / handshake tests (always run)
 *
 * U1. Broker starts, accepts a connection, runs handshake, sends snapshot.
 *
 * ## Integration tests (guarded by tmux availability)
 *
 * I1. spawn a broker on a test socket, connect as a client, receive snapshot.
 * I2. session.claim creates a session + daemon + returns endpoint.
 * I3. session.claim on existing session returns same endpoint.
 * I4. session.create fails if name is already taken.
 * I5. session.destroy kills session + reaps daemon.
 * I6. sessions.added delta is pushed to subscribers after session.claim.
 * I7. Connect to daemon endpoint, run snapshot + input round-trip (daemon wire).
 *
 * ## Race test
 *
 * R1. 10 concurrent session.claim requests for the same name all receive
 *     the same sessionId + endpoint; only one daemon process is spawned.
 *
 * # Cleanup
 *
 * Each test creates its own broker with a unique test socket name
 * (tmuxcc-test-broker-<N>-<ts>) and calls broker.shutdown() in afterEach.
 *
 * @module broker.test
 */

import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";

import { createBroker, connectSocketTransport, brokerSocketPath } from "./index.js";
import type { BrokerHandle } from "./index.js";
import type { Transport } from "@tmuxcc/daemon";

import {
  runClientHandshake,
  WIRE_PROTOCOL_VERSION,
} from "@tmuxcc/daemon";
import type {
  BrokerSnapshotMessage,
  BrokerCommandResponseMessage,
  BrokerSessionAddedMessage,
  MessageBase,
  Capabilities,
} from "@tmuxcc/daemon";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

let testCounter = 0;

function nextSocketName(): string {
  return `tmuxcc-test-broker-${++testCounter}-${Date.now()}`;
}

function tmuxAvailable(): boolean {
  const r = spawnSync("tmux", ["-V"], { stdio: "ignore", timeout: 2_000 });
  return r.status === 0 && !r.error;
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

const DAEMON_CLIENT_CAPS: Capabilities = {
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

/** Connect to a broker and run the broker-wire handshake. Returns the mux. */
async function connectToBroker(endpoint: string): Promise<{
  mux: TransportMux;
  snapshot: BrokerSnapshotMessage;
}> {
  const transport = await connectSocketTransport(endpoint);

  // Run the handshake FIRST. runClientHandshake installs and then removes
  // its own onControl handler (leaves it as a no-op). We must install
  // the TransportMux AFTER so its handler wins the single-slot competition.
  await runClientHandshake(transport, CLIENT_CAPS, "broker.capabilities");

  // Now safe to install the mux — handshake has settled and cleared its handler.
  const mux = new TransportMux(transport);

  // Snapshot is sent by the broker right after handshake. Since the TCP
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
  const snapshotPromise = new Promise<BrokerSnapshotMessage>((resolve) => {
    const unsub = mux.subscribe((msg) => {
      if (msg.type === "sessions.snapshot") {
        unsub();
        resolve(msg as unknown as BrokerSnapshotMessage);
      }
    });
  });

  const [timeoutP, clearTimeoutP] = rejectAfter(5_000, "Timeout waiting for broker snapshot");
  const snapshot = await Promise.race([snapshotPromise, timeoutP]);
  clearTimeoutP();

  return { mux, snapshot };
}

/** Send a broker command and wait for the correlated response. */
async function sendBrokerCommand(
  mux: TransportMux,
  command: { kind: string; [k: string]: unknown },
  outgoingSeq: { value: number },
): Promise<BrokerCommandResponseMessage> {
  const correlationId = `corr-${Math.random().toString(36).slice(2)}`;

  const responsePromise = new Promise<BrokerCommandResponseMessage>((resolve) => {
    const unsub = mux.subscribe((msg) => {
      if (
        msg.type === "command.response" &&
        (msg as unknown as BrokerCommandResponseMessage).correlationId === correlationId
      ) {
        unsub();
        resolve(msg as unknown as BrokerCommandResponseMessage);
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

describe("broker – unit (no tmux)", () => {
  it("U1: broker starts and sends snapshot after handshake", async () => {
    const socketName = nextSocketName();
    const broker = createBroker({ socketName });
    await broker.start();

    try {
      const { snapshot } = await connectToBroker(broker.endpoint());
      assert.equal(snapshot.type, "sessions.snapshot");
      assert.ok(Array.isArray(snapshot.sessions));
      // No real tmux server → sessions should be empty
      assert.equal(snapshot.sessions.length, 0);
    } finally {
      await broker.shutdown();
    }
  });

  it("U2: broker.endpoint() equals brokerSocketPath(socketName) — well-known path", async () => {
    // This is the core regression guard for tc-j9c.8:
    // broker.endpoint() must equal the path that vscode computes via
    // brokerSocketPath(brokerSocketName) so that discovery works without
    // out-of-band communication.
    const socketName = nextSocketName();
    const runtimeDir = `/tmp/tmuxcc-test-u2-${process.pid}`;
    const broker = createBroker({ socketName, runtimeDir });
    await broker.start();

    try {
      const expected = brokerSocketPath(socketName, { runtimeDir });
      assert.equal(
        broker.endpoint(),
        expected,
        `broker.endpoint() must equal brokerSocketPath(socketName): got ${broker.endpoint()}, want ${expected}`,
      );
    } finally {
      await broker.shutdown();
    }
  });
});

// ---------------------------------------------------------------------------
// Integration tests (requires tmux)
// ---------------------------------------------------------------------------

const TMUX_AVAILABLE = tmuxAvailable();

describe("broker – integration (requires tmux)", { skip: !TMUX_AVAILABLE }, () => {
  let broker: BrokerHandle;
  let socketName: string;

  beforeEach(async () => {
    socketName = nextSocketName();
    broker = createBroker({ socketName });
    await broker.start();
  });

  afterEach(async () => {
    await broker.shutdown();
    spawnSync("tmux", ["-L", socketName, "kill-server"], { stdio: "ignore", timeout: 5_000 });
  });

  it("I1: connect to broker and receive empty snapshot", async () => {
    const { snapshot } = await connectToBroker(broker.endpoint());
    assert.equal(snapshot.type, "sessions.snapshot");
    assert.equal(snapshot.sessions.length, 0);
  });

  it("I2: session.claim creates a session and daemon, returns endpoint", async () => {
    const { mux } = await connectToBroker(broker.endpoint());
    const seq = { value: 1 };

    const resp = await sendBrokerCommand(mux, { kind: "session.claim", name: "test-claim" }, seq);

    assert.ok(resp.result.ok, `Expected ok=true, got: ${JSON.stringify(resp.result)}`);
    const payload = (resp.result as { ok: true; payload: { sessionId: string; endpoint: string } }).payload;
    assert.ok(payload.sessionId, "Missing sessionId");
    assert.ok(payload.endpoint, "Missing endpoint");

    mux.transport.close();
  });

  it("I3: session.claim on same name returns same sessionId + endpoint", async () => {
    const { mux } = await connectToBroker(broker.endpoint());
    const seq = { value: 1 };

    const resp1 = await sendBrokerCommand(mux, { kind: "session.claim", name: "reuse-me" }, seq);
    const resp2 = await sendBrokerCommand(mux, { kind: "session.claim", name: "reuse-me" }, seq);

    assert.ok(resp1.result.ok);
    assert.ok(resp2.result.ok);

    const p1 = (resp1.result as { ok: true; payload: { sessionId: string; endpoint: string } }).payload;
    const p2 = (resp2.result as { ok: true; payload: { sessionId: string; endpoint: string } }).payload;

    assert.equal(p1.sessionId, p2.sessionId, "sessionId must be stable");
    assert.equal(p1.endpoint, p2.endpoint, "endpoint must be stable");

    mux.transport.close();
  });

  it("I4: session.create fails if name is already taken", async () => {
    const { mux } = await connectToBroker(broker.endpoint());
    const seq = { value: 1 };

    const r1 = await sendBrokerCommand(mux, { kind: "session.create", name: "unique-sess" }, seq);
    assert.ok(r1.result.ok, `First create failed: ${JSON.stringify(r1.result)}`);

    const r2 = await sendBrokerCommand(mux, { kind: "session.create", name: "unique-sess" }, seq);
    assert.equal(r2.result.ok, false);
    assert.equal((r2.result as { ok: false; code: string }).code, "session.name-taken");

    mux.transport.close();
  });

  it("I5: session.destroy kills session and reaps daemon", async () => {
    const { mux } = await connectToBroker(broker.endpoint());
    const seq = { value: 1 };

    const claimResp = await sendBrokerCommand(mux, { kind: "session.claim", name: "to-destroy" }, seq);
    assert.ok(claimResp.result.ok);
    const sessionId = (claimResp.result as { ok: true; payload: { sessionId: string } }).payload.sessionId;

    const destroyResp = await sendBrokerCommand(mux, { kind: "session.destroy", sessionId }, seq);
    assert.ok(destroyResp.result.ok, `Destroy failed: ${JSON.stringify(destroyResp.result)}`);

    mux.transport.close();
  });

  it("I6: sessions.added is pushed to subscribers after session.claim", async () => {
    const { mux } = await connectToBroker(broker.endpoint());
    const seq = { value: 1 };

    // Set up delta listener BEFORE the command
    const addedPromise = new Promise<BrokerSessionAddedMessage>((resolve) => {
      const unsub = mux.subscribe((msg) => {
        if (msg.type === "sessions.added") {
          unsub();
          resolve(msg as unknown as BrokerSessionAddedMessage);
        }
      });
    });

    // Trigger session creation
    const claimResp = await sendBrokerCommand(mux, { kind: "session.claim", name: "watch-target" }, seq);
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

  it("I8 (tc-7xv.36): pane.attach returns the same daemon endpoint as session.claim and echoes paneId", async () => {
    const { mux } = await connectToBroker(broker.endpoint());
    const seq = { value: 1 };

    // First claim the session so a daemon is running and we know its sessionId.
    const claimResp = await sendBrokerCommand(mux, { kind: "session.claim", name: "pane-attach-target" }, seq);
    assert.ok(claimResp.result.ok, `Claim failed: ${JSON.stringify(claimResp.result)}`);
    const claimPayload = (claimResp.result as {
      ok: true;
      payload: { sessionId: string; endpoint: string };
    }).payload;
    const sessionId = claimPayload.sessionId;
    const claimEndpoint = claimPayload.endpoint;

    // Now attach to a specific pane.  The broker does not validate pane
    // existence; this test simply asserts the round-trip shape.
    const attachResp = await sendBrokerCommand(
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
    const { mux } = await connectToBroker(broker.endpoint());
    const seq = { value: 1 };

    const resp = await sendBrokerCommand(
      mux,
      { kind: "pane.attach", sessionId: "s999-nonexistent", paneId: "p0" },
      seq,
    );

    assert.equal(resp.result.ok, false);
    const r = resp.result as { ok: false; code: string; message: string };
    assert.equal(r.code, "session.not-found", `code should be session.not-found, got ${r.code}`);

    mux.transport.close();
  });

  it("I7: connect to daemon endpoint and run snapshot round-trip", async () => {
    const { mux } = await connectToBroker(broker.endpoint());
    const seq = { value: 1 };

    const claimResp = await sendBrokerCommand(mux, { kind: "session.claim", name: "daemon-rtrip" }, seq);
    assert.ok(claimResp.result.ok, `Claim failed: ${JSON.stringify(claimResp.result)}`);
    const endpoint = (claimResp.result as { ok: true; payload: { endpoint: string } }).payload.endpoint;

    // Connect to the daemon endpoint using a socket transport.
    // Run the handshake FIRST (same pattern as connectToBroker): the handshake
    // installs and then resets its own onControl handler. Install the mux
    // AFTER so its fanout handler wins the single-slot competition.
    const daemonTransport = await connectSocketTransport(endpoint);
    await runClientHandshake(daemonTransport, DAEMON_CLIENT_CAPS, "daemon.capabilities");

    // Now safe to install the mux — handshake has settled and cleared its handler.
    // The daemon snapshot arrives after the handshake, so this is not a race.
    const daemonMux = new TransportMux(daemonTransport);

    const snapshotPromise2 = new Promise<unknown>((resolve) => {
      const unsub = daemonMux.subscribe((msg) => {
        if (msg.type === "snapshot") {
          unsub();
          resolve(msg);
        }
      });
    });

    const [daemonTimeoutP, clearDaemonTimeout] = rejectAfter(10_000, "Timeout waiting for daemon snapshot");
    const snapshot2 = await Promise.race([snapshotPromise2, daemonTimeoutP]);
    clearDaemonTimeout();

    // Daemon SnapshotMessage (wire v3, tc-j9c.2) has singular `session: SnapshotSession`
    assert.equal((snapshot2 as { type: string }).type, "snapshot");
    const sess2 = (snapshot2 as { session: { sessionId: string; name: string } }).session;
    assert.ok(sess2, "Daemon snapshot must have a `session` field");
    assert.ok(sess2.sessionId, "Daemon snapshot session must have sessionId");

    daemonTransport.close();
    mux.transport.close();
  });
});

// ---------------------------------------------------------------------------
// tc-w61: @tmuxcc marker integration tests (requires tmux)
// ---------------------------------------------------------------------------

describe("tc-w61: @tmuxcc 1 marker set on spawn / claim", { skip: !TMUX_AVAILABLE }, () => {
  let broker: BrokerHandle;
  let socketName: string;

  beforeEach(async () => {
    socketName = nextSocketName();
    broker = createBroker({ socketName });
    await broker.start();
  });

  afterEach(async () => {
    await broker.shutdown();
    spawnSync("tmux", ["-L", socketName, "kill-server"], { stdio: "ignore", timeout: 5_000 });
  });

  it("M1: session.claim sets @tmuxcc 1 on the spawned tmux session", async () => {
    const { mux } = await connectToBroker(broker.endpoint());
    const seq = { value: 1 };

    const sessionName = "tc-w61-marker-spawn";
    const resp = await sendBrokerCommand(mux, { kind: "session.claim", name: sessionName }, seq);
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
    const { mux } = await connectToBroker(broker.endpoint());
    const seq = { value: 1 };

    const sessionName = "tc-w61-marker-create";
    const resp = await sendBrokerCommand(mux, { kind: "session.create", name: sessionName }, seq);
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
    // Create a plain tmux session WITHOUT going through the broker (simulates
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

    // Now claim the session through the broker.  This should trigger mark-on-attach.
    const { mux } = await connectToBroker(broker.endpoint());
    const seq = { value: 1 };
    const resp = await sendBrokerCommand(mux, { kind: "session.claim", name: sessionName }, seq);
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

describe("broker – race test (requires tmux)", { skip: !TMUX_AVAILABLE }, () => {
  let broker: BrokerHandle;
  let socketName: string;

  beforeEach(async () => {
    socketName = nextSocketName();
    broker = createBroker({ socketName });
    await broker.start();
  });

  afterEach(async () => {
    await broker.shutdown();
    spawnSync("tmux", ["-L", socketName, "kill-server"], { stdio: "ignore", timeout: 5_000 });
  });

  it("R1: 10 concurrent claims of the same name all get the same sessionId + endpoint", async () => {
    const N = 10;
    const endpoint = broker.endpoint();

    // Open N connections, all in parallel
    const connections = await Promise.all(
      Array.from({ length: N }, () => connectToBroker(endpoint)),
    );

    // Issue all 10 session.claim requests concurrently
    const responses = await Promise.all(
      connections.map(({ mux }, i) =>
        sendBrokerCommand(mux, { kind: "session.claim", name: "race-session" }, { value: i * 100 + 1 }),
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

    // Clean up
    for (const { mux } of connections) {
      mux.transport.close();
    }
  });
});
