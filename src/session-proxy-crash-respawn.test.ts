/**
 * session-proxy-crash-respawn.test.ts — tc-ukq acceptance: a session session-proxy dies
 * unexpectedly while the server-proxy survives (ext-a-design-context.md §6.3
 * "Crash while the server-proxy lives").
 *
 * # Scenario (C1, two sessions)
 *
 * In-process serverProxy (so the daemons are direct children of THIS test
 * process and can be located + SIGKILLed), two claimed sessions A and B,
 * one session-proxy-wire client attached to each.  SIGKILL session-proxy A:
 *
 *   a. A's attached client sees its session-proxy connection CLOSE — the
 *      session-scoped disconnect (per SCHEMA.md "SessionProxy errors": a dead
 *      session-proxy connection means reconnect via serverProxy).  NOT server-proxy-wide:
 *      B's client connection stays open.
 *   b. The server-proxy does NOT broadcast `sessions.removed` for A — the tmux
 *      session is still alive; only the session-proxy died.
 *   c. Respawn is LAZY (§6.2): no new session-proxy appears until the next claim.
 *   d. The server-proxy itself keeps serving (no self-exit, no restart): the
 *      ORIGINAL server-proxy-wire connection performs the re-claim.
 *   e. Re-claim of A returns the same sessionId + endpoint and a FRESH
 *      session-proxy process (new pid) whose `-CC attach` works: a session-proxy-wire
 *      handshake + snapshot round-trip succeeds against the new session-proxy.
 *   f. Sibling session B is untouched throughout: same session-proxy pid, its
 *      client connection never closes, and a live resync.request →
 *      snapshot round-trip succeeds after the crash.
 *
 * # Cleanup
 *
 * Unique tmux socket name (tmuxcc-test-crash-…) + unique runtime dir per
 * run.  The finally block closes all transports, shuts the server-proxy down
 * (reaping its session-proxy children), SIGKILLs any session-proxy pid this test saw,
 * kills the tmux test server, pkills anything still holding the unique
 * socket name in argv, and removes the runtime dir — even on assertion
 * failure.
 *
 * @module session-proxy-crash-respawn.test
 */

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import { createServerProxy, connectSocketTransport } from "./index.js";
import type { ServerProxySelfExitReason } from "./index.js";
import { runClientHandshake, WIRE_PROTOCOL_VERSION } from "@tmuxcc/session-proxy";
import type {
  Transport,
  Capabilities,
  ServerProxySnapshotMessage,
  ServerProxyCommandResponseMessage,
  MessageBase,
} from "@tmuxcc/session-proxy";

// ---------------------------------------------------------------------------
// Guards + small helpers
// ---------------------------------------------------------------------------

function tmuxAvailable(): boolean {
  const r = spawnSync("tmux", ["-V"], { stdio: "ignore", timeout: 2_000 });
  return r.status === 0 && !r.error;
}
const TMUX_AVAILABLE = tmuxAvailable();

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/** True if a process with this pid exists (EPERM counts as alive). */
function alive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err: unknown) {
    return (err as NodeJS.ErrnoException).code === "EPERM";
  }
}

/** Poll `predicate` every 25 ms until truthy; throw on timeout. */
async function waitFor(
  predicate: () => boolean,
  timeoutMs: number,
  what: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) return;
    if (Date.now() > deadline) throw new Error(`Timeout (${timeoutMs}ms) waiting for ${what}`);
    await sleep(25);
  }
}

function killQuiet(pid: number | undefined): void {
  if (pid === undefined) return;
  try {
    process.kill(pid, "SIGKILL");
  } catch {
    // already gone
  }
}

/**
 * Find the session-proxy process for a session: a direct child of THIS test
 * process (the server-proxy runs in-process, so the supervisor's children are
 * ours) whose argv carries this test's unique socket name AND the session
 * name — the supervisor spawns session-proxy-entry with
 * `--socket-name <socketName> --session-name <sessionName>`.
 */
function findSessionProxyPid(socketName: string, sessionName: string): number | undefined {
  // NB: the pattern must not start with "-" or pgrep parses it as an option.
  const r = spawnSync(
    "pgrep",
    [
      "-P",
      String(process.pid),
      "-f",
      `session-proxy-entry.*--socket-name ${socketName} --session-name ${sessionName} --socket-path`,
    ],
    { encoding: "utf8", timeout: 3_000 },
  );
  const line = (r.stdout ?? "").trim().split("\n")[0];
  const pid = line ? parseInt(line, 10) : NaN;
  return Number.isNaN(pid) ? undefined : pid;
}

// ---------------------------------------------------------------------------
// Wire helpers (minimal versions of the server-proxy.test.ts harness)
// ---------------------------------------------------------------------------

const CLIENT_CAPS: Capabilities = {
  protocolVersion: WIRE_PROTOCOL_VERSION,
  features: ["sessions-watch", "session-create", "session-destroy", "session-claim", "pane-attach"],
};

const SESSION_PROXY_CLIENT_CAPS: Capabilities = {
  protocolVersion: WIRE_PROTOCOL_VERSION,
  features: ["pane-lifecycle", "layout-updates", "focus-events", "input-forwarding"],
};

/**
 * Fan-out wrapper around a Transport's single (replace-last-wins) onControl
 * slot, so multiple awaiters can subscribe concurrently.
 */
class TransportMux {
  readonly transport: Transport;
  private _handlers: Array<(msg: MessageBase) => void> = [];

  constructor(transport: Transport) {
    this.transport = transport;
    transport.onControl((msg) => {
      for (const h of this._handlers.slice()) h(msg as unknown as MessageBase);
    });
  }

  subscribe(handler: (msg: MessageBase) => void): () => void {
    this._handlers.push(handler);
    return () => {
      this._handlers = this._handlers.filter((h) => h !== handler);
    };
  }

  /** Await the next message matching `type` (subscribe BEFORE the trigger when racing). */
  next<T>(type: string, timeoutMs: number, what: string): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        unsub();
        reject(new Error(`Timeout (${timeoutMs}ms) waiting for ${what}`));
      }, timeoutMs);
      timer.unref();
      const unsub = this.subscribe((msg) => {
        if (msg.type === type) {
          clearTimeout(timer);
          unsub();
          resolve(msg as unknown as T);
        }
      });
    });
  }
}

/** Connect + handshake on the server-proxy wire; consume the initial snapshot. */
async function connectToServerProxy(endpoint: string): Promise<TransportMux> {
  const transport = await connectSocketTransport(endpoint);
  // Handshake FIRST: it installs and then clears its own onControl handler;
  // the mux must be installed after so its handler wins the single slot.
  await runClientHandshake(transport, CLIENT_CAPS, "server-proxy.capabilities");
  const mux = new TransportMux(transport);
  await mux.next<ServerProxySnapshotMessage>("sessions.snapshot", 5_000, "server-proxy snapshot");
  return mux;
}

/** Send a server-proxy command and await the correlated response. */
async function sendServerProxyCommand(
  mux: TransportMux,
  command: { kind: string; [k: string]: unknown },
  outgoingSeq: { value: number },
): Promise<ServerProxyCommandResponseMessage> {
  const correlationId = `corr-${Math.random().toString(36).slice(2)}`;
  const responsePromise = new Promise<ServerProxyCommandResponseMessage>((resolve, reject) => {
    const timer = setTimeout(() => {
      unsub();
      reject(new Error(`Timeout waiting for ${command.kind} response`));
    }, 20_000);
    timer.unref();
    const unsub = mux.subscribe((msg) => {
      if (
        msg.type === "command.response" &&
        (msg as unknown as ServerProxyCommandResponseMessage).correlationId === correlationId
      ) {
        clearTimeout(timer);
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

/** Claim a session via the server-proxy wire; assert success; return the payload. */
async function claim(
  mux: TransportMux,
  name: string,
  seq: { value: number },
): Promise<{ sessionId: string; endpoint: string }> {
  const resp = await sendServerProxyCommand(mux, { kind: "session.claim", name }, seq);
  assert.ok(resp.result.ok, `session.claim '${name}' failed: ${JSON.stringify(resp.result)}`);
  return (resp.result as { ok: true; payload: { sessionId: string; endpoint: string } }).payload;
}

/** SessionProxy-wire snapshot shape (the fields this test asserts on). */
interface SessionProxySnapshot {
  type: "snapshot";
  session: { sessionId: string; name: string };
}

/**
 * Connect to a session-proxy endpoint, handshake, and await the initial snapshot —
 * proof of a working `-CC attach` (the snapshot is built from tmux state).
 * Returns the mux, the snapshot, and a closed() probe fed by onClose.
 */
async function connectToSessionProxy(endpoint: string): Promise<{
  mux: TransportMux;
  snapshot: SessionProxySnapshot;
  closed: () => boolean;
}> {
  const transport = await connectSocketTransport(endpoint);
  await runClientHandshake(transport, SESSION_PROXY_CLIENT_CAPS, "session-proxy.capabilities");
  // onClose is single-slot like onControl, and runClientHandshake installs
  // (then no-ops) its own handler — install ours AFTER the handshake.
  let isClosed = false;
  transport.onClose(() => {
    isClosed = true;
  });
  const mux = new TransportMux(transport);
  const snapshot = await mux.next<SessionProxySnapshot>("snapshot", 10_000, "session-proxy snapshot");
  return { mux, snapshot, closed: () => isClosed };
}

// ---------------------------------------------------------------------------
// C1 — the acceptance scenario
// ---------------------------------------------------------------------------

describe(
  "tc-ukq: session-proxy crash → reap + lazy respawn on next claim (requires tmux)",
  { skip: !TMUX_AVAILABLE ? "tmux not found on PATH" : false },
  () => {
    it(
      "C1: SIGKILL session-proxy A → A's client disconnected (session-scoped); no sessions.removed; lazy fresh session-proxy on re-claim; server-proxy + sibling B unaffected",
      { timeout: 60_000 },
      async () => {
        const socketName = `tmuxcc-test-crash-${process.pid}-${Date.now()}`;
        const runtimeDir = fs.mkdtempSync(path.join(os.tmpdir(), "tmuxcc-test-crash-"));
        const nameA = "crash-a";
        const nameB = "crash-b";

        // Long idle window: only an unexpected lifecycle event could exit the server-proxy.
        const serverProxy = createServerProxy({ socketName, runtimeDir, idleExitMs: 600_000 });
        const selfExits: ServerProxySelfExitReason[] = [];
        serverProxy.onSelfExit((reason) => selfExits.push(reason));

        let pidA1: number | undefined;
        let pidA2: number | undefined;
        let pidB: number | undefined;
        const transports: Transport[] = [];

        try {
          await serverProxy.start();
          const serverProxyEndpoint = serverProxy.endpoint();

          // ── Arrange: one server-proxy client, two claimed sessions, one session-proxy-
          // wire client attached to each session's session-proxy. ───────────────────
          const serverProxyMux = await connectToServerProxy(serverProxyEndpoint);
          transports.push(serverProxyMux.transport);
          const seq = { value: 1 };

          // Record every sessions.removed the server-proxy broadcasts on this
          // connection — assertion (b) checks A never appears.
          const removedIds: string[] = [];
          serverProxyMux.subscribe((msg) => {
            if (msg.type === "sessions.removed") {
              removedIds.push((msg as unknown as { sessionId: string }).sessionId);
            }
          });

          const claimA1 = await claim(serverProxyMux, nameA, seq);
          const claimB = await claim(serverProxyMux, nameB, seq);

          pidA1 = findSessionProxyPid(socketName, nameA);
          pidB = findSessionProxyPid(socketName, nameB);
          assert.ok(pidA1 !== undefined, "session-proxy A pid must be discoverable after claim");
          assert.ok(pidB !== undefined, "session-proxy B pid must be discoverable after claim");
          assert.ok(alive(pidA1), "sanity: session-proxy A alive");
          assert.ok(alive(pidB), "sanity: session-proxy B alive");

          const clientA = await connectToSessionProxy(claimA1.endpoint);
          transports.push(clientA.mux.transport);
          assert.equal(clientA.snapshot.session.name, nameA, "session-proxy A serves session A");

          const clientB = await connectToSessionProxy(claimB.endpoint);
          transports.push(clientB.mux.transport);
          assert.equal(clientB.snapshot.session.name, nameB, "session-proxy B serves session B");

          // ── Act: the session-proxy dies unexpectedly (stand-in for parser fault /
          // OOM — SIGKILL gives it no chance to clean up). ───────────────────
          process.kill(pidA1, "SIGKILL");

          // ── Assert (a): session-scoped disconnect — A's attached client
          // sees its session-proxy connection close; B's stays open. ────────────────
          await waitFor(clientA.closed, 5_000, "session-proxy-wire close on session A's client");
          assert.equal(
            clientB.closed(),
            false,
            "sibling session B's session-proxy-wire connection must stay open (NOT server-proxy-wide teardown)",
          );

          // ── Assert (c): respawn is LAZY (§6.2) — with no new claim, the
          // crashed session-proxy stays gone.  Settle long enough for any (buggy)
          // eager-respawn path to have produced a process. ───────────────────
          await sleep(750);
          assert.equal(
            findSessionProxyPid(socketName, nameA),
            undefined,
            "no session-proxy may be respawned for A before the next session.claim (lazy respawn)",
          );

          // ── Assert (b): no sessions.removed for A — the tmux session is
          // alive; only its session-proxy died.  (B must not be removed either.) ────
          assert.deepEqual(
            removedIds,
            [],
            `server-proxy must not broadcast sessions.removed on a session-proxy crash; got ${JSON.stringify(removedIds)}`,
          );
          const hasA = spawnSync("tmux", ["-L", socketName, "has-session", "-t", nameA], {
            stdio: "ignore",
            timeout: 5_000,
          });
          assert.equal(hasA.status, 0, "tmux session A must survive its session-proxy's death");

          // ── Assert (d): the server-proxy never restarted — no self-exit, socket
          // still bound, and the ORIGINAL server-proxy-wire connection performs the
          // re-claim below. ──────────────────────────────────────────────────
          assert.deepEqual(selfExits, [], "server-proxy must not self-exit on a session-proxy crash");
          assert.ok(fs.existsSync(serverProxyEndpoint), "server-proxy socket must still exist");

          // ── Assert (e): re-claim → same identity, FRESH sessionProxy, working
          // -CC attach. ──────────────────────────────────────────────────────
          const claimA2 = await claim(serverProxyMux, nameA, seq);
          assert.equal(claimA2.sessionId, claimA1.sessionId, "sessionId must be stable across the respawn");
          assert.equal(claimA2.endpoint, claimA1.endpoint, "endpoint path must be stable across the respawn");

          pidA2 = findSessionProxyPid(socketName, nameA);
          assert.ok(pidA2 !== undefined, "re-claim must spawn a session-proxy for A");
          assert.notEqual(pidA2, pidA1, "re-claim must spawn a FRESH session-proxy process");
          assert.ok(alive(pidA2), "fresh session-proxy A must be alive");

          const clientA2 = await connectToSessionProxy(claimA2.endpoint);
          transports.push(clientA2.mux.transport);
          assert.equal(
            clientA2.snapshot.session.name,
            nameA,
            "fresh session-proxy must serve a snapshot for the SAME surviving tmux session",
          );
          assert.equal(
            clientA2.snapshot.session.sessionId,
            claimA1.sessionId,
            "fresh session-proxy snapshot must carry the stable sessionId",
          );

          // ── Assert (f): sibling B fully functional after the whole dance —
          // same pid, connection open, live resync round-trip. ──────────────
          assert.equal(findSessionProxyPid(socketName, nameB), pidB, "session-proxy B pid unchanged");
          assert.ok(alive(pidB), "session-proxy B still alive");
          assert.equal(clientB.closed(), false, "B's session-proxy-wire connection still open");
          const bResync = clientB.mux.next<SessionProxySnapshot>("snapshot", 5_000, "B resync snapshot");
          clientB.mux.transport.sendControl({
            type: "resync.request",
            seq: 2,
          } as unknown as Parameters<typeof clientB.mux.transport.sendControl>[0]);
          const bSnap = await bResync;
          assert.equal(bSnap.session.name, nameB, "B answers a live resync.request after A's crash");
        } finally {
          // Reap everything this test may have spawned, even on failure.
          for (const t of transports) {
            try { t.close(); } catch { /* already closed */ }
          }
          try {
            await serverProxy.shutdown(); // reaps session-proxy children, unlinks socket
          } catch { /* already down */ }
          killQuiet(pidA1);
          killQuiet(pidA2);
          killQuiet(pidB);
          spawnSync("tmux", ["-L", socketName, "kill-server"], { stdio: "ignore", timeout: 5_000 });
          // Backstop: anything still holding the unique socket name in argv
          // (daemons, PTY bridges).
          spawnSync("pkill", ["-KILL", "-f", socketName], { stdio: "ignore", timeout: 5_000 });
          fs.rmSync(runtimeDir, { recursive: true, force: true });
        }
      },
    );
  },
);
