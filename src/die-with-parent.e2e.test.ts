/**
 * die-with-parent.e2e.test.ts — tc-2c5 acceptance: SIGKILL the serverProxy, the
 * session-proxy dies with it; tmux survives; a fresh server-proxy recovers.
 *
 * # Why a subprocess server-proxy
 *
 * The other server-proxy tests run createServerProxy() in-process and shut down
 * gracefully.  This suite spawns server-proxy-entry as a REAL child process so it
 * can be SIGKILLed — the one teardown path that delivers NO signal to the
 * server-proxy's children and therefore exercises the session-proxy's own die-with-parent
 * watchdog (tc-2c5; getppid poll installed in session-proxy-entry.ts).  Without that
 * watchdog the session-proxy would be silently reparented to init and serve forever
 * (the 2026-06-08 orphan observation).
 *
 * # Scenarios
 *
 * D1. serverProxy(subprocess) + session.claim → session-proxy + PTY bridge running.
 *     SIGKILL the serverProxy:
 *       a. the session-proxy process exits ≤ 3 s (poll ≤ 1 s + graceful stop)
 *       b. the python PTY bridge exits with it (enforcement one level down)
 *       c. the tmux server + session SURVIVE — tmux is the persistence layer
 *     Then the recovery path (ext-a §6.3, server-proxy README "Lifetime"):
 *       d. a fresh server-proxy spawns, claims the SAME session against the
 *          surviving tmux state, and gets a FRESH sessionProxy (new pid, no
 *          orphan-and-reclaim).
 *
 * # Cleanup
 *
 * Unique tmux socket name (tmuxcc-test-dwp-…) and a unique runtime dir per
 * run.  The finally block SIGKILLs both brokers and any surviving session-proxy /
 * bridge pids, kills the tmux test server, and removes the runtime dir —
 * even on assertion failure.
 *
 * @module die-with-parent.e2e.test
 */

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join, dirname } from "node:path";
import * as fs from "node:fs";

import { connectSocketTransport, serverProxySocketPath } from "./index.js";
import { runClientHandshake, WIRE_PROTOCOL_VERSION } from "@tmuxcc/session-proxy";
import type {
  Transport,
  ServerProxyCommandResponseMessage,
  Capabilities,
  MessageBase,
} from "@tmuxcc/session-proxy";

// ---------------------------------------------------------------------------
// Paths + guards
// ---------------------------------------------------------------------------

const __dir = dirname(fileURLToPath(import.meta.url));
const PACKAGE_ROOT = join(__dir, "..");
const SERVER_PROXY_ENTRY = join(__dir, "server-proxy-entry.ts");

function tmuxAvailable(): boolean {
  const r = spawnSync("tmux", ["-V"], { stdio: "ignore", timeout: 2_000 });
  return r.status === 0 && !r.error;
}
const TMUX_AVAILABLE = tmuxAvailable();

// ---------------------------------------------------------------------------
// Process helpers
// ---------------------------------------------------------------------------

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

/**
 * Poll until pid is gone; returns elapsed ms.  The poll deadline is larger
 * than the latency budget under test so a slow-but-working path fails with
 * the actual latency in the message instead of a bare timeout.
 */
async function waitUntilGone(pid: number, timeoutMs: number): Promise<number> {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    if (!alive(pid)) return Date.now() - t0;
    await sleep(25);
  }
  throw new Error(`pid ${pid} still alive after ${timeoutMs}ms`);
}

async function waitForPid(
  find: () => number | undefined,
  timeoutMs: number,
  what: string,
): Promise<number> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const pid = find();
    if (pid !== undefined) return pid;
    await sleep(50);
  }
  throw new Error(`Timeout (${timeoutMs}ms) locating ${what}`);
}

function killQuiet(pid: number | undefined): void {
  if (pid === undefined) return;
  try {
    process.kill(pid, "SIGKILL");
  } catch {
    // already gone
  }
}

/** pgrep for a direct child of `parentPid` whose argv matches `pattern`. */
function findChildPid(parentPid: number, pattern: string): number | undefined {
  const r = spawnSync("pgrep", ["-P", String(parentPid), "-f", pattern], {
    encoding: "utf8",
    timeout: 3_000,
  });
  const line = (r.stdout ?? "").trim().split("\n")[0];
  const pid = line ? parseInt(line, 10) : NaN;
  return Number.isNaN(pid) ? undefined : pid;
}

/** Wait for a line on proc stdout. */
function waitForLine(proc: ChildProcess, line: string, timeoutMs: number): Promise<void> {
  return new Promise<void>((resolve, reject) => {
    let buf = "";
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(new Error(`Timeout (${timeoutMs}ms) waiting for "${line}"; stdout so far: ${buf}`));
    }, timeoutMs);
    timer.unref();
    proc.stdout?.on("data", (chunk: Buffer) => {
      if (settled) return;
      buf += chunk.toString("utf8");
      if (buf.split("\n").some((l) => l.trim() === line)) {
        settled = true;
        clearTimeout(timer);
        resolve();
      }
    });
    proc.once("exit", (code, signal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(new Error(`Process exited (code=${code}, signal=${signal}) before "${line}"; stdout: ${buf}`));
    });
  });
}

// ---------------------------------------------------------------------------
// ServerProxy-wire helpers (minimal versions of the server-proxy.test.ts harness)
// ---------------------------------------------------------------------------

const CLIENT_CAPS: Capabilities = {
  protocolVersion: WIRE_PROTOCOL_VERSION,
  features: ["sessions-watch", "session-create", "session-destroy", "session-claim", "pane-attach"],
};

/** Spawn server-proxy-entry as a real subprocess and wait for READY. */
async function spawnServerProxyProcess(socketName: string, runtimeDir: string): Promise<ChildProcess> {
  const proc = spawn(
    process.execPath,
    [
      "--import",
      "tsx",
      SERVER_PROXY_ENTRY,
      "--socket-name",
      socketName,
      "--runtime-dir",
      runtimeDir,
    ],
    { stdio: ["ignore", "pipe", "pipe"], cwd: PACKAGE_ROOT },
  );
  let stderrBuf = "";
  proc.stderr?.on("data", (c: Buffer) => {
    stderrBuf += c.toString("utf8");
  });
  try {
    await waitForLine(proc, "READY", 20_000);
  } catch (err) {
    killQuiet(proc.pid);
    throw new Error(`server-proxy-entry did not become READY: ${String(err)}; stderr: ${stderrBuf}`);
  }
  return proc;
}

/**
 * Connect to a server-proxy socket, run the handshake, claim `sessionName`, return
 * the claim payload.  Closes the transport before returning.
 */
async function claimSession(
  endpoint: string,
  sessionName: string,
): Promise<{ sessionId: string; endpoint: string }> {
  const transport: Transport = await connectSocketTransport(endpoint);
  try {
    await runClientHandshake(transport, CLIENT_CAPS, "server-proxy.capabilities");

    const correlationId = `corr-${Math.random().toString(36).slice(2)}`;
    const responsePromise = new Promise<ServerProxyCommandResponseMessage>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error("Timeout waiting for session.claim response")),
        20_000,
      );
      timer.unref();
      transport.onControl((msg) => {
        const m = msg as unknown as MessageBase & { correlationId?: string };
        if (m.type === "command.response" && m.correlationId === correlationId) {
          clearTimeout(timer);
          resolve(msg as unknown as ServerProxyCommandResponseMessage);
        }
      });
    });

    transport.sendControl({
      type: "command.request",
      seq: 1,
      correlationId,
      command: { kind: "session.claim", name: sessionName },
    } as unknown as Parameters<typeof transport.sendControl>[0]);

    const resp = await responsePromise;
    assert.ok(resp.result.ok, `session.claim failed: ${JSON.stringify(resp.result)}`);
    return (resp.result as { ok: true; payload: { sessionId: string; endpoint: string } }).payload;
  } finally {
    try {
      transport.close();
    } catch {
      // already closed
    }
  }
}

// ---------------------------------------------------------------------------
// D1 — the acceptance scenario
// ---------------------------------------------------------------------------

describe(
  "tc-2c5: die-with-parent e2e (requires tmux)",
  { skip: !TMUX_AVAILABLE ? "tmux not found on PATH" : false },
  () => {
    it(
      "D1: SIGKILL server-proxy → session-proxy + bridge die ≤ 3 s; tmux survives; fresh server-proxy recovers",
      { timeout: 60_000 },
      async () => {
        const socketName = `tmuxcc-test-dwp-${process.pid}-${Date.now()}`;
        const runtimeDir = `/tmp/${socketName}-rt`;
        const sessionName = "dwp-e2e";

        let broker1: ChildProcess | undefined;
        let broker2: ChildProcess | undefined;
        let sessionProxyPid: number | undefined;
        let bridgePid: number | undefined;
        let daemon2Pid: number | undefined;

        try {
          // ── Arrange: real server-proxy subprocess + claimed session ─────────────
          broker1 = await spawnServerProxyProcess(socketName, runtimeDir);
          const endpoint = serverProxySocketPath(socketName, { runtimeDir });
          await claimSession(endpoint, sessionName);

          // Locate the spawned sessionProxy (direct child of the server-proxy running
          // session-proxy-entry) and its python PTY bridge (direct child of the
          // sessionProxy).  Both exist by the time session.claim resolves — the
          // supervisor waits for the session-proxy's READY, which follows
          // sessionProxy.start() (bridge spawned).
          const serverProxyPid = broker1.pid!;
          sessionProxyPid = await waitForPid(
            () => findChildPid(serverProxyPid, "session-proxy-entry"),
            5_000,
            "session-proxy process (child of server-proxy matching session-proxy-entry)",
          );
          bridgePid = await waitForPid(
            () => findChildPid(sessionProxyPid!, "tmux-pty-bridge"),
            5_000,
            "PTY bridge (child of session-proxy matching tmux-pty-bridge)",
          );

          assert.ok(alive(sessionProxyPid), "sanity: session-proxy alive before server-proxy kill");
          assert.ok(alive(bridgePid), "sanity: bridge alive before server-proxy kill");

          // ── Act: SIGKILL the server-proxy — no signal reaches its children ──────
          broker1.kill("SIGKILL");

          // ── Assert (a): session-proxy exits ≤ 3 s — tc-2c5 acceptance budget.
          // getppid poll (1 s cadence) + self-SIGTERM + graceful stop; the
          // 1.5 s hard-exit backstop bounds a stalled stop.
          const sessionProxyGoneMs = await waitUntilGone(sessionProxyPid, 10_000);
          assert.ok(
            sessionProxyGoneMs <= 3_000,
            `session-proxy must exit ≤ 3000 ms after server-proxy SIGKILL; took ${sessionProxyGoneMs} ms`,
          );

          // ── Assert (b): the PTY bridge dies with the sessionProxy (it is reaped
          // by the session-proxy's graceful stop; its own getppid watch + bounded
          // teardown backstop the hard-exit path).
          const bridgeGoneMs = await waitUntilGone(bridgePid, 3_000);
          assert.ok(
            bridgeGoneMs >= 0,
            `bridge gone ${bridgeGoneMs} ms after session-proxy`, // waitUntilGone throws on timeout
          );

          // ── Assert (c): tmux is the persistence layer — server + session
          // must SURVIVE the server-proxy+session-proxy death.
          const has = spawnSync("tmux", ["-L", socketName, "has-session", "-t", sessionName], {
            stdio: "ignore",
            timeout: 5_000,
          });
          assert.equal(
            has.status,
            0,
            "tmux session must survive server-proxy SIGKILL (tmux is the persistence layer)",
          );

          // ── Assert (d): recovery path — fresh serverProxy, fresh sessionProxy, same
          // surviving session.  No orphan-and-reclaim: the new session-proxy is a
          // NEW process, not an adopted old one.
          broker2 = await spawnServerProxyProcess(socketName, runtimeDir);
          const payload2 = await claimSession(endpoint, sessionName);
          assert.ok(payload2.endpoint, "recovery claim must return a session-proxy endpoint");

          daemon2Pid = await waitForPid(
            () => findChildPid(broker2!.pid!, "session-proxy-entry"),
            5_000,
            "fresh session-proxy process under the fresh server-proxy",
          );
          assert.notEqual(
            daemon2Pid,
            sessionProxyPid,
            "recovery must spawn a FRESH session-proxy process (no orphan-and-reclaim)",
          );
          assert.ok(alive(daemon2Pid), "fresh session-proxy must be alive after recovery claim");
        } finally {
          // Kill everything we may have spawned, even on assertion failure.
          killQuiet(broker2?.pid);
          killQuiet(broker1?.pid);
          killQuiet(daemon2Pid);
          killQuiet(sessionProxyPid);
          killQuiet(bridgePid);
          // The tmux server intentionally survives the scenario — reap it.
          spawnSync("tmux", ["-L", socketName, "kill-server"], { stdio: "ignore", timeout: 5_000 });
          // Backstop: anything still holding the unique socket name in argv.
          spawnSync("pkill", ["-KILL", "-f", socketName], { stdio: "ignore", timeout: 5_000 });
          fs.rmSync(runtimeDir, { recursive: true, force: true });
        }
      },
    );
  },
);
