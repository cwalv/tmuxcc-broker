/**
 * SessionProxy supervisor — manages per-session session-proxy child processes.
 *
 * # Responsibility
 *
 * The supervisor owns the lifecycle of per-session session-proxy processes:
 *   - Spawning a session-proxy for a session (if one is not already running)
 *   - Waiting for the session-proxy to signal readiness
 *   - Reaping session-proxies on session removal or unexpected exit
 *   - Per-name atomicity: concurrent claim requests for the same session name
 *     are serialized so only one session-proxy process is ever spawned
 *
 * # SessionProxy entry point
 *
 * The supervisor spawns Node.js running the session-proxy entry script
 * `src/session-proxy-entry.js` (compiled from session-proxy-entry.ts in this package).
 * The entry script accepts `--socket-name`, `--session-name`, and
 * `--socket-path` arguments and writes "READY\n" to stdout once the session-proxy
 * is listening.
 *
 * # Data-plane endpoint convention
 *
 * Per SCHEMA.md: "SCHEMA.md mandates multiplexing control + data plane on the
 * same per-session-proxy socket via the 0xCC magic byte (already implemented in
 * tmuxcc-daemon's framing.ts). The endpoint string IS the multiplexed socket."
 *
 * The endpoint returned by the supervisor IS the unix socket path on which the
 * session-proxy listens for both control-plane (JSON) and data-plane (0xCC) traffic.
 * Clients do not need a separate data socket.
 *
 * @module session-proxy-supervisor
 */

import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { join, dirname } from "node:path";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** A running session-proxy entry. */
interface SessionProxyEntry {
  /** The unix socket path the session-proxy is listening on. */
  socketPath: string;
  /** Stable server-proxy-assigned session id. */
  sessionId: string;
  /** The session-proxy child process. */
  proc: ChildProcess;
  /** Promise that resolves once the session-proxy is ready. */
  ready: Promise<string>;
}

/**
 * Exit details passed to the crash handler. `code`/`signal` come from the
 * child's `exit` event — exactly one of them is non-null.
 */
export interface SessionProxyExitInfo {
  /** Session name the session-proxy was bound to (for logging). */
  sessionName: string;
  /** Exit code, or null if the session-proxy was killed by a signal. */
  code: number | null;
  /** Terminating signal (e.g. "SIGKILL"), or null if the session-proxy exited on its own. */
  signal: NodeJS.Signals | null;
}

/**
 * Called when a session-proxy exits UNEXPECTEDLY (ext-a §6.3 "Crash while the
 * server-proxy lives").  Intentional reaps (reapSessionProxy / reapAll) delete the
 * registry entry before killing and therefore never reach this handler.
 * By the time the handler runs, the supervisor has already reaped the
 * sessionId → session-proxy registry entry, so the next ensureSessionProxy() for the
 * session spawns a fresh session-proxy.
 */
export type SessionProxyCrashHandler = (sessionId: string, info: SessionProxyExitInfo) => void;

// ---------------------------------------------------------------------------
// Path to the session-proxy entry script
// ---------------------------------------------------------------------------

const __dir = dirname(fileURLToPath(import.meta.url));

/**
 * Whether this module is being run from TypeScript source (via tsx) rather
 * than a compiled dist.
 *
 * True when the import.meta.url ends with ".ts" (tsx loader presents the
 * original source URL). False when running from a compiled .js dist bundle.
 *
 * Used by both `sessionProxyEntryScript()` and `sessionProxySpawnArgs()` to match the
 * entry-point extension and the `--import tsx` loader flag.
 */
const _runningFromSource = import.meta.url.endsWith(".ts");

/**
 * Path to the compiled session-proxy-entry script.
 * In dist/ layout the entry is dist/session-proxy-entry.js relative to this file.
 */
function sessionProxyEntryScript(): string {
  // When running from dist/ (compiled), session-proxy-entry.js is a sibling.
  // When running from src/ via tsx, session-proxy-entry.ts is a sibling.
  const ext = _runningFromSource ? ".ts" : ".js";
  return join(__dir, `session-proxy-entry${ext}`);
}

/**
 * Build the Node.js argument array for spawning the session-proxy entry script.
 *
 * In development (TypeScript source, tsx loader): prepend `--import tsx` so
 * the `.ts` entry script can be executed directly.
 *
 * In production (compiled dist, `.js` entry): omit `--import tsx` entirely —
 * tsx is a devDependency and will not be present in a production install or
 * on a remote host.
 *
 * This mirrors the logic in `sessionProxyEntryScript()` so the two always agree on
 * whether TypeScript sources or compiled JS files are in use.
 */
function sessionProxySpawnArgs(script: string, socketName: string, sessionName: string, socketPath: string): string[] {
  const scriptArgs = [
    script,
    "--socket-name", socketName,
    "--session-name", sessionName,
    "--socket-path", socketPath,
  ];
  if (_runningFromSource) {
    return ["--import", "tsx", ...scriptArgs];
  }
  return scriptArgs;
}

// ---------------------------------------------------------------------------
// SessionProxySupervisor
// ---------------------------------------------------------------------------

export interface SessionProxySupervisor {
  /**
   * Ensure a session-proxy is running for the given session and return its socket path.
   *
   * If a session-proxy for `sessionId` is already running, returns its socket path
   * immediately. If not, spawns one and waits for it to signal readiness.
   *
   * Per-name atomicity: concurrent calls for the same `sessionId` share the
   * same in-flight spawn promise — only one session-proxy process is ever spawned.
   */
  ensureSessionProxy(
    sessionId: string,
    sessionName: string,
    socketName: string,
    sessionProxySocketPath: string,
  ): Promise<string>;

  /**
   * PID of the running session-proxy for `sessionId`, or `null` when no session-proxy is
   * running (never claimed, reaped, or crashed) or its spawn is still
   * in-flight (tc-k6v `server-proxy.info`).  Read-only; never blocks on a spawn.
   */
  sessionProxyPid(sessionId: string): number | null;

  /**
   * Kill the session-proxy for a session (if running). Called on session removal.
   */
  reapSessionProxy(sessionId: string): void;

  /**
   * Kill all running session-proxies. Called on server-proxy shutdown.
   */
  reapAll(): void;

  /**
   * Register a handler called when a session-proxy exits unexpectedly.
   */
  onCrash(handler: SessionProxyCrashHandler): void;
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

class SessionProxySupervisorImpl implements SessionProxySupervisor {
  /**
   * Map from sessionId to running session-proxy entry (or in-progress spawn promise).
   * The value is the entry OR a Promise for the in-flight spawn, allowing
   * per-name serialization.
   */
  private _sessionProxies = new Map<string, SessionProxyEntry | Promise<SessionProxyEntry>>();
  private _crashHandler: SessionProxyCrashHandler | null = null;

  onCrash(handler: SessionProxyCrashHandler): void {
    this._crashHandler = handler;
  }

  async ensureSessionProxy(
    sessionId: string,
    sessionName: string,
    socketName: string,
    sessionProxySockPath: string,
  ): Promise<string> {
    // Fast path: session-proxy already running
    const existing = this._sessionProxies.get(sessionId);
    if (existing !== undefined) {
      if (existing instanceof Promise) {
        // In-flight spawn — wait for it
        const entry = await existing;
        return entry.socketPath;
      }
      return existing.socketPath;
    }

    // Slow path: spawn a new session-proxy
    const spawnPromise = this._spawnSessionProxy(
      sessionId,
      sessionName,
      socketName,
      sessionProxySockPath,
    );

    // Register the promise immediately so concurrent callers share it
    this._sessionProxies.set(sessionId, spawnPromise);

    let entry: SessionProxyEntry;
    try {
      entry = await spawnPromise;
    } catch (err) {
      // Spawn failed — remove the stale promise
      this._sessionProxies.delete(sessionId);
      throw err;
    }

    // Replace promise with resolved entry
    this._sessionProxies.set(sessionId, entry);
    return entry.socketPath;
  }

  sessionProxyPid(sessionId: string): number | null {
    const entry = this._sessionProxies.get(sessionId);
    if (entry === undefined || entry instanceof Promise) return null;
    return entry.proc.pid ?? null;
  }

  reapSessionProxy(sessionId: string): void {
    const entry = this._sessionProxies.get(sessionId);
    if (!entry) return;

    this._sessionProxies.delete(sessionId);

    if (entry instanceof Promise) {
      // In-flight spawn — kill after it resolves
      void entry.then((e) => this._killEntry(e)).catch(() => {});
    } else {
      this._killEntry(entry);
    }
  }

  reapAll(): void {
    for (const sessionId of this._sessionProxies.keys()) {
      this.reapSessionProxy(sessionId);
    }
  }

  // ---------------------------------------------------------------------------
  // Private helpers
  // ---------------------------------------------------------------------------

  private async _spawnSessionProxy(
    sessionId: string,
    sessionName: string,
    socketName: string,
    sessionProxySockPath: string,
  ): Promise<SessionProxyEntry> {
    const script = sessionProxyEntryScript();

    const proc = spawn(
      process.execPath, // node — always the VS Code Remote Server's own Node binary
      sessionProxySpawnArgs(script, socketName, sessionName, sessionProxySockPath),
      {
        stdio: ["ignore", "pipe", "pipe"],
        // tc-2c5 / ext-a §6.3 invariant: session-proxies are REGULAR children — never
        // `detached: true`, no PID file, no "find my old session-proxies" on startup.
        // Die-with-parent is enforced inside the session-proxy itself (getppid
        // watchdog in session-proxy-entry.ts); recovery from server-proxy death is a fresh
        // server-proxy spawning fresh session-proxies, never reclaiming old ones.
        detached: false,
        env: { ...process.env },
      },
    );

    // Capture stderr so failures aren't opaque. The session-proxy prints actionable
    // diagnostics (e.g. "SessionProxy start failed: …") to stderr before exit;
    // without surfacing it, the supervisor's error is unactionable.
    let stderrBuf = "";
    proc.stderr?.on("data", (chunk: Buffer) => {
      stderrBuf += chunk.toString("utf8");
    });

    // Wait for "READY\n" on stdout with a 30-second timeout
    const readyPromise = new Promise<void>((resolve, reject) => {
      let buf = "";
      const timeout = setTimeout(() => {
        const stderrTail = stderrBuf.trim();
        reject(new Error(
          `SessionProxy for session '${sessionName}' did not signal READY within 30s` +
          (stderrTail ? `. stderr: ${stderrTail}` : ""),
        ));
      }, 30_000);

      proc.stdout?.on("data", (chunk: Buffer) => {
        buf += chunk.toString("utf8");
        if (buf.includes("READY\n")) {
          clearTimeout(timeout);
          resolve();
        }
      });

      proc.once("exit", (code) => {
        clearTimeout(timeout);
        const stderrTail = stderrBuf.trim();
        reject(new Error(
          `SessionProxy for session '${sessionName}' exited before READY (code=${code})` +
          (stderrTail ? `. stderr: ${stderrTail}` : ""),
        ));
      });

      proc.once("error", (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });

    await readyPromise;

    // Unref the child process so it doesn't keep the Node.js event loop alive.
    // The process continues running; we can still kill/track it, but the parent
    // won't be prevented from exiting due to this handle.
    proc.unref();

    // Wire up crash detection AFTER readiness so "exited before READY" is a
    // spawn error, not a crash.
    //
    // tc-ukq (ext-a §6.3 "Crash while the server-proxy lives"): on unexpected
    // exit the registry entry is reaped HERE, synchronously with the exit
    // event, so the next ensureSessionProxy() spawns a fresh session-proxy.  Respawn is
    // LAZY (§6.2): nothing is spawned until the next claim.
    proc.once("exit", (code, signal) => {
      const fire = () => {
        const current = this._sessionProxies.get(sessionId);
        // Only treat as a crash if THIS process is still the registered
        // session-proxy.  The proc identity check guards the ABA case: an old
        // session-proxy's late exit event (delivered after reapSessionProxy() + a fresh
        // spawn under the same sessionId) must not reap the healthy
        // replacement entry.
        if (current !== undefined && !(current instanceof Promise) && current.proc === proc) {
          this._sessionProxies.delete(sessionId);
          this._crashHandler?.(sessionId, { sessionName, code, signal });
        }
      };
      const current = this._sessionProxies.get(sessionId);
      if (current instanceof Promise) {
        // The exit raced ensureSessionProxy()'s `await spawnPromise` continuation:
        // the map still holds the in-flight promise this entry will be
        // registered under.  Re-check after it settles (ensureSessionProxy's
        // continuation — registered first — replaces the promise with the
        // entry before our continuation runs).
        void current.then(
          () => queueMicrotask(fire),
          () => { /* spawn failed; ensureSessionProxy already removed the entry */ },
        );
      } else {
        fire();
      }
    });

    const entry: SessionProxyEntry = {
      sessionId,
      socketPath: sessionProxySockPath,
      proc,
      ready: Promise.resolve(sessionProxySockPath),
    };

    return entry;
  }

  private _killEntry(entry: SessionProxyEntry): void {
    try {
      entry.proc.kill("SIGTERM");
    } catch {
      // already dead
    }
  }
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

export function createSessionProxySupervisor(): SessionProxySupervisor {
  return new SessionProxySupervisorImpl();
}
