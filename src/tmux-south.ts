/**
 * tmux south-side — broker's thin connection to the tmux server.
 *
 * # Design
 *
 * The broker holds ONE thin `tmux -L <socketName> -CC attach` connection
 * purely to receive `%sessions-changed` notifications. Per SCHEMA.md:
 *   "Hold one thin `tmux -L <socketName> -CC attach` connection purely to
 *    receive `%sessions-changed` notifications. Do NOT process pane events."
 *
 * For state reads the broker shells out to `tmux list-sessions` etc.
 *
 * # Watcher lifecycle (tc-3iv, ext-a-design-context.md §6.2)
 *
 * A watcher instance is single-flight:
 *
 *   1. Pre-attach: while no session exists, poll `list-sessions` with
 *      exponential backoff (250 ms doubling up to 8 s) until a session
 *      appears, then attach.  `tmux -CC attach` requires at least one
 *      session — with `exit-empty on` (the modern default) "no sessions"
 *      also means "no server", so there is nothing to attach to yet.
 *   2. Attached: invoke `onChanged` for each `%sessions-changed` line.
 *   3. EOF: when the attached `-CC` process exits for ANY reason, invoke
 *      `onEof` exactly once and go inert.  The watcher does NOT reconnect
 *      on its own — the owner (the broker) disambiguates via
 *      `probeTmuxAlive` and either re-spawns a fresh watcher (watcher died,
 *      tmux alive) or self-exits (tmux genuinely gone).
 *
 * `onEof` is never invoked after `stop()` (e.g. for the SIGTERM that
 * `stop()` itself delivers to the `-CC` child).
 *
 * # Why the watcher runs through the PTY bridge
 *
 * tmux's client calls tcgetattr() on stdin even in `-CC` control mode; with
 * stdin on a plain pipe (or /dev/null) it exits immediately with "tcgetattr
 * failed: Inappropriate ioctl for device".  A direct `child_process.spawn`
 * watcher therefore dies instantly and silently degrades into a poller.
 * The watcher reuses the daemon's `createTmuxHost` (python PTY bridge) so the
 * `-CC` connection is genuinely live — required for EOF to be a meaningful
 * tmux-death signal.  It attaches with client flags `no-output,ignore-size`:
 * the thin watcher must not receive pane output (§6.2 "do NOT process pane
 * events") and must not influence session sizing for real clients.
 *
 * @module tmux-south
 */

import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { createTmuxHost } from "@tmuxcc/daemon";
import type { TmuxHost } from "@tmuxcc/daemon";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Handler invoked when session state may have changed. */
export type SessionsChangedHandler = () => void;

/**
 * Handler invoked exactly once when an attached `-CC` watcher process EOFs
 * (exits).  Not invoked for pre-attach poll retries, and not after `stop()`.
 */
export type WatcherEofHandler = () => void;

/** Returned by createTmuxWatcher — call stop() to close. */
export interface TmuxWatcher {
  /** Stop the watcher and kill the tmux -CC child process. */
  stop(): void;
}

// ---------------------------------------------------------------------------
// Session list query
// ---------------------------------------------------------------------------

/** A row from `tmux list-sessions`. */
export interface TmuxSessionRow {
  /** tmux session id, e.g. "$1" */
  tmuxId: string;
  /** session name */
  name: string;
  /** number of windows */
  windowCount: number;
  /** number of attached clients */
  attachedCount: number;
}

/**
 * Run `tmux -L <socketName> list-sessions -F '...'` synchronously and
 * return the parsed rows.
 *
 * Returns an empty array if the tmux server is not running or has no sessions.
 */
export function listSessions(socketName: string): TmuxSessionRow[] {
  const FORMAT = "#{session_id} #{session_name} #{session_windows} #{session_attached}";
  const result = spawnSync(
    "tmux",
    ["-L", socketName, "list-sessions", "-F", FORMAT],
    { encoding: "utf8", timeout: 5_000 },
  );

  if (result.status !== 0 || result.error) {
    // Server not running or no sessions — return empty
    return [];
  }

  return (result.stdout ?? "")
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => {
      const [tmuxId, name, windows, attached] = line.split(" ");
      return {
        tmuxId: tmuxId ?? "",
        name: name ?? "",
        windowCount: parseInt(windows ?? "0", 10) || 0,
        attachedCount: parseInt(attached ?? "0", 10) || 0,
      };
    });
}

/**
 * PID of the tmux server on `socketName`, or `null` when the server is not
 * running (tc-k6v `broker.info`).
 *
 * Uses `list-sessions -F '#{pid}'` rather than `display-message -p`: the
 * format variable `#{pid}` is the server pid in any format context, and
 * `list-sessions` needs no attached client or target.  A server with zero
 * sessions effectively does not exist under the modern `exit-empty on`
 * default, so "no rows" ⇒ `null` is the right degradation.
 */
export function getTmuxServerPid(socketName: string): number | null {
  const result = spawnSync(
    "tmux",
    ["-L", socketName, "list-sessions", "-F", "#{pid}"],
    { encoding: "utf8", timeout: 5_000 },
  );
  if (result.status !== 0 || result.error) return null;
  const first = (result.stdout ?? "").trim().split("\n")[0] ?? "";
  const pid = parseInt(first, 10);
  return Number.isNaN(pid) ? null : pid;
}

/**
 * Pane counts per session (tc-k6v `broker.info`): maps tmux session id
 * (e.g. `"$1"`) → number of panes across all of the session's windows.
 *
 * Runs `tmux -L <socketName> list-panes -a -F '#{session_id}'` and tallies
 * rows.  Returns an empty map when the server is not running.
 */
export function countPanesBySession(socketName: string): Map<string, number> {
  const counts = new Map<string, number>();
  const result = spawnSync(
    "tmux",
    ["-L", socketName, "list-panes", "-a", "-F", "#{session_id}"],
    { encoding: "utf8", timeout: 5_000 },
  );
  if (result.status !== 0 || result.error) return counts;
  for (const line of (result.stdout ?? "").trim().split("\n")) {
    if (!line) continue;
    counts.set(line, (counts.get(line) ?? 0) + 1);
  }
  return counts;
}

/**
 * Count tmuxcc-owned `-CC` clients per session (tc-3y8.7).
 *
 * tmuxcc attaches its own control-mode clients to every claimed session:
 *
 *   - **Daemon** (`-CC attach -t <session>`): flags include `control-mode`
 *     but NOT `no-output` or `ignore-size`.  One per claimed session.
 *   - **Watcher** (`-CC attach -f no-output,ignore-size`): flags include
 *     `control-mode`, `no-output`, AND `ignore-size`.  One per broker.
 *
 * Both are distinguishable from real human clients by the presence of
 * `control-mode` in their `client_flags` — a tmux control-mode connection
 * is never opened by a regular terminal user.
 *
 * Empirical evidence (tmux 3.4, tmuxcc test socket):
 *   daemon client: `attached,focused,control-mode,UTF-8`
 *   watcher client: `attached,focused,control-mode,ignore-size,no-output,UTF-8`
 *
 * Returns a map of tmux session id (e.g. `"$1"`) → count of tmuxcc-owned
 * clients attached to that session.  An empty map is returned when the
 * server is not running or has no clients.
 */
export function countTmuxccClientsBySession(socketName: string): Map<string, number> {
  const counts = new Map<string, number>();
  const result = spawnSync(
    "tmux",
    ["-L", socketName, "list-clients", "-F", "#{client_flags} #{session_id}"],
    { encoding: "utf8", timeout: 5_000 },
  );
  if (result.status !== 0 || result.error) return counts;
  for (const line of (result.stdout ?? "").trim().split("\n")) {
    if (!line) continue;
    const spaceIdx = line.lastIndexOf(" ");
    if (spaceIdx < 0) continue;
    const flags = line.slice(0, spaceIdx);
    const sessionId = line.slice(spaceIdx + 1);
    // `control-mode` is present on ALL tmuxcc-owned clients (daemon + watcher).
    // Regular terminal users never open a control-mode connection.
    if (flags.includes("control-mode")) {
      counts.set(sessionId, (counts.get(sessionId) ?? 0) + 1);
    }
  }
  return counts;
}

/**
 * Run `tmux -L <socketName> new-session -d -s <name>` to create a detached
 * session.  Throws if the command fails (including name-already-taken).
 *
 * After a successful `new-session`, immediately sets the Phase 2 marker
 * `@tmuxcc 1` on the session (tc-w61) so that `listTmuxccSessions` can
 * surface it.  The `set-option` failure is intentionally non-fatal — if the
 * marker cannot be set (extremely unusual), the session still exists and the
 * daemon will start; the session just won't appear in the attach-picker until
 * the marker is applied on the next claim.
 */
export function createSession(socketName: string, name: string): void {
  const result = spawnSync(
    "tmux",
    ["-L", socketName, "new-session", "-d", "-s", name],
    { encoding: "utf8", timeout: 10_000 },
  );
  if (result.status !== 0 || result.error) {
    throw new Error(
      `tmux new-session failed: ${result.stderr?.trim() ?? result.error?.message ?? "unknown error"}`,
    );
  }

  // Set the @tmuxcc 1 marker so the Phase 2 probe can discover this session.
  setSessionMarker(socketName, name);
}

/**
 * Set the Phase 2 `@tmuxcc 1` user option on an existing tmux session.
 *
 * Used both immediately after `createSession` (new sessions) and on
 * mark-on-attach (existing sessions that are being claimed by tmuxcc for
 * the first time).  The `set-option` call is idempotent — re-setting
 * `@tmuxcc 1` on an already-marked session is a no-op.
 *
 * Non-fatal: if `set-option` fails (e.g. the session was killed between
 * `new-session` and this call), the error is silently ignored.  The caller
 * can detect the missing marker via `listTmuxccSessions` if needed.
 *
 * Note: `set-option -t` does not support the `=<name>` literal-match prefix;
 * we pass the session name directly.  tmuxcc session names are
 * workspace-derived (stable, unique per workspace) so ambiguity is not a
 * concern in practice.
 */
export function setSessionMarker(socketName: string, name: string): void {
  spawnSync(
    "tmux",
    ["-L", socketName, "set-option", "-t", name, "@tmuxcc", "1"],
    { encoding: "utf8", timeout: 3_000 },
  );
}

/**
 * Set `synchronize-panes` for a specific tmux window.
 *
 * Runs `tmux -L <socketName> set-option -wt @<windowNum> synchronize-panes on|off`.
 *
 * `windowNum` is the numeric tmux window id (the N in `@N`).
 * `on` specifies the desired state.
 *
 * Throws if the command fails (tmux server unavailable or invalid window target).
 *
 * tc-7xv.12: broker-side surface for `setSynchronizePanes`.  Routes through
 * a connected daemon's `set-synchronize-panes` WireCommand when a daemon
 * is available; falls back to this direct-tmux path otherwise.
 */
export function setWindowSynchronizePanes(
  socketName: string,
  windowNum: number,
  on: boolean,
): void {
  const result = spawnSync(
    "tmux",
    ["-L", socketName, "set-option", "-wt", `@${windowNum}`, "synchronize-panes", on ? "on" : "off"],
    { encoding: "utf8", timeout: 5_000 },
  );
  if (result.status !== 0 || result.error) {
    throw new Error(
      `tmux set-option synchronize-panes failed: ${result.stderr?.trim() ?? result.error?.message ?? "unknown error"}`,
    );
  }
}

/**
 * Set `monitor-activity` for a specific tmux window.
 *
 * Runs `tmux -L <socketName> set-option -wt @<windowNum> monitor-activity on|off`.
 *
 * `windowNum` is the numeric tmux window id (the N in `@N`).
 * `on` specifies the desired state.
 *
 * Throws if the command fails (tmux server unavailable or invalid window target).
 *
 * tc-7xv.15: broker-side surface for `setMonitorActivity`.
 */
export function setWindowMonitorActivity(
  socketName: string,
  windowNum: number,
  on: boolean,
): void {
  const result = spawnSync(
    "tmux",
    ["-L", socketName, "set-option", "-wt", `@${windowNum}`, "monitor-activity", on ? "on" : "off"],
    { encoding: "utf8", timeout: 5_000 },
  );
  if (result.status !== 0 || result.error) {
    throw new Error(
      `tmux set-option monitor-activity failed: ${result.stderr?.trim() ?? result.error?.message ?? "unknown error"}`,
    );
  }
}

/**
 * Set `monitor-silence` for a specific tmux window.
 *
 * Runs `tmux -L <socketName> set-option -wt @<windowNum> monitor-silence <seconds>`.
 * Pass `seconds = 0` to disable (tmux interprets `monitor-silence 0` as off).
 *
 * `windowNum` is the numeric tmux window id (the N in `@N`).
 *
 * Throws if the command fails (tmux server unavailable or invalid window target).
 *
 * tc-7xv.15: broker-side surface for `setMonitorSilence`.
 */
export function setWindowMonitorSilence(
  socketName: string,
  windowNum: number,
  seconds: number,
): void {
  const result = spawnSync(
    "tmux",
    ["-L", socketName, "set-option", "-wt", `@${windowNum}`, "monitor-silence", String(seconds)],
    { encoding: "utf8", timeout: 5_000 },
  );
  if (result.status !== 0 || result.error) {
    throw new Error(
      `tmux set-option monitor-silence failed: ${result.stderr?.trim() ?? result.error?.message ?? "unknown error"}`,
    );
  }
}

/**
 * Run `tmux -L <socketName> kill-session -t <id>` to destroy a session.
 * `id` can be a session name or tmux `$N` id.
 * Throws if the command fails.
 */
export function killSession(socketName: string, id: string): void {
  const result = spawnSync(
    "tmux",
    ["-L", socketName, "kill-session", "-t", id],
    { encoding: "utf8", timeout: 10_000 },
  );
  if (result.status !== 0 || result.error) {
    throw new Error(
      `tmux kill-session failed: ${result.stderr?.trim() ?? result.error?.message ?? "unknown error"}`,
    );
  }
}

// ---------------------------------------------------------------------------
// tmux liveness probe (tc-3iv)
// ---------------------------------------------------------------------------

/**
 * Probe whether the tmux server on `socketName` is alive by running
 * `tmux -L <socketName> ls` with a hard timeout (§6.2 watcher-EOF
 * disambiguation).
 *
 * Resolves `true` iff the command exits with status 0 within `timeoutMs`.
 * Any other outcome — non-zero exit ("no server running on …"), spawn
 * failure, or timeout — resolves `false`.
 *
 * Async (unlike the other helpers in this module) so the broker stays
 * responsive to connected clients during the probe window.
 */
export function probeTmuxAlive(socketName: string, timeoutMs: number): Promise<boolean> {
  return new Promise((resolve) => {
    let settled = false;
    const settle = (alive: boolean): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve(alive);
    };

    let proc: ChildProcess;
    try {
      proc = spawn("tmux", ["-L", socketName, "ls"], { stdio: "ignore" });
    } catch {
      resolve(false);
      return;
    }

    const timer = setTimeout(() => {
      try {
        proc.kill("SIGKILL");
      } catch {
        // ignore
      }
      settle(false);
    }, timeoutMs);
    timer.unref();

    proc.on("exit", (code) => {
      settle(code === 0);
    });
    proc.on("error", () => {
      settle(false);
    });
  });
}

// ---------------------------------------------------------------------------
// %sessions-changed watcher
// ---------------------------------------------------------------------------

/** Pre-attach poll backoff configuration. */
const POLL_INIT_MS = 250;
const POLL_MAX_MS = 8_000;
const POLL_FACTOR = 2;

/**
 * Start a single-flight `tmux -L <socketName> -CC attach` watcher (see the
 * module doc's "Watcher lifecycle" section).
 *
 * - `onChanged` fires for each `%sessions-changed` line, and once per
 *   pre-attach poll tick (to force a full refresh — sessions may have been
 *   created externally while we had nothing to attach to).
 * - `onEof` fires exactly once when the attached `-CC` process exits, after
 *   which the watcher is inert.  The owner decides whether to re-spawn a
 *   fresh watcher or self-exit (probe disambiguation, §6.2).
 *
 * Returns a `TmuxWatcher` handle whose `stop()` method terminates the watcher
 * (and suppresses `onEof` for the resulting child exit).
 */
export function createTmuxWatcher(
  socketName: string,
  onChanged: SessionsChangedHandler,
  onEof: WatcherEofHandler,
): TmuxWatcher {
  let stopped = false;
  /** Set after onEof fired — the instance is inert from then on. */
  let done = false;
  let currentHost: TmuxHost | null = null;
  let pollMs = POLL_INIT_MS;
  let pollTimer: ReturnType<typeof setTimeout> | null = null;

  function connect(): void {
    if (stopped || done) return;

    // Check if the server is actually running before attaching.
    // `tmux -CC attach` without sessions exits immediately; we defer until
    // there's at least one session or until the server is reachable.
    const rows = listSessions(socketName);
    if (rows.length === 0) {
      // Nothing to attach to yet — schedule a retry
      schedulePoll();
      return;
    }

    // Use the first available session name as the attach target so tmux
    // doesn't try to attach to the most-recently-used session interactively.
    const target = rows[0]?.name ?? "";

    // Attach via the PTY bridge (see module doc): tmux's client requires a
    // TTY on stdin even in control mode.  Client flags:
    //   no-output   — suppress %output blocks; the watcher only consumes
    //                 session-lifecycle notifications (§6.2)
    //   ignore-size — a thin 220x50 watcher must not drive session resizing
    //                 for daemons / real clients attached to the same session
    // Note: no `-d` — detaching other clients would disrupt daemon attaches.
    const host = createTmuxHost({
      socketName,
      sessionName: target,
      attach: true,
      args: ["-f", "no-output,ignore-size"],
    });
    currentHost = host;

    let lineBuf = "";
    let eofFired = false;

    // The host can report exit and (spawn) error; fire onEof at most once.
    const fireEof = (): void => {
      if (eofFired) return;
      eofFired = true;
      currentHost = null;
      done = true;
      if (!stopped) onEof();
    };

    host.onData((chunk: Uint8Array) => {
      lineBuf += Buffer.from(chunk).toString("utf8");
      const lines = lineBuf.split("\n");
      lineBuf = lines.pop() ?? "";
      for (const line of lines) {
        if (line.trimStart().startsWith("%sessions-changed")) {
          onChanged();
        }
      }
    });

    host.onExit(() => {
      fireEof();
    });
    // Register an error handler so stream errors are not re-emitted as
    // uncaughtException by the host; lifecycle is handled via onExit/catch.
    host.onError(() => {});

    void host.start().catch(() => {
      fireEof();
    });
  }

  function schedulePoll(): void {
    if (stopped || done) return;
    pollTimer = setTimeout(() => {
      pollTimer = null;
      // Notify on each poll tick so the broker does a full refresh
      onChanged();
      connect();
      pollMs = Math.min(pollMs * POLL_FACTOR, POLL_MAX_MS);
    }, pollMs);
    // Don't let a pending pre-attach poll keep the event loop alive.
    pollTimer.unref();
  }

  // Start immediately
  connect();

  return {
    stop(): void {
      stopped = true;
      if (pollTimer !== null) {
        clearTimeout(pollTimer);
        pollTimer = null;
      }
      if (currentHost) {
        // Fire-and-forget: TmuxHost.stop() escalates to SIGKILL internally.
        void currentHost.stop();
        currentHost = null;
      }
    },
  };
}
