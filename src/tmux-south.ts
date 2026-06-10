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
 * # Reconnect
 *
 * If the `tmux -CC` connection drops, we reconnect with exponential backoff
 * (starting at 250 ms, doubling up to 8 s).  A caller-supplied `onChanged`
 * callback is invoked each time a `%sessions-changed` line is received,
 * and once on reconnect (to force a full refresh since we may have missed
 * notifications during the outage).
 *
 * # Notes on `tmux -CC attach` without an existing session
 *
 * `tmux -CC attach` requires at least one session to exist.  If none exists,
 * tmux exits immediately.  The broker handles this by deferring the connection
 * attempt until after the first session is created, and reconnecting after
 * session creation commands.
 *
 * @module tmux-south
 */

import { spawn, spawnSync, type ChildProcess } from "node:child_process";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/** Handler invoked when session state may have changed. */
export type SessionsChangedHandler = () => void;

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
// %sessions-changed watcher
// ---------------------------------------------------------------------------

/** Exponential backoff configuration. */
const BACKOFF_INIT_MS = 250;
const BACKOFF_MAX_MS = 8_000;
const BACKOFF_FACTOR = 2;

/**
 * Start a long-lived `tmux -L <socketName> -CC attach` process and invoke
 * `onChanged` whenever `%sessions-changed` appears in its output.
 *
 * The watcher reconnects automatically with exponential backoff when the
 * process exits (e.g. tmux server restarted, no sessions).
 *
 * `onChanged` is also invoked on each reconnect to force a full state refresh.
 *
 * Returns a `TmuxWatcher` handle whose `stop()` method terminates the watcher.
 */
export function createTmuxWatcher(
  socketName: string,
  onChanged: SessionsChangedHandler,
): TmuxWatcher {
  let stopped = false;
  let currentProc: ChildProcess | null = null;
  let backoffMs = BACKOFF_INIT_MS;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  function connect(): void {
    if (stopped) return;

    // Check if the server is actually running before attaching.
    // `tmux -CC attach` without sessions exits immediately; we defer until
    // there's at least one session or until the server is reachable.
    const rows = listSessions(socketName);
    if (rows.length === 0) {
      // Nothing to attach to yet — schedule a retry
      scheduleReconnect();
      return;
    }

    // Use the first available session name as the attach target so tmux
    // doesn't try to attach to the most-recently-used session interactively.
    const target = rows[0]?.name ?? "";

    // Spawn `tmux -L <socketName> -CC attach-session -t <name>` with stdio as
    // pipes (we need stdout for control-mode output).
    // Do NOT pass `-d` here: the `-d` flag causes tmux to detach ALL OTHER
    // clients from the session, including any daemon's own `-CC attach-session`
    // connection.  The watcher only needs to receive `%sessions-changed`
    // notifications; it must not disrupt other attached clients.
    const proc = spawn(
      "tmux",
      ["-L", socketName, "-CC", "attach-session", "-t", target],
      {
        stdio: ["ignore", "pipe", "ignore"],
        detached: false,
      },
    );

    currentProc = proc;
    // Unref so this child process doesn't keep the Node.js event loop alive.
    proc.unref();
    let lineBuf = "";

    proc.stdout?.on("data", (chunk: Buffer) => {
      lineBuf += chunk.toString("utf8");
      const lines = lineBuf.split("\n");
      lineBuf = lines.pop() ?? "";
      for (const line of lines) {
        if (line.trimStart().startsWith("%sessions-changed")) {
          backoffMs = BACKOFF_INIT_MS; // reset backoff on healthy activity
          onChanged();
        }
      }
    });

    proc.on("exit", () => {
      currentProc = null;
      if (!stopped) {
        scheduleReconnect();
      }
    });

    proc.on("error", () => {
      currentProc = null;
      if (!stopped) {
        scheduleReconnect();
      }
    });
  }

  function scheduleReconnect(): void {
    if (stopped) return;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      // Notify on reconnect so the broker does a full refresh
      onChanged();
      connect();
      backoffMs = Math.min(backoffMs * BACKOFF_FACTOR, BACKOFF_MAX_MS);
    }, backoffMs);
  }

  // Start immediately
  connect();

  return {
    stop(): void {
      stopped = true;
      if (reconnectTimer !== null) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      if (currentProc) {
        try {
          currentProc.kill("SIGTERM");
        } catch {
          // ignore
        }
        currentProc = null;
      }
    },
  };
}
