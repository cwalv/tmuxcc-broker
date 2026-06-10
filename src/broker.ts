/**
 * Broker — the per-socket discovery and lifecycle service (SCHEMA.md Stage 2).
 *
 * # Public API
 *
 * ```ts
 * const broker = createBroker({ socketName: "tmuxcc" });
 * await broker.start();
 * // broker is now accepting connections at broker.endpoint()
 * await broker.shutdown();
 * ```
 *
 * # Architecture
 *
 * The broker owns:
 *   1. A unix socket server at `endpoint()` for incoming broker-wire connections
 *   2. A thin tmux -CC watcher (south side) for %sessions-changed notifications
 *   3. A session table mapping session names → { sessionId, tmuxId, ... }
 *   4. A daemon supervisor that spawns/reaps per-session daemon child processes
 *   5. A set of connected client transports (fan-out for delta messages)
 *   6. Its own exit policy (tc-3iv, ext-a-design-context.md §6.2): immediate
 *      self-exit when tmux is confirmed gone (watcher EOF + failed `tmux ls`
 *      probe), and a 5-minute hysteresis self-exit at zero IPC clients.  Both
 *      paths unlink the broker socket file before reporting exit.
 *
 * # Wire protocol (Broker wire)
 *
 * Each incoming connection:
 *   1. runServerHandshake with "broker.capabilities"
 *   2. Send BrokerSnapshotMessage (seq=2)
 *   3. Accept BrokerCommandRequestMessages and send BrokerCommandResponseMessages
 *   4. Fan-out session deltas when south-side state changes
 *
 * # Session ID stability
 *
 * Session IDs are broker-assigned, stable for the lifetime of the broker.
 * A new session ID is minted when a session first appears (from list-sessions
 * or from a session.create command). The same ID is reused for the session's
 * daemon and all delta messages.
 *
 * @module broker
 */

import * as path from "node:path";
import {
  runServerHandshake,
  WIRE_PROTOCOL_VERSION,
  sessionId as mintSessionId,
} from "@tmuxcc/daemon";
import type {
  Transport,
  Capabilities,
  BrokerCapabilitiesMessage,
  BrokerSnapshotMessage,
  BrokerSessionInfo,
  BrokerSessionAddedMessage,
  BrokerSessionRemovedMessage,
  BrokerSessionRenamedMessage,
  BrokerCommandRequestMessage,
  BrokerCommandResponseMessage,
  ErrorMessage,
  MessageBase,
  PaneId,
  SessionId,
} from "@tmuxcc/daemon";

import { createSocketServer, createSocketTransport } from "./socket-transport.js";
import { brokerSocketPath, daemonSocketPath, removeSocket, restrictSocket } from "./runtime-dir.js";
import { listSessions, createSession, killSession, createTmuxWatcher, probeTmuxAlive, setWindowSynchronizePanes, setWindowMonitorActivity, setWindowMonitorSilence, setSessionMarker } from "./tmux-south.js";
import type { TmuxWatcher } from "./tmux-south.js";
import { createDaemonSupervisor } from "./daemon-supervisor.js";
import type { DaemonSupervisor } from "./daemon-supervisor.js";
import type { RuntimeDirOptions } from "./runtime-dir.js";

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/** Options for createBroker. */
export interface BrokerOptions {
  /**
   * tmux socket name (passed as `-L <socketName>`).
   * Required — no default to prevent accidental attachment to user's tmux.
   */
  socketName: string;

  /**
   * Override the runtime directory for broker + daemon sockets.
   * Default: $XDG_RUNTIME_DIR/tmuxcc or /tmp/tmuxcc-<uid>.
   */
  runtimeDir?: string;

  /**
   * Idle self-exit hysteresis (tc-3iv, §6.2): when the broker has had zero
   * IPC clients for this long, it self-exits.  Default 5 minutes — sized for
   * human-scale close+reopen workflows (reload-window gaps are sub-second).
   * Tests inject a short value instead of literally waiting 5 minutes.
   */
  idleExitMs?: number;
}

/**
 * Why the broker self-exited (tc-3iv, §6.2):
 *   - "tmux-gone": the thin `-CC` watcher EOFed AND the `tmux ls` probe
 *     confirmed the tmux server is gone.
 *   - "idle": zero IPC clients for the full hysteresis window.
 */
export type BrokerSelfExitReason = "tmux-gone" | "idle";

/** The broker handle returned by createBroker. */
export interface BrokerHandle {
  /**
   * Start the broker: create the unix socket, begin accepting connections,
   * and start the tmux watcher.
   */
  start(): Promise<void>;

  /**
   * Gracefully shut down: stop accepting connections, disconnect all clients,
   * reap all daemons, and remove the broker socket file.
   */
  shutdown(): Promise<void>;

  /**
   * The broker's unix socket path. Only valid after start() resolves.
   */
  endpoint(): string;

  // ── tc-3iv self-exit lifecycle (§6.2) ──────────────────────────────────────

  /**
   * Number of currently open IPC connections to the broker socket.
   *
   * "Client" means any open Unix-domain socket connection — counted at the
   * raw socket level, before (and regardless of) the wire handshake.  It does
   * NOT mean "has bound a terminal" or "has claimed a session"; a
   * connected-but-idle client keeps the broker alive.
   */
  readonly connectedClientCount: number;

  /**
   * Register a callback invoked after the broker has self-exited: shutdown is
   * complete and the broker socket file is already unlinked when the callback
   * runs.  The entry point wires this to `process.exit(0)`.
   *
   * Self-exit triggers (§6.2):
   *   - The thin `-CC` watcher EOFs and the 1 s `tmux ls` probe fails
   *     (tmux genuinely gone) → immediate exit, reason "tmux-gone".
   *   - Zero IPC clients for `idleExitMs` (default 5 min) → reason "idle".
   *
   * If the watcher EOFs but the probe succeeds (the watcher process itself
   * was killed while tmux lives), the broker re-spawns the watcher and does
   * NOT exit.
   */
  onSelfExit(cb: (reason: BrokerSelfExitReason) => void): void;

  /**
   * Toggle `synchronize-panes` for a tmux window (tc-7xv.12).
   *
   * `windowId` is the daemon wire WindowId (e.g. `"w3"` for tmux window `@3`).
   * `on` controls the desired state.
   *
   * Issues `tmux set-option -wt @<N> synchronize-panes on|off` synchronously.
   * The daemon connected to the session will detect the change via the
   * `%window-option-changed` notification and push a `window.sync.changed`
   * delta to all connected clients.
   *
   * Throws if tmux is unavailable or the window does not exist.
   */
  setSynchronizePanes(windowId: string, on: boolean): void;

  // ── tc-7xv.15 ──────────────────────────────────────────────────────────────

  /**
   * Set `monitor-activity` for a tmux window (tc-7xv.15).
   *
   * `windowId` is the daemon wire WindowId (e.g. `"w3"` for tmux window `@3`).
   * `on` controls the desired state.
   *
   * Issues `tmux set-option -wt @<N> monitor-activity on|off` synchronously.
   *
   * Throws if tmux is unavailable or the window does not exist.
   */
  setMonitorActivity(windowId: string, on: boolean): void;

  /**
   * Set `monitor-silence` for a tmux window (tc-7xv.15).
   *
   * `windowId` is the daemon wire WindowId (e.g. `"w3"` for tmux window `@3`).
   * `seconds` is the silence threshold (1..N), or 0/null to disable.
   *
   * Issues `tmux set-option -wt @<N> monitor-silence <seconds>` synchronously.
   * Pass `seconds = null` to disable (sends `monitor-silence 0`).
   *
   * Throws if tmux is unavailable or the window does not exist.
   */
  setMonitorSilence(windowId: string, seconds: number | null): void;

  // ── end tc-7xv.15 ─────────────────────────────────────────────────────────
}

// ---------------------------------------------------------------------------
// Internal session model
// ---------------------------------------------------------------------------

interface SessionEntry {
  sessionId: SessionId;
  /** tmux session id (e.g. "$1") */
  tmuxId: string;
  name: string;
  windowCount: number;
  /** Count of tmuxcc clients attached (tracked per daemon, 0 until a daemon is spawned) */
  attachedClientCount: number;
}

// ---------------------------------------------------------------------------
// Per-client connection state
// ---------------------------------------------------------------------------

interface ClientState {
  transport: Transport;
  /** Next outbound seq number for this client, starting at 1 */
  nextSeq: number;
}

// ---------------------------------------------------------------------------
// Broker capabilities
// ---------------------------------------------------------------------------

const BROKER_CAPABILITIES: Capabilities = {
  protocolVersion: WIRE_PROTOCOL_VERSION,
  features: [
    "sessions-watch",
    "session-create",
    "session-destroy",
    "session-claim",
    "pane-attach", // tc-7xv.36
  ],
};

// ---------------------------------------------------------------------------
// Self-exit policy constants (tc-3iv, §6.2)
// ---------------------------------------------------------------------------

/** Default idle (zero IPC clients) self-exit hysteresis: 5 minutes. */
const DEFAULT_IDLE_EXIT_MS = 5 * 60_000;

/** Timeout for the `tmux ls` liveness probe after a watcher EOF. */
const TMUX_PROBE_TIMEOUT_MS = 1_000;

/**
 * Watcher respawn backoff (probe-succeeded path).  A watcher that lived at
 * least WATCHER_HEALTHY_MS is respawned immediately (one-off signal/OOM kill);
 * short-lived watchers escalate 250 ms → 8 s so a persistently-failing spawn
 * (e.g. broken PTY bridge) cannot turn into a process-spawn storm.
 */
const WATCHER_HEALTHY_MS = 10_000;
const WATCHER_RESPAWN_BACKOFF_INIT_MS = 250;
const WATCHER_RESPAWN_BACKOFF_MAX_MS = 8_000;

// ---------------------------------------------------------------------------
// BrokerImpl
// ---------------------------------------------------------------------------

class BrokerImpl implements BrokerHandle {
  private readonly _opts: BrokerOptions;
  /**
   * The socket-name-derived identifier used as the runtime sub-directory.
   * Equals `opts.socketName` so that broker socket paths are well-known and
   * discoverable: `<runtime>/<socketName>/broker.sock`.
   * One broker per tmux socket name — no UUID needed for isolation.
   */
  private readonly _socketDirName: string;
  private readonly _runtimeDirOpts: RuntimeDirOptions;

  /** Active client connections */
  private _clients = new Map<Transport, ClientState>();
  /** Session table: sessionId → SessionEntry */
  private _sessions = new Map<SessionId, SessionEntry>();
  /** Name index: session name → SessionEntry (for fast lookups) */
  private _byName = new Map<string, SessionEntry>();

  private _supervisor: DaemonSupervisor = createDaemonSupervisor();
  private _watcher: TmuxWatcher | null = null;
  private _server: { close(): Promise<void> } | null = null;
  private _socketPath: string = "";
  private _started = false;

  // ── tc-3iv self-exit state ──────────────────────────────────────────────────
  /** Idle hysteresis window (ms) before self-exit at zero IPC clients. */
  private readonly _idleExitMs: number;
  /** Raw IPC connection count, maintained by the socket server (§6.2 "client"). */
  private _ipcClientCount = 0;
  /** Pending idle self-exit timer; armed whenever the client count is zero. */
  private _idleTimer: ReturnType<typeof setTimeout> | null = null;
  /** Set once a self-exit has been initiated; suppresses re-entry. */
  private _selfExited = false;
  /** True while a watcher-EOF tmux probe is in flight. */
  private _probeInFlight = false;
  private _selfExitHandlers: Array<(reason: BrokerSelfExitReason) => void> = [];
  /** When the current watcher was spawned (drives respawn backoff). */
  private _watcherSpawnedAt = 0;
  /** Backoff for the next respawn after a short-lived watcher. 0 = immediate. */
  private _watcherRespawnDelayMs = 0;
  /** Pending watcher respawn timer (probe-succeeded path). */
  private _respawnTimer: ReturnType<typeof setTimeout> | null = null;

  /**
   * Per-name claim locks: maps session name → in-flight claim promise.
   * Concurrent claims for the same name share this promise.
   */
  private _claimLocks = new Map<string, Promise<{ sessionId: SessionId; endpoint: string }>>();

  constructor(opts: BrokerOptions) {
    this._opts = opts;
    this._socketDirName = opts.socketName;
    this._runtimeDirOpts = opts.runtimeDir !== undefined ? { runtimeDir: opts.runtimeDir } : {};
    this._idleExitMs = opts.idleExitMs ?? DEFAULT_IDLE_EXIT_MS;
  }

  endpoint(): string {
    if (!this._started) throw new Error("Broker not started");
    return this._socketPath;
  }

  get connectedClientCount(): number {
    return this._ipcClientCount;
  }

  onSelfExit(cb: (reason: BrokerSelfExitReason) => void): void {
    this._selfExitHandlers.push(cb);
  }

  /**
   * Map a wire WindowId string ("w3") → tmux numeric id (3).
   * Convention mirrors input-path.ts defaultWindowIdToTmux.
   * Throws with code "internal" on malformed input.
   */
  private _parseWindowId(windowId: string, caller: string): number {
    if (!windowId.startsWith("w")) {
      throw Object.assign(
        new Error(`${caller}: invalid windowId "${windowId}" — must start with "w"`),
        { code: "internal" },
      );
    }
    const windowNum = parseInt(windowId.slice(1), 10);
    if (Number.isNaN(windowNum)) {
      throw Object.assign(
        new Error(`${caller}: cannot parse numeric window id from "${windowId}"`),
        { code: "internal" },
      );
    }
    return windowNum;
  }

  setSynchronizePanes(windowId: string, on: boolean): void {
    const windowNum = this._parseWindowId(windowId, "setSynchronizePanes");
    try {
      setWindowSynchronizePanes(this._opts.socketName, windowNum, on);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      throw Object.assign(new Error(`tmux.unavailable: ${msg}`), { code: "tmux.unavailable" });
    }
  }

  // ── tc-7xv.15 ──────────────────────────────────────────────────────────────

  setMonitorActivity(windowId: string, on: boolean): void {
    const windowNum = this._parseWindowId(windowId, "setMonitorActivity");
    try {
      setWindowMonitorActivity(this._opts.socketName, windowNum, on);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      throw Object.assign(new Error(`tmux.unavailable: ${msg}`), { code: "tmux.unavailable" });
    }
  }

  setMonitorSilence(windowId: string, seconds: number | null): void {
    const windowNum = this._parseWindowId(windowId, "setMonitorSilence");
    const secondsVal = seconds !== null && seconds > 0 ? seconds : 0;
    try {
      setWindowMonitorSilence(this._opts.socketName, windowNum, secondsVal);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      throw Object.assign(new Error(`tmux.unavailable: ${msg}`), { code: "tmux.unavailable" });
    }
  }

  // ── end tc-7xv.15 ─────────────────────────────────────────────────────────

  async start(): Promise<void> {
    if (this._started) throw new Error("Broker already started");
    this._selfExited = false;

    this._socketPath = brokerSocketPath(this._socketDirName, this._runtimeDirOpts);

    // Remove stale socket file if present
    removeSocket(this._socketPath);

    // Start the unix socket server.  Connection counting happens at the raw
    // socket level (see SocketServerOptions.onConnectionCountChange).
    this._server = await createSocketServer(
      this._socketPath,
      (transport) => {
        void this._handleConnection(transport);
      },
      {
        onConnectionCountChange: (count) => {
          this._onConnectionCountChange(count);
        },
      },
    );

    // Restrict socket permissions to 0600
    restrictSocket(this._socketPath);

    // Initial session load
    this._refreshSessions();

    // Start tmux watcher for %sessions-changed notifications
    this._watcher = this._spawnWatcher();

    // Wire supervisor crash handler
    this._supervisor.onCrash((sessionId) => {
      // Daemon died unexpectedly — emit sessions.removed to all broker clients
      const entry = this._sessions.get(sessionId as SessionId);
      if (entry) {
        this._sessions.delete(sessionId as SessionId);
        this._byName.delete(entry.name);
        this._broadcastRemoved(entry.sessionId);
      }
    });

    this._started = true;

    // Arm the idle-exit hysteresis: the broker starts with zero clients, and
    // a launcher that crashes before connecting must not leak a broker.
    this._startIdleTimer();
  }

  async shutdown(): Promise<void> {
    this._clearIdleTimer();
    if (this._respawnTimer !== null) {
      clearTimeout(this._respawnTimer);
      this._respawnTimer = null;
    }

    this._watcher?.stop();
    this._watcher = null;

    // Disconnect all clients
    for (const [transport] of this._clients) {
      try { transport.close(); } catch { /* ignore */ }
    }
    this._clients.clear();

    // Reap all daemons
    this._supervisor.reapAll();

    // Stop the server
    await this._server?.close();
    this._server = null;

    // Remove socket file
    removeSocket(this._socketPath);

    this._started = false;
  }

  // ---------------------------------------------------------------------------
  // Self-exit policy (tc-3iv, §6.2)
  // ---------------------------------------------------------------------------

  private _spawnWatcher(): TmuxWatcher {
    this._watcherSpawnedAt = Date.now();
    return createTmuxWatcher(
      this._opts.socketName,
      () => {
        this._refreshSessions();
      },
      () => {
        this._onWatcherEof();
      },
    );
  }

  /**
   * The thin `-CC` watcher EOFed.  Disambiguate via a `tmux ls` probe (1 s
   * timeout): probe fails → tmux genuinely gone → self-exit immediately;
   * probe succeeds → the watcher process itself died (signal/OOM) while tmux
   * lives → re-spawn the watcher and keep serving.
   */
  private _onWatcherEof(): void {
    if (!this._started || this._selfExited || this._probeInFlight) return;
    this._probeInFlight = true;

    void probeTmuxAlive(this._opts.socketName, TMUX_PROBE_TIMEOUT_MS).then((alive) => {
      this._probeInFlight = false;
      // The broker may have been shut down (or self-exited) during the probe.
      if (!this._started || this._selfExited) return;

      if (alive) {
        // Watcher died but tmux lives — re-spawn, with backoff if the watcher
        // keeps dying young (see WATCHER_HEALTHY_MS).
        const aliveMs = Date.now() - this._watcherSpawnedAt;
        if (aliveMs >= WATCHER_HEALTHY_MS) {
          this._watcherRespawnDelayMs = 0;
        } else {
          this._watcherRespawnDelayMs = Math.min(
            Math.max(this._watcherRespawnDelayMs * 2, WATCHER_RESPAWN_BACKOFF_INIT_MS),
            WATCHER_RESPAWN_BACKOFF_MAX_MS,
          );
        }
        this._scheduleWatcherRespawn(this._watcherRespawnDelayMs);
      } else {
        void this._selfExit("tmux-gone");
      }
    });
  }

  private _scheduleWatcherRespawn(delayMs: number): void {
    if (this._respawnTimer !== null) return; // already scheduled
    const timer = setTimeout(() => {
      this._respawnTimer = null;
      if (!this._started || this._selfExited) return;
      // Replace the (inert, already-EOFed) watcher and force a refresh:
      // sessions may have changed during the watcher gap.
      this._watcher?.stop();
      this._watcher = this._spawnWatcher();
      this._refreshSessions();
    }, delayMs);
    timer.unref();
    this._respawnTimer = timer;
  }

  /** Socket-level connection count changed (raw connections, pre-handshake). */
  private _onConnectionCountChange(count: number): void {
    this._ipcClientCount = count;
    if (!this._started || this._selfExited) return;
    if (count === 0) {
      // Last client gone — restart the hysteresis window.
      this._startIdleTimer();
    } else {
      this._clearIdleTimer();
    }
  }

  private _startIdleTimer(): void {
    this._clearIdleTimer();
    const timer = setTimeout(() => {
      this._idleTimer = null;
      if (this._started && !this._selfExited && this._ipcClientCount === 0) {
        void this._selfExit("idle");
      }
    }, this._idleExitMs);
    // The listening server keeps the event loop alive while the broker runs;
    // unref so a pending hysteresis window never holds the loop on its own.
    timer.unref();
    this._idleTimer = timer;
  }

  private _clearIdleTimer(): void {
    if (this._idleTimer !== null) {
      clearTimeout(this._idleTimer);
      this._idleTimer = null;
    }
  }

  /**
   * Self-exit: run a full shutdown (which unlinks the broker socket file —
   * required so the next launcher's probe doesn't get connect-then-reset),
   * then notify onSelfExit listeners.  The entry point maps that notification
   * to `process.exit(0)`.
   */
  private async _selfExit(reason: BrokerSelfExitReason): Promise<void> {
    if (this._selfExited || !this._started) return;
    this._selfExited = true;

    try {
      await this.shutdown();
    } catch {
      // Best-effort: even if shutdown failed midway, the socket file MUST be
      // gone before we report self-exit, or the next spawn stalls.
      removeSocket(this._socketPath);
    }

    for (const cb of this._selfExitHandlers.slice()) {
      try {
        cb(reason);
      } catch {
        // Listener errors must not break the exit path
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Session state management
  // ---------------------------------------------------------------------------

  private _refreshSessions(): void {
    const rows = listSessions(this._opts.socketName);

    // Build a set of current tmux ids
    const currentTmuxIds = new Set(rows.map((r) => r.tmuxId));

    // Detect removals: any session in our table whose tmuxId is no longer present
    for (const [sid, entry] of this._sessions) {
      if (!currentTmuxIds.has(entry.tmuxId)) {
        this._sessions.delete(sid);
        this._byName.delete(entry.name);
        this._supervisor.reapDaemon(sid);
        this._broadcastRemoved(sid);
      }
    }

    // Build a set of existing tmux ids in our table for addition/rename detection
    const knownTmuxIds = new Map<string, SessionEntry>();
    for (const entry of this._sessions.values()) {
      knownTmuxIds.set(entry.tmuxId, entry);
    }

    // Detect additions and renames
    for (const row of rows) {
      const existing = knownTmuxIds.get(row.tmuxId);
      if (!existing) {
        // New session
        const sessionId = mintSessionId(`s${row.tmuxId.replace("$", "")}`);
        const entry: SessionEntry = {
          sessionId,
          tmuxId: row.tmuxId,
          name: row.name,
          windowCount: row.windowCount,
          attachedClientCount: row.attachedCount,
        };
        this._sessions.set(sessionId, entry);
        this._byName.set(row.name, entry);
        this._broadcastAdded(entry);
      } else if (existing.name !== row.name) {
        // Session was renamed
        this._byName.delete(existing.name);
        existing.name = row.name;
        existing.windowCount = row.windowCount;
        this._byName.set(row.name, existing);
        this._broadcastRenamed(existing.sessionId, row.name);
      } else {
        // Update counts
        existing.windowCount = row.windowCount;
        existing.attachedClientCount = row.attachedCount;
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Connection handler
  // ---------------------------------------------------------------------------

  private async _handleConnection(transport: Transport): Promise<void> {
    // Run broker-wire handshake
    let session: Awaited<ReturnType<typeof runServerHandshake>>;
    try {
      session = await runServerHandshake(transport, BROKER_CAPABILITIES, "broker.capabilities");
    } catch (err) {
      try { transport.close(); } catch { /* ignore */ }
      return;
    }
    void session; // features not yet used in v3 alpha

    // nextSeq starts at 2: the handshake itself sent seq=1 (broker.capabilities).
    // The snapshot is the second server-side message and therefore seq=2.
    const state: ClientState = { transport, nextSeq: 2 };
    this._clients.set(transport, state);

    transport.onClose(() => {
      this._clients.delete(transport);
    });

    // Send snapshot at seq=2 per SCHEMA.md handshake sequence:
    //   broker.capabilities (seq=1) → client.capabilities (seq=1) → sessions.snapshot (seq=2)
    const snapshot = this._buildSnapshot(state.nextSeq);
    state.nextSeq++;
    transport.sendControl(snapshot as unknown as Parameters<typeof transport.sendControl>[0]);

    // Handle incoming commands
    transport.onControl((msg: MessageBase) => {
      if (msg.type === "command.request") {
        void this._handleCommand(state, msg as unknown as BrokerCommandRequestMessage);
      }
      // Other message types: emit protocol.unknown-message error
    });
  }

  // ---------------------------------------------------------------------------
  // Snapshot
  // ---------------------------------------------------------------------------

  private _buildSnapshot(seq: number): BrokerSnapshotMessage {
    const sessions: BrokerSessionInfo[] = [];
    for (const entry of this._sessions.values()) {
      sessions.push({
        sessionId: entry.sessionId,
        name: entry.name,
        windowCount: entry.windowCount,
        attachedClientCount: entry.attachedClientCount,
      });
    }
    return { type: "sessions.snapshot", seq, sessions };
  }

  // ---------------------------------------------------------------------------
  // Command dispatch
  // ---------------------------------------------------------------------------

  private async _handleCommand(
    state: ClientState,
    req: BrokerCommandRequestMessage,
  ): Promise<void> {
    const { correlationId, command } = req;

    try {
      let payload: { sessionId?: SessionId; endpoint?: string; paneId?: PaneId; ok?: true };

      switch (command.kind) {
        case "session.claim":
          payload = await this._claimSession(command.name);
          break;
        case "session.create":
          payload = await this._createSession(command.name);
          break;
        case "session.destroy":
          payload = await this._destroySession(command.sessionId);
          break;
        case "pane.attach":
          // tc-7xv.36: attach intent for a specific pane.  The broker doesn't
          // own pane-level state — it just ensures the daemon is running for
          // the named session and echoes the paneId back so the client has a
          // round-tripped acknowledgement of its targeted attach.
          payload = await this._attachPane(command.sessionId, command.paneId);
          break;
        default: {
          const _exhaustive: never = command;
          void _exhaustive;
          this._sendResponse(state, {
            correlationId,
            result: {
              ok: false,
              code: "protocol.unknown-message",
              message: `Unknown command kind`,
            },
          });
          return;
        }
      }

      this._sendResponse(state, {
        correlationId,
        result: { ok: true, payload },
      });
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      const code = errorCode(err);
      this._sendResponse(state, {
        correlationId,
        result: { ok: false, code, message: msg },
      });
    }
  }

  private _sendResponse(
    state: ClientState,
    partial: Omit<BrokerCommandResponseMessage, "type" | "seq">,
  ): void {
    const msg: BrokerCommandResponseMessage = {
      type: "command.response",
      seq: state.nextSeq,
      ...partial,
    };
    state.nextSeq++;
    try {
      state.transport.sendControl(msg as unknown as Parameters<typeof state.transport.sendControl>[0]);
    } catch {
      // Transport may have closed
    }
  }

  // ---------------------------------------------------------------------------
  // Command implementations
  // ---------------------------------------------------------------------------

  /**
   * Claim or obtain the daemon endpoint for a named session.
   * Per-name serialization via _claimLocks.
   */
  private _claimSession(name: string): Promise<{ sessionId: SessionId; endpoint: string }> {
    const inFlight = this._claimLocks.get(name);
    if (inFlight) return inFlight;

    const promise = this._doClaimSession(name).finally(() => {
      // Only remove the lock if it's still THIS promise (not a newer one)
      if (this._claimLocks.get(name) === promise) {
        this._claimLocks.delete(name);
      }
    });

    this._claimLocks.set(name, promise);
    return promise;
  }

  private async _doClaimSession(
    name: string,
  ): Promise<{ sessionId: SessionId; endpoint: string }> {
    // Refresh session list from tmux
    this._refreshSessions();

    let entry = this._byName.get(name);

    if (!entry) {
      // Session doesn't exist — create it
      try {
        createSession(this._opts.socketName, name);
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err);
        if (msg.toLowerCase().includes("duplicate")) {
          // Race: another process created it between our check and create
          this._refreshSessions();
          entry = this._byName.get(name);
          if (!entry) {
            throw Object.assign(new Error(`session.create race: ${msg}`), { code: "internal" });
          }
        } else {
          throw Object.assign(new Error(`tmux.unavailable: ${msg}`), { code: "tmux.unavailable" });
        }
      }

      if (!entry) {
        // Re-read after creation
        this._refreshSessions();
        entry = this._byName.get(name);
        if (!entry) {
          throw Object.assign(
            new Error(`Session '${name}' not found after creation`),
            { code: "internal" },
          );
        }
      }
    }

    // tc-w61: mark-on-attach — ensure the Phase 2 @tmuxcc 1 marker is set on
    // the session before starting the daemon.  For sessions created by
    // createSession() above, the marker was already set inside createSession();
    // this call is effectively a no-op in that case (idempotent).  For
    // pre-existing sessions that tmuxcc is claiming for the first time (e.g. a
    // session the user manually created then attached via tmuxcc.attachToSession),
    // this stamps them as tmuxcc-managed so they will appear in listTmuxccSessions
    // on subsequent invocations.
    setSessionMarker(this._opts.socketName, entry.name);

    // Ensure daemon is running
    const daemonSockPath = daemonSocketPath(
      this._socketDirName,
      entry.sessionId,
      this._runtimeDirOpts,
    );

    const endpoint = await this._supervisor.ensureDaemon(
      entry.sessionId,
      entry.name,
      this._opts.socketName,
      daemonSockPath,
    );

    return { sessionId: entry.sessionId, endpoint };
  }

  private async _createSession(
    name: string,
  ): Promise<{ sessionId: SessionId; endpoint: string }> {
    this._refreshSessions();

    if (this._byName.has(name)) {
      throw Object.assign(
        new Error(`Session name '${name}' is already in use`),
        { code: "session.name-taken" },
      );
    }

    // Use claim semantics — create then spawn daemon
    return this._claimSession(name);
  }

  /**
   * Handle `pane.attach` (tc-7xv.36).
   *
   * The broker doesn't track panes — it only knows about sessions.  This
   * handler:
   *   1. Verifies the named session exists in the broker's session table
   *      (after a refresh from tmux).
   *   2. Ensures the per-session daemon is running and returns its endpoint
   *      (same path used by `session.claim`).
   *   3. Echoes the supplied `paneId` back to the client as an acknowledgement
   *      of the targeted-attach intent — the client uses it to drive its host-
   *      pty binding decision.
   *
   * Pane existence is NOT validated here.  If the pane has disappeared by the
   * time the client connects to the daemon, the daemon's snapshot simply will
   * not contain it; the client is expected to detect the missing pane and
   * surface a UI signal.  Validating in the broker would require the broker
   * to inspect daemon-side state, which crosses the wire-contract invariant
   * (the broker speaks in sessions, not panes).
   */
  private async _attachPane(
    sessionId: SessionId,
    paneId: PaneId,
  ): Promise<{ sessionId: SessionId; endpoint: string; paneId: PaneId }> {
    // Refresh the session table from tmux so a recently-disappeared session
    // is detected promptly.
    this._refreshSessions();

    const entry = this._sessions.get(sessionId);
    if (!entry) {
      throw Object.assign(
        new Error(`Session '${sessionId}' not found`),
        { code: "session.not-found" },
      );
    }

    // Ensure the daemon is up.  Reuse the same per-name claim semantics so
    // concurrent pane.attach + session.claim requests share one spawn.
    const { endpoint } = await this._claimSession(entry.name);

    return { sessionId: entry.sessionId, endpoint, paneId };
  }

  private async _destroySession(
    sessionId: SessionId,
  ): Promise<{ ok: true }> {
    const entry = this._sessions.get(sessionId);
    if (!entry) {
      throw Object.assign(
        new Error(`Session '${sessionId}' not found`),
        { code: "session.not-found" },
      );
    }

    // Reap daemon first
    this._supervisor.reapDaemon(sessionId);

    // Kill the tmux session
    try {
      killSession(this._opts.socketName, entry.tmuxId);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      throw Object.assign(new Error(`tmux.unavailable: ${msg}`), { code: "tmux.unavailable" });
    }

    // Update local state
    this._sessions.delete(sessionId);
    this._byName.delete(entry.name);
    this._broadcastRemoved(sessionId);

    return { ok: true };
  }

  // ---------------------------------------------------------------------------
  // Delta broadcast
  // ---------------------------------------------------------------------------

  private _broadcastAdded(entry: SessionEntry): void {
    const delta: Omit<BrokerSessionAddedMessage, "seq"> = {
      type: "sessions.added",
      sessionId: entry.sessionId,
      name: entry.name,
      windowCount: entry.windowCount,
      attachedClientCount: entry.attachedClientCount,
    };
    this._broadcastToAll(delta);
  }

  private _broadcastRemoved(sessionId: SessionId): void {
    const delta: Omit<BrokerSessionRemovedMessage, "seq"> = {
      type: "sessions.removed",
      sessionId,
    };
    this._broadcastToAll(delta);
  }

  private _broadcastRenamed(sessionId: SessionId, newName: string): void {
    const delta: Omit<BrokerSessionRenamedMessage, "seq"> = {
      type: "sessions.renamed",
      sessionId,
      newName,
    };
    this._broadcastToAll(delta);
  }

  private _broadcastToAll(msgWithoutSeq: Omit<MessageBase, "seq">): void {
    for (const [transport, state] of this._clients) {
      const stamped = { ...msgWithoutSeq, seq: state.nextSeq };
      state.nextSeq++;
      try {
        transport.sendControl(stamped as unknown as Parameters<typeof transport.sendControl>[0]);
      } catch {
        this._clients.delete(transport);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Error code helper
// ---------------------------------------------------------------------------

function errorCode(err: unknown): string {
  if (err && typeof err === "object" && "code" in err) {
    return String((err as { code: unknown }).code);
  }
  return "internal";
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

/**
 * Create a broker for the given tmux socket.
 *
 * ```ts
 * const broker = createBroker({ socketName: "tmuxcc" });
 * await broker.start();
 * console.log("broker at", broker.endpoint());
 * // ... use the broker ...
 * await broker.shutdown();
 * ```
 *
 * Lifecycle (tc-3iv, §6.2): the broker does not manage its own auto-spawn —
 * that is the launcher's job — but it DOES self-manage exit:
 *   - watcher EOF + failed 1 s `tmux ls` probe → immediate self-exit
 *     (tmux genuinely gone; mirrors tmux's own `exit-empty on` semantics)
 *   - watcher EOF + successful probe → re-spawn the watcher, keep serving
 *   - zero IPC clients for `idleExitMs` (default 5 min) → self-exit
 * Register `onSelfExit` to observe; both paths complete shutdown() — which
 * unlinks the broker socket file — before listeners run.  There is no
 * auto-restart layer: broker crashes are bugs to fix, not UX to smooth over.
 * Nor are there orphaned daemons to reap after a broker crash: daemons are
 * non-detached children that enforce die-with-parent themselves (tc-2c5 —
 * getppid watchdog installed in daemon-entry.ts).  Recovery is launcher →
 * fresh broker → fresh daemons on next session.claim → fresh `-CC attach`
 * to the surviving tmux sessions.
 */
export function createBroker(opts: BrokerOptions): BrokerHandle {
  return new BrokerImpl(opts);
}
