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
import { listSessions, createSession, killSession, createTmuxWatcher, setWindowSynchronizePanes, setWindowMonitorActivity, setWindowMonitorSilence } from "./tmux-south.js";
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
}

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
  private _watcher: ReturnType<typeof createTmuxWatcher> | null = null;
  private _server: { close(): Promise<void> } | null = null;
  private _socketPath: string = "";
  private _started = false;

  /**
   * Per-name claim locks: maps session name → in-flight claim promise.
   * Concurrent claims for the same name share this promise.
   */
  private _claimLocks = new Map<string, Promise<{ sessionId: SessionId; endpoint: string }>>();

  constructor(opts: BrokerOptions) {
    this._opts = opts;
    this._socketDirName = opts.socketName;
    this._runtimeDirOpts = opts.runtimeDir !== undefined ? { runtimeDir: opts.runtimeDir } : {};
  }

  endpoint(): string {
    if (!this._started) throw new Error("Broker not started");
    return this._socketPath;
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

    this._socketPath = brokerSocketPath(this._socketDirName, this._runtimeDirOpts);

    // Remove stale socket file if present
    removeSocket(this._socketPath);

    // Start the unix socket server
    this._server = await createSocketServer(this._socketPath, (transport) => {
      void this._handleConnection(transport);
    });

    // Restrict socket permissions to 0600
    restrictSocket(this._socketPath);

    // Initial session load
    this._refreshSessions();

    // Start tmux watcher for %sessions-changed notifications
    this._watcher = createTmuxWatcher(this._opts.socketName, () => {
      this._refreshSessions();
    });

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
  }

  async shutdown(): Promise<void> {
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
 * Assumption on broker lifecycle supervision:
 * The broker does not manage its own auto-spawn or OS-level supervision.
 * Per SCHEMA.md "Broker lifecycle": "Broker process supervision (when the
 * broker starts, how it's restarted, how orphaned daemons are reaped after a
 * broker crash) is out of scope for the v3 spec — it is an implementation
 * concern of whatever launcher ships with each client." A launcher binary or
 * client-side autospawn is a Stage 3+ concern.
 */
export function createBroker(opts: BrokerOptions): BrokerHandle {
  return new BrokerImpl(opts);
}
