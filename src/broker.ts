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
  SessionId,
} from "@tmuxcc/daemon";

import { createSocketServer, createSocketTransport } from "./socket-transport.js";
import { brokerSocketPath, daemonSocketPath, removeSocket, restrictSocket } from "./runtime-dir.js";
import { listSessions, createSession, killSession, createTmuxWatcher } from "./tmux-south.js";
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
  features: ["sessions-watch", "session-create", "session-destroy", "session-claim"],
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
      let payload: { sessionId?: SessionId; endpoint?: string; ok?: true };

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
