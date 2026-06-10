/**
 * @tmuxcc/broker — public API.
 *
 * Primary entry point: `createBroker({ socketName, runtimeDir? })`.
 * Returns a BrokerHandle with `start()`, `shutdown()`, `endpoint()`.
 *
 * The broker is a per-tmux-socket discovery and lifecycle service:
 *   - Maintains the list of sessions on a tmux socket
 *   - Spawns per-session daemon processes on demand
 *   - Hands clients daemon socket paths via the broker wire
 *
 * See SCHEMA.md "Broker wire" for the full wire protocol spec.
 *
 * Lifecycle (ext-a-design-context.md §6.2/§6.3): auto-spawn is the
 * launcher's job, but the broker self-manages exit (tc-3iv) — immediate
 * self-exit when tmux is confirmed gone (watcher EOF + failed probe),
 * 5-minute hysteresis self-exit at zero IPC clients.  Both paths unlink the
 * broker socket file before `onSelfExit` listeners run.  Daemons need no
 * external supervision either: they are non-detached children that enforce
 * die-with-parent themselves (tc-2c5), so a dead broker leaves no orphans;
 * a fresh broker simply spawns fresh daemons against the surviving tmux state.
 */

export { createBroker } from "./broker.js";
export type { BrokerHandle, BrokerOptions, BrokerSelfExitReason } from "./broker.js";

// Socket transport utilities (useful for clients / tests)
export { createSocketTransport, connectSocketTransport, createSocketServer } from "./socket-transport.js";

// Runtime directory helpers (useful for clients that need to compute socket paths)
export { brokerSocketPath, daemonSocketPath } from "./runtime-dir.js";
