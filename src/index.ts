/**
 * @tmuxcc/server-proxy — public API.
 *
 * Primary entry point: `createServerProxy({ socketName, runtimeDir? })`.
 * Returns a ServerProxyHandle with `start()`, `shutdown()`, `endpoint()`.
 *
 * The server-proxy is a per-tmux-socket discovery and lifecycle service:
 *   - Maintains the list of sessions on a tmux socket
 *   - Spawns per-session session-proxy processes on demand
 *   - Hands clients session-proxy socket paths via the server-proxy wire
 *
 * See SCHEMA.md "ServerProxy wire" for the full wire protocol spec.
 *
 * Lifecycle (ext-a-design-context.md §6.2/§6.3): auto-spawn is the
 * launcher's job, but the server-proxy self-manages exit (tc-3iv) — immediate
 * self-exit when tmux is confirmed gone (watcher EOF + failed probe),
 * 5-minute hysteresis self-exit at zero IPC clients.  Both paths unlink the
 * server-proxy socket file before `onSelfExit` listeners run.  SessionProxys need no
 * external supervision either: they are non-detached children that enforce
 * die-with-parent themselves (tc-2c5), so a dead server-proxy leaves no orphans;
 * a fresh server-proxy simply spawns fresh daemons against the surviving tmux state.
 */

export { createServerProxy } from "./server-proxy.js";
export type { ServerProxyHandle, ServerProxyOptions, ServerProxySelfExitReason } from "./server-proxy.js";

// Socket transport utilities (useful for clients / tests)
export { createSocketTransport, connectSocketTransport, createSocketServer } from "./socket-transport.js";

// Runtime directory helpers (useful for clients that need to compute socket paths)
export { serverProxySocketPath, sessionProxySocketPath, serverProxyLogPath } from "./runtime-dir.js";
