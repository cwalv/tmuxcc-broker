/**
 * Runtime directory helpers — path resolution for server-proxy and session-proxy sockets.
 *
 * Follows SCHEMA.md "Trust and security model":
 *   - Sockets live under $XDG_RUNTIME_DIR/tmuxcc/<socketName>/ (or
 *     /tmp/tmuxcc-<uid>/<socketName>/ as fallback) with directory mode 0700
 *     and socket mode 0600.
 *   - The sub-directory name is the tmux socket name (e.g. "tmuxcc"), making
 *     the server-proxy socket path well-known and discoverable by clients:
 *     `<runtimeDir>/<socketName>/server-proxy.sock`.
 *
 * @module runtime-dir
 */

import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

/** Options controlling runtime directory resolution. */
export interface RuntimeDirOptions {
  /**
   * Override the base runtime directory.
   * Default: $XDG_RUNTIME_DIR/tmuxcc or /tmp/tmuxcc-<uid>.
   */
  runtimeDir?: string;
}

/**
 * Resolve the base tmuxcc runtime directory and ensure it exists at mode 0700.
 */
export function resolveBaseRuntimeDir(opts: RuntimeDirOptions = {}): string {
  if (opts.runtimeDir) {
    ensureDir(opts.runtimeDir, 0o700);
    return opts.runtimeDir;
  }

  const xdg = process.env["XDG_RUNTIME_DIR"];
  const base = xdg
    ? path.join(xdg, "tmuxcc")
    : path.join(os.tmpdir(), `tmuxcc-${process.getuid?.() ?? "0"}`);

  ensureDir(base, 0o700);
  return base;
}

/**
 * Resolve the server-proxy socket path: `<runtimeDir>/<socketName>/server-proxy.sock`.
 * Creates the socket-name sub-directory at mode 0700.
 *
 * The `socketName` is the tmux socket name (e.g. `"tmuxcc"`).  Using the
 * socket name as the directory means the path is well-known and clients can
 * discover it without out-of-band communication.
 */
export function serverProxySocketPath(
  socketName: string,
  opts: RuntimeDirOptions = {},
): string {
  const base = resolveBaseRuntimeDir(opts);
  const dir = path.join(base, socketName);
  ensureDir(dir, 0o700);
  return path.join(dir, "server-proxy.sock");
}

/**
 * Resolve the server-proxy log file path: `<runtimeDir>/<socketName>/server-proxy.log`
 * (tc-k6v).  Creates the socket-name sub-directory at mode 0700 (same as
 * `serverProxySocketPath`) but does NOT create the file — the server-proxy entry point
 * opens it append-only; readers (the VS Code `tmuxcc.showServerProxyLogs` tail)
 * tolerate a missing file.
 *
 * Like the server-proxy socket, the path is well-known and derivable by clients
 * without out-of-band communication.
 */
export function serverProxyLogPath(
  socketName: string,
  opts: RuntimeDirOptions = {},
): string {
  const base = resolveBaseRuntimeDir(opts);
  const dir = path.join(base, socketName);
  ensureDir(dir, 0o700);
  return path.join(dir, "server-proxy.log");
}

/**
 * Resolve a session-proxy socket path:
 * `<runtimeDir>/<socketName>/<sessionId>.sock`.
 * Re-uses the already-created socket-name sub-directory.
 */
export function sessionProxySocketPath(
  socketName: string,
  sessionId: string,
  opts: RuntimeDirOptions = {},
): string {
  const base = resolveBaseRuntimeDir(opts);
  const dir = path.join(base, socketName);
  ensureDir(dir, 0o700);
  return path.join(dir, `${sessionId}.sock`);
}

/**
 * Create a directory if it does not exist, set its permissions.
 * Does NOT recurse — parent must exist.
 */
function ensureDir(dir: string, mode: number): void {
  try {
    fs.mkdirSync(dir, { recursive: true, mode });
  } catch (err: unknown) {
    // Ignore EEXIST — directory already exists
    if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err;
  }
  // Always set mode in case the directory pre-existed with different perms
  try {
    fs.chmodSync(dir, mode);
  } catch {
    // Non-fatal — best effort
  }
}

/**
 * Remove a socket file if it exists. Ignores ENOENT.
 */
export function removeSocket(socketPath: string): void {
  try {
    fs.unlinkSync(socketPath);
  } catch (err: unknown) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
  }
}

/**
 * Set permissions on a socket file to 0600.
 * Called immediately after creating a net.Server socket.
 */
export function restrictSocket(socketPath: string): void {
  try {
    fs.chmodSync(socketPath, 0o600);
  } catch {
    // Non-fatal — best effort on platforms where this is not supported
  }
}
