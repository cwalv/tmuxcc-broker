/**
 * server-proxy-log.ts — append-only server-proxy log file (tc-k6v).
 *
 * # Why
 *
 * The server-proxy is spawned detached by its launcher, which destroys the
 * stdout/stderr pipes once "READY\n" has been read (tmuxcc-vscode
 * server-proxy-launcher.ts, tc-7xv.33).  From that point on the server-proxy's stderr
 * diagnostics — session-proxy crash notices, self-exit reasons, fatal errors —
 * go nowhere.  The 2026-06-08 server-proxy debug took minutes precisely because
 * there was nothing to tail.
 *
 * # What
 *
 * `openServerProxyLog(logPath)` opens an append-only (0600) log file and returns
 * a small appender handle.  `installStderrMirror(log)` then tees every
 * `process.stderr.write` into it, prefixing each write with an ISO-8601
 * timestamp.  ServerProxy stderr writes are line-oriented (one `write` per line),
 * so per-write timestamping yields per-line timestamps in practice.
 *
 * Deliberately simple per the bead: append-only, no rotation, best-effort
 * (a failed open or a failed append never takes the server-proxy down — logging
 * is a diagnostic aid, not a dependency).
 *
 * The well-known path is `<runtime>/<socketName>/server-proxy.log` — resolve it
 * with `serverProxyLogPath()` from runtime-dir.ts.  Clients (the VS Code
 * `tmuxcc.showServerProxyLogs` command) derive the same path independently and
 * tail the file; `server-proxy.info` also reports it as `logPath`.
 *
 * @module server-proxy-log
 */

import * as fs from "node:fs";

/** Appender handle returned by `openServerProxyLog`. */
export interface ServerProxyLog {
  /** The absolute log file path. */
  readonly path: string;
  /** Append a chunk (timestamped). Best-effort: errors are swallowed. */
  append(chunk: string | Uint8Array): void;
  /** Close the underlying file descriptor. Idempotent. */
  close(): void;
}

/**
 * Open `logPath` for appending (created 0600 if absent).
 *
 * Returns `null` when the file cannot be opened (unwritable directory,
 * permission failure) — callers run without a log in that case.
 */
export function openServerProxyLog(logPath: string): ServerProxyLog | null {
  let fd: number;
  try {
    fd = fs.openSync(logPath, "a", 0o600);
  } catch {
    return null;
  }

  let closed = false;

  return {
    path: logPath,
    append(chunk: string | Uint8Array): void {
      if (closed) return;
      try {
        const text = typeof chunk === "string" ? chunk : Buffer.from(chunk).toString("utf8");
        fs.writeSync(fd, `${new Date().toISOString()} ${text}`);
      } catch {
        // Best-effort: a full disk or revoked fd must not break the server-proxy.
      }
    },
    close(): void {
      if (closed) return;
      closed = true;
      try {
        fs.closeSync(fd);
      } catch {
        // ignore
      }
    },
  };
}

/**
 * Tee `process.stderr.write` into `log` (the original stderr still receives
 * every write — launchers that capture stderr pre-READY keep working).
 *
 * Returns an uninstall function that restores the original `write`.
 */
export function installStderrMirror(log: ServerProxyLog): () => void {
  const original = process.stderr.write;
  const origWrite = original.bind(process.stderr);

  const teeWrite: typeof process.stderr.write = (
    chunk: Uint8Array | string,
    encodingOrCb?: BufferEncoding | ((err?: Error | null) => void),
    cb?: (err?: Error | null) => void,
  ): boolean => {
    log.append(chunk);
    // Forward with the exact argument shape we received.
    if (typeof encodingOrCb === "function") {
      return origWrite(chunk, encodingOrCb);
    }
    if (encodingOrCb !== undefined) {
      return origWrite(chunk, encodingOrCb, cb);
    }
    return origWrite(chunk, cb);
  };

  process.stderr.write = teeWrite;
  return () => {
    process.stderr.write = original;
  };
}
