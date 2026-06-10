/**
 * ServerProxy entry point — spawned as a child process by the VS Code extension
 * (or any other launcher) to start a server-proxy for a given tmux socket name.
 *
 * Arguments:
 *   --socket-name <name>    tmux socket name (= server-proxy socket directory name)
 *
 * Optional arguments:
 *   --runtime-dir <path>    override the base runtime directory
 *   --idle-exit-ms <n>      zero-client self-exit hysteresis (default 5 min;
 *                           injectable so tests don't wait 5 real minutes)
 *
 * Protocol:
 *   1. Parse arguments.
 *   2. Mirror stderr into the append-only server-proxy log file at
 *      `<runtime>/<socketName>/server-proxy.log` (tc-k6v) — launchers destroy the
 *      stderr pipe post-READY, so without the log the server-proxy's diagnostics
 *      (session-proxy crashes, self-exit reasons) would vanish.
 *   3. Create and start a serverProxy (createServerProxy from ./server-proxy.js).
 *   4. Write "READY\n" to stdout so the launcher knows we are listening.
 *   5. On SIGTERM: call serverProxy.shutdown() and exit cleanly.
 *   6. On server-proxy self-exit (tc-3iv, §6.2 — tmux gone, or idle past the
 *      hysteresis window): exit 0.  The server-proxy has already unlinked its
 *      socket file by the time the self-exit callback fires.
 *
 * This mirrors the session-proxy-entry.ts pattern: a thin entry script whose
 * only job is argument parsing → start → READY signal → exit handling.
 *
 * @module server-proxy-entry
 */

import { createServerProxy } from "./server-proxy.js";
import type { ServerProxyOptions } from "./server-proxy.js";
import { serverProxyLogPath } from "./runtime-dir.js";
import { openServerProxyLog, installStderrMirror } from "./server-proxy-log.js";

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

function parseArgs(): { socketName: string; runtimeDir?: string; idleExitMs?: number } {
  const args = process.argv.slice(2);
  let socketName = "";
  let runtimeDir: string | undefined;
  let idleExitMs: number | undefined;

  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case "--socket-name":
        socketName = args[++i] ?? "";
        break;
      case "--runtime-dir":
        runtimeDir = args[++i] ?? undefined;
        break;
      case "--idle-exit-ms": {
        const parsed = parseInt(args[++i] ?? "", 10);
        if (!Number.isNaN(parsed) && parsed > 0) idleExitMs = parsed;
        break;
      }
    }
  }

  if (!socketName) {
    process.stderr.write(
      "Usage: server-proxy-entry --socket-name <name> [--runtime-dir <path>] [--idle-exit-ms <n>]\n",
    );
    process.exit(1);
  }

  return {
    socketName,
    ...(runtimeDir !== undefined ? { runtimeDir } : {}),
    ...(idleExitMs !== undefined ? { idleExitMs } : {}),
  };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const { socketName, runtimeDir, idleExitMs } = parseArgs();

  // tc-k6v: mirror stderr into the append-only server-proxy log file.  Best-effort:
  // a failed open (unwritable runtime dir) leaves the server-proxy running without
  // a log — `server-proxy.info` then reports `logPath: null`.
  const log = openServerProxyLog(
    serverProxyLogPath(socketName, runtimeDir !== undefined ? { runtimeDir } : {}),
  );
  if (log !== null) {
    installStderrMirror(log);
    process.stderr.write(`serverProxy: starting pid=${process.pid} socket=${socketName} log=${log.path}\n`);
  }

  const serverProxyOpts: ServerProxyOptions = {
    socketName,
    ...(runtimeDir !== undefined ? { runtimeDir } : {}),
    ...(idleExitMs !== undefined ? { idleExitMs } : {}),
    ...(log !== null ? { logPath: log.path } : {}),
  };
  const serverProxy = createServerProxy(serverProxyOpts);

  // tc-3iv (§6.2): the server-proxy self-manages exit — immediately when tmux is
  // confirmed gone, after the idle hysteresis at zero IPC clients.  By the
  // time this callback fires, shutdown() has completed and the server-proxy socket
  // file is unlinked, so a clean exit(0) is all that's left.
  serverProxy.onSelfExit((reason) => {
    process.stderr.write(`server-proxy self-exit: ${reason}\n`);
    process.exit(0);
  });

  await serverProxy.start();

  // Signal readiness to the launcher
  process.stdout.write("READY\n");

  // Handle SIGTERM gracefully
  process.once("SIGTERM", () => {
    process.stderr.write("serverProxy: SIGTERM received, shutting down\n");
    void serverProxy.shutdown().finally(() => {
      process.exit(0);
    });
  });
}

// Run
main().catch((err: unknown) => {
  process.stderr.write(`server-proxy-entry fatal: ${String(err)}\n`);
  process.exit(1);
});
