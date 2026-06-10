/**
 * Broker entry point — spawned as a child process by the VS Code extension
 * (or any other launcher) to start a broker for a given tmux socket name.
 *
 * Arguments:
 *   --socket-name <name>    tmux socket name (= broker socket directory name)
 *
 * Optional arguments:
 *   --runtime-dir <path>    override the base runtime directory
 *   --idle-exit-ms <n>      zero-client self-exit hysteresis (default 5 min;
 *                           injectable so tests don't wait 5 real minutes)
 *
 * Protocol:
 *   1. Parse arguments.
 *   2. Create and start a broker (createBroker from ./broker.js).
 *   3. Write "READY\n" to stdout so the launcher knows we are listening.
 *   4. On SIGTERM: call broker.shutdown() and exit cleanly.
 *   5. On broker self-exit (tc-3iv, §6.2 — tmux gone, or idle past the
 *      hysteresis window): exit 0.  The broker has already unlinked its
 *      socket file by the time the self-exit callback fires.
 *
 * This mirrors the daemon-entry.ts pattern: a thin entry script whose
 * only job is argument parsing → start → READY signal → exit handling.
 *
 * @module broker-entry
 */

import { createBroker } from "./broker.js";
import type { BrokerOptions } from "./broker.js";

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
      "Usage: broker-entry --socket-name <name> [--runtime-dir <path>] [--idle-exit-ms <n>]\n",
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

  const brokerOpts: BrokerOptions = {
    socketName,
    ...(runtimeDir !== undefined ? { runtimeDir } : {}),
    ...(idleExitMs !== undefined ? { idleExitMs } : {}),
  };
  const broker = createBroker(brokerOpts);

  // tc-3iv (§6.2): the broker self-manages exit — immediately when tmux is
  // confirmed gone, after the idle hysteresis at zero IPC clients.  By the
  // time this callback fires, shutdown() has completed and the broker socket
  // file is unlinked, so a clean exit(0) is all that's left.
  broker.onSelfExit((reason) => {
    process.stderr.write(`broker self-exit: ${reason}\n`);
    process.exit(0);
  });

  await broker.start();

  // Signal readiness to the launcher
  process.stdout.write("READY\n");

  // Handle SIGTERM gracefully
  process.once("SIGTERM", () => {
    void broker.shutdown().finally(() => {
      process.exit(0);
    });
  });
}

// Run
main().catch((err: unknown) => {
  process.stderr.write(`broker-entry fatal: ${String(err)}\n`);
  process.exit(1);
});
