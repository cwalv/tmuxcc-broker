/**
 * SessionProxy entry point — spawned as a child process by the server-proxy supervisor.
 *
 * Arguments:
 *   --socket-name  <name>   tmux socket name (passed as -L <name> to tmux)
 *   --session-name <name>   tmux session name to attach to
 *   --socket-path  <path>   unix socket path the session-proxy should listen on
 *
 * Protocol:
 *   1. Install the die-with-parent watchdog (tc-2c5, see below).
 *   2. Parse arguments.
 *   3. Create a unix socket server on `--socket-path`.
 *   4. Create and start a sessionProxy (createSessionProxy from @tmuxcc/session-proxy).
 *   5. Write "READY\n" to stdout so the supervisor knows we are listening.
 *   6. Accept client connections in a loop, calling sessionProxy.addClient(transport).
 *   7. On SIGTERM: call sessionProxy.stop() and exit cleanly.
 *
 * Die-with-parent (tc-2c5, ext-a design §6.3): this process MUST die with the
 * server-proxy that spawned it.  A SIGKILLed server-proxy delivers no signal to its
 * children — they are silently reparented — so the session-proxy polls getppid()
 * (1 s cadence) and self-SIGTERMs on reparenting, which lands in the graceful
 * SIGTERM path below (detach the -CC client, close + remove the unix socket).
 * There is NO orphan-and-reclaim: recovery from server-proxy death is launcher →
 * fresh server-proxy → fresh daemons on next session.claim → fresh `-CC attach` to
 * the surviving tmux sessions (tmux is the only persistence layer).
 *
 * This is the multiplexed-socket endpoint described in SCHEMA.md §"Data plane":
 * the same socket carries both control-plane (JSON) and data-plane (0xCC) traffic.
 *
 * @module session-proxy-entry
 */

import * as net from "node:net";
import * as fs from "node:fs";
import * as path from "node:path";
import { createSessionProxy, installDieWithParent } from "@tmuxcc/session-proxy";
import { createSocketTransport } from "./socket-transport.js";
import { removeSocket, restrictSocket } from "./runtime-dir.js";

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

function parseArgs(): { socketName: string; sessionName: string; socketPath: string } {
  const args = process.argv.slice(2);
  let socketName = "";
  let sessionName = "";
  let socketPath = "";

  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case "--socket-name":
        socketName = args[++i] ?? "";
        break;
      case "--session-name":
        sessionName = args[++i] ?? "";
        break;
      case "--socket-path":
        socketPath = args[++i] ?? "";
        break;
    }
  }

  if (!socketName || !sessionName || !socketPath) {
    process.stderr.write(
      "Usage: session-proxy-entry --socket-name <name> --session-name <name> --socket-path <path>\n",
    );
    process.exit(1);
  }

  return { socketName, sessionName, socketPath };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  // tc-2c5: enforce die-with-parent BEFORE any other work so even a session-proxy
  // that wedges during startup cannot outlive its server-proxy.  Default behavior:
  // on reparenting, self-SIGTERM (taking the graceful path installed below)
  // with a hard-exit backstop.  Worst-case exit latency after server-proxy death:
  // 1 s poll + 1.5 s grace — inside the 3 s budget asserted by the e2e test.
  installDieWithParent();

  const { socketName, sessionName, socketPath } = parseArgs();

  // Remove stale socket file if present
  removeSocket(socketPath);

  // Create the sessionProxy (not yet started)
  const sessionProxy = createSessionProxy({
    host: {
      socketName,
      sessionName,
      attach: true, // server-proxy creates session; session-proxy attaches
    },
  });

  // Create the unix socket server
  const server = net.createServer((socket) => {
    const transport = createSocketTransport(socket);
    void sessionProxy.addClient(transport);
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(socketPath, () => {
      server.off("error", reject);
      resolve();
    });
  });

  // Restrict socket permissions to 0600
  restrictSocket(socketPath);

  // Start the sessionProxy (spawns tmux -CC attach)
  try {
    await sessionProxy.start();
  } catch (err: unknown) {
    process.stderr.write(`SessionProxy start failed: ${String(err)}\n`);
    server.close();
    removeSocket(socketPath);
    process.exit(1);
  }

  // Signal readiness to the server-proxy supervisor
  process.stdout.write("READY\n");

  // Handle SIGTERM gracefully
  process.once("SIGTERM", () => {
    sessionProxy.stop().finally(() => {
      server.close(() => {
        removeSocket(socketPath);
        process.exit(0);
      });
    });
  });

  // Handle session-proxy host exit (tmux crashed or session ended)
  // The sessionProxy.start() installs an onExit handler that broadcasts session.unavailable.
  // We watch for it here to clean up the server.
  // Note: sessionProxy.host is exposed on the SessionProxy interface.
  sessionProxy.host.onExit(() => {
    // Give connected clients a moment to receive the session.unavailable error
    setTimeout(() => {
      server.close(() => {
        removeSocket(socketPath);
        process.exit(0);
      });
    }, 500);
  });
}

// Run
main().catch((err: unknown) => {
  process.stderr.write(`session-proxy-entry fatal: ${String(err)}\n`);
  process.exit(1);
});
