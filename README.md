# tmuxcc-broker

Per-socket session discovery and lifecycle service for tmuxcc. One broker
process is bound to exactly one tmux socket for its lifetime; it discovers
sessions, spawns and reaps per-session daemon child processes, and hands
clients daemon socket endpoints on demand.

In production, one broker runs per user per machine on the default socket
name `tmuxcc`, mirroring tmux's own `tmux -L tmuxcc` server identity.  See
`projects/tmuxcc/docs/ext-a-design-context.md` Part 6 for the full
component-lifetime model — the broker is a translation layer / proxy for
tmux's server-scoped control channel.  The intended new name is
**`server-proxy`**; `broker` is retained in code pending rename.

**No pane data, no tmux control-mode vocabulary, no renderer types.** The
broker speaks sessions, endpoints, and lifecycle signals — never `%output`,
never layout strings.

Depends on `tmuxcc-daemon` for shared wire types (`SessionId`, `Capabilities`,
handshake helpers, etc.). Wire protocol reference: `tmuxcc-daemon/SCHEMA.md`
— "Broker wire" section.

Part of the `tmuxcc` repoweave project.

## Architecture position

```
[ tmux server (one socket) ]
       ↕ tmux -CC (thin watcher for %sessions-changed)
[ this broker ]   ← one per tmux socket
       ↓ spawns (on session.claim)
[ daemon S1 ]     ← daemon wire: per-session, one -CC attach per daemon
[ daemon S2 ]
       ↑
[ clients ]       ← clients talk broker first, then daemon
```

The broker never holds a fat `-CC attach` connection. It uses a thin
`tmux -CC` watcher only for `%sessions-changed` push notifications and
shells out (`tmux list-sessions`, `tmux new-session`, `tmux kill-session`)
for state mutations.

## Public API

```ts
import { createBroker } from "@tmuxcc/broker";
import type { BrokerHandle, BrokerOptions } from "@tmuxcc/broker";
```

### `createBroker(opts: BrokerOptions): BrokerHandle`

Create a broker for the given tmux socket. The returned handle is NOT
started yet — call `broker.start()` to begin accepting connections.

```ts
const broker = createBroker({ socketName: "tmuxcc" });
await broker.start();
console.log("broker at", broker.endpoint());
// ... serve clients ...
await broker.shutdown();
```

#### `BrokerOptions`

| Field        | Type     | Description                                                                                          |
|--------------|----------|------------------------------------------------------------------------------------------------------|
| `socketName` | `string` | tmux socket name passed as `-L <socketName>`. Required — no default to avoid accidental attachment. |
| `runtimeDir?`| `string` | Override the base runtime directory for broker + daemon sockets. Default: `$XDG_RUNTIME_DIR/tmuxcc` or `/tmp/tmuxcc-<uid>`. |

#### `BrokerHandle`

| Method     | Signature             | Description                                                                              |
|------------|-----------------------|------------------------------------------------------------------------------------------|
| `start`    | `() => Promise<void>` | Create the unix socket, begin accepting connections, start the tmux watcher.             |
| `shutdown` | `() => Promise<void>` | Stop accepting, disconnect all clients, reap all daemons, remove the broker socket file. |
| `endpoint` | `() => string`        | The broker's unix socket path. Valid only after `start()` resolves.                      |

### Socket-path utilities (re-exported for test harnesses)

```ts
import {
  brokerSocketPath,
  daemonSocketPath,
  createSocketTransport,
  connectSocketTransport,
  createSocketServer,
} from "@tmuxcc/broker";
```

`brokerSocketPath(brokerId, opts?)` and `daemonSocketPath(brokerId, sessionId, opts?)`
are path-computation helpers. These are useful in test harnesses that need to
construct paths independently of a running broker instance. In normal client
code, the broker returns the endpoint in the `session.claim` response and
clients treat it as an opaque string.

## Socket conventions

Broker sockets live at `<runtimeDir>/<brokerId>/broker.sock`. Daemon sockets
live at `<runtimeDir>/<brokerId>/<sessionId>.sock`. Both the directory and
the socket file are created at mode 0700/0600 respectively.

Under the v3 trust model (see `tmuxcc-daemon/SCHEMA.md` — "Trust and security
model"), any local process the kernel grants socket access to is trusted.
There is no cryptographic authentication.

## Embedding in a test harness

The integration test runner in `tmuxcc-vscode/test/integration/runTest.ts`
shows the canonical broker-per-test pattern:

```ts
import { createBroker } from "@tmuxcc/broker";

const brokerSocketName = `tmuxcc-test-${process.pid}`;
const broker = createBroker({ socketName: brokerSocketName });
await broker.start();

// Inject brokerSocketName into the EDH via user-data-dir settings so
// the extension connects to this isolated broker rather than "tmuxcc".

try {
  await runTests({ /* ... */ });
} finally {
  await broker.shutdown();  // kills all daemon children, removes broker socket
}
```

Two parallel test runners never collide: each mints a broker with a unique
`tmuxcc-test-<pid>` socket name. `broker.shutdown()` is the single cleanup
call — no manual `tmux kill-server` needed.

## Lifecycle

The broker is self-supervising in production.  A launcher (the VS Code
extension's `broker-launcher.ts`) spawns it with `detached: true` on first
use; thereafter the broker manages its own exit.

### Spawn (production)

- Lazy: client launchers (e.g. `tmuxcc-vscode`) call probe-then-spawn on
  first need.  Probe = 500 ms `connect(2)` to the broker socket.
- Spawned `detached: true` with parent-side stdio destroyed after `READY\n`
  so the broker outlives its launcher process without EPIPE.

### Exit (production)

The broker self-exits on EITHER of:

- Its thin `tmux -CC` watcher EOFs — the tmux server has gone away (either
  because tmux's `exit-empty on` fired after the last session closed, or
  because the user ran `tmux kill-server`).  Immediate.
- **No IPC-connected clients for 5 minutes.**  Hysteresis covers reload-
  window, brief accidental close + reopen, and similar small interruptions
  without forcing a cold respawn.  "Client" means any open Unix-domain
  socket connection — independent of whether the client has claimed a
  session or bound a terminal.

There is no idle-TTL config knob and no auto-restart layer.  The 5
minutes is sized for the worst expected human-scale gap between close and
reopen; making it configurable would let users foot-gun themselves into
"my sessions disappeared because the broker exited" debugging.  Broker
crashes are bugs to be surfaced and fixed, not UX surfaces to smooth
over; daemon children die with the broker (see below), and the next
client launcher spawns a fresh broker against the surviving tmux state.

### Exit (tests)

Tests call `broker.shutdown()` explicitly (see "Embedding in a test
harness" above).  Shutdown is synchronous-with-respect-to-children: it
reaps daemons, removes the socket, and resolves once cleanup is done.
Test runners should always own a `shutdown()` in a `finally` block to
guarantee no leak even on assertion failure or crash.

### Daemon parent semantics

Per-session daemons are spawned as regular (non-detached) child processes
of the broker and **explicitly enforce** die-with-parent — process-group
mechanics alone do not deliver it (a SIGKILLed parent's children are
reparented to init, not signalled).  On Linux the daemon installs
`prctl(PR_SET_PDEATHSIG, SIGTERM)` at startup; on macOS it polls
`getppid()` every 1 s and exits when reparented to launchd (ppid 1).

There is no orphan-and-reclaim path — recovery is "broker re-spawn +
fresh `-CC attach` to surviving tmux sessions," not "find my orphaned
daemons and adopt them."  This is correct because tmux is the only
persistence layer; daemons hold no state worth preserving across broker
death.

If a daemon dies while the broker survives (parser fault, OOM), the
broker reaps the registry entry and re-spawns on the next `session.claim`
for that session.
