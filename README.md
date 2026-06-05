# tmuxcc-broker

Per-socket session discovery and lifecycle service for tmuxcc. One broker
process is bound to exactly one tmux socket for its lifetime; it discovers
sessions, spawns and reaps per-session daemon child processes, and hands
clients daemon socket endpoints on demand.

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

## Lifecycle supervision

The broker does not manage its own auto-spawn or OS-level supervision. A
launcher binary or client-side autospawn is required for production use.
Per `SCHEMA.md` "Broker lifecycle": supervision is an implementation concern
of whatever launcher ships with each client.
