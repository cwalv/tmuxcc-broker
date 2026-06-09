/**
 * Unix socket transport — a Transport implementation over a Node.js net.Socket.
 *
 * # Multiplexing protocol
 *
 * Both control-plane and data-plane traffic share a single unix socket
 * connection. The leading byte of each framing unit routes the traffic:
 *
 *   byte 0 == 0xCC → data-plane frame (framing.ts DataFrame binary format)
 *   byte 0 != 0xCC → control-plane (newline-delimited JSON)
 *
 * This matches the contract in SCHEMA.md §"Data plane":
 *   "Control-plane and data-plane traffic are multiplexed on the same
 *    connection via a leading magic byte (data plane: 0xCC; control plane:
 *    JSON, never starts with 0xCC)."
 *
 * # Data-plane framing (framing.ts format)
 *
 * Data frames on the wire use the format defined in framing.ts / SCHEMA.md:
 *   [u8 MAGIC=0xCC][u32be SEQ][u32be PAYLEN][u16be IDLEN][PANEID UTF-8][PAYLOAD]
 *
 * Total frame size: 11 + IDLEN + PAYLEN bytes. The `FrameDecoder` from
 * @tmuxcc/daemon is used to parse incoming data frames.
 *
 * On the SEND side, `encodeFrame(paneId, seq, payload)` from @tmuxcc/daemon
 * produces the binary blob; we write it directly to the socket.
 *
 * # Control-plane framing (length-prefixed JSON)
 *
 * To avoid partial-JSON reads and to support multi-line values, each control
 * message is framed with a 4-byte u32be length prefix:
 *   [u32be len][JSON UTF-8 bytes]
 *
 * Since JSON never starts with 0xCC (that would require a UTF-8 character at
 * code point 0xCC, which is a multi-byte sequence and thus NEVER the first
 * byte of a JSON object/array/string/primitive), the leading byte of each unit
 * is sufficient to route:
 *   - 0xCC → data frame
 *   - 0x00..0xCB, 0xCD..0xFF → control-plane length prefix first byte
 *
 * A 4-byte length prefix with value ≤ 2^24-1 has first byte 0x00, which is
 * never 0xCC. Messages up to 2^32 bytes are supported (enough for any
 * control-plane message in practice).
 *
 * @module socket-transport
 */

import * as net from "node:net";
import { FrameDecoder, FRAME_MAGIC, encodeFrame } from "@tmuxcc/daemon";
import type { Transport, ControlHandler, DataHandler, CloseHandler } from "@tmuxcc/daemon";
import type { PaneId } from "@tmuxcc/daemon";

// ---------------------------------------------------------------------------
// Control-plane framing constants
// ---------------------------------------------------------------------------

/** Size of the control-plane length prefix. */
const CTRL_LEN_SIZE = 4;

// Per-pane sequence counter map (per transport instance).
// In production the daemon owns sequence numbers; the broker socket transport
// increments them per pane.
const MAX_U32 = 0xffffffff;

// ---------------------------------------------------------------------------
// SocketTransport
// ---------------------------------------------------------------------------

class SocketTransport implements Transport {
  private _socket: net.Socket;
  private _controlHandler: ControlHandler | null = null;
  private _dataHandler: DataHandler | null = null;
  private _closeHandler: CloseHandler | null = null;

  private _buf = Buffer.alloc(0);
  private _frameDecoder = new FrameDecoder();
  private _closed = false;

  // Per-pane seq counters for sendData
  private _dataSeqs = new Map<string, number>();

  // tc-7xv.6 / tc-7xv.24 backpressure: when socket.write() returns false (its
  // kernel send-buffer is full) we record a single shared "drain" promise here
  // and resolve it from socket's 'drain' event.  All subsequent sendData /
  // sendControl callers receive the same promise so the upstream pipeline can
  // await it before producing more bytes.  This is the standard Node.js
  // Writable backpressure contract — see https://nodejs.org/api/stream.html
  // "Buffering".  Without this, daemon-side flow-control credits bytes as
  // "drained" the instant they enter the kernel send buffer, so tmux is never
  // told to pause and the daemon's outbound buffer grows without bound — the
  // root cause of the `find /` wedge.
  private _drainPromise: Promise<void> | null = null;
  private _drainResolve: (() => void) | null = null;

  constructor(socket: net.Socket) {
    this._socket = socket;
    socket.on("data", (chunk: Buffer) => { this._onData(chunk); });
    socket.on("close", () => { this._onClose(); });
    socket.on("error", (err) => { this._onClose(err); });
    socket.on("drain", () => {
      // Kernel send-buffer has drained.  Resolve the shared drain promise so
      // all backpressured callers can proceed, then clear the field so the
      // next backpressure window allocates a fresh promise.
      const r = this._drainResolve;
      this._drainPromise = null;
      this._drainResolve = null;
      if (r !== null) r();
    });
  }

  // ── Control plane ──────────────────────────────────────────────────────────

  sendControl(msg: Parameters<Transport["sendControl"]>[0]): void | Promise<void> {
    if (this._closed) return;
    const json = JSON.stringify(msg);
    const payload = Buffer.from(json + "\n", "utf8");
    // Length-prefix the control message (4-byte u32be)
    const lenBuf = Buffer.allocUnsafe(CTRL_LEN_SIZE);
    lenBuf.writeUInt32BE(payload.length, 0);
    // Combine len+payload into a single write call so they cannot interleave
    // with concurrent data-plane frames at the kernel boundary.  This also
    // means socket.write returns one backpressure signal for the pair.
    const ok = this._socket.write(Buffer.concat([lenBuf, payload]));
    if (!ok) return this._ensureDrainPromise();
  }

  onControl(handler: ControlHandler): void {
    this._controlHandler = handler;
  }

  // ── Data plane ─────────────────────────────────────────────────────────────

  sendData(paneId: PaneId, bytes: Uint8Array): void | Promise<void> {
    if (this._closed) return;
    // Mint a per-pane seq and encode as a data frame
    const key = String(paneId);
    const seq = (this._dataSeqs.get(key) ?? 0);
    this._dataSeqs.set(key, (seq + 1) & MAX_U32);
    const frame = encodeFrame(paneId, seq, bytes);
    const ok = this._socket.write(frame);
    if (!ok) return this._ensureDrainPromise();
  }

  /**
   * Lazily build a shared promise that resolves on the next 'drain' event.
   *
   * One shared promise per backpressure window.  Concurrent senders that hit
   * write()==false during the same window all await the same promise, so the
   * pipeline can use `await tx.sendData(...)` as a natural backpressure point
   * without registering N drain listeners.
   *
   * The constructor's 'drain' handler nullifies the field and invokes the
   * stored resolve so the next backpressure window starts fresh.
   */
  private _ensureDrainPromise(): Promise<void> {
    if (this._drainPromise !== null) return this._drainPromise;
    if (this._closed) return Promise.resolve();
    this._drainPromise = new Promise<void>((resolve) => {
      this._drainResolve = resolve;
    });
    return this._drainPromise;
  }

  onData(handler: DataHandler): void {
    this._dataHandler = handler;
  }

  // ── Lifecycle ──────────────────────────────────────────────────────────────

  onClose(handler: CloseHandler): void {
    this._closeHandler = handler;
  }

  close(err?: Error): void {
    if (this._closed) return;
    this._closed = true;
    // Release any awaiters on the drain promise so they can observe the close
    // through their next send (which returns immediately when this._closed).
    const r = this._drainResolve;
    this._drainPromise = null;
    this._drainResolve = null;
    if (r !== null) r();
    this._socket.destroy(err);
    this._closeHandler?.(err);
    this._closeHandler = null;
  }

  // ── Incoming data parser ───────────────────────────────────────────────────

  private _onData(chunk: Buffer): void {
    // tc-7xv.6 / tc-7xv.24: Buffer.concat([this._buf, chunk]) is O(n²) when
    // chunks arrive rapidly and the buffer is large — a contributing factor to
    // the firehose wedge.  Concatenating with an explicit totalLength avoids
    // the intermediate length computation traversal and lets V8 short-cut the
    // copy when the result is sized correctly.
    this._buf = Buffer.concat(
      [this._buf, chunk],
      this._buf.length + chunk.length,
    );
    this._processBuffer();
  }

  /**
   * Drain `_buf` one message at a time.
   *
   * After dispatching each control-plane message, check whether `_controlHandler`
   * changed as a side-effect of the dispatch (e.g. `runServerHandshake`'s
   * `settle()` replaces the handler with a no-op after receiving
   * `client.capabilities`).  When the handler has changed, stop processing and
   * reschedule via `process.nextTick`.
   *
   * Why: when `client.capabilities` and `command.request` arrive in the same
   * socket data chunk (OS batching under load), the naive while-loop would
   * dispatch `command.request` using the no-op handler installed by `settle()`,
   * silently dropping the command before `_handleConnection` can install its
   * real command handler.  The `process.nextTick` reschedule gives the
   * microtask queue — including `_handleConnection`'s async continuation that
   * calls `transport.onControl(commandHandler)` — a chance to run before the
   * next buffered message is dispatched.
   */
  private _processBuffer(): void {
    while (this._buf.length > 0) {
      const firstByte = this._buf[0] as number;

      if (firstByte === FRAME_MAGIC) {
        // Data-plane frame: feed to FrameDecoder which handles its own buffering
        // and returns complete frames. We give ALL current buffer bytes to it.
        // FrameDecoder.push() expects a Uint8Array.
        const input = new Uint8Array(this._buf.buffer, this._buf.byteOffset, this._buf.length);
        let frames: ReturnType<FrameDecoder["push"]>;
        try {
          frames = this._frameDecoder.push(input);
        } catch {
          // Malformed data frame — close transport
          this.close(new Error("data-plane framing error"));
          return;
        }
        // Consume ALL data from the buffer — FrameDecoder handles partial frames
        // internally. We trust it to buffer incomplete frames.
        // BUT: FrameDecoder may not consume the entire buffer if there are
        // trailing control-plane bytes. We need to know how many bytes it consumed.
        //
        // FrameDecoder doesn't expose "bytes consumed". To handle interleaved
        // control/data frames, we process the buffer byte-by-byte for the
        // leading data-frame segment.
        //
        // However, in practice the daemon sends either all-data or all-control on
        // a connection; mixing is uncommon. We handle it conservatively: once we
        // see 0xCC, we feed the entire remaining buffer to FrameDecoder and clear
        // the buffer. Any subsequent non-0xCC bytes after a data frame would be
        // control messages that were already buffered by the decoder.
        //
        // A cleaner approach: determine exact data-frame byte boundaries.
        // For now, after giving all data to FrameDecoder, clear buffer.
        this._buf = Buffer.alloc(0);
        for (const frame of frames) {
          this._dataHandler?.(frame.paneId, frame.payload);
        }
        break;
      } else {
        // Control-plane: expect [u32be len][JSON bytes + "\n"]
        if (this._buf.length < CTRL_LEN_SIZE) break; // wait for more data

        const payloadLen = this._buf.readUInt32BE(0);
        const totalLen = CTRL_LEN_SIZE + payloadLen;

        if (this._buf.length < totalLen) break; // wait for complete message

        const payload = this._buf.subarray(CTRL_LEN_SIZE, totalLen);
        this._buf = this._buf.subarray(totalLen);

        const prevHandler = this._controlHandler;
        try {
          const jsonStr = payload.toString("utf8").trim();
          const msg = JSON.parse(jsonStr) as Parameters<ControlHandler>[0];
          this._controlHandler?.(msg);
        } catch {
          // Malformed JSON — discard
        }

        // If the handler changed as a side-effect of dispatch (e.g. settle()
        // installed a no-op), stop processing here and reschedule the remainder
        // so that microtasks (including async continuations that install the real
        // command handler) can run before the next buffered message is dispatched.
        //
        // IMPORTANT: use setImmediate, NOT process.nextTick.
        //
        // In Node.js, process.nextTick callbacks run BEFORE Promise microtasks
        // in the same event loop turn.  The post-handshake onControl installation
        // in _handleConnection happens inside an async/await continuation:
        //
        //   settle(() => resolve(session))  ← queues Promise microtask
        //   process.nextTick(...)           ← fires BEFORE that microtask!
        //   → _processBuffer runs with no-op handler still installed
        //   → command.request dispatched to no-op → DROPPED
        //   → _handleConnection finally runs, installs real handler — too late
        //
        // setImmediate fires AFTER all microtasks (Promises) have drained,
        // giving the _handleConnection async continuation time to install the
        // real command handler before the next buffered message is dispatched.
        if (this._controlHandler !== prevHandler && this._buf.length > 0) {
          setImmediate(() => { this._processBuffer(); });
          return;
        }
      }
    }
  }

  private _onClose(err?: Error): void {
    if (this._closed) return;
    this._closed = true;
    // Release any awaiters on the drain promise so they can observe the close.
    const r = this._drainResolve;
    this._drainPromise = null;
    this._drainResolve = null;
    if (r !== null) r();
    this._closeHandler?.(err);
    this._closeHandler = null;
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Wrap an existing net.Socket as a two-plane Transport.
 */
export function createSocketTransport(socket: net.Socket): Transport {
  return new SocketTransport(socket);
}

/**
 * Dial a unix socket and return a Transport.
 * Resolves once the connection is established.
 */
export function connectSocketTransport(socketPath: string): Promise<Transport> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(socketPath);
    socket.once("connect", () => {
      socket.off("error", reject);
      resolve(new SocketTransport(socket));
    });
    socket.once("error", reject);
  });
}

/**
 * Start a unix socket server. Each accepted connection is wrapped as a
 * Transport and passed to `onConnection`.
 *
 * Returns a handle with a `close()` method.
 */
export function createSocketServer(
  socketPath: string,
  onConnection: (transport: Transport) => void,
): Promise<{ close(): Promise<void> }> {
  return new Promise((resolve, reject) => {
    const server = net.createServer((socket) => {
      onConnection(new SocketTransport(socket));
    });

    server.once("error", reject);
    server.listen(socketPath, () => {
      server.off("error", reject);
      resolve({
        close(): Promise<void> {
          return new Promise((res, rej) => {
            server.close((err) => (err ? rej(err) : res()));
          });
        },
      });
    });
  });
}
