/**
 * server-proxy-log.test.ts — unit tests for the append-only server-proxy log (tc-k6v).
 *
 * L1. openServerProxyLog creates the file (0600) and append() writes
 *     timestamp-prefixed chunks.
 * L2. append() after close() is a silent no-op (best-effort contract).
 * L3. openServerProxyLog returns null when the path is unwritable.
 * L4. installStderrMirror tees stderr writes into the log and the uninstall
 *     function restores the original write.
 *
 * @module server-proxy-log.test
 */

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import { openServerProxyLog, installStderrMirror } from "./server-proxy-log.js";

function tempLogPath(label: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `tmuxcc-test-brokerlog-${label}-`));
  return path.join(dir, "server-proxy.log");
}

/** Matches an ISO-8601 timestamp prefix, e.g. "2026-06-10T12:34:56.789Z ". */
const TS_PREFIX = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z /;

describe("server-proxy-log (tc-k6v)", () => {
  it("L1: append() writes timestamp-prefixed chunks to the file", () => {
    const logPath = tempLogPath("l1");
    const log = openServerProxyLog(logPath);
    assert.ok(log, "openServerProxyLog must succeed on a writable dir");

    try {
      log.append("first line\n");
      log.append(Buffer.from("second line\n"));

      const content = fs.readFileSync(logPath, "utf8");
      const lines = content.split("\n").filter(Boolean);
      assert.equal(lines.length, 2);
      assert.match(lines[0]!, TS_PREFIX);
      assert.ok(lines[0]!.endsWith("first line"), `got: ${lines[0]}`);
      assert.match(lines[1]!, TS_PREFIX);
      assert.ok(lines[1]!.endsWith("second line"), `got: ${lines[1]}`);

      // Append-only mode 0600 (skip the mode assert on platforms without chmod).
      const mode = fs.statSync(logPath).mode & 0o777;
      assert.equal(mode, 0o600, `log file mode must be 0600, got ${mode.toString(8)}`);
    } finally {
      log.close();
    }
  });

  it("L2: append() after close() is a silent no-op", () => {
    const logPath = tempLogPath("l2");
    const log = openServerProxyLog(logPath);
    assert.ok(log);

    log.append("before close\n");
    log.close();
    log.close(); // idempotent
    assert.doesNotThrow(() => log.append("after close\n"));

    const content = fs.readFileSync(logPath, "utf8");
    assert.ok(content.includes("before close"));
    assert.ok(!content.includes("after close"), "post-close append must not land");
  });

  it("L3: openServerProxyLog returns null when the path is unwritable", () => {
    // A path whose parent does not exist cannot be opened.
    const log = openServerProxyLog("/nonexistent-dir-tmuxcc-test/server-proxy.log");
    assert.equal(log, null);
  });

  it("L4: installStderrMirror tees stderr writes; uninstall restores", () => {
    const logPath = tempLogPath("l4");
    const log = openServerProxyLog(logPath);
    assert.ok(log);

    const origWrite = process.stderr.write;
    const uninstall = installStderrMirror(log);
    try {
      // node:test captures stderr; the write still reaches the real stderr
      // path AND the log file.
      process.stderr.write("mirrored diagnostics line\n");
      assert.notEqual(process.stderr.write, origWrite, "mirror must replace stderr.write");
    } finally {
      uninstall();
      log.close();
    }

    assert.equal(process.stderr.write, origWrite, "uninstall must restore the original write");
    const content = fs.readFileSync(logPath, "utf8");
    const line = content.split("\n").find((l) => l.includes("mirrored diagnostics line"));
    assert.ok(line, `log must contain the mirrored line, got: ${content}`);
    assert.match(line!, TS_PREFIX);
  });
});
