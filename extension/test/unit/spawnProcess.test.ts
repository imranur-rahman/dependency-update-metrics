// Tests for the one helper that spawns the child processes of the Extension
// (src/spawnProcess.ts). They spawn the Node runtime of the test.
import { test } from "node:test";
import assert from "node:assert/strict";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { spawnProcess } from "../../src/spawnProcess.ts";

const node = process.execPath;

test("a process gets the environment, the extra variables, and PYTHONIOENCODING=utf-8", async () => {
  const script = "process.stdout.write(JSON.stringify([process.env.PATH !== undefined, process.env.MTTX_X, process.env.PYTHONIOENCODING]))";
  const result = await spawnProcess(node, ["-e", script], { env: { MTTX_X: "1" } });
  assert.equal(result.exitCode, 0);
  assert.deepEqual(JSON.parse(result.stdout), [true, "1", "utf-8"]);
});

test("the helper gives stdout, stderr, and the exit code, and stderr also line by line", async () => {
  const script = "process.stdout.write('out'); process.stderr.write('a\\rb\\nc'); process.exit(3)";
  const lines: string[] = [];
  const result = await spawnProcess(node, ["-e", script], { onStderrLine: (line) => lines.push(line) });
  assert.deepEqual(
    { exitCode: result.exitCode, stdout: result.stdout, stderr: result.stderr, timedOut: result.timedOut },
    { exitCode: 3, stdout: "out", stderr: "a\rb\nc", timedOut: false },
  );
  assert.deepEqual(lines, ["a", "b", "c"]);
});

test("the timeout stops the process and says so", async () => {
  const started = Date.now();
  const result = await spawnProcess(node, ["-e", "setTimeout(() => {}, 10000)"], { timeoutMs: 200 });
  assert.ok(Date.now() - started < 5000);
  assert.equal(result.timedOut, true);
  assert.equal(result.error, undefined);
  assert.ok(result.signal);
});

test("an abort stops the process with an error", async () => {
  const abort = new AbortController();
  const pending = spawnProcess(node, ["-e", "setTimeout(() => {}, 10000)"], { signal: abort.signal });
  setTimeout(() => abort.abort(), 100);
  const result = await pending;
  assert.match(result.error ?? "", /abort/i);
});

test("a program that does not exist gives an error and no exit code", async () => {
  const result = await spawnProcess(join(tmpdir(), "no-such-program"), []);
  assert.match(result.error ?? "", /ENOENT/);
  assert.equal(result.exitCode, null);
});
