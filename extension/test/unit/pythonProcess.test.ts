// Tests for the real process runner of the Tool setup (src/pythonProcess.ts). They spawn the
// Node runtime that runs the tests, so they need no Python and no network.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { nodeToolFiles, runProcess, venvToolCommand } from "../../src/pythonProcess.ts";

test("runProcess gives the exit code, stdout, and stderr of a program", async () => {
  const script = "process.stdout.write('out'); process.stderr.write('err'); process.exit(3)";
  const result = await runProcess(process.execPath, ["-e", script]);
  assert.deepEqual(result, { exitCode: 3, stdout: "out", stderr: "err", error: undefined });
});

test("runProcess gives an error for a program that does not exist", async () => {
  const result = await runProcess(join(tmpdir(), "no-such-python"), ["-c", "pass"]);
  assert.match(result.error ?? "", /ENOENT/);
});

test("runProcess gives an error when the timeout stops the program", async () => {
  const result = await runProcess(process.execPath, ["-e", "setTimeout(() => {}, 10000)"], { timeoutMs: 200 });
  assert.match(result.error ?? "", /stopped by SIG/);
});

test("nodeToolFiles reads and writes text files, and a missing file reads as undefined", async () => {
  const file = join(mkdtempSync(join(tmpdir(), "mttx-")), "marker.txt");
  assert.equal(await nodeToolFiles.exists(file), false);
  assert.equal(await nodeToolFiles.readText(file), undefined);
  await nodeToolFiles.writeText(file, "dependency-update-metrics>=0.2,<0.3");
  assert.equal(await nodeToolFiles.exists(file), true);
  assert.equal(await nodeToolFiles.readText(file), "dependency-update-metrics>=0.2,<0.3");
});

test("the venv Python runs the cli module of the Tool", () => {
  assert.deepEqual(venvToolCommand("/storage/venv/bin/python"), {
    file: "/storage/venv/bin/python",
    args: ["-m", "dependency_metrics.cli"],
  });
});
