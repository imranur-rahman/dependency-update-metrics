// Tests for the runner that spawns the Tool process (src/processToolRunner.ts). They spawn
// the fake Tool in test/fixtures/fake-tool.mjs, so they need no network and no real Tool.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { ProcessToolRunner, pruneToolCache, toolCommand } from "../../src/processToolRunner.ts";
import type { ToolRequest } from "../../src/core/tool.ts";

const FAKE_TOOL = join(import.meta.dirname, "..", "fixtures", "fake-tool.mjs");

function request(packageName: string, timeoutSeconds = 60): ToolRequest {
  return {
    ecosystem: "npm",
    packageName,
    dataSource: "depsdev",
    startDate: "2024-10-22",
    endDate: "2026-10-22",
    weightingType: "disable",
    halfLife: null,
    outputDir: tmpdir(),
    timeoutSeconds,
  };
}

test("the runner stops a Tool run that takes longer than the package timeout", async () => {
  const runner = new ProcessToolRunner(toolCommand(FAKE_TOOL), { MTTX_FAKE_TOOL_DELAY_MS: "10000" });
  const started = Date.now();
  const outcome = await runner.run(request("express", 0.5));
  assert.ok(Date.now() - started < 5000);
  assert.deepEqual(outcome, {
    kind: "failed",
    message: "the Tool stopped after 0.5 s (mttx.packageTimeoutSeconds)",
  });
});

test("the runner gives the result that the Tool prints", async () => {
  const log = join(mkdtempSync(join(tmpdir(), "mttx-")), "args.log");
  const runner = new ProcessToolRunner(toolCommand(FAKE_TOOL), { MTTX_FAKE_TOOL_LOG: log });
  const outcome = await runner.run(request("express"));
  assert.equal(outcome.kind, "result");
  assert.equal(outcome.kind === "result" && outcome.result.summary?.mttu, 579);
  const args = JSON.parse(readFileSync(log, "utf8"));
  assert.deepEqual(args.slice(0, 8), [
    "--ecosystem", "npm", "--package", "express", "--json", "--depsdev", "--start-date", "2024-10-22",
  ]);
});

test("the runner gives the result of a failed package, which the Tool prints with exit code 1", async () => {
  const outcome = await new ProcessToolRunner(toolCommand(FAKE_TOOL)).run(request("nope"));
  assert.equal(outcome.kind === "result" && outcome.result.error, "Package not found in deps.dev");
});

test("the runner fails with stderr when the Tool prints no JSON", async () => {
  const outcome = await new ProcessToolRunner(toolCommand(FAKE_TOOL)).run(request("bad-args"));
  assert.deepEqual(outcome, {
    kind: "failed",
    message: "dependency-metrics: error: unrecognized arguments: --json",
  });
});

test("the runner fails with a message when the Tool does not exist", async () => {
  const missing = join(tmpdir(), "no-such-dependency-metrics");
  const outcome = await new ProcessToolRunner(toolCommand(missing)).run(request("express"));
  assert.equal(outcome.kind, "failed");
  assert.match(outcome.kind === "failed" ? outcome.message : "", /could not start the Tool .*no-such-dependency-metrics/);
});

test("the cache prune runs the Tool with only the prune arguments", async () => {
  const log = join(mkdtempSync(join(tmpdir(), "mttx-")), "args.log");
  const pruneArgs = ["--output-dir", "/data/mttx", "--cache-prune-gb", "5"];
  const exitCode = await pruneToolCache(toolCommand(FAKE_TOOL), pruneArgs, { MTTX_FAKE_TOOL_LOG: log });
  assert.equal(exitCode, 0);
  assert.deepEqual(JSON.parse(readFileSync(log, "utf8")), pruneArgs);
});

test("the cache prune of a Tool that does not exist gives no exit code", async () => {
  const missing = join(tmpdir(), "no-such-dependency-metrics");
  assert.equal(await pruneToolCache(toolCommand(missing), ["--cache-prune-gb", "5"]), null);
});

test("an empty Tool path runs dependency-metrics from the PATH", () => {
  assert.deepEqual(toolCommand(""), { file: "dependency-metrics", args: [] });
  assert.deepEqual(toolCommand("  /opt/bin/dependency-metrics "), { file: "/opt/bin/dependency-metrics", args: [] });
});
