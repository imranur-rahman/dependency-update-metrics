// Tests for the functions that read and build the OSV index with the Tool process
// (src/osvIndexProcess.ts). They spawn the fake Tool in test/fixtures/fake-tool.mjs.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { buildOsvIndex, readOsvIndex } from "../../src/osvIndexProcess.ts";
import { toolCommand } from "../../src/processToolRunner.ts";

const FAKE_TOOL = join(import.meta.dirname, "..", "fixtures", "fake-tool.mjs");

function outputDir(): string {
  return mkdtempSync(join(tmpdir(), "mttx-"));
}

test("a build gives progress lines, and then the Tool gives the index date", async () => {
  const dir = outputDir();
  const command = toolCommand(FAKE_TOOL);
  assert.deepEqual(await readOsvIndex(command, dir), { kind: "missing" });

  const progress: string[] = [];
  const outcome = await buildOsvIndex(command, dir, (line) => progress.push(line));

  assert.deepEqual(outcome, { ok: true });
  assert.equal(progress[0], "Downloading OSV data from https://example.test/npm/all.zip");
  assert.equal(progress.at(-1), "100%|██████████| 2.0M/2.0M");
  assert.deepEqual(await readOsvIndex(command, dir), { kind: "built", date: "2026-10-21" });
});

test("a failed build gives the last line of the Tool output", async () => {
  const command = { ...toolCommand(FAKE_TOOL), env: { ELECTRON_RUN_AS_NODE: "1", MTTX_FAKE_OSV_BUILD_FAIL: "1" } };
  const outcome = await buildOsvIndex(command, outputDir(), () => {});
  assert.deepEqual(outcome, { ok: false, message: "Failed to download https://example.test/npm/all.zip" });
});

test("a Tool that cannot start gives no index state and a failed build", async () => {
  const command = toolCommand(join(outputDir(), "no-such-tool"));
  assert.equal(await readOsvIndex(command, outputDir()), undefined);
  const outcome = await buildOsvIndex(command, outputDir(), () => {});
  assert.equal(outcome.ok, false);
  assert.match(!outcome.ok ? outcome.message : "", /could not start the Tool/);
});
