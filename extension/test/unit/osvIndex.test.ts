// Tests for the OSV index rules of the core module (src/core/osvIndex.ts) and for the hint
// text without the index. Without the index, the Tool gives an MTTR of 0 and its JSON does
// not say so, so the Extension must not show that MTTR.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { hintText, hoverMarkdown, type ToolResult } from "../../src/core/display.ts";
import {
  OSV_PROMPT,
  buildOsvArgs,
  consentFromAnswer,
  osvIndexDateArgs,
  osvIndexText,
  parseOsvIndexDate,
  progressLine,
  shouldAskForOsvIndex,
} from "../../src/core/osvIndex.ts";

const EXAMPLES = join(import.meta.dirname, "..", "..", "..", "contract", "examples");
const [okResult]: ToolResult[] = JSON.parse(
  readFileSync(join(EXAMPLES, "job_done_window_summary.json"), "utf8"),
).results;

test("without the OSV index, the hint shows MTTU and says that MTTR needs the index", () => {
  assert.equal(
    hintText({ kind: "result", result: okResult }, "mypkg", { osvIndex: false }),
    "MTTU 579 d · MTTR needs the OSV index",
  );
});

test("without the OSV index, the hover also says that MTTR needs the index", () => {
  const target = { name: "mypkg", section: "dependencies" };
  const hover = hoverMarkdown({ kind: "result", result: okResult }, target, { osvIndex: false });
  assert.match(hover, /MTTU 579\.\d days · MTTR needs the OSV index/);
});

test("the index state comes from the JSON line of --osv-index-date", () => {
  assert.deepEqual(parseOsvIndexDate('{"osv_index_date": "2026-10-21"}\n'), {
    kind: "built",
    date: "2026-10-21",
  });
  assert.deepEqual(parseOsvIndexDate('{"osv_index_date": null}\n'), { kind: "missing" });
});

test("an output without the JSON line gives no index state", () => {
  // For example, an older Tool that does not know --osv-index-date.
  assert.equal(parseOsvIndexDate(""), undefined);
  assert.equal(parseOsvIndexDate("usage: dependency-metrics …"), undefined);
});

test("the Extension asks only when the index is missing and the developer did not decline", () => {
  const built = { kind: "built", date: "2026-10-21" } as const;
  const missing = { kind: "missing" } as const;
  assert.equal(shouldAskForOsvIndex(missing, undefined), true);
  assert.equal(shouldAskForOsvIndex(missing, "declined"), false);
  assert.equal(shouldAskForOsvIndex(built, undefined), false);
  assert.equal(shouldAskForOsvIndex(undefined, undefined), false);
});

test("the prompt names the size of the download", () => {
  assert.match(OSV_PROMPT.message, /about 260 MB/);
  assert.match(OSV_PROMPT.build, /Download/);
});

test("only the download answer builds the index; any other answer declines", () => {
  assert.equal(consentFromAnswer(OSV_PROMPT.build), "accepted");
  assert.equal(consentFromAnswer(OSV_PROMPT.decline), "declined");
  // The developer closed the prompt: it must not come back on each run.
  assert.equal(consentFromAnswer(undefined), "declined");
});

test("the index text gives the build date of the index", () => {
  assert.equal(osvIndexText({ kind: "built", date: "2026-10-21" }), "OSV index built on 2026-10-21");
  assert.equal(osvIndexText({ kind: "missing" }), "no OSV index");
  assert.equal(osvIndexText(undefined), "OSV index date unknown");
});

test("the progress shows the last line of the build output, also of a tqdm bar", () => {
  assert.equal(
    progressLine("WARNING: Downloading OSV data from https://osv-vulnerabilities.storage.googleapis.com/npm/all.zip\n"),
    "Downloading OSV data from https://osv-vulnerabilities.storage.googleapis.com/npm/all.zip",
  );
  assert.equal(
    progressLine(" 10%|█         | 21.9M/219M [00:02<00:20, 9.5MB/s]\r 11%|█         | 24.1M/219M [00:02<00:20, 9.5MB/s]"),
    "11%|█         | 24.1M/219M [00:02<00:20, 9.5MB/s]",
  );
  assert.equal(progressLine("\r\n"), undefined);
});

test("the Tool arguments read and build the index in the Tool output dir", () => {
  assert.deepEqual(osvIndexDateArgs("/out"), ["--osv-index-date", "--output-dir", "/out"]);
  assert.deepEqual(buildOsvArgs("/out"), ["--build-osv", "--output-dir", "/out"]);
});
