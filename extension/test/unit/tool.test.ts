// Tests for the window and the Tool output rules of the core module (src/core/tool.ts).
// test/unit/settings.test.ts tests the Tool arguments that the settings make.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { formatDate, windowStartDate, parseToolOutput } from "../../src/core/tool.ts";

const EXAMPLES = join(import.meta.dirname, "..", "..", "..", "contract", "examples");

function exampleResult(name: string, index = 0): unknown {
  return JSON.parse(readFileSync(join(EXAMPLES, name), "utf8")).results[index];
}

test("the window starts 2 years before today", () => {
  assert.equal(windowStartDate(new Date("2026-10-22T23:30:00")), "2024-10-22");
});

test("the window that starts on a 29 February that does not exist starts on 28 February", () => {
  assert.equal(windowStartDate(new Date("2028-02-29T12:00:00")), "2026-02-28");
});

test("the result on stdout counts, also when the Tool exits with code 1", () => {
  const failed = exampleResult("job_done_with_package_error.json", 1);
  const outcome = parseToolOutput({ stdout: `${JSON.stringify(failed)}\n`, stderr: "", exitCode: 1 });
  assert.deepEqual(outcome, { kind: "result", result: failed });
});

test("a Tool that prints no JSON fails with the last line of stderr", () => {
  const stderr = "usage: dependency-metrics [-h]\ndependency-metrics: error: unrecognized arguments: --json\n";
  assert.deepEqual(parseToolOutput({ stdout: "", stderr, exitCode: 2 }), {
    kind: "failed",
    message: "dependency-metrics: error: unrecognized arguments: --json",
  });
});

test("a Tool that prints no JSON and nothing on stderr fails with its exit code", () => {
  assert.deepEqual(parseToolOutput({ stdout: "", stderr: "", exitCode: 0 }), {
    kind: "failed",
    message: "the Tool printed no result (exit code 0)",
  });
});

test("a date prints as YYYY-MM-DD in the local time zone", () => {
  assert.equal(formatDate(new Date(2026, 0, 5, 23, 59)), "2026-01-05");
  assert.equal(formatDate(new Date(2026, 10, 30, 0, 0)), "2026-11-30");
});
