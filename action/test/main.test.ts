// Tests for the run of the Action (src/main.ts) with a fake Tool, a fake pull request, a
// fake comment store, and in-memory outputs. The fake Tool prints the result bodies of
// contract/examples/, as the fake Tool of the Extension does.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import type { SpawnResult } from "../../extension/src/spawnProcess.ts";
import type { ManifestChange } from "../src/added.ts";
import type { OsvIndexSource } from "../src/inputs.ts";
import { runAction, type ActionIO, type Comment, type CommentStore, type ToolProcess } from "../src/main.ts";
import { COMMENT_MARKER } from "../src/report.ts";

const EXAMPLES = join(import.meta.dirname, "..", "..", "contract", "examples");

function example(name: string, index: number) {
  return JSON.parse(readFileSync(join(EXAMPLES, name), "utf8")).results[index];
}

function done(stdout: string, exitCode = 0): SpawnResult {
  return { exitCode, signal: null, stdout, stderr: "", timedOut: false };
}

// A fake Tool: "left-pad" has no dependencies, "nope" fails, "slow" times out, any other
// package gets the ok result (MTTU 579.00, MTTR 579.00). The OSV index has the date
// `indexDate` (null: no index); --build-osv sets it to "2026-10-23".
function fakeTool(indexDate: string | null = "2026-10-01") {
  const calls: string[][] = [];
  let date = indexDate;
  const run: ToolProcess = async (args) => {
    calls.push(args);
    const option = (name: string) => args[args.indexOf(name) + 1];
    if (args.includes("--osv-index-date")) return done(`${JSON.stringify({ osv_index_date: date })}\n`);
    if (args.includes("--build-osv")) {
      date = "2026-10-23";
      return done("");
    }
    const name = option("--package");
    if (name === "slow") return { ...done(""), exitCode: null, signal: "SIGTERM", timedOut: true };
    let result = example("job_done_window_summary.json", 0);
    let exitCode = 0;
    if (name === "nope") {
      result = example("job_done_with_package_error.json", 1);
      exitCode = 1;
    } else if (name === "left-pad") {
      result = { ...result, summary: { ...result.summary, num_dependencies: 0, status: "no_deps" } };
    }
    result = { ...result, ecosystem: option("--ecosystem"), package: name, start_date: option("--start-date"), end_date: option("--end-date") };
    return done(`Analyzing\n${JSON.stringify(result)}\n`, exitCode);
  };
  return { run, calls };
}

function fakeComments(existing: Comment[] = []): CommentStore & { comments: Comment[] } {
  const comments = [...existing];
  return {
    comments,
    async list() {
      return comments;
    },
    async create(body) {
      const comment = { id: comments.length + 1, body, url: `https://github.test/pr/1#c${comments.length + 1}` };
      comments.push(comment);
      return comment;
    },
    async update(id, body) {
      const comment = comments.find((item) => item.id === id)!;
      comment.body = body;
      return comment;
    },
  };
}

// `tool` is the Tool seam that openTool gives; `opened` records the osv-index of each call.
function fakeIO(inputs: Record<string, string>, extra: Partial<ActionIO> & { tool?: ToolProcess } = {}) {
  const { tool = fakeTool().run, ...rest } = extra;
  const opened: OsvIndexSource[] = [];
  const outputs: Record<string, string> = {};
  const files: Record<string, string> = {};
  const errors: string[] = [];
  const warnings: string[] = [];
  let summary = "";
  const io: ActionIO = {
    inputs,
    today: new Date(2026, 9, 23),
    openTool: (osvIndex) => {
      opened.push(osvIndex);
      return { tool, outputDir: "/data" };
    },
    setOutput: (name, value) => {
      outputs[name] = value;
    },
    appendSummary: (markdown) => {
      summary += markdown;
    },
    writeFile: (path, text) => {
      files[path] = text;
    },
    error: (message) => errors.push(message),
    warning: (message) => warnings.push(message),
    info: () => {},
    ...rest,
  };
  return { io, outputs, files, errors, warnings, opened, summary: () => summary };
}

test("target packages: the outputs, the job summary, and the badge files for npm, PyPI, and Cargo", async () => {
  for (const [ecosystem, name] of [["npm", "express"], ["pypi", "requests"], ["cargo", "serde"]]) {
    const tool = fakeTool();
    const run = fakeIO({ packages: `${name}, left-pad`, ecosystem }, { tool: tool.run });
    assert.equal(await runAction(run.io), 0);
    assert.deepEqual(run.opened, ["image"], "the default osv-index opens the image with the index inside");
    const results = JSON.parse(run.outputs.results);
    assert.deepEqual(results.map((row: { package: string; status: string }) => [row.package, row.status]), [
      [name, "ok"],
      ["left-pad", "no_deps"],
    ]);
    assert.equal(run.outputs.mttu, "579.00");
    assert.equal(run.outputs.mttr, "579.00");
    assert.equal(run.outputs["over-threshold"], "false");
    assert.match(run.summary(), new RegExp(`\\| \`${name}\` \\| ${ecosystem} \\| 579\\.00 \\| 579\\.00 \\|`));
    assert.match(run.summary(), /could not compute since 'left-pad' has no dependencies/);
    assert.match(run.summary(), /OSV data as of 2026-10-01/);
    assert.deepEqual(JSON.parse(run.files[`mttx-badges/${ecosystem}-${name}-mttr.json`]), {
      schemaVersion: 1,
      label: "MTTR",
      message: "579.00 days",
      color: "blue",
    });
    const analysis = tool.calls.find((args) => args.includes("--package"))!;
    assert.deepEqual(analysis, [
      "--ecosystem", ecosystem, "--package", name, "--json", "--depsdev",
      "--start-date", "2024-10-23", "--end-date", "2026-10-23",
      "--weighting-type", "disable", "--output-dir", "/data",
    ]);
  }
});

test("a value above a threshold fails the step; no dependencies and errors do not", async () => {
  const over = fakeIO({ packages: "express", ecosystem: "npm", "max-mttr-days": "30" });
  assert.equal(await runAction(over.io), 1);
  assert.equal(over.outputs["over-threshold"], "true");
  assert.deepEqual(over.errors, ["The MTTR of express (npm) is 579.00 days, more than max-mttr-days (30)."]);

  const failed = fakeIO({ packages: "left-pad nope slow", ecosystem: "npm", "max-mttr-days": "0", "max-mttu-days": "0" });
  assert.equal(await runAction(failed.io), 0);
  assert.deepEqual(failed.errors, []);
  assert.equal(failed.outputs.mttu, "");
  assert.match(failed.summary(), /error: Package not found in deps\.dev/);
  assert.match(failed.summary(), /error: the Tool stopped after 600 s \(package-timeout-seconds\)/);
});

test("an invalid input fails the step before a Tool run", async () => {
  const tool = fakeTool();
  const run = fakeIO({ packages: "express", ecosystem: "maven" }, { tool: tool.run });
  assert.equal(await runAction(run.io), 1);
  assert.deepEqual(run.errors, ['Input ecosystem "maven" is not one of npm, pypi, cargo.']);
  assert.deepEqual(tool.calls, []);
  assert.deepEqual(run.opened, []);
});

test("osv-index fresh builds the index of today when the index is older, and skips the build when it is new", async () => {
  const old = fakeTool("2026-10-01");
  const run = fakeIO({ packages: "express", ecosystem: "npm", "osv-index": "fresh" }, { tool: old.run });
  assert.equal(await runAction(run.io), 0);
  assert.ok(old.calls.some((args) => args.includes("--build-osv")));
  assert.deepEqual(run.opened, ["fresh"], "the Tool seam gets the checked osv-index input");
  assert.match(run.summary(), /OSV data as of 2026-10-23/);

  const fresh = fakeTool("2026-10-23");
  assert.equal(await runAction(fakeIO({ packages: "express", ecosystem: "npm", "osv-index": "fresh" }, { tool: fresh.run }).io), 0);
  assert.ok(!fresh.calls.some((args) => args.includes("--build-osv")));
});

test("an image without an OSV index gives a warning, because each MTTR is then 0", async () => {
  const run = fakeIO({ packages: "express", ecosystem: "npm" }, { tool: fakeTool(null).run });
  assert.equal(await runAction(run.io), 0);
  assert.match(run.warnings.join("\n"), /no OSV index/);
});

const PR_CHANGES: ManifestChange[] = [
  { path: "package.json", base: '{"dependencies": {"express": "^4"}}', head: '{"dependencies": {"express": "^4", "koa": "^2", "left-pad": "1"}}' },
  { path: "pyproject.toml", base: '[project]\ndependencies = []\n', head: '[project]\ndependencies = ["requests>=2"]\n' },
  { path: "Cargo.toml", base: null, head: '[dependencies]\nnope = "1"\n' },
];

test("target pull-request: one comment for the dependencies that each manifest kind adds, updated on a new push", async () => {
  const comments = fakeComments([{ id: 7, body: "LGTM", url: "u7" }]);
  const tool = fakeTool();
  const run = fakeIO({ target: "pull-request" }, { tool: tool.run, pullRequest: { changes: async () => PR_CHANGES, comments } });
  assert.equal(await runAction(run.io), 0);
  assert.equal(comments.comments.length, 2);
  const body = comments.comments[1].body;
  assert.ok(body.startsWith(COMMENT_MARKER));
  assert.match(body, /\| `koa` \| npm \| 579\.00 \| 579\.00 \| 1 \| `package\.json` \|/);
  assert.match(body, /\| `left-pad` \| npm \| – \| – \| 0 \| `package\.json` \| could not compute since 'left-pad' has no dependencies \|/);
  assert.match(body, /\| `requests` \| pypi \| 579\.00 \|/);
  assert.match(body, /\| `nope` \| cargo \| – \| – \| 0 \| `Cargo\.toml` \| error: Package not found in deps\.dev \|/);
  assert.equal(run.outputs["comment-url"], "https://github.test/pr/1#c2");
  assert.deepEqual(JSON.parse(run.outputs.added).map((dep: { name: string }) => dep.name), ["koa", "left-pad", "requests", "nope"]);
  assert.ok(!tool.calls.some((args) => args.includes("express")));
  assert.equal(Object.keys(run.files).length, 0);

  // A second push updates the same comment.
  const second = fakeIO({ target: "pull-request" }, {
    pullRequest: { changes: async () => PR_CHANGES.slice(0, 1), comments },
  });
  assert.equal(await runAction(second.io), 0);
  assert.equal(comments.comments.length, 2);
  assert.doesNotMatch(comments.comments[1].body, /requests/);
  assert.match(comments.comments[1].body, /`koa`/);
});

test("target pull-request: a pull request that adds no dependencies gets no comment", async () => {
  const comments = fakeComments();
  const tool = fakeTool();
  const changes = [{ path: "package.json", base: '{"dependencies": {"express": "^4"}}', head: '{"dependencies": {"express": "^5"}}' }];
  const run = fakeIO({ target: "pull-request" }, { tool: tool.run, pullRequest: { changes: async () => changes, comments } });
  assert.equal(await runAction(run.io), 0);
  assert.equal(comments.comments.length, 0);
  assert.deepEqual(tool.calls, []);
  assert.deepEqual(run.opened, [], "no package opens no Tool seam");
  assert.equal(run.outputs.added, "[]");
  assert.equal(run.outputs["comment-url"], "");
});

test("target pull-request: a comment of an earlier push says that the pull request no longer adds dependencies", async () => {
  const comments = fakeComments([{ id: 3, body: `${COMMENT_MARKER}\nold table`, url: "u3" }]);
  const run = fakeIO({ target: "pull-request" }, { pullRequest: { changes: async () => [], comments } });
  assert.equal(await runAction(run.io), 0);
  assert.equal(comments.comments.length, 1);
  assert.match(comments.comments[0].body, /This pull request adds no dependencies/);
});

test("target pull-request: a threshold fails the check after the comment is written", async () => {
  const comments = fakeComments();
  const run = fakeIO({ target: "pull-request", "max-mttu-days": "100" }, { pullRequest: { changes: async () => PR_CHANGES, comments } });
  assert.equal(await runAction(run.io), 1);
  assert.equal(comments.comments.length, 1);
  assert.equal(run.errors.length, 2);
});

test("target pull-request: a comment that cannot be written gives a warning, not a failure", async () => {
  const comments = fakeComments();
  comments.create = async () => {
    throw new Error("403 Resource not accessible by integration");
  };
  const run = fakeIO({ target: "pull-request" }, { pullRequest: { changes: async () => PR_CHANGES, comments } });
  assert.equal(await runAction(run.io), 0);
  assert.match(run.warnings.join("\n"), /could not write the pull request comment: 403/);
  assert.match(run.summary(), /`koa`/);
});

test("target pull-request outside a pull request event fails with a clear error", async () => {
  const run = fakeIO({ target: "pull-request" });
  assert.equal(await runAction(run.io), 1);
  assert.match(run.errors[0], /needs a pull_request event/);
});
