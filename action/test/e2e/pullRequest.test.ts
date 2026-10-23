// End-to-end test of the built Action (dist/index.mjs, `npm run build` first) with target
// pull-request. It makes a git repository from the fixture PR files in
// test/fixtures/pr/ (base, then push1, then push2 on top of push1), runs the bundle with the
// fake Tool of the Extension (MTTX_ACTION_TOOL) and a fake GitHub REST API on localhost, and
// checks the one comment of the Action after each push.
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync, spawn, spawnSync } from "node:child_process";
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { fakeGitHub } from "./fakeGitHub.ts";

const ACTION = join(import.meta.dirname, "..", "..");
const FIXTURES = join(ACTION, "test", "fixtures", "pr");
const FAKE_TOOL = join(ACTION, "..", "extension", "test", "fixtures", "fake-tool.mjs");

function git(cwd: string, ...args: string[]): string {
  return execFileSync("git", args, { cwd, encoding: "utf8" }).trim();
}

function commit(repo: string, message: string): string {
  git(repo, "add", "-A");
  git(repo, "commit", "-q", "--allow-empty", "-m", message);
  return git(repo, "rev-parse", "HEAD");
}

// Run the bundle as GitHub Actions runs it for a pull_request event, and return the step
// outputs, the summary, and the exit code.
async function runBundle(repo: string, apiUrl: string, base: string, head: string, inputs: Record<string, string> = {}) {
  const temp = mkdtempSync(join(tmpdir(), "mttx-step-"));
  try {
    const event = join(temp, "event.json");
    writeFileSync(event, JSON.stringify({ pull_request: { number: 1, base: { sha: base }, head: { sha: head } } }));
    for (const name of ["output", "summary"]) writeFileSync(join(temp, name), "");
    const env: Record<string, string> = {
      PATH: process.env.PATH ?? "",
      MTTX_ACTION_TOOL: FAKE_TOOL,
      MTTX_DATA_DIR: join(temp, "data"),
      GITHUB_EVENT_NAME: "pull_request",
      GITHUB_EVENT_PATH: event,
      GITHUB_WORKSPACE: repo,
      GITHUB_REPOSITORY: "o/r",
      GITHUB_API_URL: apiUrl,
      GITHUB_OUTPUT: join(temp, "output"),
      GITHUB_STEP_SUMMARY: join(temp, "summary"),
      INPUT_TARGET: "pull-request",
      INPUT_GITHUB_TOKEN: "test-token",
    };
    for (const [name, value] of Object.entries(inputs)) env[`INPUT_${name.toUpperCase().replace(/-/g, "_")}`] = value;
    // spawnSync would block the fake server in this process, so the bundle runs async.
    const child = spawn(process.execPath, [join(ACTION, "dist", "index.mjs")], { cwd: repo, env });
    let stdout = "";
    child.stdout.on("data", (chunk) => (stdout += chunk));
    child.stderr.on("data", (chunk) => (stdout += chunk));
    const exitCode = await new Promise<number | null>((resolve) => child.on("close", resolve));
    const outputs: Record<string, string> = {};
    for (const match of readFileSync(join(temp, "output"), "utf8").matchAll(/^([\w-]+)<<(\S+)\n([\s\S]*?)\n\2$/gm)) {
      outputs[match[1]] = match[3];
    }
    return { exitCode, stdout, outputs, summary: readFileSync(join(temp, "summary"), "utf8") };
  } finally {
    rmSync(temp, { recursive: true, force: true });
  }
}

test("the bundle comments once on a pull request that adds dependencies to each manifest kind, and updates the comment on a new push", async (t) => {
  assert.equal(spawnSync("git", ["--version"]).status, 0);
  const github = await fakeGitHub();
  t.after(() => github.server.close());
  const repo = mkdtempSync(join(tmpdir(), "mttx-pr-"));
  t.after(() => rmSync(repo, { recursive: true, force: true }));
  git(repo, "init", "-q", "-b", "main");
  git(repo, "config", "user.email", "test@example.test");
  git(repo, "config", "user.name", "Test");
  cpSync(join(FIXTURES, "base"), repo, { recursive: true });
  const base = commit(repo, "base");
  git(repo, "checkout", "-q", "-b", "feature");
  cpSync(join(FIXTURES, "push1"), repo, { recursive: true });
  const push1 = commit(repo, "push 1");

  const first = await runBundle(repo, github.url, base, push1);
  assert.equal(first.exitCode, 0, first.stdout);
  assert.deepEqual(
    JSON.parse(first.outputs.added).map((dep: { ecosystem: string; name: string }) => `${dep.ecosystem}:${dep.name}`),
    ["cargo:serde_json", "cargo:nope", "npm:koa", "npm:left-pad", "pypi:rich"],
  );
  assert.equal(github.comments.length, 2);
  const comment = github.comments[1];
  assert.equal(first.outputs["comment-url"], comment.html_url);
  assert.match(comment.body, /^<!-- mttx-action -->\n### MTTU and MTTR of the dependencies that this pull request adds/);
  assert.match(comment.body, /\| `koa` \| npm \| 579\.00 \| 579\.00 \| 1 \| `package\.json` \|/);
  assert.match(comment.body, /\| `rich` \| pypi \| 579\.00 \| 579\.00 \| 1 \| `pyproject\.toml` \|/);
  assert.match(comment.body, /\| `serde_json` \| cargo \| 579\.00 \| 579\.00 \| 1 \| `Cargo\.toml` \|/);
  assert.match(comment.body, /\| `left-pad` \| npm \| – \| – \| 0 \| `package\.json` \| could not compute since 'left-pad' has no dependencies \|/);
  assert.match(comment.body, /\| `nope` \| cargo \| – \| – \| 0 \| `Cargo\.toml` \| error: Package not found in deps\.dev \|/);
  assert.match(comment.body, /#### What the results mean/);
  assert.match(first.summary, /`koa`/);

  cpSync(join(FIXTURES, "push2"), repo, { recursive: true });
  const push2 = commit(repo, "push 2");
  const second = await runBundle(repo, github.url, base, push2, { "max-mttu-days": "100" });
  assert.equal(second.exitCode, 1, "the threshold fails the check");
  assert.match(second.stdout, /::error title=MTTX::The MTTU of koa \(npm\) is 579\.00 days, more than max-mttu-days \(100\)\./);
  assert.equal(github.comments.length, 2, "a new push updates the same comment");
  assert.match(github.comments[1].body, /`chalk`/);
  assert.doesNotMatch(github.comments[1].body, /`left-pad`/);
  assert.equal(github.comments[0].body, "A comment of a person");
});

test("the bundle writes no comment on a pull request that adds no dependencies", async (t) => {
  const github = await fakeGitHub();
  t.after(() => github.server.close());
  const repo = mkdtempSync(join(tmpdir(), "mttx-pr-"));
  t.after(() => rmSync(repo, { recursive: true, force: true }));
  git(repo, "init", "-q", "-b", "main");
  git(repo, "config", "user.email", "test@example.test");
  git(repo, "config", "user.name", "Test");
  cpSync(join(FIXTURES, "base"), repo, { recursive: true });
  const base = commit(repo, "base");
  writeFileSync(join(repo, "package.json"), readFileSync(join(repo, "package.json"), "utf8").replace("^4.18.0", "^5.0.0"));
  const head = commit(repo, "new express constraint");

  const run = await runBundle(repo, github.url, base, head);
  assert.equal(run.exitCode, 0, run.stdout);
  assert.equal(run.outputs.added, "[]");
  assert.equal(run.outputs["comment-url"], "");
  assert.equal(github.comments.length, 1);
});
