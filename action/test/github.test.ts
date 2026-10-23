// Tests for the GitHub adapters (src/github.ts): the manifests that a pull request changes,
// read with git from a real temporary repository, and the comment store on a fake fetch.
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { gitManifestChanges, restCommentStore } from "../src/github.ts";

function git(cwd: string, ...args: string[]): string {
  return execFileSync("git", args, { cwd, encoding: "utf8" }).trim();
}

test("the changes hold the base and head text of each changed manifest, from the merge base", async (t) => {
  const repo = mkdtempSync(join(tmpdir(), "mttx-action-"));
  t.after(() => rmSync(repo, { recursive: true, force: true }));
  git(repo, "init", "-q", "-b", "main");
  git(repo, "config", "user.email", "test@example.test");
  git(repo, "config", "user.name", "Test");
  mkdirSync(join(repo, "web"));
  writeFileSync(join(repo, "web", "package.json"), '{"dependencies": {"express": "^4"}}');
  writeFileSync(join(repo, "Cargo.toml"), '[dependencies]\nserde = "1"\n');
  writeFileSync(join(repo, "README.md"), "hello");
  git(repo, "add", ".");
  git(repo, "commit", "-q", "-m", "base");
  git(repo, "checkout", "-q", "-b", "feature");
  writeFileSync(join(repo, "web", "package.json"), '{"dependencies": {"express": "^4", "koa": "^2"}}');
  writeFileSync(join(repo, "pyproject.toml"), '[project]\ndependencies = ["rich"]\n');
  mkdirSync(join(repo, "crates"));
  renameSync(join(repo, "Cargo.toml"), join(repo, "crates", "Cargo.toml"));
  writeFileSync(join(repo, "README.md"), "changed");
  git(repo, "add", "-A");
  git(repo, "commit", "-q", "-m", "head");
  const head = git(repo, "rev-parse", "HEAD");
  // A later commit on main does not count as a change of the pull request.
  git(repo, "checkout", "-q", "main");
  writeFileSync(join(repo, "package.json"), '{"dependencies": {"lodash": "^4"}}');
  git(repo, "add", ".");
  git(repo, "commit", "-q", "-m", "main moves on");
  const base = git(repo, "rev-parse", "HEAD");

  const changes = await gitManifestChanges(repo, base, head);
  assert.deepEqual(changes, [
    { path: "crates/Cargo.toml", base: '[dependencies]\nserde = "1"\n', head: '[dependencies]\nserde = "1"\n' },
    { path: "pyproject.toml", base: null, head: '[project]\ndependencies = ["rich"]\n' },
    { path: "web/package.json", base: '{"dependencies": {"express": "^4"}}', head: '{"dependencies": {"express": "^4", "koa": "^2"}}' },
  ]);
});

test("a base commit that the checkout does not have gives an error that names fetch-depth", async (t) => {
  const repo = mkdtempSync(join(tmpdir(), "mttx-action-"));
  t.after(() => rmSync(repo, { recursive: true, force: true }));
  git(repo, "init", "-q");
  await assert.rejects(gitManifestChanges(repo, "0".repeat(40), "1".repeat(40)), /fetch-depth: 0/);
});

test("the comment store lists all pages, creates, and updates comments through the REST API", async () => {
  const calls: { method: string; url: string; body?: string }[] = [];
  const page = (from: number, count: number) =>
    Array.from({ length: count }, (_, i) => ({ id: from + i, body: `c${from + i}`, html_url: `h${from + i}` }));
  const fakeFetch = async (url: string | URL | Request, init?: RequestInit) => {
    const method = init?.method ?? "GET";
    calls.push({ method, url: String(url), ...(init?.body ? { body: String(init.body) } : {}) });
    assert.equal(new Headers(init?.headers).get("authorization"), "Bearer t0ken");
    if (method === "GET") {
      const pageNumber = Number(new URL(String(url)).searchParams.get("page"));
      return Response.json(pageNumber === 1 ? page(1, 100) : page(101, 2));
    }
    return Response.json({ id: 999, body: JSON.parse(String(init!.body)).body, html_url: "h999" });
  };
  const store = restCommentStore({ apiUrl: "https://api.github.test", repository: "o/r", number: 5, token: "t0ken", fetch: fakeFetch });
  const comments = await store.list();
  assert.equal(comments.length, 102);
  assert.deepEqual(comments[101], { id: 102, body: "c102", url: "h102" });
  assert.deepEqual(await store.create("new"), { id: 999, body: "new", url: "h999" });
  await store.update(42, "changed");
  assert.deepEqual(calls.slice(-2), [
    { method: "POST", url: "https://api.github.test/repos/o/r/issues/5/comments", body: '{"body":"new"}' },
    { method: "PATCH", url: "https://api.github.test/repos/o/r/issues/comments/42", body: '{"body":"changed"}' },
  ]);
});

test("a refused request gives an error with the status and the message of GitHub", async () => {
  const fakeFetch = async () => Response.json({ message: "Resource not accessible by integration" }, { status: 403 });
  const store = restCommentStore({ apiUrl: "https://api.github.test", repository: "o/r", number: 5, token: "t", fetch: fakeFetch });
  await assert.rejects(store.create("x"), /403 Resource not accessible by integration/);
});
