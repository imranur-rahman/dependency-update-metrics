// Tests for the step that finds the dependencies that a pull request adds (src/added.ts).
import { test } from "node:test";
import assert from "node:assert/strict";

import { addedDependencies } from "../src/added.ts";

test("a dependency counts as added when its name is in the head manifest and not in the base manifest", () => {
  const added = addedDependencies([
    {
      path: "package.json",
      base: '{"dependencies": {"express": "^4"}}',
      head: '{"dependencies": {"express": "^5", "koa": "^2"}, "devDependencies": {"left-pad": "1"}}',
    },
  ]);
  assert.deepEqual(added, [
    { ecosystem: "npm", name: "koa", files: ["package.json"] },
    { ecosystem: "npm", name: "left-pad", files: ["package.json"] },
  ]);
});

test("a changed version constraint or a move to another section does not count", () => {
  const added = addedDependencies([
    {
      path: "package.json",
      base: '{"dependencies": {"express": "^4"}}',
      head: '{"devDependencies": {"express": "^5"}}',
    },
  ]);
  assert.deepEqual(added, []);
});

test("a new manifest adds all of its dependencies, and a removed manifest adds none", () => {
  const added = addedDependencies([
    { path: "svc/Cargo.toml", base: null, head: '[dependencies]\nserde = "1"\n' },
    { path: "old/package.json", base: '{"dependencies": {"express": "^4"}}', head: null },
  ]);
  assert.deepEqual(added, [{ ecosystem: "cargo", name: "serde", files: ["svc/Cargo.toml"] }]);
});

test("the parsers of the Extension core read pyproject.toml markers and Cargo.toml renames and targets", () => {
  const added = addedDependencies([
    {
      path: "pyproject.toml",
      base: '[project]\ndependencies = ["requests>=2"]\n',
      head:
        '[project]\ndependencies = ["requests>=2", "rich[jupyter]>=13; python_version >= \'3.8\'"]\n' +
        '[dependency-groups]\ntest = ["pytest"]\n',
    },
    {
      path: "Cargo.toml",
      base: '[dependencies]\nserde = "1"\n',
      head:
        '[dependencies]\nserde = "1"\njson = { package = "serde_json", version = "1" }\n' +
        'local = { path = "../local" }\n' +
        "[target.'cfg(windows)'.dependencies]\nwinapi = \"0.3\"\n",
    },
  ]);
  assert.deepEqual(added, [
    { ecosystem: "pypi", name: "rich", files: ["pyproject.toml"] },
    { ecosystem: "pypi", name: "pytest", files: ["pyproject.toml"] },
    { ecosystem: "cargo", name: "serde_json", files: ["Cargo.toml"] },
    { ecosystem: "cargo", name: "winapi", files: ["Cargo.toml"] },
  ]);
});

test("PyPI names match as PEP 503 names, so a new spelling of a dependency does not count", () => {
  const added = addedDependencies([
    {
      path: "pyproject.toml",
      base: '[project]\ndependencies = ["Typing_Extensions"]\n',
      head: '[project]\ndependencies = ["typing-extensions"]\n',
    },
  ]);
  assert.deepEqual(added, []);
});

test("a dependency that two manifests add is one entry with both files", () => {
  const added = addedDependencies([
    { path: "a/package.json", base: "{}", head: '{"dependencies": {"koa": "^2"}}' },
    { path: "b/package.json", base: null, head: '{"dependencies": {"koa": "^2"}}' },
  ]);
  assert.deepEqual(added, [{ ecosystem: "npm", name: "koa", files: ["a/package.json", "b/package.json"] }]);
});

test("files that are not manifests, and manifests in node_modules or .venv, add nothing", () => {
  const added = addedDependencies([
    { path: "README.md", base: null, head: "koa" },
    { path: "node_modules/koa/package.json", base: null, head: '{"dependencies": {"debug": "^4"}}' },
    { path: ".venv/lib/pyproject.toml", base: null, head: '[project]\ndependencies = ["rich"]\n' },
  ]);
  assert.deepEqual(added, []);
});
