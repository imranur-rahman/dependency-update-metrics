// Tests for the pyproject.toml parser of the core module (src/core/pyproject.ts), through
// parseManifest.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { parseManifest } from "../../src/core/manifest.ts";

const FIXTURES = join(import.meta.dirname, "..", "fixtures");

function fixture(name: string): string {
  return readFileSync(join(FIXTURES, name), "utf8");
}

test("pyproject.toml gives the dependencies of every section with their lines", () => {
  const deps = parseManifest("/work/app/pyproject.toml", fixture("pyproject-all-sections.toml"));
  assert.deepEqual(
    deps.map((d) => [d.ecosystem, d.section, d.name, d.line]),
    [
      ["pypi", "project.dependencies", "requests", 6],
      ["pypi", "project.dependencies", "numpy", 7],
      ["pypi", "project.dependencies", "Flask_Login", 8],
      ["pypi", "project.optional-dependencies.test", "pytest", 12],
      ["pypi", "project.optional-dependencies.test", "coverage", 12],
      ["pypi", "project.optional-dependencies.docs", "sphinx", 14],
      ["pypi", "dependency-groups.dev", "ruff", 18],
      ["pypi", "tool.poetry.dependencies", "django", 22],
      ["pypi", "tool.poetry.dependencies", "pandas", 23],
      ["pypi", "tool.poetry.group.lint.dependencies", "black", 26],
      ["pypi", "tool.poetry.dev-dependencies", "mypy", 29],
    ],
  );
});

test("pyproject.toml gives the columns of the name in a requirement and in a Poetry key", () => {
  const text = '[project]\ndependencies = ["requests>=2"]\n[tool.poetry.dependencies]\n"django" = "5"\n';
  const deps = parseManifest("pyproject.toml", text);
  assert.deepEqual(
    deps.map((d) => [d.name, d.line, d.startCharacter, d.endCharacter]),
    [
      ["requests", 1, 17, 25],
      ["django", 3, 0, 8],
    ],
  );
});

test("pyproject.toml reads extras, markers, comments, strings, and odd key forms", () => {
  const deps = parseManifest("pyproject.toml", fixture("pyproject-odd-format.toml"));
  assert.deepEqual(
    deps.map((d) => [d.section, d.name, d.line]),
    [
      ["project.dependencies", "spaced.name", 7],
      ["project.dependencies", "zope.interface", 9],
      ["project.dependencies", "multi", 10],
      ["project.dependencies", "trailing-comma", 11],
      ["project.optional-dependencies.all", "dotted-key", 13],
      ["tool.poetry.dependencies", "quoted.name", 20],
      ["tool.poetry.dependencies", "several", 23],
      ["tool.poetry.dependencies", "table-form", 28],
      ["dependency-groups.lint", "ruff", 32],
      ["dependency-groups.lint", "mypy", 33],
    ],
  );
});

test("pyproject.toml gives the columns of the name after spaces in the requirement", () => {
  const [dep] = parseManifest("pyproject.toml", fixture("pyproject-odd-format.toml"));
  assert.equal(dep.startCharacter, 5);
  assert.equal(dep.endCharacter, 16);
});

test("pyproject.toml with a syntax error gives no dependencies", () => {
  assert.deepEqual(parseManifest("pyproject.toml", '[project]\ndependencies = ["requests"'), []);
  assert.deepEqual(parseManifest("pyproject.toml", '[project]\ndependencies = ["a"] x\n'), []);
});

test("pyproject.toml with CRLF line ends gives the same lines", () => {
  const text = '[project]\r\ndependencies = [\r\n  "requests",\r\n]\r\n';
  assert.deepEqual(
    parseManifest("pyproject.toml", text).map((d) => [d.name, d.line, d.startCharacter]),
    [["requests", 2, 3]],
  );
});
