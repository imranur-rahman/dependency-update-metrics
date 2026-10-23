// Tests for the Cargo.toml parser of the core module (src/core/cargo.ts), through
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

test("Cargo.toml gives the crates.io dependencies of every section with their lines", () => {
  const deps = parseManifest("/work/app/Cargo.toml", fixture("cargo-all-sections.toml"));
  assert.deepEqual(
    deps.map((d) => [d.ecosystem, d.section, d.name, d.line]),
    [
      ["cargo", "dependencies", "serde", 5],
      ["cargo", "dependencies", "rand", 6],
      ["cargo", "dependencies", "serde_json", 7],
      ["cargo", "dependencies", "published-path", 10],
      ["cargo", "dependencies", "tokio", 12],
      ["cargo", "dependencies", "log", 16],
      ["cargo", "dev-dependencies", "proptest", 21],
      ["cargo", "build-dependencies", "cc", 24],
      ["cargo", "target.cfg(unix).dependencies", "libc", 27],
      ["cargo", "target.cfg(windows).dev-dependencies", "winapi", 30],
      ["cargo", "target.x86_64-pc-windows-msvc.build-dependencies", "embed-resource", 32],
    ],
  );
});

test("Cargo.toml gives the columns of the key, also for a renamed dependency", () => {
  const text = '[dependencies]\njson = { package = "serde_json", version = "1" }\n[dependencies.tokio]\n';
  assert.deepEqual(
    parseManifest("Cargo.toml", text).map((d) => [d.name, d.line, d.startCharacter, d.endCharacter]),
    [
      ["serde_json", 1, 0, 4],
      ["tokio", 2, 14, 19],
    ],
  );
});

test("Cargo.toml skips a dotted key below [package], and reads a workspace dependency and the old underscore names", () => {
  const text = [
    "[package]",
    'name = "x"',
    'dependencies.anyhow = "1"',
    "[dev_dependencies]",
    "thiserror = { workspace = true }",
  ].join("\n");
  assert.deepEqual(
    parseManifest("Cargo.toml", text).map((d) => [d.section, d.name, d.line]),
    [["dev-dependencies", "thiserror", 4]],
  );
});

test("Cargo.toml with a syntax error gives no dependencies", () => {
  assert.deepEqual(parseManifest("Cargo.toml", '[dependencies\nserde = "1"\n'), []);
});

test("the file name Cargo.toml is case-sensitive, as Cargo reads it", () => {
  assert.deepEqual(parseManifest("cargo.toml", '[dependencies]\nserde = "1"\n'), []);
});
