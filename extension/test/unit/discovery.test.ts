// Tests for the manifest discovery rules of the core module (src/core/discovery.ts).
import { test } from "node:test";
import assert from "node:assert/strict";

import { isAnalyzedManifest } from "../../src/core/discovery.ts";

test("the Extension analyzes the three manifest names in the root and in subfolders", () => {
  for (const path of [
    "/work/package.json",
    "/work/packages/web/package.json",
    "/work/services/api/pyproject.toml",
    "C:\\work\\crates\\core\\Cargo.toml",
  ]) {
    assert.equal(isAnalyzedManifest(path), true, path);
  }
});

test("the Extension skips manifests in node_modules and .venv", () => {
  for (const path of [
    "/work/node_modules/express/package.json",
    "/work/packages/web/node_modules/a/package.json",
    "/work/.venv/lib/python3.12/site-packages/x/pyproject.toml",
    "C:\\work\\node_modules\\left-pad\\package.json",
  ]) {
    assert.equal(isAnalyzedManifest(path), false, path);
  }
});

test("the Extension skips other files and names that only end like a manifest", () => {
  for (const path of ["/work/tsconfig.json", "/work/my-package.json", "/work/Cargo.lock"]) {
    assert.equal(isAnalyzedManifest(path), false, path);
  }
});
