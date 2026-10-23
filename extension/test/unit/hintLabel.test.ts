// Tests for the hint label of a dependency that shares its line (src/core/hintLabel.ts).
import { test } from "node:test";
import assert from "node:assert/strict";

import { hintLabel } from "../../src/core/hintLabel.ts";
import { parseManifest } from "../../src/core/manifest.ts";

test("a dependency alone on its line gets the hint text only", () => {
  const deps = parseManifest("pyproject.toml", '[project]\ndependencies = [\n  "requests",\n]\n');
  assert.equal(hintLabel(deps[0], deps, "MTTU 3 d · MTTR 0 d"), "MTTU 3 d · MTTR 0 d");
});

test("dependencies that share a line get their names in front of the hint text", () => {
  const deps = parseManifest("pyproject.toml", '[project]\ndependencies = ["requests", "numpy"]\n');
  assert.deepEqual(
    deps.map((dep) => hintLabel(dep, deps, "computing…")),
    ["requests: computing…", "numpy: computing…"],
  );
});
