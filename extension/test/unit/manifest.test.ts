// Tests for the manifest parser of the core module (src/core/manifest.ts).
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { parseManifest } from "../../src/core/manifest.ts";

const FIXTURES = join(import.meta.dirname, "..", "fixtures");

function fixture(name: string): string {
  return readFileSync(join(FIXTURES, name), "utf8");
}

test("package.json gives the dependencies of every section with their lines", () => {
  const deps = parseManifest("/work/app/package.json", fixture("package-all-sections.json"));
  assert.deepEqual(
    deps.map((d) => [d.ecosystem, d.section, d.name, d.line]),
    [
      ["npm", "dependencies", "express", 5],
      ["npm", "dependencies", "@types/node", 6],
      ["npm", "devDependencies", "left-pad", 9],
      ["npm", "peerDependencies", "react", 12],
      ["npm", "optionalDependencies", "fsevents", 15],
    ],
  );
});

test("package.json gives the columns of the name, so that a hover can find the dependency", () => {
  const [express] = parseManifest("package.json", '{"dependencies": {"express": "^4"}}');
  assert.equal(express.line, 0);
  assert.equal(express.startCharacter, 18);
  assert.equal(express.endCharacter, 27);
});

test("package.json skips the nested objects and the keys outside the dependency sections", () => {
  const text = fixture("package-odd-format.json");
  assert.deepEqual(
    parseManifest("package.json", text).map((d) => [d.name, d.line]),
    [
      ["a-b", 8],
      ["c\"d", 9],
      ["late", 13],
    ],
  );
});

test("package.json with a syntax error gives no dependencies", () => {
  assert.deepEqual(parseManifest("package.json", '{"dependencies": {"express": '), []);
});

test("package.json with CRLF line ends gives the same lines", () => {
  const text = '{\r\n  "dependencies": {\r\n    "express": "^4"\r\n  }\r\n}\r\n';
  assert.deepEqual(
    parseManifest("package.json", text).map((d) => d.line),
    [2],
  );
});

test("a file that is not a known manifest gives no dependencies", () => {
  assert.deepEqual(parseManifest("/work/app/tsconfig.json", '{"dependencies": {"a": "1"}}'), []);
});
