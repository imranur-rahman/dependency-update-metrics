// Checks the folder that the Pages workflow publishes (webapp/site/).
import { test } from "node:test";
import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const siteDir = join(dirname(fileURLToPath(import.meta.url)), "..", "site");

test("the published folder has the homepage", () => {
  assert.ok(existsSync(join(siteDir, "index.html")), "webapp/site/index.html is missing");
});

test("every local asset that the homepage references exists", () => {
  const html = readFileSync(join(siteDir, "index.html"), "utf8");
  const refs = [...html.matchAll(/(?:href|src)="([^"]+)"/g)]
    .map((m) => m[1])
    .filter((ref) => !/^(?:[a-z]+:|\/\/|#)/i.test(ref));
  assert.ok(refs.length > 0, "the homepage references no local assets");
  const missing = refs.filter((ref) => !existsSync(join(siteDir, ref.split(/[?#]/)[0])));
  assert.deepEqual(missing, []);
});
