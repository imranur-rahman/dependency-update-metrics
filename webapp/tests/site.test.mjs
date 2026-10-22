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

test("the homepage loads Chart.js from cdnjs with an exact version", () => {
  const html = readFileSync(join(siteDir, "index.html"), "utf8");
  const urls = [...html.matchAll(/src="([^"]*chart[^"]*\.js)"/gi)].map((m) => m[1]);

  assert.equal(urls.length, 1);
  assert.match(urls[0], /^https:\/\/cdnjs\.cloudflare\.com\/ajax\/libs\/Chart\.js\/\d+\.\d+\.\d+\//);
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

test("the homepage shows the Turnstile widget with the site key of the Webapp", () => {
  const html = readFileSync(join(siteDir, "index.html"), "utf8");
  const widget = html.match(/<div[^>]*class="cf-turnstile"[^>]*>/);

  assert.ok(widget, "the Turnstile widget is missing");
  assert.match(widget[0], /data-sitekey="[0-9A-Za-z_-]+"/);
  assert.match(html, /<script src="https:\/\/challenges\.cloudflare\.com\/turnstile\/v0\/api\.js"/);
});

test("the homepage has the privacy note on the stored package names and IP hashes", () => {
  const html = readFileSync(join(siteDir, "index.html"), "utf8");
  const note = html.match(/<p[^>]*id="privacy-note"[^>]*>([\s\S]*?)<\/p>/);

  assert.ok(note, "the privacy note is missing");
  assert.match(note[1], /package names/);
  assert.match(note[1], /hash of your IP address/);
});
