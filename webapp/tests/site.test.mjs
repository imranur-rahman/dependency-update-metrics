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

test("the two line charts sit in one column, the MTTU chart above the MTTR chart", () => {
  const html = readFileSync(join(siteDir, "index.html"), "utf8");
  const figure = html.match(/<figure[^>]*id="release-charts-figure"[^>]*>([\s\S]*?)<\/figure>/);

  assert.ok(figure, "the figure of the line charts is missing");
  // Bulma columns would put the charts side by side; the charts stay in one column.
  assert.doesNotMatch(figure[1], /class="columns?[ "]/);
  const canvases = [...figure[1].matchAll(/<canvas id="(release-chart-\w+)"/g)];
  assert.deepEqual(
    canvases.map((m) => m[1]),
    ["release-chart-mttu", "release-chart-mttr"],
  );
});

test("each chart box gets its size from chartBoxStyle, not from the HTML or the CSS", () => {
  const html = readFileSync(join(siteDir, "index.html"), "utf8");
  const css = readFileSync(join(siteDir, "static", "css", "index.css"), "utf8");
  const boxes = [...html.matchAll(/<div class="chart-box"[^>]*>/g)];

  assert.equal(boxes.length, 3);
  for (const box of boxes) assert.doesNotMatch(box[0], /style=/);
  const rules = [...css.matchAll(/\.chart-box[^{]*\{([^}]*)\}/g)].map((m) => m[1]).join("");
  assert.doesNotMatch(rules, /(max-)?width|height/, "the CSS sets no size of a chart box");
});

test("each chart keeps its aria-label", () => {
  const html = readFileSync(join(siteDir, "index.html"), "utf8");
  for (const id of ["result-chart", "release-chart-mttu", "release-chart-mttr"]) {
    const canvas = html.match(new RegExp(`<canvas id="${id}"[^>]*>`));
    assert.ok(canvas, `the canvas ${id} is missing`);
    assert.match(canvas[0], /role="img"/);
    assert.match(canvas[0], /aria-label="[^"]*The table below holds the same values\."/);
  }
});

test("the results card shows the Explanation under the result table", () => {
  const html = readFileSync(join(siteDir, "index.html"), "utf8");
  const card = html.slice(html.indexOf('id="results-card"'));
  assert.ok(card.indexOf('id="results-explanation"') > card.indexOf('id="result-table"'));
});

test("the page draws the Explanation from the blocks of explanation.js", () => {
  const code = readFileSync(join(siteDir, "static", "js", "webapp.js"), "utf8");
  assert.match(code, /explanationBlocks\(job\)/);
  assert.doesNotMatch(code, /What the results mean|"Limitations"/);
});

test("the page shows errors in in-page notifications with role=\"alert\", never window.alert()", () => {
  const html = readFileSync(join(siteDir, "index.html"), "utf8");
  for (const id of ["job-notice", "results-notice", "package-limit"]) {
    assert.match(html, new RegExp(`<div[^>]*id="${id}"`), `the container ${id} is missing`);
  }
  const jsDir = join(siteDir, "static", "js");
  const notification = readFileSync(join(jsDir, "notification.js"), "utf8");
  assert.match(notification, /setAttribute\("role", "alert"\)/);
  for (const name of ["webapp.js", "index.js", "comparison.js", "errorNotice.js", "notification.js"]) {
    const code = readFileSync(join(jsDir, name), "utf8").replace(/\/\/.*$/gm, "");
    assert.doesNotMatch(code, /\balert\s*\(/, `${name} calls alert()`);
  }
});

test("the Try It section has the Sample Comparison button and the label of its date", () => {
  const html = readFileSync(join(siteDir, "index.html"), "utf8");
  const form = html.match(/<form[^>]*id="comparison-form"[^>]*>([\s\S]*?)<\/form>/);
  assert.ok(form, "the form is missing");
  const button = form[1].match(/<button[^>]*id="show-sample"[^>]*>([\s\S]*?)<\/button>/);
  assert.ok(button, "the Sample Comparison button is missing");
  // type="button", so that a click does not submit the form (no Job).
  assert.match(button[0], /type="button"/);
  assert.match(button[1], /Show a sample Comparison/);
  const card = html.slice(html.indexOf('id="results-card"'));
  assert.match(card, /<p[^>]*id="sample-label"[^>]*hidden/);
});

test("the Sample Comparison loads from the published folder, not from the Host", () => {
  const code = readFileSync(join(siteDir, "static", "js", "webapp.js"), "utf8");
  const loader = code.match(/async function showSample\(\) \{([\s\S]*?)\n\}/);
  assert.ok(loader, "showSample() is missing in webapp.js");
  assert.match(loader[1], /fetch\(SAMPLE_COMPARISON_PATH\)/);
  assert.doesNotMatch(loader[1], /hostUrl|EventSource/);
  // A failure shows the notice of the sample, not the notice of a lost Host connection.
  assert.match(loader[1], /sampleErrorNotice\(\)/);
  assert.doesNotMatch(loader[1], /connectionErrorNotice/);
  assert.match(loader[1], /sampleUrl\(/);
});
