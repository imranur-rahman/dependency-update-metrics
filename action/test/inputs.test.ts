// Tests for the check of the Action inputs (src/inputs.ts).
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { join } from "node:path";

import { parseInputs } from "../src/inputs.ts";

const ACTION_YML = readFileSync(join(import.meta.dirname, "..", "action.yml"), "utf8");

test("the defaults: the packages mode, deps.dev, a window of 2 years, no weighting, and the image index", () => {
  const parsed = parseInputs({ packages: "express, koa", ecosystem: "npm" });
  assert.ok(parsed.ok);
  const { config } = parsed;
  assert.equal(config.mode, "packages");
  assert.deepEqual(config.packages, ["express", "koa"]);
  assert.equal(config.ecosystem, "npm");
  assert.equal(config.settings.dataSource, "depsdev");
  assert.equal(config.settings.windowYears, 2);
  assert.equal(config.settings.weightingType, "disable");
  assert.equal(config.settings.halfLife, null);
  assert.equal(config.settings.packageTimeoutSeconds, 600);
  assert.equal(config.settings.parallelRuns, 2);
  assert.deepEqual(config.thresholds, { mttu: null, mttr: null });
  assert.equal(config.osvIndex, "image");
  assert.equal(config.badgeDir, "mttx-badges");
});

test("the window, the weighting, the data source, and the thresholds come from the inputs", () => {
  const parsed = parseInputs({
    packages: "requests",
    ecosystem: "pypi",
    "data-source": "native",
    "start-date": "2022-01-01",
    "end-date": "2024-01-01",
    "weighting-type": "exponential",
    "half-life": "90",
    "max-mttu-days": "100",
    "max-mttr-days": "7.5",
    "osv-index": "fresh",
  });
  assert.ok(parsed.ok);
  const { config } = parsed;
  assert.equal(config.settings.dataSource, "native");
  assert.equal(config.settings.startDate, "2022-01-01");
  assert.equal(config.settings.endDate, "2024-01-01");
  assert.equal(config.settings.halfLife, 90);
  assert.deepEqual(config.thresholds, { mttu: 100, mttr: 7.5 });
  assert.equal(config.osvIndex, "fresh");
});

test("the pull-request mode needs no packages and no ecosystem", () => {
  const parsed = parseInputs({ mode: "pull-request" });
  assert.ok(parsed.ok);
  assert.equal(parsed.config.mode, "pull-request");
  assert.deepEqual(parsed.config.packages, []);
});

test("an invalid input gives a message that names the input of the Action", () => {
  const message = (inputs: Record<string, string>) => {
    const parsed = parseInputs(inputs);
    return parsed.ok ? "" : parsed.message;
  };
  assert.match(message({ mode: "nightly" }), /^Input mode "nightly" is not one of packages, pull-request\./);
  assert.match(message({ ecosystem: "npm" }), /^Input packages needs 1 to 5 package names/);
  assert.match(message({ packages: "express" }), /^Input ecosystem "" is not one of npm, pypi, cargo\./);
  assert.match(message({ packages: "Bad Name!", ecosystem: "npm" }), /^Input packages: /);
  assert.match(message({ packages: "a", ecosystem: "npm", "window-years": "0" }), /^Input window-years must be/);
  assert.match(message({ packages: "a", ecosystem: "npm", "weighting-type": "exponential" }), /half-life from 1 to 3650/);
  assert.match(message({ packages: "a", ecosystem: "npm", "start-date": "2024-02-30" }), /^Input start-date "2024-02-30"/);
  assert.match(message({ packages: "a", ecosystem: "npm", "max-mttr-days": "soon" }), /^Input max-mttr-days must be a number of 0 or more\./);
  assert.match(message({ packages: "a", ecosystem: "npm", "osv-index": "none" }), /^Input osv-index "none" is not one of image, fresh\./);
});

test("action.yml declares each input that the check reads, with the same default", () => {
  for (const [name, fallback] of [
    ["mode", "packages"],
    ["packages", ""],
    ["ecosystem", ""],
    ["data-source", "depsdev"],
    ["window-years", "2"],
    ["start-date", ""],
    ["end-date", ""],
    ["weighting-type", "disable"],
    ["half-life", ""],
    ["package-timeout-seconds", "600"],
    ["parallel-runs", "2"],
    ["max-mttu-days", ""],
    ["max-mttr-days", ""],
    ["osv-index", "image"],
    ["badge-dir", "mttx-badges"],
  ]) {
    const block = new RegExp(`\\n  ${name}:\\n(?:    .*\\n)*?    default: '${fallback}'\\n`);
    assert.match(ACTION_YML, block, `input ${name} with default '${fallback}'`);
  }
});
