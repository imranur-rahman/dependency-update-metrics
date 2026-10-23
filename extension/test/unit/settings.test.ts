// Tests for the settings rules of the core module (src/core/settings.ts): the check of the
// `mttx.*` values, the analysis window, and the Tool request that the settings make.
import { test } from "node:test";
import assert from "node:assert/strict";

import { readFileSync } from "node:fs";
import { join } from "node:path";

import {
  cachePruneArgs,
  DATA_SOURCES,
  DEFAULT_SETTINGS,
  parseSettings,
  resultContext,
  SETTING_RANGES,
  toolRequest,
} from "../../src/core/settings.ts";
import { toolArgs, WEIGHTING_TYPES } from "../../src/core/tool.ts";
import { halfLifeError } from "../../../webapp/site/static/js/comparison.js";

const TODAY = new Date("2026-10-22T09:00:00");
const PACKAGE = { ecosystem: "npm" as const, name: "express" };

function argsFor(raw: Record<string, unknown>): string[] {
  const parsed = parseSettings(raw);
  assert.ok(parsed.ok, parsed.ok ? "" : parsed.message);
  return toolArgs(toolRequest(PACKAGE, parsed.settings, TODAY, "/out"));
}

test("the default settings run deps.dev over the 2 years before today without weighting", () => {
  assert.deepEqual(argsFor({}), [
    "--ecosystem", "npm",
    "--package", "express",
    "--json",
    "--depsdev",
    "--start-date", "2024-10-22",
    "--end-date", "2026-10-22",
    "--weighting-type", "disable",
    "--output-dir", "/out",
  ]);
});

test("the native data source, a start and end date, and exponential weighting reach the Tool", () => {
  const args = argsFor({
    dataSource: "native",
    startDate: "2020-01-01",
    endDate: "2023-06-30",
    weightingType: "exponential",
    halfLife: 180,
  });
  assert.deepEqual(args, [
    "--ecosystem", "npm",
    "--package", "express",
    "--json",
    "--start-date", "2020-01-01",
    "--end-date", "2023-06-30",
    "--weighting-type", "exponential",
    "--half-life", "180",
    "--output-dir", "/out",
  ]);
});

test("a window of 3 years with only an end date ends on the end date", () => {
  const args = argsFor({ windowYears: 3, endDate: "2024-02-29" });
  assert.deepEqual(args.slice(6, 10), ["--start-date", "2021-02-28", "--end-date", "2024-02-29"]);
});

test("a window of 5 years with only a start date starts on the start date and ends today", () => {
  const args = argsFor({ windowYears: 5, startDate: "2025-01-15" });
  assert.deepEqual(args.slice(6, 10), ["--start-date", "2025-01-15", "--end-date", "2026-10-22"]);
});

const INVALID: Array<[string, Record<string, unknown>, string]> = [
  [
    "exponential weighting without a half-life",
    { weightingType: "exponential", halfLife: null },
    "Exponential weighting needs mttx.halfLife from 1 to 3650 days.",
  ],
  [
    "a half-life above 3650 days",
    { weightingType: "exponential", halfLife: 4000 },
    "Exponential weighting needs mttx.halfLife from 1 to 3650 days.",
  ],
  ["an unknown weighting type", { weightingType: "log" }, 'mttx.weightingType "log" is not one of disable, linear, exponential, inverse.'],
  ["an unknown data source", { dataSource: "npm" }, 'mttx.dataSource "npm" is not one of depsdev, native.'],
  ["a window of 0 years", { windowYears: 0 }, "mttx.windowYears must be a whole number of 1 or more."],
  ["a window of 1.5 years", { windowYears: 1.5 }, "mttx.windowYears must be a whole number of 1 or more."],
  ["a start date that does not exist", { startDate: "2024-02-30" }, 'mttx.startDate "2024-02-30" is not a date in the form YYYY-MM-DD.'],
  ["an end date in another form", { endDate: "22/10/2026" }, 'mttx.endDate "22/10/2026" is not a date in the form YYYY-MM-DD.'],
  [
    "a start date that is not before the end date",
    { startDate: "2024-05-01", endDate: "2024-05-01" },
    "mttx.startDate must be before mttx.endDate.",
  ],
  ["a package timeout of 5 s", { packageTimeoutSeconds: 5 }, "mttx.packageTimeoutSeconds must be a whole number of 10 or more."],
  ["a negative cache size limit", { cacheSizeLimitGb: -1 }, "mttx.cacheSizeLimitGb must be a number of 0 or more."],
  ["0 parallel runs", { parallelRuns: 0 }, "mttx.parallelRuns must be a whole number from 1 to 16."],
  ["17 parallel runs", { parallelRuns: 17 }, "mttx.parallelRuns must be a whole number from 1 to 16."],
  ["automatic runs that are not on or off", { automaticRuns: "yes" }, "mttx.automaticRuns must be true or false."],
];

test("the default run stops after 600 s and keeps its cache in the default folder", () => {
  const parsed = parseSettings({});
  assert.ok(parsed.ok);
  const request = toolRequest(PACKAGE, parsed.settings, TODAY, "/storage/tool-output");
  assert.equal(request.timeoutSeconds, 600);
  assert.equal(request.outputDir, "/storage/tool-output");
  assert.deepEqual(cachePruneArgs(parsed.settings, "/storage/tool-output"), [
    "--output-dir", "/storage/tool-output", "--cache-prune-gb", "5",
  ]);
});

test("the cache folder, the cache size limit, and the package timeout reach the Tool", () => {
  const parsed = parseSettings({ cacheFolder: " /data/mttx ", cacheSizeLimitGb: 0.5, packageTimeoutSeconds: 120 });
  assert.ok(parsed.ok);
  const request = toolRequest(PACKAGE, parsed.settings, TODAY, "/storage/tool-output");
  assert.equal(request.timeoutSeconds, 120);
  assert.equal(request.outputDir, "/data/mttx");
  assert.deepEqual(cachePruneArgs(parsed.settings, "/storage/tool-output"), [
    "--output-dir", "/data/mttx", "--cache-prune-gb", "0.5",
  ]);
});

for (const [name, raw, message] of INVALID) {
  test(`the settings check fails for ${name}`, () => {
    assert.deepEqual(parseSettings(raw), { ok: false, message });
  });
}

test("a half-life without exponential weighting does not reach the Tool", () => {
  const args = argsFor({ weightingType: "linear", halfLife: 30 });
  assert.equal(args.includes("--half-life"), false);
  assert.deepEqual(args.slice(10, 12), ["--weighting-type", "linear"]);
});

test("the context of a cached result holds the window definition and the OSV index date", () => {
  const parsed = parseSettings({ windowYears: 3, endDate: "2026-01-31" });
  assert.ok(parsed.ok);
  assert.deepEqual(resultContext(parsed.settings, { kind: "built", date: "2026-10-21" }), {
    window: { windowYears: 3, startDate: "", endDate: "2026-01-31" },
    osvIndex: "2026-10-21",
  });
  assert.equal(resultContext(parsed.settings, { kind: "missing" })?.osvIndex, "none");
  // An unknown index state gives no context, so the store skips the cache.
  assert.equal(resultContext(parsed.settings, undefined), undefined);
});

test("the run control and the Tool path are checked settings with defaults", () => {
  const parsed = parseSettings({});
  assert.ok(parsed.ok);
  assert.deepEqual(
    { parallelRuns: parsed.settings.parallelRuns, automaticRuns: parsed.settings.automaticRuns, toolPath: parsed.settings.toolPath },
    { parallelRuns: 2, automaticRuns: true, toolPath: "" },
  );
  const set = parseSettings({ parallelRuns: 4, automaticRuns: false, toolPath: " /opt/bin/dependency-metrics " });
  assert.ok(set.ok);
  assert.deepEqual(
    { parallelRuns: set.settings.parallelRuns, automaticRuns: set.settings.automaticRuns, toolPath: set.settings.toolPath },
    { parallelRuns: 4, automaticRuns: false, toolPath: "/opt/bin/dependency-metrics" },
  );
});

// The Settings editor shows the defaults, the enums, and the ranges of package.json, and the
// settings check uses the core constants, so the two must not drift apart.
const PROPERTIES: Record<string, Record<string, unknown>> = JSON.parse(
  readFileSync(join(import.meta.dirname, "..", "..", "package.json"), "utf8"),
).contributes.configuration.properties;

test("package.json contributes exactly the settings that the core module checks", () => {
  assert.deepEqual(
    Object.keys(PROPERTIES).map((key) => key.replace(/^mttx\./, "")).sort(),
    Object.keys(DEFAULT_SETTINGS).sort(),
  );
});

test("the defaults, the enums, and the ranges of package.json equal the core constants", () => {
  for (const [key, property] of Object.entries(PROPERTIES)) {
    const name = key.replace(/^mttx\./, "") as keyof typeof DEFAULT_SETTINGS;
    assert.deepEqual(property.default, DEFAULT_SETTINGS[name], `default of ${key}`);
    const range = SETTING_RANGES[name as keyof typeof SETTING_RANGES] as { minimum?: number; maximum?: number } | undefined;
    assert.equal(property.minimum, range?.minimum, `minimum of ${key}`);
    assert.equal(property.maximum, range?.maximum, `maximum of ${key}`);
  }
  assert.deepEqual(PROPERTIES["mttx.dataSource"].enum, [...DATA_SOURCES]);
  assert.deepEqual(PROPERTIES["mttx.weightingType"].enum, [...WEIGHTING_TYPES]);
});

test("the half-life range equals the range of the Webapp comparison.js", () => {
  const { minimum, maximum } = SETTING_RANGES.halfLife;
  for (const [days, valid] of [[minimum - 0.5, false], [minimum, true], [maximum, true], [maximum + 1, false]] as const) {
    assert.equal(halfLifeError("exponential", String(days)) === null, valid, `${days} days`);
  }
});
