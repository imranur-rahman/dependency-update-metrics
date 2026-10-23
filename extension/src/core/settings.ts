// The `mttx.*` settings as plain values: their defaults and ranges, their check, the
// analysis window, and the Tool request that they make. This module has no `vscode` import;
// src/settings.ts reads the values from VS Code and passes them here.

import { halfLifeError } from "../../../webapp/site/static/js/comparison.js";
import type { OsvIndexState } from "./osvIndex.ts";
import type { ResultContext } from "./resultCache.ts";
import type { PackageKey } from "./resultStore.ts";
import {
  DEFAULT_WINDOW_YEARS,
  formatDate,
  windowStartDate,
  type DataSource,
  type ToolRequest,
  WEIGHTING_TYPES,
  type WeightingType,
} from "./tool.ts";

export const DATA_SOURCES: readonly DataSource[] = ["depsdev", "native"];

// The checked `mttx.*` settings. An empty startDate, endDate, cacheFolder, or toolPath means
// "not set".
export interface Settings {
  dataSource: DataSource;
  windowYears: number;
  startDate: string;
  endDate: string;
  weightingType: WeightingType;
  halfLife: number | null;
  packageTimeoutSeconds: number;
  cacheFolder: string;
  cacheSizeLimitGb: number;
  toolPath: string;
  parallelRuns: number;
  automaticRuns: boolean;
}

// The default of each setting. package.json repeats them for the Settings editor, and a unit
// test checks that both hold the same values.
export const DEFAULT_SETTINGS: Readonly<Settings> = {
  dataSource: "depsdev",
  windowYears: DEFAULT_WINDOW_YEARS,
  startDate: "",
  endDate: "",
  weightingType: "disable",
  halfLife: null,
  packageTimeoutSeconds: 600,
  cacheFolder: "",
  cacheSizeLimitGb: 5,
  toolPath: "",
  parallelRuns: 2,
  automaticRuns: true,
};

// The ranges of the number settings. package.json repeats them, and a unit test checks that
// both hold the same values. The half-life range is the range of `halfLifeError` in the
// Webapp comparison.js, which the check uses; a unit test pins it too.
export const SETTING_RANGES = {
  windowYears: { minimum: 1 },
  halfLife: { minimum: 1, maximum: 3650 },
  packageTimeoutSeconds: { minimum: 10 },
  cacheSizeLimitGb: { minimum: 0 },
  parallelRuns: { minimum: 1, maximum: 16 },
} as const;

export type ParsedSettings = { ok: true; settings: Settings } | { ok: false; message: string };

// Check the raw values of the `mttx.*` settings (keys without the prefix). A missing value
// gets its default.
export function parseSettings(raw: Partial<Record<keyof Settings, unknown>>): ParsedSettings {
  const value = <K extends keyof Settings>(key: K): unknown => raw[key] ?? DEFAULT_SETTINGS[key];
  const dataSource = value("dataSource");
  if (!DATA_SOURCES.includes(dataSource as DataSource)) {
    return invalid(`mttx.dataSource "${dataSource}" is not one of ${DATA_SOURCES.join(", ")}.`);
  }
  const weightingType = value("weightingType");
  if (!WEIGHTING_TYPES.includes(weightingType as WeightingType)) {
    return invalid(`mttx.weightingType "${weightingType}" is not one of ${WEIGHTING_TYPES.join(", ")}.`);
  }
  const windowYears = value("windowYears");
  if (!isWholeNumber(windowYears, SETTING_RANGES.windowYears)) {
    return invalid(`mttx.windowYears must be a whole number of ${SETTING_RANGES.windowYears.minimum} or more.`);
  }
  const startDate = String(value("startDate")).trim();
  const endDate = String(value("endDate")).trim();
  for (const [key, date] of [["startDate", startDate], ["endDate", endDate]]) {
    if (date !== "" && !isDate(date)) {
      return invalid(`mttx.${key} "${date}" is not a date in the form YYYY-MM-DD.`);
    }
  }
  if (startDate !== "" && endDate !== "" && startDate >= endDate) {
    return invalid("mttx.startDate must be before mttx.endDate.");
  }
  let halfLife: number | null = null;
  if (weightingType === "exponential") {
    halfLife = raw.halfLife as number;
    if (typeof halfLife !== "number" || halfLifeError(weightingType, String(halfLife)) !== null) {
      const { minimum, maximum } = SETTING_RANGES.halfLife;
      return invalid(`Exponential weighting needs mttx.halfLife from ${minimum} to ${maximum} days.`);
    }
  }
  const packageTimeoutSeconds = value("packageTimeoutSeconds");
  if (!isWholeNumber(packageTimeoutSeconds, SETTING_RANGES.packageTimeoutSeconds)) {
    const { minimum } = SETTING_RANGES.packageTimeoutSeconds;
    return invalid(`mttx.packageTimeoutSeconds must be a whole number of ${minimum} or more.`);
  }
  const cacheSizeLimitGb = value("cacheSizeLimitGb");
  if (typeof cacheSizeLimitGb !== "number" || !(cacheSizeLimitGb >= SETTING_RANGES.cacheSizeLimitGb.minimum)) {
    return invalid(`mttx.cacheSizeLimitGb must be a number of ${SETTING_RANGES.cacheSizeLimitGb.minimum} or more.`);
  }
  const parallelRuns = value("parallelRuns");
  if (!isWholeNumber(parallelRuns, SETTING_RANGES.parallelRuns)) {
    const { minimum, maximum } = SETTING_RANGES.parallelRuns;
    return invalid(`mttx.parallelRuns must be a whole number from ${minimum} to ${maximum}.`);
  }
  const automaticRuns = value("automaticRuns");
  if (typeof automaticRuns !== "boolean") return invalid("mttx.automaticRuns must be true or false.");
  return {
    ok: true,
    settings: {
      dataSource: dataSource as DataSource,
      windowYears: windowYears as number,
      startDate,
      endDate,
      weightingType: weightingType as WeightingType,
      halfLife,
      packageTimeoutSeconds: packageTimeoutSeconds as number,
      cacheFolder: String(value("cacheFolder")).trim(),
      cacheSizeLimitGb,
      toolPath: String(value("toolPath")).trim(),
      parallelRuns: parallelRuns as number,
      automaticRuns,
    },
  };
}

// True for a whole number in the range.
function isWholeNumber(value: unknown, range: { minimum: number; maximum?: number }): boolean {
  return Number.isInteger(value) && (value as number) >= range.minimum && (value as number) <= (range.maximum ?? Infinity);
}

// Return the `--output-dir` of the Tool: the cache folder of the settings, or the default
// folder in the global storage of the Extension. The Tool keeps its cache in
// <output-dir>/cache and the OSV index in <output-dir>/osv_database.parquet.
export function toolOutputDir(settings: Settings, defaultDir: string): string {
  return settings.cacheFolder || defaultDir;
}

// Return the Tool arguments that shrink the Tool cache below the size limit. The Tool runs
// only the prune with these arguments, so the Extension runs them apart from the analysis.
export function cachePruneArgs(settings: Settings, defaultDir: string): string[] {
  return ["--output-dir", toolOutputDir(settings, defaultDir), "--cache-prune-gb", String(settings.cacheSizeLimitGb)];
}

function invalid(message: string): ParsedSettings {
  return { ok: false, message };
}

// True for a YYYY-MM-DD text of a date that exists, so "2024-02-30" is false.
function isDate(text: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return false;
  const date = localDate(text);
  return formatDate(date) === text;
}

// Return the Tool request for one package. Without an end date the window ends today, and
// without a start date it starts windowYears before the end date. defaultDir is the
// `--output-dir` when the cache folder setting is empty.
export function toolRequest(key: PackageKey, settings: Settings, today: Date, defaultDir: string): ToolRequest {
  const end = settings.endDate ? localDate(settings.endDate) : today;
  return {
    ecosystem: key.ecosystem,
    packageName: key.name,
    dataSource: settings.dataSource,
    startDate: settings.startDate || windowStartDate(end, settings.windowYears),
    endDate: formatDate(end),
    weightingType: settings.weightingType,
    halfLife: settings.halfLife,
    outputDir: toolOutputDir(settings, defaultDir),
    timeoutSeconds: settings.packageTimeoutSeconds,
  };
}

// Return the context of a cached result: the window definition of the settings and the OSV
// index state. An unknown index state gives undefined, so that the result store skips the
// cache until the Tool gives the state.
export function resultContext(settings: Settings, osvIndex: OsvIndexState | undefined): ResultContext | undefined {
  if (osvIndex === undefined) return undefined;
  const { windowYears, startDate, endDate } = settings;
  return {
    window: { windowYears, startDate, endDate },
    osvIndex: osvIndex.kind === "built" ? osvIndex.date : "none",
  };
}

// Return midnight of a YYYY-MM-DD date in the local time zone.
function localDate(text: string): Date {
  const [year, month, day] = text.split("-").map(Number);
  return new Date(year, month - 1, day);
}
