// The `mttx.*` settings as plain values: their check, the analysis window, and the Tool
// request that they make. This module has no `vscode` import; src/settings.ts reads the
// values from VS Code and passes them here.

import type { OsvIndexState } from "./osvIndex.ts";
import type { ResultContext } from "./resultCache.ts";
import type { PackageKey } from "./resultStore.ts";
import {
  DEFAULT_WINDOW_YEARS,
  windowStartDate,
  type DataSource,
  type ToolRequest,
  WEIGHTING_TYPES,
  type WeightingType,
} from "./tool.ts";

const DATA_SOURCES: readonly DataSource[] = ["depsdev", "native"];

// Exponential weighting needs a half-life in this range of days. The Webapp and the Host
// check the same rule (webapp/site/static/js/comparison.js).
const MIN_HALF_LIFE_DAYS = 1;
const MAX_HALF_LIFE_DAYS = 3650;

const DEFAULT_PACKAGE_TIMEOUT_SECONDS = 600;
const MIN_PACKAGE_TIMEOUT_SECONDS = 10;
const DEFAULT_CACHE_SIZE_LIMIT_GB = 5;

// The checked settings that change a Tool run. An empty startDate, endDate, or cacheFolder
// means "not set".
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
}

export type ParsedSettings = { ok: true; settings: Settings } | { ok: false; message: string };

// Check the raw values of the `mttx.*` settings (keys without the prefix). A missing value
// gets its default.
export function parseSettings(raw: Record<string, unknown>): ParsedSettings {
  const dataSource = raw.dataSource ?? "depsdev";
  if (!DATA_SOURCES.includes(dataSource as DataSource)) {
    return invalid(`mttx.dataSource "${dataSource}" is not one of ${DATA_SOURCES.join(", ")}.`);
  }
  const weightingType = raw.weightingType ?? "disable";
  if (!WEIGHTING_TYPES.includes(weightingType as WeightingType)) {
    return invalid(`mttx.weightingType "${weightingType}" is not one of ${WEIGHTING_TYPES.join(", ")}.`);
  }
  const windowYears = raw.windowYears ?? DEFAULT_WINDOW_YEARS;
  if (!Number.isInteger(windowYears) || (windowYears as number) < 1) {
    return invalid("mttx.windowYears must be a whole number of 1 or more.");
  }
  const startDate = String(raw.startDate ?? "").trim();
  const endDate = String(raw.endDate ?? "").trim();
  for (const [key, value] of [["startDate", startDate], ["endDate", endDate]]) {
    if (value !== "" && !isDate(value)) {
      return invalid(`mttx.${key} "${value}" is not a date in the form YYYY-MM-DD.`);
    }
  }
  if (startDate !== "" && endDate !== "" && startDate >= endDate) {
    return invalid("mttx.startDate must be before mttx.endDate.");
  }
  let halfLife: number | null = null;
  if (weightingType === "exponential") {
    halfLife = raw.halfLife as number;
    if (typeof halfLife !== "number" || !(halfLife >= MIN_HALF_LIFE_DAYS && halfLife <= MAX_HALF_LIFE_DAYS)) {
      return invalid(
        `Exponential weighting needs mttx.halfLife from ${MIN_HALF_LIFE_DAYS} to ${MAX_HALF_LIFE_DAYS} days.`,
      );
    }
  }
  const packageTimeoutSeconds = raw.packageTimeoutSeconds ?? DEFAULT_PACKAGE_TIMEOUT_SECONDS;
  if (!Number.isInteger(packageTimeoutSeconds) || (packageTimeoutSeconds as number) < MIN_PACKAGE_TIMEOUT_SECONDS) {
    return invalid(`mttx.packageTimeoutSeconds must be a whole number of ${MIN_PACKAGE_TIMEOUT_SECONDS} or more.`);
  }
  const cacheSizeLimitGb = raw.cacheSizeLimitGb ?? DEFAULT_CACHE_SIZE_LIMIT_GB;
  if (typeof cacheSizeLimitGb !== "number" || !(cacheSizeLimitGb >= 0)) {
    return invalid("mttx.cacheSizeLimitGb must be a number of 0 or more.");
  }
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
      cacheFolder: String(raw.cacheFolder ?? "").trim(),
      cacheSizeLimitGb,
    },
  };
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
  return windowStartDate(date, 0) === text;
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
    endDate: windowStartDate(end, 0),
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
