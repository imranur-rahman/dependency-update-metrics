// The check of the Action inputs. The window, the weighting, and the data source have the
// names of the `mttx.*` settings of the Extension in kebab case (mttx.windowYears is
// window-years), and the settings check of the Extension core checks them, so that both
// have the same defaults and ranges.

import { COMPARISON_ECOSYSTEMS, parsePackageNames } from "../../extension/src/core/comparison.ts";
import type { Ecosystem } from "../../extension/src/core/manifest.ts";
import { parseSettings, type Settings } from "../../extension/src/core/settings.ts";
import { MAX_PACKAGES } from "../../webapp/site/static/js/comparison.js";

// packages: the Tool runs for the packages in the input `packages`.
// pull-request: the Tool runs for the dependencies that the pull request adds.
export const MODES = ["packages", "pull-request"] as const;
export type Mode = (typeof MODES)[number];

// image: the OSV index inside the image (no download). fresh: the Tool builds the index of
// today with `--build-osv`, and actions/cache keeps it for the day.
export const OSV_INDEX_SOURCES = ["image", "fresh"] as const;
export type OsvIndexSource = (typeof OSV_INDEX_SOURCES)[number];

// The limits of MTTU and MTTR in days. null: no limit.
export interface Thresholds {
  mttu: number | null;
  mttr: number | null;
}

export interface ActionConfig {
  mode: Mode;
  packages: string[];
  ecosystem: Ecosystem | null;
  settings: Settings;
  thresholds: Thresholds;
  osvIndex: OsvIndexSource;
  badgeDir: string;
}

export type ParsedInputs = { ok: true; config: ActionConfig } | { ok: false; message: string };

// The input of each `mttx.*` setting that the Action reads.
const SETTING_INPUTS: Partial<Record<keyof Settings, string>> = {
  dataSource: "data-source",
  windowYears: "window-years",
  startDate: "start-date",
  endDate: "end-date",
  weightingType: "weighting-type",
  halfLife: "half-life",
  packageTimeoutSeconds: "package-timeout-seconds",
  parallelRuns: "parallel-runs",
};

// The settings that are numbers. An input is always text.
const NUMBER_SETTINGS = new Set<keyof Settings>(["windowYears", "halfLife", "packageTimeoutSeconds", "parallelRuns"]);

// Check the inputs (names as in action.yml). An empty or missing input gets its default.
export function parseInputs(inputs: Record<string, string | undefined>): ParsedInputs {
  const input = (name: string) => (inputs[name] ?? "").trim();
  const mode = input("mode") || "packages";
  if (!(MODES as readonly string[]).includes(mode)) return oneOf("mode", mode, MODES);
  const osvIndex = input("osv-index") || "image";
  if (!(OSV_INDEX_SOURCES as readonly string[]).includes(osvIndex)) {
    return oneOf("osv-index", osvIndex, OSV_INDEX_SOURCES);
  }

  let packages: string[] = [];
  let ecosystem: Ecosystem | null = null;
  if (mode === "packages") {
    if (input("packages") === "") {
      return invalid(`Input packages needs 1 to ${MAX_PACKAGES} package names, split by commas or spaces.`);
    }
    const name = input("ecosystem");
    if (!COMPARISON_ECOSYSTEMS.includes(name as Ecosystem)) return oneOf("ecosystem", name, COMPARISON_ECOSYSTEMS);
    ecosystem = name as Ecosystem;
    const parsed = parsePackageNames(ecosystem, input("packages"));
    if ("error" in parsed) return invalid(`Input packages: ${parsed.error}`);
    packages = parsed.names;
  }

  const raw: Partial<Record<keyof Settings, unknown>> = {};
  for (const [key, name] of Object.entries(SETTING_INPUTS) as [keyof Settings, string][]) {
    const text = input(name);
    if (text === "") continue;
    raw[key] = NUMBER_SETTINGS.has(key) && text !== "" && Number.isFinite(Number(text)) ? Number(text) : text;
  }
  const settings = parseSettings(raw);
  if (!settings.ok) {
    const message = settings.message.replace(/mttx\.(\w+)/g, (_, key: keyof Settings) => `input ${SETTING_INPUTS[key]}`);
    return invalid(message.charAt(0).toUpperCase() + message.slice(1));
  }

  const thresholds: Thresholds = { mttu: null, mttr: null };
  for (const metric of ["mttu", "mttr"] as const) {
    const name = `max-${metric}-days`;
    const text = input(name);
    if (text === "") continue;
    const days = Number(text);
    if (!Number.isFinite(days) || days < 0) return invalid(`Input ${name} must be a number of 0 or more.`);
    thresholds[metric] = days;
  }

  return {
    ok: true,
    config: {
      mode: mode as Mode,
      packages,
      ecosystem,
      settings: settings.settings,
      thresholds,
      osvIndex: osvIndex as OsvIndexSource,
      badgeDir: input("badge-dir") || "mttx-badges",
    },
  };
}

function oneOf(name: string, value: string, allowed: readonly string[]): ParsedInputs {
  return invalid(`Input ${name} "${value}" is not one of ${allowed.join(", ")}.`);
}

function invalid(message: string): ParsedInputs {
  return { ok: false, message };
}
