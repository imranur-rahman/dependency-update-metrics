// The Comparison model of the core module: the input of the "MTTX: Compare packages"
// command, and the Job-shaped object that the webview gives to the Webapp comparison.js.
// This module has no `vscode` import, so that `node --test` tests it directly, and the
// webview bundle uses it too.

import { hasNoDependencies, noDependenciesText, type ToolResult } from "./display.ts";
import type { Ecosystem } from "./manifest.ts";
import type { ToolOutcome } from "./tool.ts";
import { packageErrors, resultRows } from "../../../webapp/site/static/js/comparison.js";

// The "comparison" object of a Host Job (contract/examples/).
export interface JobComparison {
  ecosystem: string;
  packages: string[];
  start_date: string;
  end_date: string;
  mode: ToolResult["mode"];
  weighting_type: string;
  half_life: number | null;
}

// The part of a done Host Job that comparison.js reads.
export interface ComparisonJob {
  job_id: string;
  state: "done";
  comparison: JobComparison;
  results: ToolResult[];
}

// What the command asks the Tool for. The window is the fallback for a Comparison where no
// Tool run gives a result; a result carries the window and the weighting of its run.
export interface ComparisonRequest {
  ecosystem: Ecosystem;
  packages: string[];
  startDate: string;
  endDate: string;
}

// One row of the result table (resultRows in comparison.js).
export interface ComparisonRow {
  package: string;
  mttu: string;
  mttr: string;
  dependencies: number;
  error: string;
}

// The ecosystems that the command offers, in the order of the Webapp form.
export const COMPARISON_ECOSYSTEMS: readonly Ecosystem[] = ["npm", "pypi", "cargo"];

// The chart colors of the Webapp (webapp/site/static/js/webapp.js), so that a Comparison
// looks the same in both: MTTU blue and MTTR orange in the bar chart, and one color for each
// package in the line charts. A unit test checks that webapp.js holds the same values.
export const SERIES_COLORS: Readonly<Record<string, string>> = { MTTU: "#2a78d6", MTTR: "#eb6834" };
export const PACKAGE_COLORS: readonly string[] = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4"];

// The arguments of the command "mttx.compare". Without them, the command asks for both.
export interface CompareArgs {
  ecosystem?: Ecosystem;
  packages?: string[];
}

// Return the Markdown link of the hover that starts a Comparison with one dependency. The
// command URI holds the JSON array of the command arguments, URI-encoded.
export function compareLink(dep: { ecosystem: Ecosystem; name: string }): string {
  const args: CompareArgs[] = [{ ecosystem: dep.ecosystem, packages: [dep.name] }];
  const query = encodeURIComponent(JSON.stringify(args)).replace(/[()]/g, (c) => `%${c.charCodeAt(0).toString(16)}`);
  return `[Compare with alternatives](command:mttx.compare?${query})`;
}

export type PackageNames = { names: string[] } | { error: string };

// Return the package names of the command input, or the first error. The names are split
// by commas or spaces. The Webapp rules check them: 1 to 5 names, each one valid in the
// ecosystem, and no name twice.
export function parsePackageNames(ecosystem: Ecosystem, text: string): PackageNames {
  const names = text.split(/[\s,]+/).filter((name) => name !== "");
  const [error] = packageErrors(ecosystem, names) as string[];
  return error ? { error } : { names };
}

// The job ID of each Job that the Extension builds. The Extension has no Host, so the ID
// only fills the field.
const JOB_ID = "mttx-extension";

// Return a done Host Job for the Tool outcomes of a Comparison, one outcome for each
// package in the order of the request, so that comparison.js shows it as the Webapp does.
// The window, the mode, and the weighting come from the first result, because the Tool
// runs of one Comparison share them. A run without a result becomes a package error.
export function comparisonJob(request: ComparisonRequest, outcomes: ToolOutcome[]): ComparisonJob {
  const results = outcomes.map((outcome, index) =>
    outcome.kind === "result"
      ? outcome.result
      : failedResult(request, request.packages[index], outcome.message),
  );
  const first = results[0];
  return {
    job_id: JOB_ID,
    state: "done",
    comparison: {
      ecosystem: request.ecosystem,
      packages: [...request.packages],
      start_date: first.start_date,
      end_date: first.end_date,
      mode: first.mode,
      weighting_type: first.weighting_type,
      half_life: first.half_life,
    },
    results,
  };
}

// Return the table rows of a Window summary Job (resultRows in comparison.js). A package
// without dependencies gets the no-dependencies text of the hints as its error.
export function comparisonRows(job: ComparisonJob): ComparisonRow[] {
  const rows = resultRows(job) as ComparisonRow[];
  return rows.map((row, index) => {
    const { summary, error } = job.results[index];
    if (error || !summary || !hasNoDependencies(summary)) return row;
    return { ...row, error: noDependenciesText(row.package) };
  });
}

function failedResult(request: ComparisonRequest, packageName: string, message: string): ToolResult {
  return {
    ecosystem: request.ecosystem,
    package: packageName,
    mode: "window_summary",
    start_date: request.startDate,
    end_date: request.endDate,
    weighting_type: "disable",
    half_life: null,
    data_source: "depsdev",
    summary: null,
    releases: [],
    error: message,
  };
}
