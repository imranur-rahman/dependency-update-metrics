// The Tool runner interface, the command-line arguments of one Tool run, and the rules that
// turn the Tool output into a dependency state. This module has no `vscode` import.

import type { Ecosystem } from "./manifest.ts";
import type { DependencyState, ToolResult } from "./display.ts";

export type DataSource = "depsdev" | "native";

// The values of `--weighting-type` in dependency_metrics/cli.py.
export const WEIGHTING_TYPES = ["disable", "linear", "exponential", "inverse"] as const;
export type WeightingType = (typeof WEIGHTING_TYPES)[number];

// One Tool run: one package of one ecosystem over a window from the start date to the end
// date (both YYYY-MM-DD). src/core/settings.ts makes the request from the `mttx.*` settings.
// halfLife is null unless the weighting is exponential. The runner stops a run after
// timeoutSeconds, because `--package-timeout` works only in the bulk CSV mode of the Tool.
export interface ToolRequest {
  ecosystem: Ecosystem;
  packageName: string;
  dataSource: DataSource;
  startDate: string;
  endDate: string;
  weightingType: WeightingType;
  halfLife: number | null;
  outputDir: string;
  timeoutSeconds: number;
}

// The end state of one Tool run.
export type ToolOutcome = Exclude<DependencyState, { kind: "computing" }>;

// Runs the Tool for one package. The real runner spawns the Tool process
// (src/processToolRunner.ts); #29 gives it the managed venv, and #32 puts a queue and a
// cache in front of it.
export interface ToolRunner {
  run(request: ToolRequest, signal?: AbortSignal): Promise<ToolOutcome>;
}

// The output of one finished Tool process.
export interface ToolOutput {
  stdout: string;
  stderr: string;
  exitCode: number | null;
}

export const DEFAULT_WINDOW_YEARS = 2;

// Return the arguments of `dependency-metrics` for one request, with the JSON output.
export function toolArgs(request: ToolRequest): string[] {
  const args = ["--ecosystem", request.ecosystem, "--package", request.packageName, "--json"];
  if (request.dataSource === "depsdev") args.push("--depsdev");
  args.push("--start-date", request.startDate, "--end-date", request.endDate);
  args.push("--weighting-type", request.weightingType);
  if (request.halfLife !== null) args.push("--half-life", String(request.halfLife));
  args.push("--output-dir", request.outputDir);
  return args;
}

// Return the start date (YYYY-MM-DD) of a window that ends today, in the local time zone.
// A 29 February that does not exist in the start year becomes 28 February.
export function windowStartDate(today: Date, years = DEFAULT_WINDOW_YEARS): string {
  const year = today.getFullYear() - years;
  const month = today.getMonth();
  const lastDay = new Date(year, month + 1, 0).getDate();
  const day = Math.min(today.getDate(), lastDay);
  return `${year}-${pad(month + 1)}-${pad(day)}`;
}

function pad(value: number): string {
  return String(value).padStart(2, "0");
}

// Turn the output of a finished Tool process into an outcome. The Tool prints its JSON
// result also when the package fails (exit code 1), so stdout counts before the exit code.
export function parseToolOutput(output: ToolOutput): ToolOutcome {
  const result = lastJsonObject(output.stdout);
  if (result) return { kind: "result", result };
  const lastLine = output.stderr.trim().split(/\r?\n/).pop()?.trim();
  return {
    kind: "failed",
    message: lastLine || `the Tool printed no result (exit code ${output.exitCode})`,
  };
}

function lastJsonObject(stdout: string): ToolResult | undefined {
  const lines = stdout.trim().split(/\r?\n/).reverse();
  for (const line of lines) {
    if (!line.startsWith("{")) continue;
    try {
      return JSON.parse(line) as ToolResult;
    } catch {
      return undefined;
    }
  }
  return undefined;
}
