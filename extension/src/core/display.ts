// The display model of the core module: the mapping from a Tool result to the hint text.
// This module has no `vscode` import, so that `node --test` tests it directly.

// A summary of the Window summary mode, or a release of the Per-release mode, as the Tool
// prints it with `--json` (the same shape as the results in contract/examples/).
export interface MetricsEntry {
  mttu: number | null;
  mttr: number | null;
  num_dependencies: number;
  status: "ok" | "no_deps" | "error";
  error: string;
}

export interface ReleaseEntry extends MetricsEntry {
  package_version: string;
  package_release_date: string;
}

// One result of `dependency-metrics --json`.
export interface ToolResult {
  ecosystem: string;
  package: string;
  mode: "window_summary" | "per_release";
  start_date: string;
  end_date: string;
  weighting_type: string;
  half_life: number | null;
  data_source: "depsdev" | "native";
  summary: MetricsEntry | null;
  releases: ReleaseEntry[];
  error: string;
}

// What the Extension knows about one dependency: the Tool still runs, the Tool gave a
// result (which can hold a package error), or the Tool gave no result at all.
export type DependencyState =
  | { kind: "computing" }
  | { kind: "result"; result: ToolResult }
  | { kind: "failed"; message: string };

const MAX_ERROR_LENGTH = 50;

// Return true when a summary or a release has no dependencies: the Tool gives the status
// "no_deps", or "ok" with 0 dependencies. The Webapp uses the same rule
// (`hasNoDependencies` in webapp/site/static/js/comparison.js).
export function hasNoDependencies(entry: Pick<MetricsEntry, "status" | "num_dependencies">): boolean {
  return entry.status === "no_deps" || (entry.status === "ok" && entry.num_dependencies === 0);
}

// Return the inlay hint text for one dependency.
export function hintText(state: DependencyState, packageName: string): string {
  if (state.kind === "computing") return "computing…";
  if (state.kind === "failed") return errorText(state.message);
  const { result } = state;
  const summary = result.summary;
  if (!summary) return errorText(result.error || "the Tool gave no summary");
  if (hasNoDependencies(summary)) {
    return `could not compute since '${packageName}' has no dependencies`;
  }
  if (summary.status !== "ok" || summary.mttu === null || summary.mttr === null) {
    return errorText(result.error || summary.error || "the Tool gave no value");
  }
  return `MTTU ${Math.round(summary.mttu)} d · MTTR ${Math.round(summary.mttr)} d`;
}

function errorText(message: string): string {
  const line = message.trim().split(/\r?\n/)[0];
  const short =
    line.length > MAX_ERROR_LENGTH ? `${line.slice(0, MAX_ERROR_LENGTH - 1)}…` : line;
  return `error: ${short}`;
}
