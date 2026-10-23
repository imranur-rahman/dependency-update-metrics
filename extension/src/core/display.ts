// The display model of the core module: the mapping from a Tool result to the hint text.
// This module has no `vscode` import, so that `node --test` tests it directly.

import { hoverExplanation } from "../../../webapp/site/static/js/explanation.js";

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
// result (which can hold a package error), or the Tool gave no result at all. `computedOn`
// is the local date (YYYY-MM-DD) when the Tool run finished; the hover shows it.
export type DependencyState =
  | { kind: "computing" }
  | { kind: "result"; result: ToolResult; computedOn?: string }
  | { kind: "failed"; message: string };

// The part of a manifest dependency that the hover names.
export interface HoverTarget {
  name: string;
  section: string;
}

const DATA_SOURCE_NAMES: Record<string, string> = {
  depsdev: "deps.dev",
  native: "the package registries",
};

const MAX_ERROR_LENGTH = 50;
const NEEDS_OSV_INDEX = "needs the OSV index";

// Return true when a summary or a release has no dependencies: the Tool gives the status
// "no_deps", or "ok" with 0 dependencies. The Webapp uses the same rule
// (`hasNoDependencies` in webapp/site/static/js/comparison.js).
export function hasNoDependencies(entry: Pick<MetricsEntry, "status" | "num_dependencies">): boolean {
  return entry.status === "no_deps" || (entry.status === "ok" && entry.num_dependencies === 0);
}

// What the hint knows besides the Tool result. `osvIndex: false` means that the OSV index
// does not exist: the Tool then gives an MTTR of 0 that is not true.
export interface HintContext {
  osvIndex: boolean;
}

// Return the inlay hint text for one dependency.
export function hintText(
  state: DependencyState,
  packageName: string,
  context: HintContext = { osvIndex: true },
): string {
  if (state.kind === "computing") return "computing…";
  if (state.kind === "failed") return errorText(state.message);
  const value = resultValue(state.result);
  if (value.kind === "error") return errorText(value.message);
  if (value.kind === "no_deps") return noDependenciesText(packageName);
  const mttr = context.osvIndex ? `${Math.round(value.mttr)} d` : NEEDS_OSV_INDEX;
  return `MTTU ${Math.round(value.mttu)} d · MTTR ${mttr}`;
}

// Return the Markdown text of the hover for one dependency: the details behind the hint,
// or the full error. The text has no command links; the `vscode` layer adds them.
export function hoverMarkdown(
  state: DependencyState,
  target: HoverTarget,
  context: HintContext = { osvIndex: true },
): string {
  const name = inlineCode(target.name);
  const lines: string[] = [`**MTTX** · ${name}`, ""];
  if (state.kind === "computing") {
    lines.push("computing…", "");
  } else if (state.kind === "failed") {
    lines.push("The Tool gave no result:", "", ...codeBlock(state.message), "");
  } else {
    const { result } = state;
    const value = resultValue(result);
    if (value.kind === "error") {
      lines.push(`The Tool could not analyze ${name}:`, "", ...codeBlock(value.message), "");
    } else if (value.kind === "no_deps") {
      lines.push(escapeMarkdown(noDependenciesText(target.name)), "");
    } else {
      const mttr = context.osvIndex ? days(value.mttr) : NEEDS_OSV_INDEX;
      lines.push(`MTTU ${days(value.mttu)} · MTTR ${mttr}`, "");
      // The Explanation line (explanation.js). Without the OSV index, the MTTR of 0 is not
      // true, so the hover has no Explanation.
      const explanation = context.osvIndex ? hoverExplanation(result) : null;
      if (explanation) lines.push(escapeMarkdown(explanation), "");
    }
    lines.push(`- Window: ${result.start_date} to ${result.end_date}`);
    if (value.kind !== "error") {
      lines.push(`- Dependencies of ${name}: ${result.summary?.num_dependencies ?? 0}`);
    }
    lines.push(`- Data source: ${DATA_SOURCE_NAMES[result.data_source] ?? result.data_source}`);
    if (state.computedOn) lines.push(`- Computed on: ${state.computedOn}`);
  }
  lines.push(`- Section: ${inlineCode(target.section)}`);
  return lines.join("\n");
}

type ResultValue =
  | { kind: "ok"; mttu: number; mttr: number }
  | { kind: "no_deps" }
  | { kind: "error"; message: string };

// Return what a Tool result says: the two values, no dependencies, or the full error text.
function resultValue(result: ToolResult): ResultValue {
  const summary = result.summary;
  if (!summary) return { kind: "error", message: result.error || "the Tool gave no summary" };
  if (hasNoDependencies(summary)) return { kind: "no_deps" };
  if (summary.status !== "ok" || summary.mttu === null || summary.mttr === null) {
    return { kind: "error", message: result.error || summary.error || "the Tool gave no value" };
  }
  return { kind: "ok", mttu: summary.mttu, mttr: summary.mttr };
}

// The text of a package without dependencies, in the hint, the hover, and the Comparison.
export function noDependenciesText(packageName: string): string {
  return `could not compute since '${packageName}' has no dependencies`;
}

function days(value: number): string {
  return `${value.toFixed(1)} days`;
}

// Return the lines of a fenced code block that shows `text` literally. The fence is longer
// than any backtick run in the text.
function codeBlock(text: string): string[] {
  const longestRun = Math.max(2, ...(text.match(/`+/g) ?? []).map((run) => run.length));
  const fence = "`".repeat(longestRun + 1);
  return [fence, text.trim(), fence];
}

// Escape the inline Markdown characters of a one-line plain text, so that the hover shows
// it literally. The text never starts a line of the hover, so the block characters stay.
function escapeMarkdown(text: string): string {
  return text.replace(/[\\`*_[\]<>|~&]/g, "\\$&");
}

// Return a Markdown code span that shows `text` literally, also when it holds backticks.
function inlineCode(text: string): string {
  const longestRun = Math.max(0, ...(text.match(/`+/g) ?? []).map((run) => run.length));
  const fence = "`".repeat(longestRun + 1);
  const pad = text.startsWith("`") || text.endsWith("`") ? " " : "";
  return `${fence}${pad}${text}${pad}${fence}`;
}

function errorText(message: string): string {
  const line = message.trim().split(/\r?\n/)[0];
  const short =
    line.length > MAX_ERROR_LENGTH ? `${line.slice(0, MAX_ERROR_LENGTH - 1)}…` : line;
  return `error: ${short}`;
}
