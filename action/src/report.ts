// The report of the Action: the rows of the `results` output, the threshold check, the
// shields.io endpoint badge files, and the Markdown of the PR comment and the job summary.
// The Markdown uses the no-dependencies text of the Extension and the Explanation of the
// Webapp, so that the comment says what the numbers mean.

import { hasNoDependencies, noDependenciesText, type ToolResult } from "../../extension/src/core/display.ts";
import type { Ecosystem } from "../../extension/src/core/manifest.ts";
import type { ToolOutcome } from "../../extension/src/core/tool.ts";
import { formatDays, resultsNote } from "../../webapp/site/static/js/comparison.js";
import { explanationBlocks } from "../../webapp/site/static/js/explanation.js";
import type { Thresholds } from "./inputs.ts";

// The first line of each comment of the Action. A new push finds the comment by it.
export const COMMENT_MARKER = "<!-- mttx-action -->";

// The Tool outcome of one package, with the manifests that add it (mode pull-request).
export interface PackageOutcome {
  ecosystem: Ecosystem;
  name: string;
  files: string[];
  outcome: ToolOutcome;
}

// One object of the `results` output.
export interface OutputRow {
  ecosystem: Ecosystem;
  package: string;
  mttu: number | null;
  mttr: number | null;
  num_dependencies: number;
  status: "ok" | "no_deps" | "error";
  error: string;
}

const METRICS = ["mttu", "mttr"] as const;
const NO_VALUE = "–";
const DATA_SOURCE_NAMES: Record<string, string> = { depsdev: "deps.dev", native: "the package registries" };

// Return the output row of each package, in order.
export function outputRows(outcomes: PackageOutcome[]): OutputRow[] {
  return outcomes.map(({ ecosystem, name, outcome }) => {
    const row: OutputRow = { ecosystem, package: name, mttu: null, mttr: null, num_dependencies: 0, status: "error", error: "" };
    if (outcome.kind === "failed") return { ...row, error: outcome.message };
    const { result } = outcome;
    const summary = result.summary;
    if (!summary) return { ...row, error: result.error || "the Tool gave no summary" };
    row.num_dependencies = summary.num_dependencies;
    if (!result.error && hasNoDependencies(summary)) return { ...row, status: "no_deps" };
    if (result.error || summary.status !== "ok" || summary.mttu === null || summary.mttr === null) {
      return { ...row, error: result.error || summary.error || "the Tool gave no value" };
    }
    return { ...row, mttu: summary.mttu, mttr: summary.mttr, status: "ok" };
  });
}

// Return one message for each value above its threshold. A package without dependencies or
// with an error has no value, so it never fails the step.
export function thresholdFailures(rows: OutputRow[], thresholds: Thresholds): string[] {
  const messages: string[] = [];
  for (const row of rows) {
    for (const metric of METRICS) {
      if (!isOver(row, metric, thresholds)) continue;
      messages.push(
        `The ${metric.toUpperCase()} of ${row.package} (${row.ecosystem}) is ${formatDays(row[metric])} days, ` +
          `more than max-${metric}-days (${thresholds[metric]}).`,
      );
    }
  }
  return messages;
}

function isOver(row: OutputRow, metric: (typeof METRICS)[number], thresholds: Thresholds): boolean {
  const limit = thresholds[metric];
  const value = row[metric];
  return limit !== null && value !== null && value > limit;
}

// The JSON of a shields.io endpoint badge (https://shields.io/badges/endpoint-badge).
export interface BadgeJson {
  schemaVersion: 1;
  label: string;
  message: string;
  color: string;
}

// Return the badge files: one for each metric of each package, named
// `<ecosystem>-<package>-<metric>.json`. A "/" in a name becomes "__" and other characters
// that are not safe in a file name are removed. The color is red above the threshold,
// bright green at or below it, blue without a threshold, and light grey without a value.
export function badgeFiles(rows: OutputRow[], thresholds: Thresholds): { fileName: string; json: BadgeJson }[] {
  return rows.flatMap((row) =>
    METRICS.map((metric) => {
      const value = row[metric];
      let message = "error";
      let color = "lightgrey";
      if (row.status === "no_deps") message = "no dependencies";
      if (value !== null) {
        message = `${formatDays(value)} days`;
        color = thresholds[metric] === null ? "blue" : isOver(row, metric, thresholds) ? "red" : "brightgreen";
      }
      const safeName = row.package.replace(/\//g, "__").replace(/[^A-Za-z0-9._-]/g, "");
      return {
        fileName: `${row.ecosystem}-${safeName}-${metric}.json`,
        json: { schemaVersion: 1 as const, label: metric.toUpperCase(), message, color },
      };
    }),
  );
}

export interface ReportOptions {
  heading: string;
  thresholds: Thresholds;
  // Show the Manifest column (mode pull-request).
  showFiles: boolean;
  // The build date of the OSV index, when the Action knows it.
  osvIndexDate?: string;
}

// Return the Markdown of the report: the marker, the heading, the window, the table, the
// note on what the numbers are, and the Explanation with its Limitations.
export function reportMarkdown(outcomes: PackageOutcome[], options: ReportOptions): string {
  const rows = outputRows(outcomes);
  const lines = [COMMENT_MARKER, `### ${options.heading}`, ""];
  const results = outcomes.flatMap((item) => (item.outcome.kind === "result" ? [item.outcome.result] : []));
  const first: ToolResult | undefined = results[0];
  if (first) {
    const source = DATA_SOURCE_NAMES[first.data_source] ?? first.data_source;
    const halfLife = first.half_life === null ? "" : `, half-life ${first.half_life} days`;
    lines.push(`Window: ${first.start_date} to ${first.end_date} · Data source: ${source} · Weighting: ${first.weighting_type}${halfLife}`, "");
  }
  const header = ["Package", "Ecosystem", "MTTU (days)", "MTTR (days)", "Dependencies", ...(options.showFiles ? ["Manifest"] : []), "Note"];
  lines.push(tableRow(header), tableRow(header.map(() => "---")));
  rows.forEach((row, index) => {
    const values = METRICS.map((metric) => {
      if (row[metric] === null) return NO_VALUE;
      const text = formatDays(row[metric]);
      return isOver(row, metric, options.thresholds) ? `**${text}**` : text;
    });
    const files = options.showFiles ? [outcomes[index].files.map(inlineCode).join(", ")] : [];
    lines.push(tableRow([inlineCode(row.package), row.ecosystem, ...values, String(row.num_dependencies), ...files, note(row, options.thresholds)]));
  });
  if (first) lines.push("", escapeMarkdown(resultsNote(first.weighting_type)));
  lines.push(...explanationMarkdown(results, options.osvIndexDate));
  return lines.join("\n");
}

// The Note cell: the no-dependencies text, the error, or the thresholds that the row is over.
function note(row: OutputRow, thresholds: Thresholds): string {
  if (row.status === "no_deps") return escapeCell(noDependenciesText(row.package));
  if (row.status === "error") return `error: ${escapeCell(row.error)}`;
  return METRICS.filter((metric) => isOver(row, metric, thresholds))
    .map((metric) => `${metric.toUpperCase()} is more than max-${metric}-days (${thresholds[metric]})`)
    .join("; ");
}

// The Explanation blocks of explanation.js as Markdown.
function explanationMarkdown(results: ToolResult[], osvIndexDate: string | undefined): string[] {
  const job = { results, ...(osvIndexDate ? { osv_index_date: osvIndexDate } : {}) };
  const lines: string[] = [];
  for (const block of explanationBlocks(job)) {
    if (block.kind === "heading" || block.kind === "subheading") {
      if (lines.at(-1) !== "") lines.push("");
      lines.push(`#### ${block.text}`, "");
    }
    if (block.kind === "paragraph") lines.push(`**${escapeMarkdown(block.title)}**: ${escapeMarkdown(block.text)}`, "");
    if (block.kind === "list") lines.push(...block.items.map((item: string) => `- ${escapeMarkdown(item)}`));
  }
  while (lines.at(-1) === "") lines.pop();
  return lines;
}

function tableRow(cells: string[]): string {
  return `| ${cells.join(" | ")} |`;
}

// A table cell on one line, with the table and HTML characters escaped.
function escapeCell(text: string): string {
  return text
    .trim()
    .replace(/\s*\r?\n\s*/g, " ")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\|/g, "\\|");
}

// Escape the inline Markdown characters of a plain text.
function escapeMarkdown(text: string): string {
  return text.replace(/[\\`*_[\]<>|~]/g, "\\$&");
}

// A code span that shows the text literally, also in a table cell.
function inlineCode(text: string): string {
  return `\`${text.replace(/`/g, "'").replace(/\|/g, "\\|")}\``;
}
