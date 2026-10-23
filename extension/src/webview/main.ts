// The page code of the Comparison webview. esbuild bundles it with Chart.js and the Webapp
// comparison.js into dist/webview.js, so that the page loads no script from the network
// and the content security policy stays strict. The page draws the Job that the panel
// (src/comparisonPanel.ts) sends, with the same charts and table as the Webapp.
/// <reference lib="dom" />

import Chart from "chart.js/auto";
import type { ChartOptions } from "chart.js";

import {
  barChartData,
  barChartOptions,
  chartBoxStyle,
  chartHasValues,
  lineChartData,
  lineChartOptions,
  resultRows,
  resultsNote,
  tableColumns,
} from "../../../webapp/site/static/js/comparison.js";
import { explanationBlocks } from "../../../webapp/site/static/js/explanation.js";
import {
  barChartJob,
  comparisonRows,
  explanationContent,
  noDependenciesNotes,
  PACKAGE_COLORS,
  PACKAGE_POINT_STYLES,
  SERIES_COLORS,
  type ComparisonJob,
  type ExplanationBlock,
  type ExplanationContent,
  type PanelMessage,
  type WebviewMessage,
} from "../core/comparison.ts";

declare function acquireVsCodeApi(): {
  postMessage(message: WebviewMessage): void;
  getState(): PanelMessage | undefined;
  setState(state: PanelMessage): void;
};

const vscode = acquireVsCodeApi();
const app = document.getElementById("app")!;
const violations: string[] = [];
let charts: Chart[] = [];
// The webview drops the page when the panel is hidden; the state brings back the last view.
let current = vscode.getState();

document.addEventListener("securitypolicyviolation", (event) => {
  violations.push(`${event.violatedDirective} ${event.blockedURI}`);
});

window.addEventListener("message", (event: MessageEvent<PanelMessage>) => {
  if (event.data?.type !== "status" && event.data?.type !== "job") return;
  current = event.data;
  vscode.setState(current);
  render();
});

// VS Code changes the class of the body (vscode-light, vscode-dark, vscode-high-contrast)
// when the theme changes. The charts then take the new theme colors.
new MutationObserver(render).observe(document.body, { attributes: true, attributeFilter: ["class"] });

render();
vscode.postMessage({ type: "ready" });

function render(): void {
  for (const chart of charts) chart.destroy();
  charts = [];
  app.replaceChildren();
  if (!current) return;
  if (current.type === "status") {
    app.append(element("p", current.text, "status"));
    return;
  }
  const job = current.job;
  useThemeColors();
  app.append(element("p", resultsNote(job.comparison.weighting_type), "note"));
  const perRelease = job.comparison.mode === "per_release";
  const drawn = perRelease ? drawLineCharts(job) : drawBarChart(job);
  // A package without dependencies has no bar; a note under the chart gives its text.
  const notes = perRelease ? [] : noDependenciesNotes(job);
  for (const note of notes) app.append(element("p", note, "note"));
  const explanation = drawExplanation(job);
  const rows = drawTable(job);
  vscode.postMessage({
    type: "rendered",
    report: { charts: drawn, notes, explanation, rows, violations: [...violations] },
  });
}

// Add the Explanation under the charts: the blocks of explanation.js, the same as in the
// Webapp. A Job without a value adds nothing. Return the text that the report gives.
function drawExplanation(job: ComparisonJob): ExplanationContent {
  const blocks = explanationBlocks(job) as ExplanationBlock[];
  if (blocks.length === 0) return explanationContent(job);
  const box = element("section", "", "explanation");
  for (const block of blocks) {
    if (block.kind === "heading") box.append(element("h3", block.text));
    else if (block.kind === "subheading") box.append(element("p", block.text, "note"));
    else if (block.kind === "paragraph") box.append(element("p", `${block.title}: ${block.text}`));
    else {
      const list = element("ul", "", "note");
      for (const text of block.items) list.append(element("li", text));
      box.append(list);
    }
  }
  app.append(box);
  return explanationContent(job);
}

// Chart.js draws on a canvas, so it does not see the CSS variables of the theme.
function useThemeColors(): void {
  const style = getComputedStyle(document.body);
  const variable = (name: string, fallback: string) => style.getPropertyValue(name).trim() || fallback;
  Chart.defaults.color = variable("--vscode-foreground", "#888888");
  Chart.defaults.borderColor = variable("--vscode-panel-border", "rgba(128, 128, 128, 0.3)");
  Chart.defaults.font.family = variable("--vscode-font-family", Chart.defaults.font.family ?? "sans-serif");
}

// Draw the grouped bar chart of a Window summary Job. A package without dependencies has no
// slot in the chart, and a package without a value has no bar.
function drawBarChart(job: ComparisonJob): string[] {
  const chartJob = barChartJob(job);
  const data = barChartData(chartJob);
  if (!chartHasValues(data)) return [];
  for (const dataset of data.datasets) {
    const color = SERIES_COLORS[dataset.label];
    Object.assign(dataset, { backgroundColor: color, borderColor: color });
  }
  const options = barChartOptions(chartJob) as ChartOptions<"bar">;
  charts.push(new Chart(chartCanvas("bar"), { type: "bar", data, options }));
  return ["bar"];
}

// Draw the MTTU and the MTTR line charts of a Per-release Job: one line for each package.
function drawLineCharts(job: ComparisonJob): string[] {
  const metrics = ["mttu", "mttr"];
  const chartData = metrics.map((metric) => lineChartData(job, metric));
  if (!chartData.some(chartHasValues)) return [];
  metrics.forEach((metric, chartIndex) => {
    const data = chartData[chartIndex];
    data.datasets.forEach((dataset: Record<string, unknown>, index: number) => {
      const color = PACKAGE_COLORS[index % PACKAGE_COLORS.length];
      Object.assign(dataset, {
        borderColor: color,
        backgroundColor: color,
        pointStyle: PACKAGE_POINT_STYLES[index % PACKAGE_POINT_STYLES.length],
        spanGaps: false,
      });
    });
    const canvas = chartCanvas(metric);
    const options = lineChartOptions(job) as ChartOptions<"line">;
    charts.push(new Chart(canvas, { type: "line", data, options }));
  });
  return metrics;
}

// Add a chart box with the shared size and position of the Webapp, and return its canvas.
function chartCanvas(id: string): HTMLCanvasElement {
  const box = element("div", "", "chart-box");
  Object.assign(box.style, chartBoxStyle());
  const canvas = document.createElement("canvas");
  canvas.id = id;
  box.append(canvas);
  app.append(box);
  return canvas;
}

// Add the result table, and return the text of its cells, the header row first.
function drawTable(job: ComparisonJob): string[][] {
  const columns = tableColumns(job.comparison.mode) as Array<[string, string]>;
  const rows: Array<Record<string, unknown>> =
    job.comparison.mode === "per_release" ? resultRows(job) : comparisonRows(job);
  const text = [columns.map(([, header]) => header), ...rows.map((row) => columns.map(([key]) => String(row[key])))];
  const table = element("table", "", "results");
  const head = table.createTHead().insertRow();
  for (const header of text[0]) head.append(element("th", header));
  const body = table.createTBody();
  for (const cells of text.slice(1)) {
    const row = body.insertRow();
    for (const cell of cells) row.insertCell().textContent = cell;
  }
  app.append(table);
  return text;
}

function element<K extends keyof HTMLElementTagNameMap>(tag: K, text: string, className = ""): HTMLElementTagNameMap[K] {
  const node = document.createElement(tag);
  node.textContent = text;
  if (className) node.className = className;
  return node;
}
