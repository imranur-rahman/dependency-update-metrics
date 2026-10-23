// The command "MTTX: Compare packages" (mttx.compare): it asks for the ecosystem and 1 to 5
// package names, runs the Tool for each package in a Window summary with the settings, and
// shows the Comparison in a webview (src/webview/main.ts).
import { randomUUID } from "node:crypto";

import * as vscode from "vscode";

import {
  COMPARISON_ECOSYSTEMS,
  comparisonJob,
  parsePackageNames,
  type CompareArgs,
  type ComparisonReport,
  type ComparisonRequest,
  type PanelMessage,
  type WebviewMessage,
} from "./core/comparison.ts";
import type { Ecosystem } from "./core/manifest.ts";
import type { PackageKey } from "./core/resultStore.ts";
import type { ToolOutcome } from "./core/tool.ts";
import { toolRequest } from "./core/settings.ts";
import { readSettings } from "./settings.ts";

const TITLE = "MTTX: Compare packages";

// Run one Comparison. analyze runs the Tool through the result store of the hints
// (`ResultStore.analyze`), with its queue limit (`mttx.parallelRuns`), its one-day cache, and
// the request of the settings, so that a Comparison reuses the hint values and gives the same
// values. A settings change resets that store, and its runs end with a failed outcome, which
// the table shows as a package error. Return what the webview drew, or undefined when the
// user cancels.
export async function compare(
  extensionUri: vscode.Uri,
  analyze: (keys: PackageKey[]) => Promise<ToolOutcome[]>,
  args: unknown,
): Promise<ComparisonReport | undefined> {
  const parsed = readSettings();
  if (!parsed.ok) {
    void vscode.window.showErrorMessage(`MTTX: ${parsed.message}`);
    return undefined;
  }
  const given = compareArgs(args);
  const ecosystem =
    given.ecosystem ??
    ((await vscode.window.showQuickPick([...COMPARISON_ECOSYSTEMS], {
      title: TITLE,
      placeHolder: "The ecosystem of the packages",
    })) as Ecosystem | undefined);
  if (!ecosystem) return undefined;

  const value = given.packages?.length ? `${given.packages.join(", ")}, ` : "";
  const text = await vscode.window.showInputBox({
    title: TITLE,
    prompt: `1 to 5 ${ecosystem} package names, split by commas or spaces`,
    value,
    valueSelection: [value.length, value.length],
    validateInput: (input) => {
      const names = parsePackageNames(ecosystem, input);
      return "error" in names ? names.error : undefined;
    },
  });
  if (text === undefined) return undefined;
  const names = parsePackageNames(ecosystem, text);
  if ("error" in names) return undefined;

  // The window and the weighting of the settings, the same as in each Tool request.
  const window = toolRequest({ ecosystem, name: names.names[0] }, parsed.settings, new Date(), "");
  const request: ComparisonRequest = {
    ecosystem,
    packages: names.names,
    startDate: window.startDate,
    endDate: window.endDate,
    weightingType: window.weightingType,
    halfLife: window.halfLife,
    dataSource: window.dataSource,
  };
  const panel = new ComparisonPanel(extensionUri);
  panel.show({ type: "status", text: `Computing MTTU and MTTR of ${names.names.join(", ")}…` });
  // A closed panel stops the wait. The runs go on and fill the hints and the cache.
  const keys = names.names.map((name) => ({ ecosystem, name }));
  const outcomes = await Promise.race([analyze(keys), panel.rendered]);
  if (!Array.isArray(outcomes)) return undefined;
  panel.show({ type: "job", job: comparisonJob(request, outcomes) });
  return panel.rendered;
}

// Keep only the arguments of the right types. The hover link gives them, and any command
// URI can, so the command checks them.
function compareArgs(args: unknown): CompareArgs {
  const raw = (args ?? {}) as Record<string, unknown>;
  const ecosystem = COMPARISON_ECOSYSTEMS.find((name) => name === raw.ecosystem);
  const packages = Array.isArray(raw.packages)
    ? raw.packages.filter((name): name is string => typeof name === "string")
    : undefined;
  return { ecosystem, packages };
}

// One webview panel of a Comparison. The page asks for the last message when it loads
// ("ready"), so a message that the panel sends before the page listens is not lost.
class ComparisonPanel {
  readonly rendered: Promise<ComparisonReport | undefined>;
  private readonly panel: vscode.WebviewPanel;
  private last: PanelMessage | undefined;

  constructor(extensionUri: vscode.Uri) {
    const dist = vscode.Uri.joinPath(extensionUri, "dist");
    this.panel = vscode.window.createWebviewPanel("mttx.comparison", "MTTX Comparison", vscode.ViewColumn.Beside, {
      enableScripts: true,
      localResourceRoots: [dist],
    });
    this.panel.webview.html = pageHtml(this.panel.webview, vscode.Uri.joinPath(dist, "webview.js"));
    this.rendered = new Promise((resolve) => {
      this.panel.onDidDispose(() => resolve(undefined));
      this.panel.webview.onDidReceiveMessage((message: WebviewMessage) => {
        if (message.type === "ready" && this.last) void this.panel.webview.postMessage(this.last);
        if (message.type === "rendered") resolve(message.report);
      });
    });
  }

  show(message: PanelMessage): void {
    this.last = message;
    void this.panel.webview.postMessage(message);
  }
}

// The page loads only the bundled script, with a nonce, and its own style element. Chart.js
// sets the canvas sizes through the CSSOM, which the policy does not block.
function pageHtml(webview: vscode.Webview, script: vscode.Uri): string {
  const nonce = randomUUID().replace(/-/g, "");
  const csp = [
    "default-src 'none'",
    `img-src ${webview.cspSource} data:`,
    `style-src 'nonce-${nonce}'`,
    `script-src 'nonce-${nonce}'`,
  ].join("; ");
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="Content-Security-Policy" content="${csp}">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MTTX Comparison</title>
<style nonce="${nonce}">
  body { color: var(--vscode-foreground); background: var(--vscode-editor-background); font-family: var(--vscode-font-family); font-size: var(--vscode-font-size); padding: 0 16px 16px; }
  .note, .status { color: var(--vscode-descriptionForeground); }
  .chart-box { margin-top: 12px; margin-bottom: 12px; }
  table.results { border-collapse: collapse; margin-top: 12px; }
  table.results th, table.results td { border-bottom: 1px solid var(--vscode-panel-border); padding: 4px 12px 4px 0; text-align: left; vertical-align: top; }
</style>
</head>
<body>
<main id="app"></main>
<script nonce="${nonce}" src="${webview.asWebviewUri(script)}"></script>
</body>
</html>`;
}
