// The integration suite. It runs inside the VS Code extension host (test/integration/run.ts
// starts it), opens the fixture package.json, and reads the inlay hints that the Extension
// gives with the fake Tool.
import assert from "node:assert/strict";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import * as vscode from "vscode";

import type { ComparisonReport } from "../../src/core/comparison.ts";

const TIMEOUT_MS = 30_000;

async function hintLabels(document: vscode.TextDocument): Promise<Map<number, string>> {
  const range = new vscode.Range(0, 0, document.lineCount, 0);
  const hints = await vscode.commands.executeCommand<vscode.InlayHint[]>(
    "vscode.executeInlayHintProvider",
    document.uri,
    range,
  );
  const labels = new Map<number, string>();
  for (const hint of hints) {
    const label = typeof hint.label === "string" ? hint.label : hint.label.map((p) => p.value).join("");
    labels.set(hint.position.line, label);
  }
  return labels;
}

async function hoverText(document: vscode.TextDocument, position: vscode.Position): Promise<string> {
  const hovers = await vscode.commands.executeCommand<vscode.Hover[]>(
    "vscode.executeHoverProvider",
    document.uri,
    position,
  );
  return hovers
    .flatMap((hover) => hover.contents)
    .map((content) => (typeof content === "string" ? content : content.value))
    .join("\n");
}

async function waitFor(check: () => Promise<boolean>): Promise<void> {
  const end = Date.now() + TIMEOUT_MS;
  while (!(await check())) {
    if (Date.now() > end) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}

export async function run(): Promise<void> {
  const folder = vscode.workspace.workspaceFolders![0].uri.fsPath;
  const document = await vscode.workspace.openTextDocument(join(folder, "package.json"));
  await vscode.window.showTextDocument(document);
  await vscode.extensions.getExtension("imranur-rahman.mttx")!.activate();

  // Lines of the fixture: express 5, left-pad 6, nope 9.
  await waitFor(async () => (await hintLabels(document)).size === 3);
  const first = await hintLabels(document);
  assert.equal(first.get(5), "computing…");

  // The fresh user data dir has no OSV index, so MTTR waits for the index (#30).
  const expected = new Map([
    [5, "MTTU 579 d · MTTR needs the OSV index"],
    [6, "could not compute since 'left-pad' has no dependencies"],
    [9, "error: Package not found in deps.dev"],
  ]);
  await waitForLabels(document, expected);

  // The rebuild command builds the index of the fake Tool, and the hints get the MTTR.
  await vscode.commands.executeCommand("mttx.rebuildOsvIndex");
  expected.set(5, "MTTU 579 d · MTTR 579 d");
  await waitForLabels(document, expected);

  // The hover shows the details behind a hint, and the full error of a failed package.
  const expressHover = await hoverText(document, new vscode.Position(5, 6));
  assert.match(expressHover, /Window: \d{4}-\d{2}-\d{2} to \d{4}-\d{2}-\d{2}/);
  assert.match(expressHover, /Section: `dependencies`/);
  const nopeHover = await hoverText(document, new vscode.Position(9, 6));
  assert.match(nopeHover, /Package not found in deps\.dev/);
  assert.match(nopeHover, /Section: `devDependencies`/);
  assert.match(expressHover, /\[Compare with alternatives\]\(command:mttx\.compare\?/);

  await checkComparison();

  // The command analyzes the open manifest. The values are fresh, so the hints stay.
  await vscode.commands.executeCommand("mttx.analyzeDependencies");
  assert.deepEqual(Object.fromEntries(await hintLabels(document)), Object.fromEntries(expected));

  // A settings change refreshes the hints. Exponential weighting without a half-life fails
  // the settings check, so each hint shows the message of the check.
  const config = vscode.workspace.getConfiguration("mttx");
  await config.update("weightingType", "exponential", vscode.ConfigurationTarget.Global);
  await waitFor(async () => (await hintLabels(document)).get(5)?.startsWith("error: Exponential weighting") ?? false);
  await config.update("weightingType", undefined, vscode.ConfigurationTarget.Global);

  await vscode.commands.executeCommand("mttx.openSettings");

  await expectHints(join(folder, "pyproject.toml"), [
    [4, "MTTU 579 d · MTTR 579 d"],
    [8, "pytest: MTTU 579 d · MTTR 579 dleft-pad: could not compute since 'left-pad' has no dependencies"],
  ]);
  await expectHints(join(folder, "Cargo.toml"), [
    [5, "MTTU 579 d · MTTR 579 d"],
    [7, "error: Package not found in deps.dev"],
  ]);
  await checkEditedManifest(folder);
  console.log("MTTX integration test passed");
}

// The arguments of the hover link fill the input box; the test accepts it. The command
// returns what the webview drew, so the test sees that comparison.js and Chart.js ran
// under the content security policy.
async function checkComparison(): Promise<void> {
  let settled = false;
  const reportPromise = Promise.resolve(
    vscode.commands.executeCommand<ComparisonReport | undefined>("mttx.compare", {
      ecosystem: "npm",
      packages: ["express", "left-pad"],
    }),
  ).finally(() => (settled = true));
  await waitFor(async () => {
    if (!settled) await vscode.commands.executeCommand("workbench.action.acceptSelectedQuickOpenItem");
    return settled;
  });
  const report = await reportPromise;
  assert.ok(report, "the command returns the report of the webview");
  assert.deepEqual(report.violations, []);
  assert.deepEqual(report.charts, ["bar"]);
  assert.deepEqual(report.notes, ["could not compute since 'left-pad' has no dependencies"]);
  assert.deepEqual(report.rows, [
    ["Package", "MTTU (days)", "MTTR (days)", "Dependencies", "Error"],
    ["express", "579.0", "579.0", "1", ""],
    ["left-pad", "–", "–", "0", "could not compute since 'left-pad' has no dependencies"],
  ]);
  const tabs = vscode.window.tabGroups.all.flatMap((group) => group.tabs);
  assert.ok(tabs.some((tab) => tab.input instanceof vscode.TabInputWebview && tab.label === "MTTX Comparison"));
}

// A manifest that a tool other than the editor writes (for example `npm install` or a Git
// checkout) gets the hints of its new dependencies.
async function checkEditedManifest(folder: string): Promise<void> {
  const dir = join(folder, "edited");
  const path = join(dir, "package.json");
  const manifest = (deps: Record<string, string>) => `${JSON.stringify({ dependencies: deps }, null, 2)}\n`;
  mkdirSync(dir, { recursive: true });
  try {
    writeFileSync(path, manifest({ express: "^4.18.0" }));
    await expectHints(path, [[2, "MTTU 579 d · MTTR 579 d"]]);
    writeFileSync(path, manifest({ express: "^4.18.0", "left-pad": "1.3.0" }));
    await expectHints(path, [
      [2, "MTTU 579 d · MTTR 579 d"],
      [3, "could not compute since 'left-pad' has no dependencies"],
    ]);
  } finally {
    await vscode.commands.executeCommand("workbench.action.closeActiveEditor");
    rmSync(dir, { recursive: true, force: true });
  }
}

// Open a manifest and wait for the final hint labels on its lines. The labels of the hints on
// one line join without a separator.
async function expectHints(path: string, lines: Array<[number, string]>): Promise<void> {
  const document = await vscode.workspace.openTextDocument(path);
  await vscode.window.showTextDocument(document);
  const expected = new Map(lines);
  let last = new Map<number, string>();
  try {
    await waitFor(async () => {
      last = await joinedHintLabels(document);
      return last.size === expected.size && [...expected].every(([line, label]) => last.get(line) === label);
    });
  } catch {
    assert.deepEqual(Object.fromEntries(last), Object.fromEntries(expected));
  }
}

async function waitForLabels(document: vscode.TextDocument, expected: Map<number, string>): Promise<void> {
  let last = new Map<number, string>();
  try {
    await waitFor(async () => {
      last = await hintLabels(document);
      return [...expected].every(([line, label]) => last.get(line) === label);
    });
  } catch {
    assert.deepEqual(Object.fromEntries(last), Object.fromEntries(expected));
  }
}

async function joinedHintLabels(document: vscode.TextDocument): Promise<Map<number, string>> {
  const range = new vscode.Range(0, 0, document.lineCount, 0);
  const hints = await vscode.commands.executeCommand<vscode.InlayHint[]>(
    "vscode.executeInlayHintProvider",
    document.uri,
    range,
  );
  const labels = new Map<number, string>();
  for (const hint of hints) {
    const label = typeof hint.label === "string" ? hint.label : hint.label.map((p) => p.value).join("");
    labels.set(hint.position.line, (labels.get(hint.position.line) ?? "") + label);
  }
  return labels;
}
