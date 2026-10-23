// The integration suite. It runs inside the VS Code extension host (test/integration/run.ts
// starts it), opens the fixture package.json, and reads the inlay hints that the Extension
// gives with the fake Tool.
import assert from "node:assert/strict";
import { join } from "node:path";

import * as vscode from "vscode";

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
  console.log("MTTX integration test passed");
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
