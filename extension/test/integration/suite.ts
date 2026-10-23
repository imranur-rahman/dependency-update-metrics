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
  console.log("MTTX integration test passed");
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
