// The thin `vscode` layer: it registers the inlay hint provider for package.json and
// connects it to the core module and the Tool runner.
import { mkdirSync } from "node:fs";
import { join } from "node:path";

import * as vscode from "vscode";

import { hintText } from "./core/display.ts";
import { parseManifest } from "./core/manifest.ts";
import { ResultStore } from "./core/resultStore.ts";
import { windowStartDate } from "./core/tool.ts";
import {
  PYTHON_DOWNLOAD_URL,
  prepareTool,
  SetupToolRunner,
  ToolSetupCache,
  type ToolSetup,
  type WithProgress,
} from "./core/toolEnvironment.ts";
import { ProcessToolRunner, toolCommand } from "./processToolRunner.ts";
import { nodeToolFiles, runProcess, venvToolCommand } from "./pythonProcess.ts";

// `scheme: "file"` skips untitled documents and Git diffs. A filter with only a pattern
// matches any language. #28 adds pyproject.toml and Cargo.toml.
const MANIFESTS: vscode.DocumentSelector = [{ scheme: "file", pattern: "**/package.json" }];

class HintProvider implements vscode.InlayHintsProvider, vscode.Disposable {
  private readonly changed = new vscode.EventEmitter<void>();
  readonly onDidChangeInlayHints = this.changed.event;
  private store: ResultStore;
  private readonly makeStore: () => ResultStore;

  constructor(makeStore: () => ResultStore) {
    this.makeStore = makeStore;
    this.store = this.newStore();
  }

  // Forget all states and run the Tool again, for example after a settings change.
  reset(): void {
    this.store.dispose();
    this.store = this.newStore();
    this.changed.fire();
  }

  // Show no hints, for example on a machine without Python 3.10 or later.
  setHidden(hidden: boolean): void {
    this.hidden = hidden;
    this.changed.fire();
  }

  private hidden = false;

  provideInlayHints(document: vscode.TextDocument, range: vscode.Range): vscode.InlayHint[] {
    if (this.hidden) return [];
    if (/[\\/]node_modules[\\/]/.test(document.uri.fsPath)) return [];
    const hints: vscode.InlayHint[] = [];
    for (const dep of parseManifest(document.fileName, document.getText())) {
      const end = document.lineAt(dep.line).range.end;
      if (!range.contains(end)) continue;
      const hint = new vscode.InlayHint(end, hintText(this.store.get(dep), dep.name));
      hint.paddingLeft = true;
      hints.push(hint);
    }
    return hints;
  }

  dispose(): void {
    this.store.dispose();
    this.changed.dispose();
  }

  private newStore(): ResultStore {
    const store = this.makeStore();
    store.onDidUpdate(() => this.changed.fire());
    return store;
  }
}

export function activate(context: vscode.ExtensionContext): void {
  const outputDir = join(context.globalStorageUri.fsPath, "tool-output");
  mkdirSync(outputDir, { recursive: true });

  // The Tool setup (#29): `mttx.toolPath`, or the venv in the global storage folder.
  let provider: HintProvider | undefined;
  const venvDir = join(context.globalStorageUri.fsPath, "venv");
  const setups = new ToolSetupCache(async (toolPath) => {
    const setup = await prepareTool({
      toolPath,
      venvDir,
      platform: process.platform,
      run: runProcess,
      files: nodeToolFiles,
      withProgress: notificationProgress,
    });
    provider?.setHidden(setup.kind === "noPython");
    void showSetupMessage(setup);
    return setup;
  });

  const makeStore = () => {
    const toolPath = vscode.workspace.getConfiguration("mttx").get<string>("toolPath", "");
    const runner = new SetupToolRunner(setups.get(toolPath), (setup) =>
      new ProcessToolRunner(setup.kind === "venv" ? venvToolCommand(setup.python) : toolCommand(toolPath)),
    );
    return new ResultStore(runner, (key) => ({
      ecosystem: key.ecosystem,
      packageName: key.name,
      startDate: windowStartDate(new Date()),
      outputDir,
    }));
  };
  provider = new HintProvider(makeStore);

  context.subscriptions.push(
    provider,
    vscode.languages.registerInlayHintsProvider(MANIFESTS, provider),
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration("mttx")) provider.reset();
    }),
  );
}

// Show the install of the Tool in a notification, because the first install takes tens of
// seconds.
const notificationProgress: WithProgress = (task) =>
  Promise.resolve(
    vscode.window.withProgress({ location: vscode.ProgressLocation.Notification, title: "MTTX" }, (progress) =>
      task((message) => progress.report({ message })),
    ),
  );

async function showSetupMessage(setup: ToolSetup): Promise<void> {
  if (setup.kind === "failed") {
    void vscode.window.showErrorMessage(setup.message);
  } else if (setup.kind === "noPython") {
    const download = "Download Python";
    if ((await vscode.window.showWarningMessage(setup.message, download)) === download) {
      void vscode.env.openExternal(vscode.Uri.parse(PYTHON_DOWNLOAD_URL));
    }
  }
}

export function deactivate(): void {}
