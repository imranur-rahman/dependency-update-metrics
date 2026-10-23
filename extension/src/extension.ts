// The thin `vscode` layer: it registers the inlay hint provider and the hover provider for
// the manifests and connects them to the core module and the Tool runner.
import { mkdirSync } from "node:fs";
import { join } from "node:path";

import * as vscode from "vscode";

import { hintText, hoverMarkdown } from "./core/display.ts";
import { dependencyAt, hintLabel } from "./core/hintLabel.ts";
import { parseManifest } from "./core/manifest.ts";
import { ResultStore } from "./core/resultStore.ts";
import { cachePruneArgs, toolRequest } from "./core/settings.ts";
import type { ToolRequest, ToolRunner } from "./core/tool.ts";
import {
  PYTHON_DOWNLOAD_URL,
  prepareTool,
  SetupToolRunner,
  ToolSetupCache,
  type ToolSetup,
  type WithProgress,
} from "./core/toolEnvironment.ts";
import { ProcessToolRunner, pruneToolCache, toolCommand, type ToolCommand } from "./processToolRunner.ts";
import { nodeToolFiles, runProcess, venvToolCommand } from "./pythonProcess.ts";
import { readSettings } from "./settings.ts";

// `scheme: "file"` skips untitled documents and Git diffs. A filter with only a pattern
// matches any language, which the TOML files need, because VS Code has no TOML language.
const MANIFESTS: vscode.DocumentSelector = [
  { scheme: "file", pattern: "**/package.json" },
  { scheme: "file", pattern: "**/pyproject.toml" },
  { scheme: "file", pattern: "**/Cargo.toml" },
];

class HintProvider implements vscode.InlayHintsProvider, vscode.HoverProvider, vscode.Disposable {
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
    const deps = parseManifest(document.fileName, document.getText());
    for (const dep of deps) {
      const end = document.lineAt(dep.line).range.end;
      if (!range.contains(end)) continue;
      const text = hintText(this.store.get(dep), dep.name);
      const hint = new vscode.InlayHint(end, hintLabel(dep, deps, text));
      hint.paddingLeft = true;
      hints.push(hint);
    }
    return hints;
  }

  // The hover of a dependency line shows the details behind the hint, or the full error.
  provideHover(document: vscode.TextDocument, position: vscode.Position): vscode.Hover | undefined {
    if (/[\\/]node_modules[\\/]/.test(document.uri.fsPath)) return undefined;
    const deps = parseManifest(document.fileName, document.getText());
    const dep = dependencyAt(deps, position.line, position.character);
    if (!dep) return undefined;
    const markdown = new vscode.MarkdownString(hoverMarkdown(this.store.get(dep), dep));
    return new vscode.Hover(markdown, document.lineAt(dep.line).range);
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

  // The Tool setup (#29): `mttx.toolPath`, or the venv in the global storage folder. A
  // setup ends after `provider` exists, because prepareTool always waits at least once.
  const toolPathSetting = () => vscode.workspace.getConfiguration("mttx").get<string>("toolPath", "");
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
    provider.setHidden(setup.kind === "noPython");
    void showSetupMessage(setup);
    return setup;
  });

  const makeStore = () => {
    const parsed = readSettings();
    if (!parsed.ok) {
      // Each hint shows the message, and the store never starts the Tool.
      void vscode.window.showWarningMessage(`MTTX: ${parsed.message}`);
      const failed: ToolRunner = { run: async () => ({ kind: "failed", message: parsed.message }) };
      return new ResultStore(failed, () => ({}) as ToolRequest);
    }
    const setup = setups.get(toolPathSetting());
    const runner = new SetupToolRunner(setup, (ready) => new ProcessToolRunner(setupCommand(ready)));
    return new ResultStore(runner, (key) => toolRequest(key, parsed.settings, new Date(), outputDir));
  };
  const provider = new HintProvider(makeStore);

  // The Tool prunes its cache only in a run of its own, so the Extension prunes it at the
  // start and after a change to the cache settings.
  const pruneCache = () => {
    const parsed = readSettings();
    if (!parsed.ok) return;
    void setups.get(toolPathSetting()).then((setup) => {
      if (setup.kind === "venv" || setup.kind === "toolPath") {
        return pruneToolCache(setupCommand(setup), cachePruneArgs(parsed.settings, outputDir));
      }
    });
  };
  pruneCache();

  context.subscriptions.push(
    provider,
    vscode.languages.registerInlayHintsProvider(MANIFESTS, provider),
    vscode.languages.registerHoverProvider(MANIFESTS, provider),
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration("mttx")) provider.reset();
      if (event.affectsConfiguration("mttx.cacheFolder") || event.affectsConfiguration("mttx.cacheSizeLimitGb")) {
        pruneCache();
      }
    }),
    vscode.commands.registerCommand("mttx.openSettings", () =>
      vscode.commands.executeCommand("workbench.action.openSettings", "@ext:imranur-rahman.mttx"),
    ),
  );
}

// The command that runs the Tool of a setup: the venv Python, or `mttx.toolPath`.
function setupCommand(setup: ToolSetup): ToolCommand {
  if (setup.kind === "venv") return venvToolCommand(setup.python);
  return toolCommand(setup.kind === "toolPath" ? setup.path : "");
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
