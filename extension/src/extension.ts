// The thin `vscode` layer: it registers the inlay hint provider and the hover provider for
// the manifests and connects them to the core module and the Tool runner.
import { mkdirSync } from "node:fs";
import { join } from "node:path";

import * as vscode from "vscode";

import { MANIFEST_PATTERN, isAnalyzedManifest } from "./core/discovery.ts";
import { hintText, hoverMarkdown, type HintContext } from "./core/display.ts";
import { dependencyAt, hintLabel } from "./core/hintLabel.ts";
import { parseManifest } from "./core/manifest.ts";
import { ResultCache } from "./core/resultCache.ts";
import { DEFAULT_PARALLEL_RUNS, ResultStore } from "./core/resultStore.ts";
import { cachePruneArgs, toolOutputDir, toolRequest } from "./core/settings.ts";
import type { ToolRequest, ToolRunner } from "./core/tool.ts";
import { OsvIndex, REBUILD_COMMAND, type ReadyCommand } from "./osvIndex.ts";
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
// matches any language, which the TOML files need, because VS Code has no TOML language. The
// pattern covers the three manifest names in any subfolder.
const MANIFESTS: vscode.DocumentSelector = [{ scheme: "file", pattern: MANIFEST_PATTERN }];

class HintProvider implements vscode.InlayHintsProvider, vscode.HoverProvider, vscode.Disposable {
  private readonly changed = new vscode.EventEmitter<void>();
  readonly onDidChangeInlayHints = this.changed.event;
  private store: ResultStore;
  private readonly makeStore: () => ResultStore;
  private readonly hintContext: () => HintContext;

  constructor(makeStore: () => ResultStore, hintContext: () => HintContext) {
    this.makeStore = makeStore;
    this.hintContext = hintContext;
    this.store = this.newStore();
  }

  // Show the hints again with the same states, for example after the OSV index state changes.
  redraw(): void {
    this.changed.fire();
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

  // Run the Tool for the dependencies of the manifest (the command "MTTX: Analyze
  // dependencies").
  async analyze(document: vscode.TextDocument): Promise<void> {
    await this.store.analyze(parseManifest(document.fileName, document.getText()));
  }

  provideInlayHints(document: vscode.TextDocument, range: vscode.Range): vscode.InlayHint[] {
    if (this.hidden) return [];
    if (!isAnalyzedManifest(document.uri.fsPath)) return [];
    const hints: vscode.InlayHint[] = [];
    const deps = parseManifest(document.fileName, document.getText());
    for (const dep of deps) {
      const end = document.lineAt(dep.line).range.end;
      const state = range.contains(end) ? this.store.get(dep) : undefined;
      if (!state) continue;
      const text = hintText(state, dep.name, this.hintContext());
      const hint = new vscode.InlayHint(end, hintLabel(dep, deps, text));
      hint.paddingLeft = true;
      hints.push(hint);
    }
    return hints;
  }

  // The hover of a dependency line shows the details behind the hint, or the full error.
  provideHover(document: vscode.TextDocument, position: vscode.Position): vscode.Hover | undefined {
    if (!isAnalyzedManifest(document.uri.fsPath)) return undefined;
    const deps = parseManifest(document.fileName, document.getText());
    const dep = dependencyAt(deps, position.line, position.character);
    const state = dep && this.store.get(dep);
    if (!dep || !state) return undefined;
    const markdown = new vscode.MarkdownString(hoverMarkdown(state, dep, this.hintContext()));
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

  // The results persist across VS Code restarts in global state.
  const cache = new ResultCache(context.globalState);

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
  // The command of the Tool after the setup, or the message of a setup without a Tool. The
  // cache prune and the OSV index processes wait for it, the same way the Tool runs do.
  const readyCommand = async (): Promise<ReadyCommand> => {
    const setup = await setups.get(toolPathSetting());
    if (setup.kind === "venv" || setup.kind === "toolPath") return { ok: true, command: setupCommand(setup) };
    return { ok: false, message: setup.message };
  };

  const makeStore = () => {
    const config = vscode.workspace.getConfiguration("mttx");
    const runControl = {
      parallelRuns: config.get<number>("parallelRuns", DEFAULT_PARALLEL_RUNS),
      automaticRuns: config.get<boolean>("automaticRuns", true),
    };
    const parsed = readSettings();
    if (!parsed.ok) {
      // Each hint shows the message, and the store never starts the Tool.
      void vscode.window.showWarningMessage(`MTTX: ${parsed.message}`);
      const failed: ToolRunner = { run: async () => ({ kind: "failed", message: parsed.message }) };
      return new ResultStore({ runner: failed, makeRequest: () => ({}) as ToolRequest, ...runControl });
    }
    const setup = setups.get(toolPathSetting());
    const runner = new SetupToolRunner(setup, (ready) => new ProcessToolRunner(setupCommand(ready)));
    return new ResultStore({
      runner,
      makeRequest: (key) => toolRequest(key, parsed.settings, new Date(), outputDir),
      cache,
      ...runControl,
    });
  };
  // The OSV index (#30) is in the Tool output dir. Without it, MTTR "needs the OSV index".
  const osvIndex = new OsvIndex(
    context,
    () => {
      const parsed = readSettings();
      return parsed.ok ? toolOutputDir(parsed.settings, outputDir) : outputDir;
    },
    readyCommand,
    (rebuilt) => (rebuilt ? provider.reset() : provider.redraw()),
  );
  const provider = new HintProvider(makeStore, () => ({ osvIndex: osvIndex.available }));
  void osvIndex.start();

  // The Tool prunes its cache only in a run of its own, so the Extension prunes it at the
  // start and after a change to the cache settings.
  const pruneCache = () => {
    const parsed = readSettings();
    if (!parsed.ok) return;
    void readyCommand().then((ready) => {
      if (ready.ok) return pruneToolCache(ready.command, cachePruneArgs(parsed.settings, outputDir));
    });
  };
  pruneCache();

  context.subscriptions.push(
    vscode.commands.registerCommand(REBUILD_COMMAND, () => osvIndex.rebuild()),
    provider,
    vscode.languages.registerInlayHintsProvider(MANIFESTS, provider),
    vscode.languages.registerHoverProvider(MANIFESTS, provider),
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration("mttx")) provider.reset();
      if (event.affectsConfiguration("mttx.cacheFolder") || event.affectsConfiguration("mttx.cacheSizeLimitGb")) {
        pruneCache();
      }
      if (event.affectsConfiguration("mttx.cacheFolder") || event.affectsConfiguration("mttx.toolPath")) {
        void osvIndex.start();
      }
    }),
    vscode.commands.registerCommand("mttx.openSettings", () =>
      vscode.commands.executeCommand("workbench.action.openSettings", "@ext:imranur-rahman.mttx"),
    ),
    vscode.commands.registerCommand("mttx.analyzeDependencies", () => analyzeOpenManifest(provider)),
  );
}

async function analyzeOpenManifest(provider: HintProvider): Promise<void> {
  const document = vscode.window.activeTextEditor?.document;
  if (!document || document.uri.scheme !== "file" || !isAnalyzedManifest(document.uri.fsPath)) {
    void vscode.window.showInformationMessage(
      "MTTX: open a package.json, pyproject.toml, or Cargo.toml file to analyze its dependencies.",
    );
    return;
  }
  await vscode.window.withProgress(
    { location: vscode.ProgressLocation.Window, title: "MTTX: analyzing dependencies" },
    () => provider.analyze(document),
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
