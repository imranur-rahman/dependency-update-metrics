// The thin `vscode` layer: it registers the inlay hint provider and the hover provider for
// the manifests and connects them to the core module and the Tool runner.
import { mkdirSync } from "node:fs";
import { join } from "node:path";

import * as vscode from "vscode";

import { MANIFEST_PATTERN, isAnalyzedManifest } from "./core/discovery.ts";
import { hintText, hoverMarkdown } from "./core/display.ts";
import { dependencyAt, hintLabel } from "./core/hintLabel.ts";
import { parseManifest } from "./core/manifest.ts";
import { ResultCache } from "./core/resultCache.ts";
import { DEFAULT_PARALLEL_RUNS, ResultStore } from "./core/resultStore.ts";
import { cachePruneArgs, toolRequest } from "./core/settings.ts";
import type { ToolRequest, ToolRunner } from "./core/tool.ts";
import { ProcessToolRunner, pruneToolCache, toolCommand } from "./processToolRunner.ts";
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

  // Run the Tool for the dependencies of the manifest (the command "MTTX: Analyze
  // dependencies").
  async analyze(document: vscode.TextDocument): Promise<void> {
    await this.store.analyze(parseManifest(document.fileName, document.getText()));
  }

  provideInlayHints(document: vscode.TextDocument, range: vscode.Range): vscode.InlayHint[] {
    if (!isAnalyzedManifest(document.uri.fsPath)) return [];
    const hints: vscode.InlayHint[] = [];
    const deps = parseManifest(document.fileName, document.getText());
    for (const dep of deps) {
      const end = document.lineAt(dep.line).range.end;
      const state = range.contains(end) ? this.store.get(dep) : undefined;
      if (!state) continue;
      const hint = new vscode.InlayHint(end, hintLabel(dep, deps, hintText(state, dep.name)));
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
    const markdown = new vscode.MarkdownString(hoverMarkdown(state, dep));
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

  const command = () => toolCommand(vscode.workspace.getConfiguration("mttx").get<string>("toolPath", ""));
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
    return new ResultStore({
      runner: new ProcessToolRunner(command()),
      makeRequest: (key) => toolRequest(key, parsed.settings, new Date(), outputDir),
      cache,
      ...runControl,
    });
  };
  const provider = new HintProvider(makeStore);

  // The Tool prunes its cache only in a run of its own, so the Extension prunes it at the
  // start and after a change to the cache settings.
  const pruneCache = () => {
    const parsed = readSettings();
    if (parsed.ok) void pruneToolCache(command(), cachePruneArgs(parsed.settings, outputDir));
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

export function deactivate(): void {}
