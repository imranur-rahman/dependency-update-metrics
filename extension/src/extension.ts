// The thin `vscode` layer: it registers the inlay hint provider and the hover provider for
// package.json and connects them to the core module and the Tool runner.
import { mkdirSync } from "node:fs";
import { join } from "node:path";

import * as vscode from "vscode";

import { MANIFEST_PATTERN, isAnalyzedManifest } from "./core/discovery.ts";
import { hintText, hoverMarkdown } from "./core/display.ts";
import { parseManifest } from "./core/manifest.ts";
import { ResultCache } from "./core/resultCache.ts";
import { DEFAULT_PARALLEL_RUNS, ResultStore } from "./core/resultStore.ts";
import { windowStartDate } from "./core/tool.ts";
import { ProcessToolRunner, toolCommand } from "./processToolRunner.ts";

// `scheme: "file"` skips untitled documents and Git diffs. A filter with only a pattern
// matches any language, in any subfolder.
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
    for (const dep of parseManifest(document.fileName, document.getText())) {
      const end = document.lineAt(dep.line).range.end;
      const state = range.contains(end) ? this.store.get(dep) : undefined;
      if (!state) continue;
      const hint = new vscode.InlayHint(end, hintText(state, dep.name));
      hint.paddingLeft = true;
      hints.push(hint);
    }
    return hints;
  }

  // The hover of a dependency line shows the details behind the hint, or the full error.
  provideHover(document: vscode.TextDocument, position: vscode.Position): vscode.Hover | undefined {
    if (!isAnalyzedManifest(document.uri.fsPath)) return undefined;
    const dep = parseManifest(document.fileName, document.getText()).find(
      (candidate) => candidate.line === position.line,
    );
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

  const makeStore = () => {
    const config = vscode.workspace.getConfiguration("mttx");
    const runner = new ProcessToolRunner(toolCommand(config.get<string>("toolPath", "")));
    return new ResultStore({
      runner,
      makeRequest: (key) => ({
        ecosystem: key.ecosystem,
        packageName: key.name,
        startDate: windowStartDate(new Date()),
        outputDir,
      }),
      cache,
      parallelRuns: config.get<number>("parallelRuns", DEFAULT_PARALLEL_RUNS),
      automaticRuns: config.get<boolean>("automaticRuns", true),
    });
  };
  const provider = new HintProvider(makeStore);

  context.subscriptions.push(
    provider,
    vscode.languages.registerInlayHintsProvider(MANIFESTS, provider),
    vscode.languages.registerHoverProvider(MANIFESTS, provider),
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration("mttx")) provider.reset();
    }),
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
