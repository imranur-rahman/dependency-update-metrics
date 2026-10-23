// The thin `vscode` layer: it registers the inlay hint provider and the hover provider for
// package.json and connects them to the core module and the Tool runner.
import { mkdirSync } from "node:fs";
import { join } from "node:path";

import * as vscode from "vscode";

import { hintText, hoverMarkdown } from "./core/display.ts";
import { parseManifest } from "./core/manifest.ts";
import { ResultStore } from "./core/resultStore.ts";
import { windowStartDate } from "./core/tool.ts";
import { ProcessToolRunner, toolCommand } from "./processToolRunner.ts";

// `scheme: "file"` skips untitled documents and Git diffs. A filter with only a pattern
// matches any language. #28 adds pyproject.toml and Cargo.toml.
const MANIFESTS: vscode.DocumentSelector = [{ scheme: "file", pattern: "**/package.json" }];

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

  provideInlayHints(document: vscode.TextDocument, range: vscode.Range): vscode.InlayHint[] {
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

  // The hover of a dependency line shows the details behind the hint, or the full error.
  provideHover(document: vscode.TextDocument, position: vscode.Position): vscode.Hover | undefined {
    if (/[\\/]node_modules[\\/]/.test(document.uri.fsPath)) return undefined;
    const dep = parseManifest(document.fileName, document.getText()).find(
      (candidate) => candidate.line === position.line,
    );
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

  const makeStore = () => {
    const toolPath = vscode.workspace.getConfiguration("mttx").get<string>("toolPath", "");
    const runner = new ProcessToolRunner(toolCommand(toolPath));
    return new ResultStore(runner, (key) => ({
      ecosystem: key.ecosystem,
      packageName: key.name,
      startDate: windowStartDate(new Date()),
      outputDir,
    }));
  };
  const provider = new HintProvider(makeStore);

  context.subscriptions.push(
    provider,
    vscode.languages.registerInlayHintsProvider(MANIFESTS, provider),
    vscode.languages.registerHoverProvider(MANIFESTS, provider),
    vscode.workspace.onDidChangeConfiguration((event) => {
      if (event.affectsConfiguration("mttx")) provider.reset();
    }),
  );
}

export function deactivate(): void {}
