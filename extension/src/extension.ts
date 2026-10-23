// The thin `vscode` layer: it registers the inlay hint provider and the hover provider for
// the manifests and connects them to the core module and the Tool runner.
import { mkdirSync } from "node:fs";
import { join } from "node:path";

import * as vscode from "vscode";

import { hintText, hoverMarkdown, type HintContext } from "./core/display.ts";
import { dependencyAt, hintLabel } from "./core/hintLabel.ts";
import { parseManifest } from "./core/manifest.ts";
import { ResultStore } from "./core/resultStore.ts";
import { cachePruneArgs, toolOutputDir, toolRequest } from "./core/settings.ts";
import type { ToolRequest, ToolRunner } from "./core/tool.ts";
import { OsvIndex, REBUILD_COMMAND } from "./osvIndex.ts";
import { ProcessToolRunner, pruneToolCache, toolCommand } from "./processToolRunner.ts";
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

  provideInlayHints(document: vscode.TextDocument, range: vscode.Range): vscode.InlayHint[] {
    if (/[\\/]node_modules[\\/]/.test(document.uri.fsPath)) return [];
    const hints: vscode.InlayHint[] = [];
    const deps = parseManifest(document.fileName, document.getText());
    for (const dep of deps) {
      const end = document.lineAt(dep.line).range.end;
      if (!range.contains(end)) continue;
      const text = hintText(this.store.get(dep), dep.name, this.hintContext());
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
    const markdown = new vscode.MarkdownString(hoverMarkdown(this.store.get(dep), dep, this.hintContext()));
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

  const command = () => toolCommand(vscode.workspace.getConfiguration("mttx").get<string>("toolPath", ""));
  const makeStore = () => {
    const parsed = readSettings();
    if (!parsed.ok) {
      // Each hint shows the message, and the store never starts the Tool.
      void vscode.window.showWarningMessage(`MTTX: ${parsed.message}`);
      const failed: ToolRunner = { run: async () => ({ kind: "failed", message: parsed.message }) };
      return new ResultStore(failed, () => ({}) as ToolRequest);
    }
    return new ResultStore(new ProcessToolRunner(command()), (key) =>
      toolRequest(key, parsed.settings, new Date(), outputDir),
    );
  };
  // The OSV index (#30) is in the Tool output dir. Without it, MTTR "needs the OSV index".
  const osvIndex = new OsvIndex(
    context,
    () => {
      const parsed = readSettings();
      return parsed.ok ? toolOutputDir(parsed.settings, outputDir) : outputDir;
    },
    command,
    (rebuilt) => (rebuilt ? provider.reset() : provider.redraw()),
  );
  const provider = new HintProvider(makeStore, () => ({ osvIndex: osvIndex.available }));
  void osvIndex.start();

  // The Tool prunes its cache only in a run of its own, so the Extension prunes it at the
  // start and after a change to the cache settings.
  const pruneCache = () => {
    const parsed = readSettings();
    if (parsed.ok) void pruneToolCache(command(), cachePruneArgs(parsed.settings, outputDir));
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
  );
}

export function deactivate(): void {}
