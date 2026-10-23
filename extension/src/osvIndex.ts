// The thin `vscode` layer of the OSV index: the prompt before the first build, the command
// "MTTX: Rebuild OSV index" with its progress, the status bar item with the build date, and
// the index state for the hints.
// The rules are in src/core/osvIndex.ts; the Tool processes are in src/osvIndexProcess.ts.
import * as vscode from "vscode";

import {
  OSV_DOWNLOAD_SIZE,
  OSV_PROMPT,
  consentFromAnswer,
  osvIndexChanged,
  osvIndexStatus,
  osvIndexText,
  shouldAskForOsvIndex,
  type OsvConsent,
  type OsvIndexState,
} from "./core/osvIndex.ts";
import { buildOsvIndex, readOsvIndex } from "./osvIndexProcess.ts";
import type { ToolCommand } from "./processToolRunner.ts";

export const REBUILD_COMMAND = "mttx.rebuildOsvIndex";
const CONSENT_KEY = "mttx.osvIndexConsent";
// The last index state that the Tool gave for an output dir, in global state. A new session
// starts with it, so that the cached values show before the Tool gives the state again.
const stateKey = (outputDir: string) => `mttx.osvIndexState:${outputDir}`;

// The command of the Tool after its setup, or the message of a setup without a Tool.
export type ReadyCommand = { ok: true; command: ToolCommand } | { ok: false; message: string };

export class OsvIndex implements vscode.Disposable {
  // The last state that the Tool gave for the output dir. Undefined before the first state,
  // or when the Tool cannot give it.
  state: OsvIndexState | undefined;
  private building: Promise<void> | undefined;
  // Shows the build date while a manifest is open. VS Code has no dynamic command title or
  // setting description, so the status bar is the place that shows the date without a run.
  private readonly status = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right);
  private readonly context: vscode.ExtensionContext;
  // The Tool output dir (`mttx.cacheFolder` or the default), read on each use.
  private readonly outputDir: () => string;
  // Waits for the Tool setup, so that the index processes use the venv Tool too.
  private readonly command: () => Promise<ReadyCommand>;
  // Called when the state changes. `rebuilt` is true after a build or a new index state, so
  // that the hints run the Tool again and get the MTTR values of the new index.
  private readonly onDidChange: (rebuilt: boolean) => void;

  constructor(
    context: vscode.ExtensionContext,
    outputDir: () => string,
    command: () => Promise<ReadyCommand>,
    onDidChange: (rebuilt: boolean) => void,
  ) {
    this.context = context;
    this.outputDir = outputDir;
    this.command = command;
    this.onDidChange = onDidChange;
    this.state = this.saved();
    this.status.command = REBUILD_COMMAND;
    this.showStatus();
  }

  // Show the status bar item only while the active editor holds a manifest.
  setStatusVisible(visible: boolean): void {
    if (visible) this.status.show();
    else this.status.hide();
  }

  dispose(): void {
    this.status.dispose();
  }

  // True unless the Tool says that there is no index. An unknown state shows the MTTR.
  get available(): boolean {
    return this.state?.kind !== "missing";
  }

  // Read the index state, and ask once before the first build.
  async start(): Promise<void> {
    await this.refresh();
    const consent = this.context.globalState.get<OsvConsent>(CONSENT_KEY);
    if (!shouldAskForOsvIndex(this.state, consent)) return;
    const answer = await vscode.window.showInformationMessage(
      OSV_PROMPT.message,
      OSV_PROMPT.build,
      OSV_PROMPT.decline,
    );
    const newConsent = consentFromAnswer(answer);
    await this.context.globalState.update(CONSENT_KEY, newConsent);
    // The rebuild command can build the index while the prompt waits.
    if (newConsent === "accepted" && this.state?.kind === "missing") await this.rebuild();
  }

  // The command "MTTX: Rebuild OSV index". It shows the current index date in its progress.
  rebuild(): Promise<void> {
    this.building ??= this.build().finally(() => (this.building = undefined));
    return this.building;
  }

  private async build(): Promise<void> {
    const ready = await this.command();
    if (!ready.ok) {
      void vscode.window.showErrorMessage(`MTTX: the OSV index build failed: ${ready.message}`);
      return;
    }
    const title = `MTTX: Building the OSV index, ${OSV_DOWNLOAD_SIZE} (now: ${osvIndexText(this.state)})`;
    const outcome = await vscode.window.withProgress(
      { location: vscode.ProgressLocation.Notification, title, cancellable: true },
      (progress, token) => {
        const abort = new AbortController();
        token.onCancellationRequested(() => abort.abort());
        return buildOsvIndex(
          ready.command,
          this.outputDir(),
          (line) => progress.report({ message: line }),
          abort.signal,
        );
      },
    );
    await this.refresh(outcome.ok);
    if (outcome.ok) {
      void vscode.window.showInformationMessage(`MTTX: ${osvIndexText(this.state)}`);
    } else {
      void vscode.window.showErrorMessage(`MTTX: the OSV index build failed: ${outcome.message}`);
    }
  }

  // Read the state from the Tool. `rebuilt` is also true when the Tool gives a new state,
  // so that the hints drop the values of the old state.
  private async refresh(rebuilt = false): Promise<void> {
    const outputDir = this.outputDir();
    // The output dir can change with `mttx.cacheFolder`; its last state applies at once.
    const before = (this.state = this.saved());
    this.showStatus();
    // A setup without a Tool leaves the state unknown, so the prompt does not show.
    const ready = await this.command();
    this.state = ready.ok ? await readOsvIndex(ready.command, outputDir) : undefined;
    if (this.state) await this.context.globalState.update(stateKey(outputDir), this.state);
    this.showStatus();
    this.onDidChange(rebuilt || osvIndexChanged(before, this.state));
  }

  private showStatus(): void {
    Object.assign(this.status, osvIndexStatus(this.state));
  }

  private saved(): OsvIndexState | undefined {
    return this.context.globalState.get<OsvIndexState>(stateKey(this.outputDir()));
  }
}
