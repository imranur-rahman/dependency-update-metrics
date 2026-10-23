// The thin `vscode` layer of the OSV index (#30): the prompt before the first build, the
// command "MTTX: Rebuild OSV index" with its progress, and the index state for the hints.
// The rules are in src/core/osvIndex.ts; the Tool processes are in src/osvIndexProcess.ts.
import * as vscode from "vscode";

import {
  OSV_PROMPT,
  consentFromAnswer,
  osvIndexText,
  shouldAskForOsvIndex,
  type OsvConsent,
  type OsvIndexState,
} from "./core/osvIndex.ts";
import { buildOsvIndex, readOsvIndex } from "./osvIndexProcess.ts";
import type { ToolCommand } from "./processToolRunner.ts";

export const REBUILD_COMMAND = "mttx.rebuildOsvIndex";
const CONSENT_KEY = "mttx.osvIndexConsent";

export class OsvIndex {
  // Undefined until the Tool gives the state, or when the Tool cannot give it.
  state: OsvIndexState | undefined;
  private building: Promise<void> | undefined;
  private readonly context: vscode.ExtensionContext;
  // The Tool output dir (`mttx.cacheFolder` or the default), read on each use.
  private readonly outputDir: () => string;
  private readonly command: () => ToolCommand;
  // Called when the state changes. `rebuilt` is true after a build, so that the hints run
  // the Tool again and get the MTTR values.
  private readonly onDidChange: (rebuilt: boolean) => void;

  constructor(
    context: vscode.ExtensionContext,
    outputDir: () => string,
    command: () => ToolCommand,
    onDidChange: (rebuilt: boolean) => void,
  ) {
    this.context = context;
    this.outputDir = outputDir;
    this.command = command;
    this.onDidChange = onDidChange;
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
    const title = `MTTX: Building the OSV index, about 260 MB (now: ${osvIndexText(this.state)})`;
    const outcome = await vscode.window.withProgress(
      { location: vscode.ProgressLocation.Notification, title, cancellable: true },
      (progress, token) => {
        const abort = new AbortController();
        token.onCancellationRequested(() => abort.abort());
        return buildOsvIndex(
          this.command(),
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

  private async refresh(rebuilt = false): Promise<void> {
    this.state = await readOsvIndex(this.command(), this.outputDir());
    this.onDidChange(rebuilt);
  }
}
