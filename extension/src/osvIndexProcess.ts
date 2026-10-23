// Read and build the OSV index with the Tool process. This module has no `vscode`
// import, so that `node --test` runs it with the fake Tool script.

import {
  buildOsvArgs,
  osvIndexDateArgs,
  parseOsvIndexDate,
  progressLine,
  type OsvIndexState,
} from "./core/osvIndex.ts";
import { runToolCommand, type ToolCommand } from "./processToolRunner.ts";

export type BuildOutcome = { ok: true } | { ok: false; message: string };

// Return the index state that `--osv-index-date` prints, or undefined when the Tool does
// not start or prints no such line (for example, an older Tool).
export async function readOsvIndex(
  command: ToolCommand,
  outputDir: string,
): Promise<OsvIndexState | undefined> {
  const output = await runToolCommand(command, osvIndexDateArgs(outputDir));
  return output.error ? undefined : parseOsvIndexDate(output.stdout);
}

// Build the index with `--build-osv`. The Tool prints its progress on stderr; each line goes
// to onProgress as it arrives.
export async function buildOsvIndex(
  command: ToolCommand,
  outputDir: string,
  onProgress: (line: string) => void,
  signal?: AbortSignal,
): Promise<BuildOutcome> {
  let last: string | undefined;
  const output = await runToolCommand(command, buildOsvArgs(outputDir), {
    signal,
    onStderrLine: (text) => {
      const line = progressLine(text);
      if (line === undefined) return;
      last = line;
      onProgress(line);
    },
  });
  if (output.error) return { ok: false, message: `could not start the Tool ${command.file}: ${output.error}` };
  if (output.exitCode === 0) return { ok: true };
  return { ok: false, message: last ?? `the Tool stopped with exit code ${output.exitCode}` };
}
