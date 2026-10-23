// Read and build the OSV index with the Tool process (#30). This module has no `vscode`
// import, so that `node --test` runs it with the fake Tool script.

import { spawn } from "node:child_process";

import {
  buildOsvArgs,
  osvIndexDateArgs,
  parseOsvIndexDate,
  progressLine,
  type OsvIndexState,
} from "./core/osvIndex.ts";
import type { ToolCommand } from "./processToolRunner.ts";

export type BuildOutcome = { ok: true } | { ok: false; message: string };

// Return the index state that `--osv-index-date` prints, or undefined when the Tool does
// not start or prints no such line (for example, an older Tool).
export async function readOsvIndex(
  command: ToolCommand,
  outputDir: string,
): Promise<OsvIndexState | undefined> {
  const output = await runTool(command, osvIndexDateArgs(outputDir), () => {});
  return output.started ? parseOsvIndexDate(output.stdout) : undefined;
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
  const output = await runTool(
    command,
    buildOsvArgs(outputDir),
    (line) => {
      last = line;
      onProgress(line);
    },
    signal,
  );
  if (!output.started) return { ok: false, message: output.error };
  if (output.exitCode === 0) return { ok: true };
  return { ok: false, message: last ?? `the Tool stopped with exit code ${output.exitCode}` };
}

type ToolProcessOutput =
  | { started: true; stdout: string; exitCode: number | null }
  | { started: false; error: string };

function runTool(
  command: ToolCommand,
  toolArgs: string[],
  onLine: (line: string) => void,
  signal?: AbortSignal,
): Promise<ToolProcessOutput> {
  const { file, args, env } = command;
  return new Promise((resolve) => {
    const child = spawn(file, [...args, ...toolArgs], {
      signal,
      windowsHide: true,
      env: { ...process.env, ...env, PYTHONIOENCODING: "utf-8" },
    });
    const stdout: Buffer[] = [];
    let pending = "";
    // A chunk can end in the middle of a line, so the last part waits for the next chunk.
    const emit = (text: string) => {
      const line = progressLine(text);
      if (line !== undefined) onLine(line);
    };
    child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk: string) => {
      const parts = (pending + chunk).split(/[\r\n]/);
      pending = parts.pop() ?? "";
      parts.forEach(emit);
    });
    child.on("error", (error) => {
      resolve({ started: false, error: `could not start the Tool ${file}: ${error.message}` });
    });
    child.on("close", (exitCode) => {
      emit(pending);
      resolve({ started: true, stdout: Buffer.concat(stdout).toString("utf8"), exitCode });
    });
  });
}
