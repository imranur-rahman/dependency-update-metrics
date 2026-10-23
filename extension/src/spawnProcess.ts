// The one helper that spawns the child processes of the Extension: the Tool runs, the cache
// prune, the OSV index processes, and the Python processes of the Tool setup. It has no
// `vscode` import, so that `node --test` runs it.

import { spawn } from "node:child_process";
import { StringDecoder } from "node:string_decoder";

export interface SpawnOptions {
  // Variables on top of the environment of the Extension host.
  env?: Record<string, string>;
  // The helper stops the process after this time, and the result says `timedOut`.
  timeoutMs?: number;
  // An abort stops the process, and the result has an `error`.
  signal?: AbortSignal;
  // Gets each line of stderr as it arrives. A tqdm bar rewrites its line with "\r", so "\r"
  // also ends a line.
  onStderrLine?: (line: string) => void;
}

export interface SpawnResult {
  exitCode: number | null;
  // The signal that stopped the process, for example after the timeout.
  signal: NodeJS.Signals | null;
  stdout: string;
  stderr: string;
  // The message when the process did not start (for example ENOENT) or an abort stopped it.
  error?: string;
  timedOut: boolean;
}

// Run a program with an args array and collect its output. An args array needs no shell, so
// a package name never reaches a shell parser. The Tool prints UTF-8 on each platform,
// because PYTHONIOENCODING sets the encoding of the Python streams.
export function spawnProcess(file: string, args: string[], options: SpawnOptions = {}): Promise<SpawnResult> {
  return new Promise((resolve) => {
    const child = spawn(file, args, {
      signal: options.signal,
      windowsHide: true,
      env: { ...process.env, ...options.env, PYTHONIOENCODING: "utf-8" },
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    // The decoder keeps a UTF-8 character that two chunks split, such as a tqdm block.
    const decoder = new StringDecoder("utf8");
    let pending = "";
    let error: string | undefined;
    let timedOut = false;
    let done = false;

    // A chunk can end in the middle of a line, so the last part waits for the next chunk.
    const emitLines = (text: string, flush: boolean) => {
      if (!options.onStderrLine) return;
      const parts = (pending + text).split(/[\r\n]/);
      pending = flush ? "" : (parts.pop() ?? "");
      for (const line of parts) if (line !== "") options.onStderrLine(line);
    };
    const finish = (exitCode: number | null, signal: NodeJS.Signals | null) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      emitLines(decoder.end(), true);
      resolve({
        exitCode,
        signal,
        stdout: Buffer.concat(stdout).toString("utf8"),
        stderr: Buffer.concat(stderr).toString("utf8"),
        error,
        timedOut,
      });
    };

    const timer =
      options.timeoutMs === undefined
        ? undefined
        : setTimeout(() => {
            timedOut = true;
            child.kill();
          }, options.timeoutMs);
    child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => {
      stderr.push(chunk);
      emitLines(decoder.write(chunk), false);
    });
    // A process that never started gets no "close" event, so the helper ends here. An abort
    // of a started process ends at "close".
    child.on("error", (err) => {
      error = err.message;
      if (child.pid === undefined) finish(null, null);
    });
    // "close" comes after the process ends and its output streams close, so stdout is whole.
    child.on("close", (exitCode, signal) => finish(exitCode, signal));
  });
}
