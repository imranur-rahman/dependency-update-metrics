// The real process runner and file system for src/core/toolEnvironment.ts, and the command
// that runs the Tool from the venv. This module has no `vscode` import.

import { spawn } from "node:child_process";
import { access, readFile, writeFile } from "node:fs/promises";

import type { ProcessResult, RunProcess, ToolFiles } from "./core/toolEnvironment.ts";
import type { ToolCommand } from "./processToolRunner.ts";

// Run a program with an args array (no shell) and collect its output. A program that does
// not start, or that its timeout stops, gives a result with `error`.
export const runProcess: RunProcess = (file, args, options = {}) =>
  new Promise<ProcessResult>((resolve) => {
    const child = spawn(file, args, {
      timeout: options.timeoutMs,
      windowsHide: true,
      env: { ...process.env, PYTHONIOENCODING: "utf-8" },
    });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    let error: string | undefined;
    child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
    child.on("error", (err) => {
      error = err.message;
    });
    child.on("close", (exitCode, signal) => {
      if (!error && signal) error = `stopped by ${signal}`;
      resolve({
        exitCode,
        stdout: Buffer.concat(stdout).toString("utf8"),
        stderr: Buffer.concat(stderr).toString("utf8"),
        error,
      });
    });
  });

export const nodeToolFiles: ToolFiles = {
  exists: (path) => access(path).then(
    () => true,
    () => false,
  ),
  readText: (path) => readFile(path, "utf8").catch(() => undefined),
  writeText: (path, text) => writeFile(path, text, "utf8"),
};

// The Tool has no `__main__.py`, so the venv Python runs its `cli` module, which calls
// `main()` under `if __name__ == "__main__"`.
export function venvToolCommand(python: string): ToolCommand {
  return { file: python, args: ["-m", "dependency_metrics.cli"] };
}
