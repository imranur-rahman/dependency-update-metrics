// The real process runner and file system for src/core/toolEnvironment.ts, and the command
// that runs the Tool from the venv. This module has no `vscode` import.

import { access, readFile, writeFile } from "node:fs/promises";

import type { RunProcess, ToolFiles } from "./core/toolEnvironment.ts";
import type { ToolCommand } from "./processToolRunner.ts";
import { spawnProcess } from "./spawnProcess.ts";

// Run a program and collect its output. A program that does not start, or that its timeout
// stops, gives a result with `error`.
export const runProcess: RunProcess = async (file, args, options = {}) => {
  const result = await spawnProcess(file, args, { timeoutMs: options.timeoutMs });
  const error = result.error ?? (result.signal ? `stopped by ${result.signal}` : undefined);
  return { exitCode: result.exitCode, stdout: result.stdout, stderr: result.stderr, error };
};

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
