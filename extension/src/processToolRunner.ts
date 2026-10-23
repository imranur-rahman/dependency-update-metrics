// The real Tool runner: it spawns `dependency-metrics` as a child process. It has no
// `vscode` import, so that `node --test` runs it with the fake Tool script.

import { spawn } from "node:child_process";

import { parseToolOutput, toolArgs, type ToolOutcome, type ToolRequest, type ToolRunner } from "./core/tool.ts";

// The program that runs the Tool, and the arguments before the Tool arguments.
// #29 adds the managed venv: { file: <venv python>, args: ["-m", "dependency_metrics.cli"] }.
export interface ToolCommand {
  file: string;
  args: string[];
  env?: Record<string, string>;
}

// Return the command for the `mttx.toolPath` setting. An empty path runs
// `dependency-metrics` from the PATH. A JavaScript file runs with the Node runtime of the
// Extension host: the tests use it for the fake Tool, because Windows cannot spawn a
// script file without a shell.
export function toolCommand(toolPath: string): ToolCommand {
  const file = toolPath.trim();
  if (file === "") return { file: "dependency-metrics", args: [] };
  if (/\.(?:mjs|cjs|js)$/i.test(file)) {
    return { file: process.execPath, args: [file], env: { ELECTRON_RUN_AS_NODE: "1" } };
  }
  return { file, args: [] };
}

// Run the Tool once with the arguments of cachePruneArgs() (src/core/settings.ts), and
// return its exit code, or null when the Tool does not start. The output does not matter.
export function pruneToolCache(
  command: ToolCommand,
  pruneArgs: string[],
  env: Record<string, string> = {},
): Promise<number | null> {
  return new Promise((resolve) => {
    const child = spawn(command.file, [...command.args, ...pruneArgs], {
      stdio: "ignore",
      windowsHide: true,
      env: { ...process.env, ...command.env, ...env },
    });
    child.on("error", () => resolve(null));
    child.on("close", (exitCode) => resolve(exitCode));
  });
}

export class ProcessToolRunner implements ToolRunner {
  private readonly command: ToolCommand;
  private readonly env: Record<string, string>;

  constructor(command: ToolCommand, env: Record<string, string> = {}) {
    this.command = command;
    this.env = env;
  }

  run(request: ToolRequest, signal?: AbortSignal): Promise<ToolOutcome> {
    const { file, args, env } = this.command;
    return new Promise((resolve) => {
      // An args array needs no shell, so a package name never reaches a shell parser.
      const child = spawn(file, [...args, ...toolArgs(request)], {
        signal,
        windowsHide: true,
        env: { ...process.env, ...env, ...this.env, PYTHONIOENCODING: "utf-8" },
      });
      const stdout: Buffer[] = [];
      const stderr: Buffer[] = [];
      child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
      child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
      // The Tool has no timeout in single-package mode, so the runner stops a slow run.
      let timedOut = false;
      const timer = setTimeout(() => {
        timedOut = true;
        child.kill();
      }, request.timeoutSeconds * 1000);
      // ENOENT (no such Tool) and an abort arrive here, and then no "close" event follows
      // for a process that never started.
      child.on("error", (error) => {
        clearTimeout(timer);
        resolve({ kind: "failed", message: `could not start the Tool ${file}: ${error.message}` });
      });
      // "close" comes after the process ends and its output streams close, so stdout is whole.
      child.on("close", (exitCode) => {
        clearTimeout(timer);
        if (timedOut) {
          const message = `the Tool stopped after ${request.timeoutSeconds} s (mttx.packageTimeoutSeconds)`;
          resolve({ kind: "failed", message });
          return;
        }
        resolve(
          parseToolOutput({
            stdout: Buffer.concat(stdout).toString("utf8"),
            stderr: Buffer.concat(stderr).toString("utf8"),
            exitCode,
          }),
        );
      });
    });
  }
}
