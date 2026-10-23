// The real Tool runner: it runs `dependency-metrics` as a child process. It has no
// `vscode` import, so that `node --test` runs it with the fake Tool script.

import { parseToolOutput, toolArgs, type ToolOutcome, type ToolRequest, type ToolRunner } from "./core/tool.ts";
import { spawnProcess, type SpawnOptions, type SpawnResult } from "./spawnProcess.ts";

// The program that runs the Tool, the arguments before the Tool arguments, and the extra
// variables of its environment. The venv command is in src/pythonProcess.ts.
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

// Run the Tool command with more arguments through the spawn helper. The variables of the
// command come before `env`.
export function runToolCommand(
  command: ToolCommand,
  toolArgs: string[],
  options: SpawnOptions = {},
): Promise<SpawnResult> {
  return spawnProcess(command.file, [...command.args, ...toolArgs], {
    ...options,
    env: { ...command.env, ...options.env },
  });
}

// Run the Tool once with the arguments of cachePruneArgs() (src/core/settings.ts), and
// return its exit code, or null when the Tool does not start. The output does not matter.
export async function pruneToolCache(
  command: ToolCommand,
  pruneArgs: string[],
  env: Record<string, string> = {},
): Promise<number | null> {
  return (await runToolCommand(command, pruneArgs, { env })).exitCode;
}

export class ProcessToolRunner implements ToolRunner {
  private readonly command: ToolCommand;
  private readonly env: Record<string, string>;

  constructor(command: ToolCommand, env: Record<string, string> = {}) {
    this.command = command;
    this.env = env;
  }

  // The Tool has no timeout in single-package mode, so the runner stops a slow run.
  async run(request: ToolRequest, signal?: AbortSignal): Promise<ToolOutcome> {
    const output = await runToolCommand(this.command, toolArgs(request), {
      env: this.env,
      signal,
      timeoutMs: request.timeoutSeconds * 1000,
    });
    if (output.timedOut) {
      return { kind: "failed", message: `the Tool stopped after ${request.timeoutSeconds} s (mttx.packageTimeoutSeconds)` };
    }
    if (output.error) return { kind: "failed", message: `could not start the Tool ${this.command.file}: ${output.error}` };
    return parseToolOutput(output);
  }
}
