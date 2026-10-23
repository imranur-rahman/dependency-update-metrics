// The rules that prepare the Tool: find Python 3.10 or later, make a venv in the global
// storage folder of the Extension, and install or upgrade the Tool in it (ADR 0003). This
// module has no `vscode` import and no `node:child_process` import: the caller injects the
// process runner and the file system, so that `node --test` runs it with fakes.

import * as path from "node:path";

import type { ToolOutcome, ToolRequest, ToolRunner } from "./tool.ts";

// The Tool versions that this Extension release supports. The `--json` output of the Tool
// can change only with a new minor version (ADR 0003), so the range allows one minor version.
// The Tool got `--json` after 0.1.0, so the first Extension release needs Tool 0.2.0 on PyPI.
// Change only this line to pin a new range: the Extension then upgrades the venv.
export const TOOL_REQUIREMENT = "dependency-update-metrics>=0.2,<0.3";

export const PYTHON_DOWNLOAD_URL = "https://www.python.org/downloads/";
export const NO_PYTHON_MESSAGE =
  "MTTX needs Python 3.10 or later to compute MTTU and MTTR. Install Python, then reload the window.";

const MARKER_NAME = "mttx-tool-requirement.txt";
const VENV_TIMEOUT_MS = 2 * 60_000;
const INSTALL_TIMEOUT_MS = 10 * 60_000;

// The output of one finished process. `error` holds the message when the process did not
// start (for example ENOENT) or when its timeout stopped it.
export interface ProcessResult {
  exitCode: number | null;
  stdout: string;
  stderr: string;
  error?: string;
}

export type RunProcess = (file: string, args: string[], options?: { timeoutMs?: number }) => Promise<ProcessResult>;

export type Platform = NodeJS.Platform;

// Prints the absolute path and the version of the Python that runs it.
const PROBE = "import sys, json; print(json.dumps({'exe': sys.executable, 'v': list(sys.version_info[:2])}))";
const PROBE_TIMEOUT_MS = 10_000;

// The commands that can start Python, in the order of the search. On Windows, `py -3` is the
// Python launcher. `python` can be the Microsoft Store alias, which prints no JSON.
function pythonCandidates(platform: Platform): Array<{ file: string; args: string[] }> {
  if (platform === "win32") {
    return [
      { file: "py", args: ["-3"] },
      { file: "python", args: [] },
      { file: "python3", args: [] },
    ];
  }
  return [
    { file: "python3", args: [] },
    { file: "python", args: [] },
  ];
}

// Return the absolute path of the first Python 3.10 or later that the PATH gives, or
// undefined when there is none.
export async function findPython(run: RunProcess, platform: Platform): Promise<string | undefined> {
  for (const candidate of pythonCandidates(platform)) {
    const result = await run(candidate.file, [...candidate.args, "-c", PROBE], { timeoutMs: PROBE_TIMEOUT_MS });
    if (result.error || result.exitCode !== 0) continue;
    const exe = supportedPython(result.stdout);
    if (exe) return exe;
  }
  return undefined;
}

// The text files that prepareTool reads and writes.
export interface ToolFiles {
  exists(path: string): Promise<boolean>;
  readText(path: string): Promise<string | undefined>;
  writeText(path: string, text: string): Promise<void>;
}

// Runs a task and shows its progress messages, for example in a VS Code notification.
export type WithProgress = <T>(task: (report: (message: string) => void) => Promise<T>) => Promise<T>;

export interface PrepareOptions {
  toolPath: string;
  venvDir: string;
  platform: Platform;
  run: RunProcess;
  files: ToolFiles;
  withProgress: WithProgress;
  requirement?: string;
}

// How the Extension runs the Tool:
// - toolPath: the `mttx.toolPath` setting names an installed Tool.
// - venv: the Python of the Extension venv runs `-m dependency_metrics.cli`.
// - noPython: the machine has no Python 3.10 or later, so the Extension shows no hints.
// - failed: the venv or the install failed with this message.
export type ToolSetup =
  | { kind: "toolPath"; path: string }
  | { kind: "venv"; python: string }
  | { kind: "noPython"; message: string }
  | { kind: "failed"; message: string };

// Return the Python of a venv. A venv has `bin` (`Scripts` on Windows), and its Python runs
// without an activation.
export function venvPython(venvDir: string, platform: Platform): string {
  return platform === "win32"
    ? path.win32.join(venvDir, "Scripts", "python.exe")
    : path.posix.join(venvDir, "bin", "python");
}

// The file in the venv that holds the requirement of the last install that succeeded.
function markerFile(venvDir: string, platform: Platform): string {
  return (platform === "win32" ? path.win32 : path.posix).join(venvDir, MARKER_NAME);
}

// Decide how to run the Tool, and make or upgrade the venv when the Extension needs it.
// The venv is up to date when its marker file holds the pinned requirement, so an
// Extension release that pins a new range upgrades the Tool.
export async function prepareTool(options: PrepareOptions): Promise<ToolSetup> {
  const toolPath = options.toolPath.trim();
  if (toolPath !== "") return { kind: "toolPath", path: toolPath };

  const { venvDir, platform, run, files } = options;
  const requirement = options.requirement ?? TOOL_REQUIREMENT;
  const python = venvPython(venvDir, platform);
  const marker = markerFile(venvDir, platform);
  const hasVenv = await files.exists(python);
  if (hasVenv && (await files.readText(marker))?.trim() === requirement) return { kind: "venv", python };

  let basePython: string | undefined;
  if (!hasVenv) {
    basePython = await findPython(run, platform);
    if (!basePython) return { kind: "noPython", message: NO_PYTHON_MESSAGE };
  }

  return options.withProgress(async (report) => {
    if (basePython) {
      report("Making the Python environment of MTTX");
      const made = await run(basePython, ["-m", "venv", venvDir], { timeoutMs: VENV_TIMEOUT_MS });
      if (!succeeded(made)) return failed("could not make the Python environment", made);
    }
    report(`Installing ${requirement}`);
    const installed = await run(
      python,
      ["-m", "pip", "install", "--disable-pip-version-check", "--upgrade", requirement],
      { timeoutMs: INSTALL_TIMEOUT_MS },
    );
    if (!succeeded(installed)) return failed(`could not install ${requirement}`, installed);
    await files.writeText(marker, requirement);
    return { kind: "venv", python };
  });
}

function succeeded(result: ProcessResult): boolean {
  return !result.error && result.exitCode === 0;
}

// The message of a failed step, with the last lines of its stderr. For example, `python -m
// venv` fails on Debian and Ubuntu without the `python3-venv` package and says so on stderr.
function failed(step: string, result: ProcessResult): ToolSetup {
  const detail = result.error ?? lastLines(result.stderr) ?? lastLines(result.stdout) ?? `exit code ${result.exitCode}`;
  return { kind: "failed", message: `MTTX ${step}: ${detail}` };
}

function lastLines(text: string, count = 5): string | undefined {
  const lines = text.trim().split(/\r?\n/).filter((line) => line.trim() !== "");
  return lines.length > 0 ? lines.slice(-count).join("\n") : undefined;
}

function hasTool(setup: ToolSetup): boolean {
  return setup.kind === "venv" || setup.kind === "toolPath";
}

// Keeps one setup for each value of `mttx.toolPath`, so that all Tool runs wait for the same
// install. A setup that ends without a Tool (no Python, a failed install) does not stay: the
// next get starts a new one, for example after a settings change.
export class ToolSetupCache {
  private readonly prepare: (toolPath: string) => Promise<ToolSetup>;
  private current: { toolPath: string; setup: Promise<ToolSetup>; usable: boolean } | undefined;

  constructor(prepare: (toolPath: string) => Promise<ToolSetup>) {
    this.prepare = prepare;
  }

  get(toolPath: string): Promise<ToolSetup> {
    const current = this.current;
    if (current && current.toolPath === toolPath && current.usable) return current.setup;
    const entry = { toolPath, setup: this.prepare(toolPath), usable: true };
    entry.setup.then((setup) => {
      entry.usable = hasTool(setup);
    });
    this.current = entry;
    return entry.setup;
  }
}

// A Tool runner that waits for the setup. A setup without a Tool makes each run fail with
// the message of the setup.
export class SetupToolRunner implements ToolRunner {
  private readonly runner: Promise<ToolRunner | ToolOutcome>;

  constructor(setup: Promise<ToolSetup>, makeRunner: (setup: ToolSetup) => ToolRunner) {
    this.runner = setup.then(
      (ready): ToolRunner | ToolOutcome =>
        ready.kind === "noPython" || ready.kind === "failed" ? { kind: "failed", message: ready.message } : makeRunner(ready),
    );
  }

  async run(request: ToolRequest, signal?: AbortSignal): Promise<ToolOutcome> {
    const runner = await this.runner;
    return "run" in runner ? runner.run(request, signal) : runner;
  }
}

function supportedPython(stdout: string): string | undefined {
  try {
    const { exe, v } = JSON.parse(stdout.trim()) as { exe: unknown; v: unknown };
    if (typeof exe !== "string" || exe === "" || !Array.isArray(v)) return undefined;
    const [major, minor] = v as number[];
    return major > 3 || (major === 3 && minor >= 10) ? exe : undefined;
  } catch {
    return undefined;
  }
}
