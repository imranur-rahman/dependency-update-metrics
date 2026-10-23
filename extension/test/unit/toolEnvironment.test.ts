// Tests for the rules that prepare the Tool (src/core/toolEnvironment.ts): find Python 3.10
// or later, make the venv, and install or upgrade the Tool in it. A fake process runner and
// an in-memory file system replace the real ones, so the tests install nothing.
import { test } from "node:test";
import assert from "node:assert/strict";

import {
  findPython,
  prepareTool,
  SetupToolRunner,
  TOOL_REQUIREMENT,
  ToolSetupCache,
  type ProcessResult,
  type ToolSetup,
} from "../../src/core/toolEnvironment.ts";
import type { ToolRequest, ToolRunner } from "../../src/core/tool.ts";

type Handler = (file: string, args: string[]) => ProcessResult | undefined;

// A fake process runner. It records each call as "file arg arg ..." and answers with the
// first handler that returns a result. A call that no handler answers fails like ENOENT.
function fakeRun(...handlers: Handler[]) {
  const calls: string[] = [];
  const run = async (file: string, args: string[]): Promise<ProcessResult> => {
    calls.push([file, ...args].join(" "));
    for (const handler of handlers) {
      const result = handler(file, args);
      if (result) return result;
    }
    return { exitCode: null, stdout: "", stderr: "", error: `spawn ${file} ENOENT` };
  };
  return { run, calls };
}

// A Python on the PATH as `command` (with the arguments `pre` before -c) that reports its
// executable and its version.
function python(command: string, exe: string, version: [number, number], pre: string[] = []): Handler {
  return (file, args) => {
    if (file !== command || args.slice(0, pre.length).join(" ") !== pre.join(" ")) return undefined;
    if (args[pre.length] !== "-c") return undefined;
    return { exitCode: 0, stdout: JSON.stringify({ exe, v: version }) + "\n", stderr: "" };
  };
}

test("findPython gives the executable of the first Python 3.10 or later on the PATH", async () => {
  const { run } = fakeRun(
    python("python3", "/usr/bin/python3.9", [3, 9]),
    python("python", "/opt/py/bin/python3.12", [3, 12]),
  );
  assert.equal(await findPython(run, "linux"), "/opt/py/bin/python3.12");
});

test("findPython on Windows tries the py launcher first and skips the Store alias, which prints no JSON", async () => {
  const { run, calls } = fakeRun(
    (file) => (file === "python" ? { exitCode: 9009, stdout: "", stderr: "Python was not found" } : undefined),
    python("python3", "C:\\Python313\\python.exe", [3, 13]),
  );
  assert.equal(await findPython(run, "win32"), "C:\\Python313\\python.exe");
  assert.deepEqual(
    calls.map((call) => call.split(" -c ")[0]),
    ["py -3", "python", "python3"],
  );
});

test("findPython gives undefined when no Python 3.10 or later is on the PATH", async () => {
  const { run } = fakeRun(python("python3", "/usr/bin/python3.8", [3, 8]));
  assert.equal(await findPython(run, "darwin"), undefined);
});

// An in-memory file system with the text files that the test gives.
function fakeFiles(initial: Record<string, string> = {}) {
  const files = new Map(Object.entries(initial));
  return {
    files,
    exists: async (path: string) => files.has(path),
    readText: async (path: string) => files.get(path),
    writeText: async (path: string, text: string) => void files.set(path, text),
  };
}

// The options of prepareTool for a POSIX machine, with a progress reporter that records.
function options(run: ReturnType<typeof fakeRun>["run"], files: ReturnType<typeof fakeFiles>, toolPath = "") {
  const progress: string[] = [];
  return {
    progress,
    options: {
      toolPath,
      venvDir: VENV,
      platform: "linux" as const,
      run,
      files,
      requirement: "dependency-update-metrics>=0.2,<0.3",
      withProgress: <T>(task: (report: (message: string) => void) => Promise<T>) =>
        task((message) => progress.push(message)),
    },
  };
}

const VENV = "/storage/venv";
const VENV_PYTHON = "/storage/venv/bin/python";
const MARKER = "/storage/venv/mttx-tool-requirement.txt";

test("prepareTool uses the mttx.toolPath setting and does not look for Python or make a venv", async () => {
  const { run, calls } = fakeRun(python("python3", "/usr/bin/python3", [3, 12]));
  const files = fakeFiles();
  const setup = await prepareTool(options(run, files, " /opt/bin/dependency-metrics ").options);
  assert.deepEqual(setup, { kind: "toolPath", path: "/opt/bin/dependency-metrics" });
  assert.deepEqual(calls, []);
  assert.equal(files.files.size, 0);
});

// A process that exits with 0 when its command line starts with `prefix`.
function succeeds(prefix: string): Handler {
  return (file, args) => ([file, ...args].join(" ").startsWith(prefix) ? { exitCode: 0, stdout: "", stderr: "" } : undefined);
}

test("prepareTool makes the venv with the found Python and installs the pinned Tool range in it", async () => {
  const { run, calls } = fakeRun(
    python("python3", "/usr/bin/python3.12", [3, 12]),
    succeeds(`/usr/bin/python3.12 -m venv ${VENV}`),
    succeeds(`${VENV_PYTHON} -m pip install`),
  );
  const files = fakeFiles();
  const { options: opts, progress } = options(run, files);
  const setup = await prepareTool(opts);
  assert.deepEqual(setup, { kind: "venv", python: VENV_PYTHON });
  assert.deepEqual(calls.slice(1), [
    `/usr/bin/python3.12 -m venv ${VENV}`,
    `${VENV_PYTHON} -m pip install --disable-pip-version-check --upgrade dependency-update-metrics>=0.2,<0.3`,
  ]);
  assert.equal(files.files.get(MARKER), "dependency-update-metrics>=0.2,<0.3");
  assert.ok(progress.length > 0);
});

test("prepareTool starts no process when the venv holds the pinned range", async () => {
  const { run, calls } = fakeRun();
  const files = fakeFiles({ [VENV_PYTHON]: "", [MARKER]: "dependency-update-metrics>=0.2,<0.3\n" });
  const { options: opts, progress } = options(run, files);
  assert.deepEqual(await prepareTool(opts), { kind: "venv", python: VENV_PYTHON });
  assert.deepEqual(calls, []);
  assert.deepEqual(progress, []);
});

test("prepareTool upgrades the venv when a new Extension release pins a newer range", async () => {
  const { run, calls } = fakeRun(succeeds(`${VENV_PYTHON} -m pip install`));
  const files = fakeFiles({ [VENV_PYTHON]: "", [MARKER]: "dependency-update-metrics>=0.1,<0.2" });
  assert.deepEqual(await prepareTool(options(run, files).options), { kind: "venv", python: VENV_PYTHON });
  assert.deepEqual(calls, [
    `${VENV_PYTHON} -m pip install --disable-pip-version-check --upgrade dependency-update-metrics>=0.2,<0.3`,
  ]);
  assert.equal(files.files.get(MARKER), "dependency-update-metrics>=0.2,<0.3");
});

test("prepareTool gives the no-Python message and makes no venv without Python 3.10 or later", async () => {
  const { run, calls } = fakeRun(python("python3", "/usr/bin/python3.9", [3, 9]));
  const files = fakeFiles();
  const { options: opts, progress } = options(run, files);
  const setup = await prepareTool(opts);
  assert.equal(setup.kind, "noPython");
  assert.match(setup.kind === "noPython" ? setup.message : "", /Python 3\.10 or later/);
  assert.ok(calls.every((call) => !call.includes(" -m ")));
  assert.deepEqual(progress, []);
  assert.equal(files.files.size, 0);
});

test("prepareTool gives the pip error of a failed install and keeps the old requirement", async () => {
  const { run } = fakeRun((file, args) =>
    file === VENV_PYTHON && args[1] === "pip"
      ? { exitCode: 1, stdout: "", stderr: "Collecting dependency-update-metrics\nERROR: No matching distribution found\n" }
      : undefined,
  );
  const files = fakeFiles({ [VENV_PYTHON]: "", [MARKER]: "dependency-update-metrics>=0.1,<0.2" });
  const setup = await prepareTool(options(run, files).options);
  assert.equal(setup.kind, "failed");
  assert.match(setup.kind === "failed" ? setup.message : "", /could not install .*ERROR: No matching distribution found/s);
  assert.equal(files.files.get(MARKER), "dependency-update-metrics>=0.1,<0.2");
});

test("prepareTool gives the venv error, for example a missing python3-venv package", async () => {
  const { run, calls } = fakeRun(python("python3", "/usr/bin/python3.12", [3, 12]), (file, args) =>
    args[1] === "venv" ? { exitCode: 1, stdout: "The virtual environment was not created successfully.\napt install python3.12-venv\n", stderr: "" } : undefined,
  );
  const setup = await prepareTool(options(run, fakeFiles()).options);
  assert.equal(setup.kind, "failed");
  assert.match(setup.kind === "failed" ? setup.message : "", /could not make the Python environment: .*python3\.12-venv/s);
  assert.ok(!calls.some((call) => call.includes("pip install")));
});

test("prepareTool uses Scripts\\python.exe of the venv on Windows", async () => {
  const venv = "C:\\Users\\me\\AppData\\Roaming\\Code\\User\\globalStorage\\imranur-rahman.mttx\\venv";
  const { run, calls } = fakeRun(
    python("py", "C:\\Python313\\python.exe", [3, 13], ["-3"]),
    succeeds(`C:\\Python313\\python.exe -m venv ${venv}`),
    succeeds(`${venv}\\Scripts\\python.exe -m pip install`),
  );
  const files = fakeFiles();
  const opts = { ...options(run, files).options, venvDir: venv, platform: "win32" as const };
  assert.deepEqual(await prepareTool(opts), { kind: "venv", python: `${venv}\\Scripts\\python.exe` });
  assert.equal(calls.length, 3);
  assert.ok(files.files.has(`${venv}\\mttx-tool-requirement.txt`));
});

test("ToolSetupCache keeps one setup for each toolPath, and starts a new one after a failure", async () => {
  const results: ToolSetup[] = [
    { kind: "failed", message: "no network" },
    { kind: "venv", python: VENV_PYTHON },
    { kind: "toolPath", path: "/opt/dm" },
  ];
  const started: string[] = [];
  const cache = new ToolSetupCache(async (toolPath) => {
    started.push(toolPath);
    return results.shift()!;
  });
  const first = cache.get("");
  assert.equal(cache.get(""), first);
  assert.deepEqual(await first, { kind: "failed", message: "no network" });
  assert.deepEqual(await cache.get(""), { kind: "venv", python: VENV_PYTHON });
  assert.deepEqual(await cache.get(""), { kind: "venv", python: VENV_PYTHON });
  assert.deepEqual(await cache.get("/opt/dm"), { kind: "toolPath", path: "/opt/dm" });
  assert.deepEqual(started, ["", "", "/opt/dm"]);
});

test("SetupToolRunner waits for the setup, then runs the Tool, or fails with the setup message", async () => {
  const request: ToolRequest = { ecosystem: "npm", packageName: "express", startDate: "2024-10-22", outputDir: "/out" };
  const used: ToolSetup[] = [];
  const makeRunner = (setup: ToolSetup): ToolRunner => {
    used.push(setup);
    return { run: async () => ({ kind: "failed", message: "ran" }) };
  };
  const ready = new SetupToolRunner(Promise.resolve({ kind: "venv", python: VENV_PYTHON }), makeRunner);
  assert.deepEqual(await ready.run(request), { kind: "failed", message: "ran" });
  assert.deepEqual(used, [{ kind: "venv", python: VENV_PYTHON }]);

  const broken = new SetupToolRunner(Promise.resolve({ kind: "failed", message: "MTTX could not install" }), makeRunner);
  assert.deepEqual(await broken.run(request), { kind: "failed", message: "MTTX could not install" });
  assert.equal(used.length, 1);
});

test("the pinned Tool range is the minor version after 0.1.0, the first one with --json", () => {
  assert.equal(TOOL_REQUIREMENT, "dependency-update-metrics>=0.2,<0.3");
});
