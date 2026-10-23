// The entry point of the Action (dist/index.mjs). It makes the real ActionIO from the
// environment of the step: the inputs (INPUT_*), the output files of GitHub Actions, the
// Docker seam, git, and the REST API.
//
// MTTX_ACTION_TOOL: the path of a JavaScript file that replaces the Docker seam. The CI of
// the Action sets it to the fake Tool of the Extension; users never set it.

import { appendFileSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { dirname, resolve } from "node:path";

import { runToolCommand, toolCommand } from "../../extension/src/processToolRunner.ts";
import imageJson from "../image.json" with { type: "json" };
import { dockerTool, IMAGE_OUTPUT_DIR, imageReference, type PinnedImage } from "./docker.ts";
import { gitManifestChanges, restCommentStore } from "./github.ts";
import type { OsvIndexSource } from "./inputs.ts";
import { runAction, type ActionIO, type PullRequest, type ToolSeam } from "./main.ts";

const env = process.env;

// The inputs, with the names of action.yml: INPUT_DATA_SOURCE is data-source.
function inputs(): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [key, value] of Object.entries(env)) {
    if (key.startsWith("INPUT_") && value !== undefined) result[key.slice(6).toLowerCase().replace(/_/g, "-")] = value;
  }
  return result;
}

// A workflow command text: GitHub reads "%", "\r", and "\n" as escapes.
function command(name: string, message: string): void {
  const text = message.replace(/%/g, "%25").replace(/\r/g, "%0D").replace(/\n/g, "%0A");
  process.stdout.write(`::${name} title=MTTX::${text}\n`);
}

// The Tool seam: the fake Tool of MTTX_ACTION_TOOL, or the GHCR image for the osv-index
// input. The runner folder MTTX_DATA_DIR holds the Tool cache and the OSV index.
function openTool(osvIndex: OsvIndexSource): ToolSeam {
  const dataDir = env.MTTX_DATA_DIR || resolve(env.RUNNER_TEMP ?? ".", "mttx-data");
  mkdirSync(dataDir, { recursive: true });
  const fake = env.MTTX_ACTION_TOOL;
  if (fake) {
    const fakeCommand = toolCommand(fake);
    return { tool: (args, timeoutMs) => runToolCommand(fakeCommand, args, timeoutMs === undefined ? {} : { timeoutMs }), outputDir: dataDir };
  }
  const image = imageReference(imageJson as PinnedImage, osvIndex);
  process.stdout.write(`MTTX runs the Tool in ${image}\n`);
  const mount = { hostDir: dataDir, uid: process.getuid?.() ?? 0, gid: process.getgid?.() ?? 0 };
  return { tool: dockerTool(image, mount), outputDir: IMAGE_OUTPUT_DIR };
}

// The pull request of the event, or undefined for another event.
function pullRequest(token: string): PullRequest | undefined {
  if (!env.GITHUB_EVENT_PATH) return undefined;
  const event = JSON.parse(readFileSync(env.GITHUB_EVENT_PATH, "utf8"));
  const pr = event.pull_request;
  if (!pr?.base?.sha || !pr?.head?.sha) return undefined;
  const workspace = env.GITHUB_WORKSPACE ?? process.cwd();
  return {
    changes: () => gitManifestChanges(workspace, pr.base.sha, pr.head.sha),
    comments: restCommentStore({
      apiUrl: env.GITHUB_API_URL ?? "https://api.github.com",
      repository: env.GITHUB_REPOSITORY ?? "",
      number: pr.number,
      token,
    }),
  };
}

async function main(): Promise<number> {
  const values = inputs();
  const pr = pullRequest(values["github-token"] ?? "");
  const io: ActionIO = {
    inputs: values,
    today: new Date(),
    openTool,
    ...(pr ? { pullRequest: pr } : {}),
    setOutput(name, value) {
      if (!env.GITHUB_OUTPUT) return;
      const delimiter = `mttx_${randomUUID()}`;
      appendFileSync(env.GITHUB_OUTPUT, `${name}<<${delimiter}\n${value}\n${delimiter}\n`);
    },
    appendSummary(markdown) {
      if (env.GITHUB_STEP_SUMMARY) appendFileSync(env.GITHUB_STEP_SUMMARY, markdown);
    },
    writeFile(path, text) {
      const target = resolve(env.GITHUB_WORKSPACE ?? process.cwd(), path);
      mkdirSync(dirname(target), { recursive: true });
      writeFileSync(target, text);
    },
    error: (message) => command("error", message),
    warning: (message) => command("warning", message),
    info: (message) => process.stdout.write(`${message}\n`),
  };
  return runAction(io);
}

main().then(
  (code) => {
    process.exitCode = code;
  },
  (error: unknown) => {
    command("error", error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  },
);
