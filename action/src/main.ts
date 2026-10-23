// The run of the Action: check the inputs, get the OSV index, run the Tool for each package,
// and write the outputs, the job summary, the badge files (target packages) or the pull
// request comment (target pull-request). Every effect goes through ActionIO, so that the tests
// run the Action with fakes. src/index.ts makes the real ActionIO.

import { buildOsvArgs, osvIndexDateArgs, parseOsvIndexDate } from "../../extension/src/core/osvIndex.ts";
import { toolRequest } from "../../extension/src/core/settings.ts";
import { parseToolOutput, toolArgs, type ToolOutcome } from "../../extension/src/core/tool.ts";
import { formatDate } from "../../extension/src/core/tool.ts";
import type { SpawnResult } from "../../extension/src/spawnProcess.ts";
import { formatDays } from "../../webapp/site/static/js/comparison.js";
import { addedDependencies, type AddedDependency, type ManifestChange } from "./added.ts";
import { parseInputs, type ActionConfig } from "./inputs.ts";
import { badgeFiles, COMMENT_MARKER, outputRows, reportMarkdown, thresholdFailures, type PackageOutcome } from "./report.ts";

// The one seam of the Tool: run `dependency-metrics` with these arguments. The real seam runs
// the Tool in the GHCR image (src/docker.ts); the tests pass a fake. timeoutMs stops a slow
// run.
export type ToolProcess = (args: string[], timeoutMs?: number) => Promise<SpawnResult>;

export interface Comment {
  id: number;
  body: string;
  url: string;
}

// The comments of the pull request (src/github.ts).
export interface CommentStore {
  list(): Promise<Comment[]>;
  create(body: string): Promise<Comment>;
  update(id: number, body: string): Promise<Comment>;
}

// The pull request of the event: the manifests that it changes, and its comments.
export interface PullRequest {
  changes(): Promise<ManifestChange[]>;
  comments: CommentStore;
}

export interface ActionIO {
  inputs: Record<string, string>;
  today: Date;
  // The `--output-dir` of the Tool, as the Tool sees it.
  outputDir: string;
  tool: ToolProcess;
  // Undefined when the event is not a pull request.
  pullRequest?: PullRequest;
  setOutput(name: string, value: string): void;
  appendSummary(markdown: string): void;
  writeFile(path: string, text: string): void;
  error(message: string): void;
  warning(message: string): void;
  info(message: string): void;
}

const NO_DEPENDENCIES_ADDED = "This pull request adds no dependencies.";

// Run the Action and return the exit code of the step.
export async function runAction(io: ActionIO): Promise<number> {
  const parsed = parseInputs(io.inputs);
  if (!parsed.ok) {
    io.error(parsed.message);
    return 1;
  }
  const { config } = parsed;
  if (config.target === "pull-request" && !io.pullRequest) {
    io.error("Input target pull-request needs a pull_request event (on: pull_request or pull_request_target).");
    return 1;
  }

  let packages: AddedDependency[];
  if (config.target === "packages") {
    packages = config.packages.map((name) => ({ ecosystem: config.ecosystem!, name, files: [] }));
  } else {
    packages = addedDependencies(await io.pullRequest!.changes());
    io.setOutput("added", JSON.stringify(packages));
    io.info(`The pull request adds ${packages.length} dependencies.`);
  }

  if (packages.length === 0) {
    io.setOutput("results", "[]");
    io.setOutput("mttu", "");
    io.setOutput("mttr", "");
    io.setOutput("over-threshold", "false");
    io.setOutput("comment-url", (await updateOldComment(io)) ?? "");
    io.appendSummary(`${NO_DEPENDENCIES_ADDED}\n`);
    return 0;
  }

  const osvIndexDate = await prepareOsvIndex(io, config);
  const outcomes = await runPool(packages, config.settings.parallelRuns, async (dep): Promise<PackageOutcome> => {
    io.info(`Analyzing ${dep.name} (${dep.ecosystem})`);
    return { ...dep, outcome: await analyze(io, config, dep) };
  });

  const rows = outputRows(outcomes);
  const failures = thresholdFailures(rows, config.thresholds);
  const first = rows[0];
  io.setOutput("results", JSON.stringify(rows));
  io.setOutput("mttu", first.mttu === null ? "" : formatDays(first.mttu));
  io.setOutput("mttr", first.mttr === null ? "" : formatDays(first.mttr));
  io.setOutput("over-threshold", String(failures.length > 0));

  const pullRequest = config.target === "pull-request";
  const markdown = reportMarkdown(outcomes, {
    heading: pullRequest ? "MTTU and MTTR of the dependencies that this pull request adds" : "MTTU and MTTR",
    thresholds: config.thresholds,
    showFiles: pullRequest,
    ...(osvIndexDate ? { osvIndexDate } : {}),
  });
  io.appendSummary(`${markdown}\n`);
  if (pullRequest) {
    io.setOutput("comment-url", (await writeComment(io, markdown)) ?? "");
  } else {
    for (const file of badgeFiles(rows, config.thresholds)) {
      io.writeFile(`${config.badgeDir}/${file.fileName}`, `${JSON.stringify(file.json)}\n`);
    }
  }
  for (const message of failures) io.error(message);
  return failures.length > 0 ? 1 : 0;
}

// Return the build date of the OSV index that the Tool uses, or undefined when it has none.
// With osv-index fresh, the Tool builds the index when it is not from today.
async function prepareOsvIndex(io: ActionIO, config: ActionConfig): Promise<string | undefined> {
  let state = parseOsvIndexDate((await io.tool(osvIndexDateArgs(io.outputDir))).stdout);
  const today = formatDate(io.today);
  if (config.osvIndex === "fresh" && !(state?.kind === "built" && state.date === today)) {
    io.info("Building the OSV index of today (about 260 MB download)");
    const build = await io.tool(buildOsvArgs(io.outputDir));
    if (build.exitCode !== 0) io.warning(`The OSV index build failed: ${lastLine(build)}`);
    state = parseOsvIndexDate((await io.tool(osvIndexDateArgs(io.outputDir))).stdout);
  }
  if (state?.kind === "built") return state.date;
  io.warning("The Tool has no OSV index, so each MTTR is 0. Use the default image, or osv-index: fresh.");
  return undefined;
}

async function analyze(io: ActionIO, config: ActionConfig, dep: AddedDependency): Promise<ToolOutcome> {
  const request = toolRequest(dep, config.settings, io.today, io.outputDir);
  const output = await io.tool(toolArgs(request), request.timeoutSeconds * 1000);
  if (output.timedOut) {
    return { kind: "failed", message: `the Tool stopped after ${request.timeoutSeconds} s (package-timeout-seconds)` };
  }
  if (output.error) return { kind: "failed", message: `could not start the Tool: ${output.error}` };
  return parseToolOutput(output);
}

// Write the report as the one comment of the Action: update the comment of an earlier push,
// or create it. Return its URL. A comment that cannot be written (for example, the read-only
// token of a pull request from a fork) gives a warning; the job summary still has the report.
async function writeComment(io: ActionIO, markdown: string): Promise<string | undefined> {
  const { comments } = io.pullRequest!;
  try {
    const old = (await comments.list()).find((comment) => comment.body.startsWith(COMMENT_MARKER));
    const comment = old ? await comments.update(old.id, markdown) : await comments.create(markdown);
    return comment.url;
  } catch (error) {
    io.warning(`The Action could not write the pull request comment: ${(error as Error).message}`);
    return undefined;
  }
}

// When the pull request adds no dependencies, a comment of an earlier push gets the text
// that it adds none, so that it shows no old values. No comment stays no comment.
async function updateOldComment(io: ActionIO): Promise<string | undefined> {
  if (!io.pullRequest) return undefined;
  const { comments } = io.pullRequest;
  try {
    const old = (await comments.list()).find((comment) => comment.body.startsWith(COMMENT_MARKER));
    if (!old) return undefined;
    return (await comments.update(old.id, `${COMMENT_MARKER}\n${NO_DEPENDENCIES_ADDED}\n`)).url;
  } catch (error) {
    io.warning(`The Action could not write the pull request comment: ${(error as Error).message}`);
    return undefined;
  }
}

// Run `work` for each item with at most `limit` at a time, and return the results in order.
async function runPool<T, R>(items: T[], limit: number, work: (item: T) => Promise<R>): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let next = 0;
  const worker = async () => {
    while (next < items.length) {
      const index = next++;
      results[index] = await work(items[index]);
    }
  };
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return results;
}

function lastLine(output: SpawnResult): string {
  return output.stderr.trim().split(/\r?\n/).pop() || output.error || `exit code ${output.exitCode}`;
}
