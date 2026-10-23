// A fake `dependency-metrics --json` for the tests. It prints a result body from
// contract/examples/ for the package in --package:
//   left-pad    -> status no_deps
//   nope        -> the package error of the contract, exit code 1
//   bad-args    -> no JSON, an argparse error on stderr, exit code 2
//   any other   -> the ok result of the contract (MTTU 579 d, MTTR 579 d)
// It waits MTTX_FAKE_TOOL_DELAY_MS milliseconds (default 0) before it prints, so that a test
// can see "computing…". It writes its arguments to MTTX_FAKE_TOOL_LOG when that is set.
import { appendFileSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const examples = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "..", "contract", "examples");
const args = process.argv.slice(2);
const option = (name) => args[args.indexOf(name) + 1];
const packageName = option("--package");

if (process.env.MTTX_FAKE_TOOL_LOG) {
  appendFileSync(process.env.MTTX_FAKE_TOOL_LOG, `${JSON.stringify(args)}\n`);
}

function example(name, index) {
  return JSON.parse(readFileSync(join(examples, name), "utf8")).results[index];
}

await new Promise((resolve) => setTimeout(resolve, Number(process.env.MTTX_FAKE_TOOL_DELAY_MS ?? 0)));

process.stderr.write(`Analyzing ${packageName}\n`);
if (packageName === "bad-args") {
  process.stderr.write("dependency-metrics: error: unrecognized arguments: --json\n");
  process.exit(2);
}
let result;
let exitCode = 0;
if (packageName === "nope") {
  result = example("job_done_with_package_error.json", 1);
  exitCode = 1;
} else if (packageName === "left-pad") {
  const ok = example("job_done_window_summary.json", 0);
  result = { ...ok, summary: { ...ok.summary, mttu: 0, mttr: 0, num_dependencies: 0, status: "no_deps" } };
} else {
  result = example("job_done_window_summary.json", 0);
}
result = { ...result, ecosystem: option("--ecosystem"), package: packageName, start_date: option("--start-date") };
process.stdout.write(`${JSON.stringify(result)}\n`);
process.exit(exitCode);
