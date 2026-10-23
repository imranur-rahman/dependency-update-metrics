// The release check of the release job in .github/workflows/github-action.yml (src/release.ts).
// It writes publish, tag, and major to GITHUB_OUTPUT, and fails with an error annotation
// when the tag or image.json is wrong. A missing MTTX_ACTION_TOKEN gives a notice and
// publish=false.
import { appendFileSync, readFileSync } from "node:fs";
import { join } from "node:path";

import { releasePlan } from "../src/release.ts";

const root = join(import.meta.dirname, "..");
const plan = releasePlan({
  tag: process.env.GITHUB_REF_NAME ?? "",
  version: JSON.parse(readFileSync(join(root, "package.json"), "utf8")).version,
  image: JSON.parse(readFileSync(join(root, "image.json"), "utf8")),
  hasToken: (process.env.MTTX_ACTION_TOKEN ?? "") !== "",
});
for (const error of plan.errors) console.log(`::error title=MTTX Action release::${error}`);
if (plan.notice) console.log(`::notice title=MTTX Action release::${plan.notice}`);
if (process.env.GITHUB_OUTPUT) {
  appendFileSync(process.env.GITHUB_OUTPUT, `publish=${plan.publish}\ntag=${plan.tag}\nmajor=${plan.major}\n`);
}
process.exitCode = plan.errors.length > 0 ? 1 : 0;
