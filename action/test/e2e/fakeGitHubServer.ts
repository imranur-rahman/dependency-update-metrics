// Start the fake GitHub REST API (fakeGitHub.ts) on the port in the first argument, and keep
// it running. The CI job run-action-pull-request starts it in the background.
//   node test/e2e/fakeGitHubServer.ts <port>
import { fakeGitHub } from "./fakeGitHub.ts";

const github = await fakeGitHub(Number(process.argv[2] ?? 0));
process.stdout.write(`The fake GitHub API listens on ${github.url}\n`);
