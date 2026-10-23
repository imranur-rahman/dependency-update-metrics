// Starts VS Code with the Extension and runs the integration suite (out/test/suite.js).
// The user settings point `mttx.toolPath` at the fake Tool, because `mttx.toolPath` is a
// machine setting, which VS Code ignores in workspace settings.
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { runTests } from "@vscode/test-electron";

const root = join(import.meta.dirname, "..", "..");
// A short path, because VS Code puts its IPC socket in this folder, and a socket path on
// macOS and Linux holds at most 103 characters.
const userDataDir = mkdtempSync(join(tmpdir(), "mttx-"));
mkdirSync(join(userDataDir, "User"), { recursive: true });
writeFileSync(
  join(userDataDir, "User", "settings.json"),
  JSON.stringify({ "mttx.toolPath": join(root, "test", "fixtures", "fake-tool.mjs") }),
);

try {
  await runTests({
    extensionDevelopmentPath: root,
    extensionTestsPath: join(root, "out", "test", "suite.js"),
    launchArgs: [join(root, "test", "fixtures", "workspace"), "--disable-extensions", `--user-data-dir=${userDataDir}`],
    // The fake Tool waits, so that the suite sees "computing…" before the values.
    extensionTestsEnv: { MTTX_FAKE_TOOL_DELAY_MS: "1500" },
  });
} catch (error) {
  console.error(error);
  process.exitCode = 1;
} finally {
  rmSync(userDataDir, { recursive: true, force: true, maxRetries: 5 });
}
