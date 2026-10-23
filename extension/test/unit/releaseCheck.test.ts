// Tests for the release check (scripts/releaseCheck.ts) that the publish job runs
// before it packages the Extension: the tag must match the version, and each
// registry needs a credential.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { checkRelease } from "../../scripts/releaseCheck.ts";

const BOTH_TOKENS = { VSCE_PAT: "vsce-token", OVSX_PAT: "ovsx-token" };

test("a tag that names the manifest version passes", () => {
  const plan = checkRelease({ tag: "extension-v0.1.0", version: "0.1.0", env: BOTH_TOKENS });
  assert.equal(plan.version, "0.1.0");
});

test("a tag that names another version stops the release and names both versions", () => {
  assert.throws(
    () => checkRelease({ tag: "extension-v0.2.0", version: "0.1.0", env: BOTH_TOKENS }),
    /tag extension-v0\.2\.0 does not match the version 0\.1\.0 in extension\/package\.json/,
  );
});

test("a version with a pre-release suffix stops the release, because the Marketplace takes only major.minor.patch", () => {
  assert.throws(
    () => checkRelease({ tag: "extension-v0.1.0-beta.1", version: "0.1.0-beta.1", env: BOTH_TOKENS }),
    /0\.1\.0-beta\.1 is not major\.minor\.patch/,
  );
});

test("with the two token secrets, both registries publish with a token", () => {
  const plan = checkRelease({ tag: "extension-v0.1.0", version: "0.1.0", env: BOTH_TOKENS });
  assert.equal(plan.marketplace, "pat");
  assert.equal(plan.openVsx, "pat");
});

test("without credentials, the release stops and names the setup for both registries", () => {
  assert.throws(
    () => checkRelease({ tag: "extension-v0.1.0", version: "0.1.0", env: {} }),
    (error: Error) => {
      assert.match(error.message, /VS Code Marketplace/);
      assert.match(error.message, /VSCE_TRUSTED_PUBLISHING/);
      assert.match(error.message, /VSCE_PAT/);
      assert.match(error.message, /Open VSX/);
      assert.match(error.message, /OVSX_TRUSTED_PUBLISHING/);
      assert.match(error.message, /OVSX_PAT/);
      assert.match(error.message, /extension\/README\.md/);
      return true;
    },
  );
});

// GitHub Actions sets ACTIONS_ID_TOKEN_REQUEST_URL only in a job with `id-token: write`.
const ID_TOKEN = { ACTIONS_ID_TOKEN_REQUEST_URL: "https://token.actions.example/" };

test("with trusted publishing turned on, both registries publish without a token", () => {
  const plan = checkRelease({
    tag: "extension-v0.1.0",
    version: "0.1.0",
    env: { ...ID_TOKEN, VSCE_TRUSTED_PUBLISHING: "true", OVSX_TRUSTED_PUBLISHING: "true" },
  });
  assert.equal(plan.marketplace, "oidc");
  assert.equal(plan.openVsx, "trusted");
});

test("trusted publishing wins over a token secret, because the Marketplace token ends on 2026-12-01", () => {
  const plan = checkRelease({
    tag: "extension-v0.1.0",
    version: "0.1.0",
    env: { ...ID_TOKEN, ...BOTH_TOKENS, VSCE_TRUSTED_PUBLISHING: "true" },
  });
  assert.equal(plan.marketplace, "oidc");
  assert.equal(plan.openVsx, "pat");
});

test("trusted publishing without an OIDC token stops the release and names the job permission", () => {
  assert.throws(
    () =>
      checkRelease({
        tag: "extension-v0.1.0",
        version: "0.1.0",
        env: { VSCE_TRUSTED_PUBLISHING: "true", OVSX_PAT: "token" },
      }),
    /id-token: write/,
  );
});

const SCRIPT = join(import.meta.dirname, "..", "..", "scripts", "releaseCheck.ts");
const VERSION = JSON.parse(readFileSync(join(import.meta.dirname, "..", "..", "package.json"), "utf8")).version;

function runScript(env: Record<string, string>) {
  const outputFile = join(mkdtempSync(join(tmpdir(), "mttx-release-")), "output");
  writeFileSync(outputFile, "");
  const result = spawnSync(process.execPath, ["--disable-warning=MODULE_TYPELESS_PACKAGE_JSON", SCRIPT], {
    env: { PATH: process.env.PATH ?? "", GITHUB_OUTPUT: outputFile, ...env },
    encoding: "utf8",
  });
  return { ...result, output: readFileSync(outputFile, "utf8") };
}

test("the script checks the tag against extension/package.json and writes the plan to GITHUB_OUTPUT", () => {
  const result = runScript({ GITHUB_REF_NAME: `extension-v${VERSION}`, ...BOTH_TOKENS });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.output, /^marketplace=pat$/m);
  assert.match(result.output, /^open_vsx=pat$/m);
});

test("the script fails with a GitHub error annotation when the check fails", () => {
  const result = runScript({ GITHUB_REF_NAME: "extension-v999.0.0", ...BOTH_TOKENS });
  assert.equal(result.status, 1);
  assert.match(result.stdout, /^::error::The tag extension-v999\.0\.0 does not match/m);
  assert.equal(result.output, "");
});

test("an empty secret counts as a missing secret", () => {
  assert.throws(
    () => checkRelease({ tag: "extension-v0.1.0", version: "0.1.0", env: { VSCE_PAT: "", OVSX_PAT: "token" } }),
    (error: Error) => /VSCE_PAT/.test(error.message) && !/OVSX_PAT/.test(error.message),
  );
});
