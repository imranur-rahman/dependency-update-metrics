// The release check of the publish job (.github/workflows/extension.yml).
// It stops the job before packaging when the tag does not match the version,
// or when a registry has no credential. extension/README.md lists the setup.
//
// Run: node scripts/releaseCheck.ts (reads GITHUB_REF_NAME, the credential variables,
// and package.json; writes `marketplace=` and `open_vsx=` to GITHUB_OUTPUT).
import { appendFileSync, readFileSync } from "node:fs";
import { join } from "node:path";

export interface ReleaseInput {
  tag: string;
  version: string;
  env: Record<string, string | undefined>;
}

export interface ReleasePlan {
  version: string;
  /** `vsce publish --oidc`, or `vsce publish` with VSCE_PAT. */
  marketplace: "oidc" | "pat";
  /** `ovsx publish --trusted-publishing`, or `ovsx publish` with OVSX_PAT. */
  openVsx: "trusted" | "pat";
}

const MISSING_MARKETPLACE =
  "VS Code Marketplace: no credential. Set the repository variable VSCE_TRUSTED_PUBLISHING to true " +
  "(trusted publishing), or set the secret VSCE_PAT.";
const MISSING_OPEN_VSX =
  "Open VSX: no credential. Set the repository variable OVSX_TRUSTED_PUBLISHING to true " +
  "(trusted publishing), or set the secret OVSX_PAT.";

function isSet(value: string | undefined): boolean {
  return value !== undefined && value !== "";
}

export function checkRelease(input: ReleaseInput): ReleasePlan {
  if (!/^\d+\.\d+\.\d+$/.test(input.version)) {
    throw new Error(
      `The version ${input.version} is not major.minor.patch. ` +
        "The VS Code Marketplace accepts only versions such as 1.2.3.",
    );
  }
  const expected = `extension-v${input.version}`;
  if (input.tag !== expected) {
    throw new Error(
      `The tag ${input.tag} does not match the version ${input.version} in extension/package.json. ` +
        `Push the tag ${expected}, or change the version.`,
    );
  }

  const { env } = input;
  const problems: string[] = [];
  const marketplace = credential(env, "VSCE", "VS Code Marketplace", "oidc", MISSING_MARKETPLACE, problems);
  const openVsx = credential(env, "OVSX", "Open VSX", "trusted", MISSING_OPEN_VSX, problems);
  if (marketplace === undefined || openVsx === undefined) {
    throw new Error([...problems, "extension/README.md (Publishing) lists the setup."].join("\n"));
  }
  return { version: input.version, marketplace, openVsx };
}

// Trusted publishing wins over a token: the variable is an explicit choice, and the
// Marketplace token (an Azure DevOps global PAT) stops working on 2026-12-01.
function credential<T extends string>(
  env: ReleaseInput["env"],
  prefix: "VSCE" | "OVSX",
  registry: string,
  trustedMode: T,
  missing: string,
  problems: string[],
): T | "pat" | undefined {
  if (env[`${prefix}_TRUSTED_PUBLISHING`] === "true") {
    if (isSet(env.ACTIONS_ID_TOKEN_REQUEST_URL)) return trustedMode;
    problems.push(
      `${registry}: ${prefix}_TRUSTED_PUBLISHING is true, but the job gets no OIDC token. ` +
        "Give the job the permission id-token: write.",
    );
    return undefined;
  }
  if (isSet(env[`${prefix}_PAT`])) return "pat";
  problems.push(missing);
  return undefined;
}

function main(): void {
  const manifest = JSON.parse(readFileSync(join(import.meta.dirname, "..", "package.json"), "utf8"));
  try {
    const plan = checkRelease({
      tag: process.env.GITHUB_REF_NAME ?? "",
      version: manifest.version,
      env: process.env,
    });
    const output = process.env.GITHUB_OUTPUT;
    if (output) appendFileSync(output, `marketplace=${plan.marketplace}\nopen_vsx=${plan.openVsx}\n`);
    console.log(
      `Release ${plan.version}: Marketplace with ${plan.marketplace}, Open VSX with ${plan.openVsx}.`,
    );
  } catch (error) {
    for (const line of (error as Error).message.split("\n")) console.log(`::error::${line}`);
    process.exitCode = 1;
  }
}

if (import.meta.main) main();
