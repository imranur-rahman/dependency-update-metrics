// The release check of the Action. A pushed tag action-v<version> copies the built Action to
// the public repository imranur-rahman/mttx-action, tags it v<version>, and moves the major
// tag (v1). The copy needs the secret MTTX_ACTION_TOKEN; without it, the job skips the copy
// with a notice, so that a release works before the repository exists.

import { pinnedImageErrors, type PinnedImage } from "./docker.ts";

export const ACTION_REPOSITORY = "imranur-rahman/mttx-action";
export const TOKEN_SECRET = "MTTX_ACTION_TOKEN";

export interface ReleaseInput {
  tag: string;
  version: string;
  image: PinnedImage;
  hasToken: boolean;
}

export interface ReleasePlan {
  errors: string[];
  notice: string | null;
  publish: boolean;
  tag: string;
  major: string;
}

export function releasePlan(input: ReleaseInput): ReleasePlan {
  const errors: string[] = [];
  if (!/^\d+\.\d+\.\d+$/.test(input.version) || input.tag !== `action-v${input.version}`) {
    errors.push(`The tag "${input.tag}" must be action-v${input.version}, the version in action/package.json.`);
  }
  errors.push(...pinnedImageErrors(input.image));
  const notice = input.hasToken
    ? null
    : `The secret ${TOKEN_SECRET} is not set, so the release skips the copy to ${ACTION_REPOSITORY}. ` +
      "action/README.md (Releases) lists the one-time setup.";
  return {
    errors,
    notice,
    publish: errors.length === 0 && input.hasToken,
    tag: `v${input.version}`,
    major: `v${input.version.split(".")[0]}`,
  };
}
