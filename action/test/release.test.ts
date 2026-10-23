// Tests for the release check of the Action (src/release.ts), which the release job of
// .github/workflows/action.yml runs before it copies the Action to imranur-rahman/mttx-action.
import { test } from "node:test";
import assert from "node:assert/strict";

import { releasePlan } from "../src/release.ts";
import type { PinnedImage } from "../src/docker.ts";

const IMAGE: PinnedImage = {
  repository: "ghcr.io/imranur-rahman/mttx",
  version: "0.2.0",
  osvTag: "0.2.0-osv-2026-10-21",
  digest: `sha256:${"a".repeat(64)}`,
  osvDigest: `sha256:${"b".repeat(64)}`,
};

test("a tag action-v<version> with a pinned image and the token publishes and moves the major tag", () => {
  assert.deepEqual(releasePlan({ tag: "action-v1.2.3", version: "1.2.3", image: IMAGE, hasToken: true }), {
    errors: [],
    notice: null,
    publish: true,
    tag: "v1.2.3",
    major: "v1",
  });
});

test("a missing token skips the publish with a notice that names the secret", () => {
  const plan = releasePlan({ tag: "action-v1.2.3", version: "1.2.3", image: IMAGE, hasToken: false });
  assert.deepEqual(plan.errors, []);
  assert.equal(plan.publish, false);
  assert.match(plan.notice!, /MTTX_ACTION_TOKEN/);
  assert.match(plan.notice!, /imranur-rahman\/mttx-action/);
});

test("a tag that does not match package.json, or an image without digests, is an error", () => {
  const plan = releasePlan({ tag: "action-v1.2", version: "1.2.3", image: { ...IMAGE, digest: null }, hasToken: true });
  assert.deepEqual(plan.errors, [
    'The tag "action-v1.2" must be action-v1.2.3, the version in action/package.json.',
    "image.json has no digest of the tag 0.2.0.",
  ]);
  assert.equal(plan.publish, false);
});
