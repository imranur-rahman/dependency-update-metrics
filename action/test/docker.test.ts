// Tests for the Docker seam of the Action (src/docker.ts): the image that each Action
// version pins and the `docker run` arguments.
import { test } from "node:test";
import assert from "node:assert/strict";

import { dockerRunArgs, imageReference, pinnedImageErrors, type PinnedImage } from "../src/docker.ts";

const PINNED: PinnedImage = {
  repository: "ghcr.io/imranur-rahman/mttx",
  version: "0.2.0",
  osvTag: "0.2.0-osv-2026-10-21",
  digest: `sha256:${"a".repeat(64)}`,
  osvDigest: `sha256:${"b".repeat(64)}`,
};

test("osv-index image uses the -osv- tag, and fresh uses the plain tag, each with its digest", () => {
  assert.equal(imageReference(PINNED, "image"), `ghcr.io/imranur-rahman/mttx:0.2.0-osv-2026-10-21@sha256:${"b".repeat(64)}`);
  assert.equal(imageReference(PINNED, "fresh"), `ghcr.io/imranur-rahman/mttx:0.2.0@sha256:${"a".repeat(64)}`);
});

test("an image without a digest runs by its tag, and the release check refuses it", () => {
  const unpinned = { ...PINNED, digest: null, osvDigest: null };
  assert.equal(imageReference(unpinned, "fresh"), "ghcr.io/imranur-rahman/mttx:0.2.0");
  assert.deepEqual(pinnedImageErrors(PINNED), []);
  assert.deepEqual(pinnedImageErrors({ ...unpinned, osvTag: null }), [
    "image.json has no osvTag (the tag <version>-osv-<YYYY-MM-DD>).",
    "image.json has no digest of the tag 0.2.0.",
    "image.json has no digest of the -osv- tag.",
  ]);
  assert.deepEqual(pinnedImageErrors({ ...PINNED, osvTag: "0.1.0-osv-2026-10-21" }), [
    'image.json osvTag "0.1.0-osv-2026-10-21" does not match <version>-osv-<YYYY-MM-DD> for version 0.2.0.',
  ]);
});

test("the container mounts the data folder at /data and runs as the runner user", () => {
  assert.deepEqual(dockerRunArgs("img", { hostDir: "/tmp/mttx-data", uid: 1001, gid: 118 }), [
    "run", "--rm", "--init", "-v", "/tmp/mttx-data:/data", "--user", "1001:118", "-e", "HOME=/tmp", "img",
  ]);
});
