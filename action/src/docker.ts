// The Docker seam of the Action: it runs the Tool in the GHCR image of the Tool
// (ghcr.io/imranur-rahman/mttx, issue #51). The entrypoint of the image is the
// `dependency-metrics` CLI, and its output dir is /data. Each Action version pins one image
// digest in image.json; the release job refuses an image.json without the digests.

import { spawnProcess } from "../../extension/src/spawnProcess.ts";
import type { OsvIndexSource } from "./inputs.ts";
import type { ToolProcess } from "./main.ts";

// The content of image.json. osvTag is `<version>-osv-<YYYY-MM-DD>`: the image with the OSV
// index of that date inside. The digests are `sha256:...`; null until the image exists.
export interface PinnedImage {
  repository: string;
  version: string;
  osvTag: string | null;
  digest: string | null;
  osvDigest: string | null;
}

// The output dir of the Tool in the image.
export const IMAGE_OUTPUT_DIR = "/data";

// Return the image for the osv-index input: the -osv- tag for "image" (the index inside, no
// download), the plain tag for "fresh" (the Tool builds the index in the mounted folder). A
// pinned digest follows the tag, so that Docker checks it.
export function imageReference(image: PinnedImage, osvIndex: OsvIndexSource): string {
  const [tag, digest] = osvIndex === "image" ? [image.osvTag ?? image.version, image.osvDigest] : [image.version, image.digest];
  return `${image.repository}:${tag}${digest ? `@${digest}` : ""}`;
}

// Return the reasons why image.json is not ready for a release, or [] when it is.
export function pinnedImageErrors(image: PinnedImage): string[] {
  const errors: string[] = [];
  const digest = /^sha256:[0-9a-f]{64}$/;
  if (!image.osvTag) errors.push("image.json has no osvTag (the tag <version>-osv-<YYYY-MM-DD>).");
  else if (!new RegExp(`^${image.version.replace(/\./g, "\\.")}-osv-\\d{4}-\\d{2}-\\d{2}$`).test(image.osvTag)) {
    errors.push(`image.json osvTag "${image.osvTag}" does not match <version>-osv-<YYYY-MM-DD> for version ${image.version}.`);
  }
  if (!digest.test(image.digest ?? "")) errors.push(`image.json has no digest of the tag ${image.version}.`);
  if (!digest.test(image.osvDigest ?? "")) errors.push("image.json has no digest of the -osv- tag.");
  return errors;
}

// The folder of the runner that the container mounts at /data, and the user that writes it.
export interface DataMount {
  hostDir: string;
  uid: number;
  gid: number;
}

// Return the `docker run` arguments in front of the Tool arguments. --init forwards the stop
// signal of a timeout to the Tool. The runner folder on /data keeps the Tool cache across the
// runs of one job, and with osv-index fresh it also keeps the index. The -osv- tag keeps its
// own index in /opt/mttx/osv (DEPENDENCY_METRICS_OSV_INDEX), so the mount does not hide it.
// The image runs as root, so the container runs as the runner user, so that the runner (and
// actions/cache) owns the files; HOME is a folder that this user can write.
export function dockerRunArgs(image: string, mount: DataMount): string[] {
  return [
    "run", "--rm", "--init",
    "-v", `${mount.hostDir}:${IMAGE_OUTPUT_DIR}`,
    "--user", `${mount.uid}:${mount.gid}`,
    "-e", "HOME=/tmp",
    image,
  ];
}

// The real Tool seam: each call is one `docker run` of the image.
export function dockerTool(image: string, mount: DataMount): ToolProcess {
  const prefix = dockerRunArgs(image, mount);
  return (args, timeoutMs) => spawnProcess("docker", [...prefix, ...args], timeoutMs === undefined ? {} : { timeoutMs });
}
