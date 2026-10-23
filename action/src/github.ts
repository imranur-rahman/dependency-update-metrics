// The GitHub adapters of the Action: the manifests that a pull request changes (read with git
// from the checkout) and the comments of the pull request (the REST API).

import { isAnalyzedManifest } from "../../extension/src/core/discovery.ts";
import { spawnProcess } from "../../extension/src/spawnProcess.ts";
import type { ManifestChange } from "./added.ts";
import type { Comment, CommentStore } from "./main.ts";

async function git(cwd: string, args: string[]): Promise<{ ok: boolean; stdout: string; stderr: string }> {
  const result = await spawnProcess("git", ["-C", cwd, ...args]);
  return { ok: result.exitCode === 0, stdout: result.stdout, stderr: result.stderr.trim() || result.error || "" };
}

// Return the manifests that the pull request changes, with their text at the merge base of
// the base and the head, and at the head. A later commit on the base branch does not count.
// A renamed manifest reads its base text from the old path.
export async function gitManifestChanges(cwd: string, baseSha: string, headSha: string): Promise<ManifestChange[]> {
  const mergeBase = await git(cwd, ["merge-base", baseSha, headSha]);
  if (!mergeBase.ok) {
    throw new Error(
      `git cannot find the merge base of ${baseSha} and ${headSha} (${mergeBase.stderr}). ` +
        "Check out the full history: actions/checkout with fetch-depth: 0.",
    );
  }
  const base = mergeBase.stdout.trim();
  const diff = await git(cwd, ["diff", "--name-status", "-z", "-M", base, headSha]);
  if (!diff.ok) throw new Error(`git diff failed: ${diff.stderr}`);
  const fields = diff.stdout.split("\0").filter((field) => field !== "");
  const changes: ManifestChange[] = [];
  for (let index = 0; index < fields.length; ) {
    const status = fields[index++];
    const oldPath = fields[index++];
    const path = /^[RC]/.test(status) ? fields[index++] : oldPath;
    if (!isAnalyzedManifest(path)) continue;
    changes.push({ path, base: await fileAt(cwd, base, oldPath), head: await fileAt(cwd, headSha, path) });
  }
  return changes.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
}

// The text of a file at a commit, or null when the file does not exist there.
async function fileAt(cwd: string, commit: string, path: string): Promise<string | null> {
  const result = await git(cwd, ["show", `${commit}:${path}`]);
  return result.ok ? result.stdout : null;
}

export interface RestOptions {
  apiUrl: string;
  repository: string;
  number: number;
  token: string;
  fetch?: typeof fetch;
}

// Return the comment store of one pull request on the REST API of GitHub. A pull request is
// an issue there, so its comments are issue comments.
export function restCommentStore(options: RestOptions): CommentStore {
  const doFetch = options.fetch ?? fetch;
  const base = `${options.apiUrl.replace(/\/$/, "")}/repos/${options.repository}`;
  const request = async (method: string, url: string, body?: unknown): Promise<unknown> => {
    const response = await doFetch(url, {
      method,
      headers: {
        authorization: `Bearer ${options.token}`,
        accept: "application/vnd.github+json",
        "x-github-api-version": "2022-11-28",
        ...(body === undefined ? {} : { "content-type": "application/json" }),
      },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    });
    const json = (await response.json().catch(() => ({}))) as { message?: string };
    if (!response.ok) throw new Error(`${response.status} ${json.message ?? response.statusText}`);
    return json;
  };
  const comment = (json: unknown): Comment => {
    const value = json as { id: number; body: string; html_url: string };
    return { id: value.id, body: value.body ?? "", url: value.html_url };
  };
  return {
    async list() {
      const comments: Comment[] = [];
      for (let page = 1; ; page++) {
        const items = (await request("GET", `${base}/issues/${options.number}/comments?per_page=100&page=${page}`)) as unknown[];
        comments.push(...items.map(comment));
        if (items.length < 100) return comments;
      }
    },
    async create(body) {
      return comment(await request("POST", `${base}/issues/${options.number}/comments`, { body }));
    },
    async update(id, body) {
      return comment(await request("PATCH", `${base}/issues/comments/${id}`, { body }));
    },
  };
}
