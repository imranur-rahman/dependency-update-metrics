// The step that finds the dependencies that a pull request adds. It reads the manifests with
// the parsers of the Extension core, so that the Action and the Extension find the same
// dependencies (the same sections, renames, markers, and target tables).

import { isAnalyzedManifest } from "../../extension/src/core/discovery.ts";
import { parseManifest, type Ecosystem } from "../../extension/src/core/manifest.ts";
import { packageKey } from "../../webapp/site/static/js/comparison.js";

// One manifest that the pull request changes: its text in the base and in the head. null
// means that the file does not exist on that side.
export interface ManifestChange {
  path: string;
  base: string | null;
  head: string | null;
}

// One dependency that the pull request adds, with the manifests that add it.
export interface AddedDependency {
  ecosystem: Ecosystem;
  name: string;
  files: string[];
}

// Return the dependencies whose name is in the head manifest and not in the base manifest,
// in the order of the changes and of the text. A changed version constraint does not count.
// Two names of one package match by `packageKey` of the Webapp (PEP 503 for PyPI). A
// dependency that more than one manifest adds is one entry.
export function addedDependencies(changes: ManifestChange[]): AddedDependency[] {
  const added = new Map<string, AddedDependency>();
  for (const change of changes) {
    if (!isAnalyzedManifest(change.path) || change.head === null) continue;
    const before = new Set(
      parseManifest(change.path, change.base ?? "").map((dep) => packageKey(dep.ecosystem, dep.name)),
    );
    for (const dep of parseManifest(change.path, change.head)) {
      const key = packageKey(dep.ecosystem, dep.name);
      if (before.has(key)) continue;
      const id = `${dep.ecosystem}:${key}`;
      const entry = added.get(id);
      if (!entry) added.set(id, { ecosystem: dep.ecosystem, name: dep.name, files: [change.path] });
      else if (!entry.files.includes(change.path)) entry.files.push(change.path);
    }
  }
  return [...added.values()];
}
