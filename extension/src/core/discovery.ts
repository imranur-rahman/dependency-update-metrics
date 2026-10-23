// The manifest discovery rules of the core module: which files the Extension analyzes. This
// module has no `vscode` import. The Extension analyzes a manifest only when the developer
// opens it, because VS Code asks for inlay hints only for an open editor.

// The manifest names in any folder of the workspace.
export const MANIFEST_NAMES = ["package.json", "pyproject.toml", "Cargo.toml"] as const;

// The folders that hold installed packages. Their manifests are not the developer's.
export const SKIPPED_FOLDERS = ["node_modules", ".venv"] as const;

// The glob pattern of the manifests in the workspace, for a VS Code document selector.
export const MANIFEST_PATTERN = `**/{${MANIFEST_NAMES.join(",")}}`;

// Return true when the Extension analyzes the file at this path: a manifest name outside
// the skipped folders. The path can use `/` or `\` as separator.
export function isAnalyzedManifest(path: string): boolean {
  const parts = path.split(/[\\/]/);
  const name = parts.pop() ?? "";
  if (!(MANIFEST_NAMES as readonly string[]).includes(name)) return false;
  return !parts.some((part) => (SKIPPED_FOLDERS as readonly string[]).includes(part));
}
