// Reads the `mttx.*` settings from VS Code. The check and the Tool arguments are in
// src/core/settings.ts, which has no `vscode` import.
import * as vscode from "vscode";

import { DEFAULT_SETTINGS, parseSettings, type ParsedSettings, type Settings } from "./core/settings.ts";

// Read and check all `mttx.*` settings. Each part of the Extension that needs a setting
// calls this, so that one check covers every setting.
export function readSettings(): ParsedSettings {
  const config = vscode.workspace.getConfiguration("mttx");
  const raw: Partial<Record<keyof Settings, unknown>> = {};
  for (const key of Object.keys(DEFAULT_SETTINGS) as Array<keyof Settings>) raw[key] = config.get(key);
  return parseSettings(raw);
}

// Return the checked settings, or undefined when the check fails. A part that shows the
// message of the check calls readSettings() instead.
export function checkedSettings(): Settings | undefined {
  const parsed = readSettings();
  return parsed.ok ? parsed.settings : undefined;
}
