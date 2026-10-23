// Reads the `mttx.*` settings from VS Code. The check and the Tool arguments are in
// src/core/settings.ts, which has no `vscode` import.
import * as vscode from "vscode";

import { parseSettings, type ParsedSettings } from "./core/settings.ts";

// The settings that src/core/settings.ts checks. #32 and #29 read their own settings.
const KEYS = [
  "dataSource",
  "windowYears",
  "startDate",
  "endDate",
  "weightingType",
  "halfLife",
  "packageTimeoutSeconds",
  "cacheFolder",
  "cacheSizeLimitGb",
] as const;

// Read and check the settings that change a Tool run. Each view that runs the Tool (the
// hints, the Compare view) calls this before it makes a request with toolRequest().
export function readSettings(): ParsedSettings {
  const config = vscode.workspace.getConfiguration("mttx");
  const raw: Record<string, unknown> = {};
  for (const key of KEYS) raw[key] = config.get(key);
  return parseSettings(raw);
}
