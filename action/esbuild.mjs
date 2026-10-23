// Bundles the Action into one file, dist/index.mjs, with the Extension core parsers and the
// Webapp Explanation inside. The release job copies dist/ with action.yml to the repository
// imranur-rahman/mttx-action. esbuild does not check types; `npm run check-types` runs tsc.
import * as esbuild from "esbuild";

await esbuild.build({
  bundle: true,
  format: "esm",
  platform: "node",
  target: "node20",
  entryPoints: ["src/index.ts"],
  outfile: "dist/index.mjs",
  logLevel: "info",
});
