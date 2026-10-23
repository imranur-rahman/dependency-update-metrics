// Bundles the Extension (dist/extension.js) and the integration suite (out/test/suite.js).
// esbuild does not check types; `npm run check-types` runs tsc for that.
import * as esbuild from "esbuild";

const production = process.argv.includes("--production");
const common = {
  bundle: true,
  format: "cjs",
  platform: "node",
  target: "node20",
  external: ["vscode"],
  sourcemap: !production,
  sourcesContent: false,
  logLevel: "info",
};

await esbuild.build({
  ...common,
  minify: production,
  entryPoints: ["src/extension.ts"],
  outfile: "dist/extension.js",
});

// The page code of the Comparison webview, with Chart.js and the Webapp comparison.js.
await esbuild.build({
  bundle: true,
  format: "iife",
  platform: "browser",
  target: "es2022",
  minify: production,
  sourcemap: !production,
  sourcesContent: false,
  logLevel: "info",
  entryPoints: ["src/webview/main.ts"],
  outfile: "dist/webview.js",
});

if (!production) {
  await esbuild.build({
    ...common,
    entryPoints: ["test/integration/suite.ts"],
    outfile: "out/test/suite.js",
  });
}
