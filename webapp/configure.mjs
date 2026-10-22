// Puts the production Host URL and the Turnstile site key into the published homepage.
//
// webapp/site/index.html keeps the local values (http://localhost:8080 and the Cloudflare test
// site key), so that a local run works without changes. The Pages workflow runs this script
// on the copy that it publishes, with the repository variables DM_HOST_URL and
// DM_TURNSTILE_SITE_KEY:
//
//   node webapp/configure.mjs <index.html>
//
// The script refuses a missing or malformed value, so a deploy never ships the local values.
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const HOST_URL = /^https:\/\/[A-Za-z0-9.-]+(?::\d+)?$/;
const SITE_KEY = /^[0-9A-Za-z_-]+$/;
const HOST_URL_ATTRIBUTE = /(id="try-it" data-host-url=")[^"]*(")/;
const SITE_KEY_ATTRIBUTE = /(class="cf-turnstile" data-sitekey=")[^"]*(")/;

export function configureHomepage(html, { hostUrl, siteKey }) {
  if (!hostUrl) throw new Error("The Host URL is missing (DM_HOST_URL)");
  if (!HOST_URL.test(hostUrl)) {
    throw new Error(`The Host URL must be HTTPS with no path or trailing slash: ${hostUrl}`);
  }
  if (!siteKey || !SITE_KEY.test(siteKey)) {
    throw new Error("The Turnstile site key is missing or malformed (DM_TURNSTILE_SITE_KEY)");
  }
  for (const attribute of [HOST_URL_ATTRIBUTE, SITE_KEY_ATTRIBUTE]) {
    if (!attribute.test(html)) throw new Error(`The homepage has no ${attribute} attribute`);
  }
  return html
    .replace(HOST_URL_ATTRIBUTE, `$1${hostUrl}$2`)
    .replace(SITE_KEY_ATTRIBUTE, `$1${siteKey}$2`);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const path = process.argv[2];
  if (!path) {
    console.error("Usage: node webapp/configure.mjs <index.html>");
    process.exit(2);
  }
  const html = configureHomepage(readFileSync(path, "utf8"), {
    hostUrl: process.env.DM_HOST_URL,
    siteKey: process.env.DM_TURNSTILE_SITE_KEY,
  });
  writeFileSync(path, html);
  console.log(`Configured ${path} for ${process.env.DM_HOST_URL}`);
}
