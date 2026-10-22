// Tests for webapp/configure.mjs, the step of the Pages workflow that puts the production Host
// URL and the Turnstile site key into the published homepage.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { configureHomepage } from "../configure.mjs";

const homepage = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "..", "site", "index.html"),
  "utf8",
);

const production = {
  hostUrl: "https://dm-host.duckdns.org",
  siteKey: "0x4AAAAAAAexampleKey",
};

test("the homepage gets the production Host URL and site key", () => {
  const html = configureHomepage(homepage, production);

  assert.match(html, /id="try-it" data-host-url="https:\/\/dm-host\.duckdns\.org"/);
  assert.match(html, /class="cf-turnstile" data-sitekey="0x4AAAAAAAexampleKey"/);
  assert.doesNotMatch(html, /data-host-url="http:\/\/localhost/);
  assert.doesNotMatch(html, /data-sitekey="1x00000000000000000000AA"/);
});

test("a Host URL that is not HTTPS is refused", () => {
  assert.throws(
    () => configureHomepage(homepage, { ...production, hostUrl: "http://dm-host.duckdns.org" }),
    /HTTPS/,
  );
});

test("a missing value is refused, so the deploy never ships the local defaults", () => {
  assert.throws(() => configureHomepage(homepage, { ...production, siteKey: "" }), /site key/);
  assert.throws(() => configureHomepage(homepage, { ...production, hostUrl: "" }), /Host URL/);
});

test("a site key with characters outside the key alphabet is refused", () => {
  assert.throws(
    () => configureHomepage(homepage, { ...production, siteKey: 'x" onload="alert(1)' }),
    /site key/,
  );
});

test("a Host URL with a path or a trailing slash is refused", () => {
  assert.throws(
    () => configureHomepage(homepage, { ...production, hostUrl: "https://dm-host.duckdns.org/" }),
    /Host URL/,
  );
});
