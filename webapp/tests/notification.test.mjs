// Tests for the scroll of the Webapp notification helper (webapp/site/static/js/notification.js).
// When a Job ends, the page scrolls to the results or to the error. A user who asks the
// system for reduced motion gets a jump, not a smooth scroll. A fake element records the call.
import { test, afterEach } from "node:test";
import assert from "node:assert/strict";

import { scrollToElement } from "../site/static/js/notification.js";

const originalWindow = globalThis.window;
afterEach(() => {
  globalThis.window = originalWindow;
});

function fakeElement() {
  const calls = [];
  return { calls, scrollIntoView: (options) => calls.push(options) };
}

function fakeWindow(reduceMotion) {
  return {
    matchMedia: (query) => ({
      matches: query === "(prefers-reduced-motion: reduce)" && reduceMotion,
    }),
  };
}

test("the page scrolls smoothly to an element when the user does not ask for reduced motion", () => {
  globalThis.window = fakeWindow(false);
  const element = fakeElement();
  scrollToElement(element);
  assert.deepEqual(element.calls, [{ behavior: "smooth", block: "start" }]);
});

test("the page jumps to an element when the user asks for reduced motion", () => {
  globalThis.window = fakeWindow(true);
  const element = fakeElement();
  scrollToElement(element, { block: "center" });
  assert.deepEqual(element.calls, [{ behavior: "auto", block: "center" }]);
});

test("a browser without matchMedia gets a smooth scroll", () => {
  globalThis.window = {};
  const element = fakeElement();
  scrollToElement(element);
  assert.deepEqual(element.calls, [{ behavior: "smooth", block: "start" }]);
});
