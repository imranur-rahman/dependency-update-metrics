// In-page notifications of the Webapp: a Bulma notification with a title, a message, and an
// optional next step. The page code uses it for every message that the user must not miss,
// in place of window.alert(). The notification has role="alert", so a screen reader reads it
// at once. The texts come from pure modules (for example errorNotice.js).

// The Bulma color of each tone: "danger" (red) for an error, "info" for a hint, "warning".
const TONE_CLASSES = { danger: "is-danger", info: "is-info", warning: "is-warning" };

// Show one notification in the container, in place of the one that it shows now.
// notice: { title, message, nextStep } (title and nextStep are optional). options.tone picks the color
// (default "danger"). options.scroll (default true) scrolls the notification into view.
// Return the notification element.
export function showNotification(container, notice, { tone = "danger", scroll = true } = {}) {
  const box = document.createElement("div");
  box.className = `notification ${TONE_CLASSES[tone] ?? TONE_CLASSES.danger} is-light`;
  box.setAttribute("role", "alert");
  const close = document.createElement("button");
  close.className = "delete";
  close.type = "button";
  close.setAttribute("aria-label", "Close this message");
  close.addEventListener("click", () => clearNotification(container));
  box.append(close);
  if (notice.title) {
    const title = document.createElement("p");
    title.className = "has-text-weight-bold";
    title.textContent = notice.title;
    box.append(title);
  }
  const message = document.createElement("p");
  message.textContent = notice.message;
  box.append(message);
  if (notice.nextStep) {
    const nextStep = document.createElement("p");
    nextStep.className = "mt-1";
    nextStep.textContent = notice.nextStep;
    box.append(nextStep);
  }
  container.replaceChildren(box);
  container.hidden = false;
  if (scroll) scrollToElement(box, { block: "center" });
  return box;
}

// Scroll the element into view: smoothly, or with a jump when the user asks the system for
// reduced motion. block is the scrollIntoView alignment (default "start").
export function scrollToElement(element, { block = "start" } = {}) {
  const reduce = window.matchMedia?.("(prefers-reduced-motion: reduce)").matches;
  element.scrollIntoView({ behavior: reduce ? "auto" : "smooth", block });
}

// Remove the notification of the container.
export function clearNotification(container) {
  container.replaceChildren();
  container.hidden = true;
}
