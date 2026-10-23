// The "Use MTTX in Your Workflow" section: the stage tabs and the copy button of each command.
// This file holds only DOM code; the section works without it as a list of links.

const tabs = [...document.querySelectorAll("#workflow [role=tab]")];

// Show the panel of one tab and hide the others (the WAI-ARIA tabs pattern).
function selectTab(tab, focus = false) {
  for (const other of tabs) {
    const selected = other === tab;
    other.setAttribute("aria-selected", String(selected));
    other.tabIndex = selected ? 0 : -1;
    other.classList.toggle("is-active", selected);
    document.getElementById(other.getAttribute("aria-controls")).hidden = !selected;
  }
  if (focus) tab.focus();
}

for (const [index, tab] of tabs.entries()) {
  tab.addEventListener("click", () => selectTab(tab));
  // Arrow keys, Home, and End move between the tabs.
  tab.addEventListener("keydown", (event) => {
    const next = { ArrowRight: index + 1, ArrowDown: index + 1, ArrowLeft: index - 1, ArrowUp: index - 1, Home: 0, End: tabs.length - 1 }[event.key];
    if (next === undefined) return;
    event.preventDefault();
    selectTab(tabs[(next + tabs.length) % tabs.length], true);
  });
}

// The sample button of the Select stage runs the sample of the Try It section.
document.getElementById("workflow-show-sample")?.addEventListener("click", () => {
  document.getElementById("show-sample")?.click();
  document.getElementById("try-it")?.scrollIntoView({ behavior: "smooth", block: "start" });
});

// A copy button on each command block.
for (const pre of document.querySelectorAll("#workflow pre.cli")) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "copy-button";
  button.textContent = "Copy";
  button.setAttribute("aria-label", "Copy the command");
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(pre.querySelector("code").innerText);
      button.textContent = "Copied";
    } catch {
      button.textContent = "Select and copy";
    }
    setTimeout(() => (button.textContent = "Copy"), 1500);
  });
  pre.append(button);
}
