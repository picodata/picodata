(function () {
  "use strict";

  async function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      try {
        await navigator.clipboard.writeText(text);
        return;
      } catch (_) {
        // Fall back when clipboard access is unavailable or denied.
      }
    }

    var activeElement = document.activeElement;
    var textarea = document.createElement("textarea");
    textarea.value = text;
    textarea.readOnly = true;
    textarea.style.cssText = "position: fixed; top: 0; left: 0; opacity: 0;";
    document.body.appendChild(textarea);
    textarea.select();
    try {
      if (!document.execCommand("copy")) {
        throw new Error("Copy failed");
      }
    } finally {
      textarea.remove();
      if (activeElement) activeElement.focus({ preventScroll: true });
    }
  }

  function init() {
    var headers = document.querySelectorAll("th[data-copy-column]");
    if (!headers.length) return;

    var status = document.createElement("div");
    status.className = "settings-copy-status";
    status.setAttribute("role", "status");
    status.hidden = true;
    document.body.appendChild(status);
    var statusTimer;

    function showStatus(message) {
      clearTimeout(statusTimer);
      status.textContent = message;
      status.hidden = false;
      statusTimer = setTimeout(function () {
        status.hidden = true;
      }, 3000);
    }

    headers.forEach(function (header) {
      header.closest("table").querySelectorAll("tbody tr").forEach(function (row) {
        var cell = row.cells[header.cellIndex];
        if (!cell) return;

        // Read only the code, excluding the theme's copy button and tooltip.
        var content = cell.querySelector("code") || cell;
        var value = content.textContent.trim();
        if (!value) return;

        var button = document.createElement("button");
        button.type = "button";
        button.className = "settings-copy";
        button.title = "Скопировать в буфер обмена";
        button.setAttribute("aria-label", "Скопировать: " + value);
        while (content.firstChild) button.appendChild(content.firstChild);
        content.appendChild(button);

        button.addEventListener("click", async function () {
          try {
            await copyText(value);
            showStatus("Скопировано в буфер обмена");
          } catch (_) {
            showStatus("Не удалось скопировать. Выделите и скопируйте текст вручную.");
          }
        });
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
