export const copyText = async (text: string): Promise<boolean> => {
  // navigator.clipboard can exist on insecure (non-HTTPS) origins but still
  // throw when called, since writeText requires a secure context.
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch {
      // fall back to execCommand below instead of failing outright.
    }
  }

  try {
    const textarea = document.createElement("textarea");
    textarea.value = text;

    // Append next to the currently focused element (e.g. the clicked copy
    // button) rather than document.body: MUI Modal/Dialog traps focus and
    // yanks it back as soon as it moves outside the modal's subtree, which
    // would otherwise steal the selection back before execCommand runs.
    const container =
      document.activeElement instanceof HTMLElement
        ? document.activeElement
        : document.body;
    container.appendChild(textarea);
    textarea.select();

    const copied = document.execCommand("copy");

    textarea.remove();

    return copied;
  } catch {
    return false;
  }
};
