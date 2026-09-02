#!/usr/bin/env python3

"""
Adds syntax highlighting to an html coverage report rendered by `llvm-cov show`.

The grammars are fetched from a CDN, so a report browsed without
a network connection simply stays unhighlighted.
"""

import argparse
from pathlib import Path

# Every page of the report loads these two files from its root,
# which makes them the cheapest place to inject our own code.
ASSETS = ("control.js", "style.css")

MARKER = "/* added by tools/coverage-syntax-highlight.py */"

HIGHLIGHT_JS = r"""
(function () {
  const CDN = "https://cdn.jsdelivr.net/npm";
  const RUNTIME = `${CDN}/web-tree-sitter@0.25.10/tree-sitter.js`;

  const RUST = `${CDN}/tree-sitter-rust@0.24.0`;
  const C = `${CDN}/tree-sitter-c@0.24.1`;
  const CPP = `${CDN}/tree-sitter-cpp@0.23.4`;

  // Grammars & highlight queries (the latter are concatenated), by extension.
  const LANGUAGES = {
    rs: { wasm: `${RUST}/tree-sitter-rust.wasm`, queries: [RUST] },
    c: { wasm: `${C}/tree-sitter-c.wasm`, queries: [C] },
    h: { wasm: `${C}/tree-sitter-c.wasm`, queries: [C] },
    cc: { wasm: `${CPP}/tree-sitter-cpp.wasm`, queries: [C, CPP] },
    cpp: { wasm: `${CPP}/tree-sitter-cpp.wasm`, queries: [C, CPP] },
    hpp: { wasm: `${CPP}/tree-sitter-cpp.wasm`, queries: [C, CPP] },
  };

  // Capture kinds we actually paint; everything else (punctuation, operators,
  // plain variables) is left alone so that we don't bloat the DOM for nothing.
  const STYLED = new Set(`
    attribute boolean character comment constant constructor escape function
    keyword label number property string type
  `.split(/\s+/).filter(Boolean));

  function emit(fragment, cls, text) {
    if (!text) return;
    if (!cls) return fragment.append(text);

    const span = document.createElement("span");
    span.className = cls;
    span.textContent = text;
    fragment.append(span);
  }

  // Wrap every run of equally classified characters in a span of its own.
  // We only ever touch text nodes, so llvm-cov's own markup stays intact.
  function paint(line, classes, offset) {
    const walker = document.createTreeWalker(line, NodeFilter.SHOW_TEXT);

    // The walker doesn't tolerate mutations, so we collect the nodes first.
    const nodes = [];
    while (walker.nextNode()) nodes.push(walker.currentNode);

    for (const node of nodes) {
      const text = node.nodeValue;
      const fragment = document.createDocumentFragment();

      let start = 0;
      for (let end = 1; end <= text.length; end++) {
        if (end < text.length && classes[offset + end] === classes[offset + start]) continue;
        emit(fragment, classes[offset + start], text.slice(start, end));
        start = end;
      }

      offset += text.length;
      node.replaceWith(fragment);
    }
  }

  async function highlight() {
    const lines = [...document.querySelectorAll("td.code pre")];
    const title = document.querySelector(".source-name-title pre");
    if (!lines.length || !title) return;

    const language = LANGUAGES[title.textContent.split(".").pop()];
    if (!language) return;

    const { Parser, Language, Query } = await import(RUNTIME);
    await Parser.init();

    const [wasm, ...scm] = await Promise.all([
      fetch(language.wasm).then((response) => response.arrayBuffer()),
      ...language.queries.map((url) => fetch(`${url}/queries/highlights.scm`).then((r) => r.text())),
    ]);

    const grammar = await Language.load(new Uint8Array(wasm));
    const parser = new Parser();
    parser.setLanguage(grammar);

    // llvm-cov renders the file line by line, so we have to put it back together.
    const text = lines.map((line) => line.textContent).join("\n");
    const tree = parser.parse(text);

    // One class per character. Captures are applied largest first,
    // so that the innermost (i.e. most specific) one always wins.
    const classes = new Array(text.length);
    const captures = new Query(grammar, scm.join("\n")).captures(tree.rootNode);
    captures.sort((a, b) => b.node.endIndex - b.node.startIndex - (a.node.endIndex - a.node.startIndex));
    for (const capture of captures) {
      const kind = capture.name.split(".")[0];
      if (!STYLED.has(kind)) continue;
      classes.fill(`hl-${kind}`, capture.node.startIndex, capture.node.endIndex);
    }

    let offset = 0;
    for (const line of lines) {
      const length = line.textContent.length;
      paint(line, classes, offset);
      offset += length + 1; // the newline we've joined the lines with
    }
  }

  // Highlighting is a nice-to-have: leave the page as is if anything goes wrong.
  addEventListener("DOMContentLoaded", () => highlight().catch(() => {}));
})();
"""

HIGHLIGHT_CSS = """
.hl-comment     { color: #6a8759; font-style: italic; }
.hl-string      { color: #a31515; }
.hl-character   { color: #a31515; }
.hl-escape      { color: #ee0000; }
.hl-number      { color: #098658; }
.hl-boolean     { color: #098658; }
.hl-keyword     { color: #0033b3; font-weight: 600; }
.hl-type        { color: #267f99; }
.hl-constructor { color: #267f99; }
.hl-function    { color: #795e26; }
.hl-constant    { color: #0070c1; }
.hl-property    { color: #871094; }
.hl-attribute   { color: #9e880d; }
.hl-label       { color: #9e880d; }

@media (prefers-color-scheme: dark) {
  .hl-comment     { color: #7ec699; }
  .hl-string      { color: #ce9178; }
  .hl-character   { color: #ce9178; }
  .hl-escape      { color: #d7ba7d; }
  .hl-number      { color: #b5cea8; }
  .hl-boolean     { color: #b5cea8; }
  .hl-keyword     { color: #569cd6; }
  .hl-type        { color: #4ec9b0; }
  .hl-constructor { color: #4ec9b0; }
  .hl-function    { color: #dcdcaa; }
  .hl-constant    { color: #4fc1ff; }
  .hl-property    { color: #c586c0; }
  .hl-attribute   { color: #d7ba7d; }
  .hl-label       { color: #d7ba7d; }
}
"""


def enhance(report_dir: Path) -> None:
    """
    Append our snippets to the report's shared assets.
    Note that `llvm-cov` rewrites those files on every run.
    """

    for name, snippet in zip(ASSETS, (HIGHLIGHT_JS, HIGHLIGHT_CSS)):
        file = report_dir / name
        if not file.exists():
            raise Exception(f"{file} is missing; is {report_dir} an html coverage report?")

        # Don't enhance the same report twice.
        if MARKER in file.read_text():
            continue

        with file.open("a") as stream:
            stream.write(f"\n{MARKER}\n{snippet.strip()}\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("report_dir", metavar="DIR", type=Path, help="rendered html report")
    args = parser.parse_args()

    enhance(args.report_dir)


if __name__ == "__main__":
    main()
