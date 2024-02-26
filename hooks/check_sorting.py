import os
import re
from mkdocs.plugins import get_plugin_logger
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page


log = get_plugin_logger(os.path.basename(__file__))


def on_page_markdown(markdown: str, page: Page, config: MkDocsConfig, files: Files):
    if page.file.src_uri != "reference/cli.md":
        return

    lines: list[str] = re.sub("<!--.*?-->", "", markdown, flags=re.DOTALL).split("\n")
    headers: list[str] = list(filter(lambda line: re.match("^##+ ", line), lines))

    last_h2: str = ""
    index: dict[str, list[str]] = dict()

    for h in headers:
        if h.startswith("## "):
            last_h2 = h
            index[last_h2] = []
        elif h.startswith("### "):
            arg = re.findall(r"--[\w-]+", h)[0]
            index[last_h2].append(arg)

    for h2, args in index.items():
        if args != sorted(args):
            log.warning(f"INCORRECT SORTING @ {page.file.src_uri}: {h2}")
