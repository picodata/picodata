import os
import re
from mkdocs.plugins import get_plugin_logger
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page


log = get_plugin_logger(os.path.basename(__file__))


def on_page_markdown(markdown: str, page: Page, config: MkDocsConfig, files: Files):
    path: str = page.file.src_uri
    markdown = re.sub("<!--.*?-->", "", markdown, flags=re.DOTALL)
    lines: list[str] = markdown.split("\n")

    for n, line in enumerate(lines):
        if re.match("^##+ ", line):
            prev = lines[n - 1]
            next = lines[n + 1]

            if re.match("^---+$", prev):
                prev = lines[n - 2]

            if prev != "" or next != "":
                log.warning(f"MISSING PADDING AROUND HEADER @ {path}")
                log.info("")
                log.info(lines[n - 2])
                log.info(lines[n - 1])
                log.info(lines[n])
                log.info(lines[n + 1])
                log.info("")
