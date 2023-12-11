import os
import re
from urllib.parse import urlsplit
from mkdocs.plugins import get_plugin_logger
from mkdocs.config.defaults import MkDocsConfig
from mkdocs.structure.files import Files
from mkdocs.structure.pages import Page


log = get_plugin_logger(os.path.basename(__file__))


# https://www.mkdocs.org/dev-guide/plugins/#events
def on_page_markdown(markdown: str, page: Page, config: MkDocsConfig, files: Files):
    markdown: str = re.sub("<!--.*?-->", "", markdown, flags=re.DOTALL)

    for match in re.finditer(r"(!?)\[([^\[\]]*?)\]\((.+?)\)", markdown, flags=re.DOTALL):
        link = match[0].replace("\n", " ")
        is_image, text, href = match.groups()
        text = text.replace("\n", " ")
        url = urlsplit(href.replace("\n", " "))

        if is_image:
            # Skip ![](image.png)
            continue

        if url.scheme or url.netloc:
            # Skip external links
            continue

        if not url.path:
            target = page.file.src_path
        else:
            target = os.path.join(os.path.dirname(page.file.src_path), url.path)
            target = os.path.normpath(target)

        if target not in config["anchors"]:
            log.info(f"BROKEN LINK @ {page.file.src_path}: {link}")
            continue

        if url.fragment and url.fragment not in config["anchors"][target]:
            log.info(f"BROKEN LINK ANCHOR @ {page.file.src_path}: {link}")
            continue
